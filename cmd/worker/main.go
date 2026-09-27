package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"

	"task-queue/internal/lock"
)

const (
	StreamName    = "payment_stream"
	ConsumerGroup = "payment_workers"
)

type Worker struct {
	id     string
	rdb    *redis.Client
	db     *sql.DB
	locker *lock.RedisLocker
}

func NewWorker(id string, rdb *redis.Client, db *sql.DB) *Worker {
	return &Worker{
		id:     id,
		rdb:    rdb,
		db:     db,
		locker: lock.NewRedisLocker(rdb),
	}
}

func (w *Worker) initStreamAndGroup(ctx context.Context) error {
	err := w.rdb.XGroupCreateMkStream(ctx, StreamName, ConsumerGroup, "$").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return err
	}
	return nil
}

func (w *Worker) processMessage(ctx context.Context, msg redis.XMessage) error {
	idempotencyKey := fmt.Sprintf("%v", msg.Values["idempotency_key"])
	accountID := fmt.Sprintf("%v", msg.Values["account_id"])
	amountStr := fmt.Sprintf("%v", msg.Values["amount"])
	amount, _ := strconv.ParseFloat(amountStr, 64)

	lockToken := uuid.New().String()
	acquired, err := w.locker.TryAcquire(ctx, idempotencyKey, lockToken, 10*time.Second)
	if err != nil {
		return fmt.Errorf("redis lock error: %w", err)
	}
	if !acquired {
		log.Printf("[%s] Task with key %s locked by another worker. Skipping.", w.id, idempotencyKey)
		return nil
	}
	defer w.locker.Release(ctx, idempotencyKey, lockToken)

	tx, err := w.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var existingID int
	err = tx.QueryRowContext(ctx, "SELECT id FROM transactions WHERE idempotency_key = $1", idempotencyKey).Scan(&existingID)
	if err == nil {
		log.Printf("[%s] Idempotency hit: %s already processed. Acking.", w.id, idempotencyKey)
		return w.rdb.XAck(ctx, StreamName, ConsumerGroup, msg.ID).Err()
	} else if err != sql.ErrNoRows {
		return fmt.Errorf("database query error: %w", err)
	}

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO transactions (idempotency_key, account_id, amount, status) VALUES ($1, $2, $3, $4)",
		idempotencyKey, accountID, amount, "PROCESSED",
	)
	if err != nil {
		return fmt.Errorf("database insert error: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("transaction commit failed: %w", err)
	}

	log.Printf("[%s] Processed and committed transaction: %s", w.id, idempotencyKey)
	return w.rdb.XAck(ctx, StreamName, ConsumerGroup, msg.ID).Err()
}

func (w *Worker) Start(ctx context.Context) {
	log.Printf("Worker %s listening for stream messages...", w.id)
	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker %s stopping...", w.id)
			return
		default:
			streams, err := w.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    ConsumerGroup,
				Consumer: w.id,
				Streams:  []string{StreamName, ">"},
				Count:    1,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err != redis.Nil {
					log.Printf("ReadGroup error: %v", err)
				}
				continue
			}

			for _, stream := range streams {
				for _, message := range stream.Messages {
					if err := w.processMessage(ctx, message); err != nil {
						log.Printf("Message %s processing failed: %v", message.ID, err)
					}
				}
			}
		}
	}
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	db, err := sql.Open("postgres", "postgres://queue_user:queue_password@localhost:5432/queue_db?sslmode=disable")
	if err != nil {
		log.Fatalf("Postgres connection error: %v", err)
	}
	defer db.Close()

	workerID := "worker-" + uuid.New().String()[:8]
	worker := NewWorker(workerID, rdb, db)

	ctx := context.Background()
	if err := worker.initStreamAndGroup(ctx); err != nil {
		log.Fatalf("Consumer group initialization error: %v", err)
	}

	worker.Start(ctx)
}
