package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

const StreamName = "payment_stream"

type PaymentRequest struct {
	AccountID string  `json:"account_id"`
	Amount    float64 `json:"amount"`
}

type API struct {
	rdb *redis.Client
}

func (a *API) handlePayment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idempotencyKey := r.Header.Get("X-Idempotency-Key")
	if idempotencyKey == "" {
		http.Error(w, "Missing X-Idempotency-Key header", http.StatusBadRequest)
		return
	}

	var req PaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Amount <= 0 || req.AccountID == "" {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	args := &redis.XAddArgs{
		Stream: StreamName,
		Values: map[string]interface{}{
			"idempotency_key": idempotencyKey,
			"account_id":      req.AccountID,
			"amount":          req.Amount,
			"timestamp":       time.Now().UnixNano(),
		},
	}

	msgID, err := a.rdb.XAdd(ctx, args).Result()
	if err != nil {
		log.Printf("XADD error: %v", err)
		http.Error(w, "Internal queue error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "queued",
		"event_id": msgID,
	})
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	api := &API{rdb: rdb}
	http.HandleFunc("/payments", api.handlePayment)

	log.Println("API Gateway listening on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
