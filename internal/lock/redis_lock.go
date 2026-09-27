package lock

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// releaseLockLua ensures a worker only deletes its own lock
var releaseLockLua = redis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
`)

type RedisLocker struct {
	client *redis.Client
}

func NewRedisLocker(client *redis.Client) *RedisLocker {
	return &RedisLocker{client: client}
}

// TryAcquire attempts to lock a key using SET key token NX PX ttl
func (l *RedisLocker) TryAcquire(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	lockKey := "lock:idempotency:" + key
	ok, err := l.client.SetNX(ctx, lockKey, token, ttl).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

// Release safely unlocks the key via Lua script evaluation
func (l *RedisLocker) Release(ctx context.Context, key, token string) error {
	lockKey := "lock:idempotency:" + key
	return releaseLockLua.Run(ctx, l.client, []string{lockKey}, token).Err()
}
