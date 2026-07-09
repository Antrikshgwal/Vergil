package feature

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)


type Store interface {
	Velocity(ctx context.Context, userID, txnID string) (int, error)
}

type RedisStore struct {
	client *redis.Client
	window time.Duration
}

func NewRedisStore(addr string, window time.Duration) *RedisStore {
	return &RedisStore{
		client: redis.NewClient(&redis.Options{Addr: addr}),
		window: window,
	}
}

func (s *RedisStore) Velocity(ctx context.Context, userID, txnID string) (int, error) {
	key := "velocity:" + userID
	now := time.Now()
	score := float64(now.UnixNano())
	cutoff := now.Add(-s.window).UnixNano()

	pipe := s.client.TxPipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: txnID})
	pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprint(cutoff))
	countCmd := pipe.ZCard(ctx, key)
	pipe.Expire(ctx, key, s.window)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return 0, err
	}
	count, err := countCmd.Result()
	if err != nil {
		return 0, err
	}

	return int(count), err
}
