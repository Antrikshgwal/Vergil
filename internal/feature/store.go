package feature

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)


type Store interface {
	Velocity(ctx context.Context, userID, txnID string) (int, error)
	AmountSum(ctx context.Context, userID string, amount float64) (float64, error)
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

// AmountSum accumulates a user's spend inside a fixed time window and returns
// the running total for the current window.
//
// The key is bucketed by window index (now / window), so every key owns a hard
// [start, start+window) slot. INCRBYFLOAT adds this txn's amount to the slot;
// EXPIRE lets the slot self-clean once it can no longer be hit.
//
// Trade-off vs the sliding sorted-set Velocity above: this is O(1) memory and a
// single round trip — cheap — but the window boundary is a cliff. A user can
// spend up to the limit at the tail of one slot and again at the head of the
// next, pushing ~2x the intended amount through in a burst that straddles the
// boundary. The sliding window smooths that, but pays by storing every event.
func (s *RedisStore) AmountSum(ctx context.Context, userID string, amount float64) (float64, error) {
	bucket := time.Now().Unix() / int64(s.window.Seconds())
	key := fmt.Sprintf("amount:%s:%d", userID, bucket)

	pipe := s.client.TxPipeline()
	sumCmd := pipe.IncrByFloat(ctx, key, amount)
	pipe.Expire(ctx, key, s.window)
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}

	sum, err := sumCmd.Result()
	if err != nil {
		return 0, err
	}

	return sum, nil
}
