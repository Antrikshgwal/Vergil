package feature

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Snapshot is the set of per-user features read for one transaction.
type Snapshot struct {
	Velocity  int     // sliding-window transaction count
	AmountSum float64 // fixed-window running spend total
}

type Store interface {
	Snapshot(ctx context.Context, userID, txnID string, amount float64) (Snapshot, error)
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

// Snapshot computes both features for a user in a single Redis round trip.
//
// Velocity is a sliding window over a sorted set: ZADD this txn, drop members
// older than the window, ZCARD the survivors. AmountSum is a fixed window: a key
// bucketed by window index (now/window) accumulated with INCRBYFLOAT. Both keys
// get an EXPIRE so they self-clean.
//
// The two were originally two separate pipelines (two round trips per request);
// profiling under load showed the Redis round trip dominated decision latency,
// so they are merged into one TxPipeline here — one RTT on the hot path.
//
// Fixed-window trade-off: the amount window boundary is a hard cliff, so a burst
// straddling it can push ~2x the intended amount through. The sliding velocity
// window smooths that but pays by storing every event.
func (s *RedisStore) Snapshot(ctx context.Context, userID, txnID string, amount float64) (Snapshot, error) {
	now := time.Now()

	velKey := "velocity:" + userID
	score := float64(now.UnixNano())
	cutoff := now.Add(-s.window).UnixNano()

	bucket := now.Unix() / int64(s.window.Seconds())
	amtKey := fmt.Sprintf("amount:%s:%d", userID, bucket)

	pipe := s.client.TxPipeline()
	pipe.ZAdd(ctx, velKey, redis.Z{Score: score, Member: txnID})
	pipe.ZRemRangeByScore(ctx, velKey, "-inf", fmt.Sprint(cutoff))
	countCmd := pipe.ZCard(ctx, velKey)
	pipe.Expire(ctx, velKey, s.window)
	sumCmd := pipe.IncrByFloat(ctx, amtKey, amount)
	pipe.Expire(ctx, amtKey, s.window)
	if _, err := pipe.Exec(ctx); err != nil {
		return Snapshot{}, err
	}

	count, err := countCmd.Result()
	if err != nil {
		return Snapshot{}, err
	}
	sum, err := sumCmd.Result()
	if err != nil {
		return Snapshot{}, err
	}

	return Snapshot{Velocity: int(count), AmountSum: sum}, nil
}
