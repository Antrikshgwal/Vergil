package feature

import (
	"context"
	"time"

	"github.com/go-redis/redis"
)

var rdb = redis.NewClient(&redis.Options{
	Addr: "localhost:6379", // Redis server address
})

type Store interface {
	Velocity(ctx context.Context, key string) (int, error)
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

func (s *RedisStore) Velocity(ctx context.Context, UserID string) (int, error) {
	key := "velocity:" + UserID
	count, err := s.client.Incr( key).Result()
	if err != nil {
		return 0, err
	}

	if count == 1 {
		err = s.client.Expire(key, s.window).Err()
	}
	return int(count), err
}
