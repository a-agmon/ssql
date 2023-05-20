package drivers

import (
	"context"
	"github.com/go-redis/redis/v8"
)

type RedisDriver struct {
	client *redis.Client
}

// NewRedisDriver initializes a Redis client and returns a new RedisDriver instance
func NewRedisDriver(addr, password string, db int) *RedisDriver {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})

	return &RedisDriver{
		client: rdb,
	}
}

func (rd *RedisDriver) ExecuteQuery(entity string, filterFields string, selectFields string) (string, error) {
	ctx := context.Background()
	value, err := rd.client.HGet(ctx, entity, filterFields).Result()
	if err != nil {
		return "", err
	}
	return value, nil
}
