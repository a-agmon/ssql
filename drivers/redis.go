package drivers

import (
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

func (d *RedisDriver) ExecuteQuery(entity string, filterFields string, selectFields string) (string, error) {
	return "", nil
}
