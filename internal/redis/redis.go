package repository

import (
	"context"
	"fmt"
	"log"
	"shortly/helpers"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis struct{}

type RedisConnect struct {
	*redis.Client
}

var redisInstance *RedisConnect
var onceRedis sync.Once

func (r *Redis) GetRedisConn() *RedisConnect {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	onceRedis.Do(func() {

		client := redis.NewClient(&redis.Options{
			Addr: getRedisAddr(),
		})

		if _, err := client.Ping(ctx).Result(); err != nil {
			log.Fatalf("redis conn err : %v", err)
		}

		redisInstance = &RedisConnect{
			Client: client,
		}
	})
	log.Println("Redis connected !!")

	return redisInstance
}

func getRedisAddr() string {

	host := helpers.GetEnv("REDIS_HOST")
	port := helpers.GetEnv("REDIS_PORT")

	return fmt.Sprintf("%s:%s", host, port)
}
