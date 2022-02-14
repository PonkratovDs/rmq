package rmq

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

type RedisUniversal struct {
	uClient redis.UniversalClient
}

var _ RedisWrapper = &RedisUniversal{}

func (universal *RedisUniversal) Set(key string, value string, expiration time.Duration) error {
	return universal.uClient.Set(context.Background(), key, value, expiration).Err()
}

func (universal *RedisUniversal) Del(key string) (affected int64, err error) {
	return universal.uClient.Del(context.Background(), key).Result()
}

func (universal *RedisUniversal) TTL(key string) (ttl time.Duration, err error) {
	return universal.uClient.TTL(context.Background(), key).Result()
}

func (universal *RedisUniversal) LPush(key string, value ...string) (total int64, err error) {
	return universal.uClient.LPush(context.Background(), key, value).Result()
}

func (universal *RedisUniversal) LLen(key string) (affected int64, err error) {
	return universal.uClient.LLen(context.Background(), key).Result()
}

func (universal *RedisUniversal) LRem(key string, count int64, value string) (affected int64, err error) {
	return universal.uClient.LRem(context.Background(), key, count, value).Result()
}

func (universal *RedisUniversal) LTrim(key string, start int64, stop int64) error {
	return universal.uClient.LTrim(context.Background(), key, start, stop).Err()
}

func (universal *RedisUniversal) RPopLPush(source string, destination string) (value string, err error) {
	return universal.uClient.RPopLPush(context.Background(), source, destination).Result()
}

func (universal *RedisUniversal) SAdd(key string, value string) (total int64, err error) {
	return universal.uClient.SAdd(context.Background(), key, value).Result()
}

func (universal *RedisUniversal) SMembers(key string) (members []string, err error) {
	return universal.uClient.SMembers(context.Background(), key).Result()
}

func (universal *RedisUniversal) SRem(key string, value string) (affected int64, err error) {
	return universal.uClient.SRem(context.Background(), key, value).Result()
}

func (universal *RedisUniversal) FlushDb() error {
	return universal.uClient.FlushDB(context.Background()).Err()
}
