package rmq

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

var unusedContext = context.TODO()

type RedisWrapper interface {
	Set(key string, value string, expiration time.Duration) error
	Del(key string) (affected int64, err error)
	TTL(key string) (ttl time.Duration, err error)
	LPush(key string, value ...string) (total int64, err error)
	LLen(key string) (affected int64, err error)
	LRem(key string, count int64, value string) (affected int64, err error)
	LTrim(key string, start int64, stop int64) error
	RPopLPush(source string, destination string) (value string, err error)
	SAdd(key string, value string) (total int64, err error)
	SMembers(key string) (members []string, err error)
	SRem(key string, value string) (affected int64, err error)
	FlushDb() error
}

type RedisWrapperImpl struct {
	rawClient redis.Cmdable
}

var _ RedisWrapper = &RedisWrapperImpl{}

func (wrapper RedisWrapperImpl) Set(key string, value string, expiration time.Duration) error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.Set(unusedContext, key, value, expiration).Err()
}

func (wrapper RedisWrapperImpl) Del(key string) (affected int64, err error) {
	return wrapper.rawClient.Del(unusedContext, key).Result()
}

func (wrapper RedisWrapperImpl) TTL(key string) (ttl time.Duration, err error) {
	return wrapper.rawClient.TTL(unusedContext, key).Result()
}

func (wrapper RedisWrapperImpl) LPush(key string, value ...string) (total int64, err error) {
	return wrapper.rawClient.LPush(unusedContext, key, value).Result()
}

func (wrapper RedisWrapperImpl) LLen(key string) (affected int64, err error) {
	return wrapper.rawClient.LLen(unusedContext, key).Result()
}

func (wrapper RedisWrapperImpl) LRem(key string, count int64, value string) (affected int64, err error) {
	return wrapper.rawClient.LRem(unusedContext, key, int64(count), value).Result()
}

func (wrapper RedisWrapperImpl) LTrim(key string, start, stop int64) error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.LTrim(unusedContext, key, int64(start), int64(stop)).Err()
}

func (wrapper RedisWrapperImpl) RPopLPush(source, destination string) (value string, err error) {
	value, err = wrapper.rawClient.RPopLPush(unusedContext, source, destination).Result()
	// println("RPopLPush", source, destination, value, err)
	switch err {
	case nil:
		return value, nil
	case redis.Nil:
		return value, ErrorNotFound
	default:
		return value, err
	}
}

func (wrapper RedisWrapperImpl) SAdd(key, value string) (total int64, err error) {
	return wrapper.rawClient.SAdd(unusedContext, key, value).Result()
}

func (wrapper RedisWrapperImpl) SMembers(key string) (members []string, err error) {
	return wrapper.rawClient.SMembers(unusedContext, key).Result()
}

func (wrapper RedisWrapperImpl) SRem(key, value string) (affected int64, err error) {
	return wrapper.rawClient.SRem(unusedContext, key, value).Result()
}

func (wrapper RedisWrapperImpl) FlushDb() error {
	// NOTE: using Err() here because Result() string is always "OK"
	return wrapper.rawClient.FlushDB(unusedContext).Err()
}
