package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func newClient(opts ...Option) redis.UniversalClient {
	cfg := &Config{}
	for _, opt := range opts {
		opt(cfg)
	}

	return redis.NewUniversalClient(
		&redis.UniversalOptions{
			Addrs:        cfg.addresses,
			MaxRetries:   cfg.maxRetries,
			DialTimeout:  cfg.dialTimeout,
			ReadTimeout:  cfg.readTimeout,
			WriteTimeout: cfg.writeTimeout,
			PoolSize:     cfg.poolSize,
			MinIdleConns: cfg.minIdleConns,
		},
	)
}

type storage interface {
	Close() error
}

type stringStorage interface {
	storage
	Set(
		ctx context.Context,
		key string,
		value any,
		expiration time.Duration,
	) *redis.StatusCmd
}

type clientStorage interface {
	stringStorage
}

func NewClient(opts ...Option) *Client {
	return &Client{
		storage: newClient(opts...),
	}
}

type Cache struct {
	Key        string
	Value      any
	Expiration time.Duration
}

type Client struct {
	storage redis.UniversalClient
}

func (c *Client) Set(
	ctx context.Context,
	cache Cache,
) (string, error) {
	result, err := c.storage.Set(
		ctx,
		cache.Key,
		cache.Value,
		cache.Expiration,
	).Result()

	if err != nil {
		return "", err
	}

	return result, nil
}

func (c *Client) SetMany(
	ctx context.Context,
	keyValues []Cache,
) error {
	for _, kv := range keyValues {
		_, err := c.Set(ctx, kv)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) Close() error {
	return c.storage.Close()
}
