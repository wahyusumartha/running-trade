package redis

import "time"

type Config struct {
	addresses []string

	maxRetries   int
	dialTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	poolSize     int
	minIdleConns int
}

type Option func(*Config)

func WithAddresses(addrs ...string) Option {
	return func(c *Config) {
		c.addresses = addrs
	}
}

func WithMaxRetries(maxRetries int) Option {
	return func(c *Config) {
		c.maxRetries = maxRetries
	}
}

func WithDialTimeout(dialTimeout time.Duration) Option {
	return func(c *Config) {
		c.dialTimeout = dialTimeout
	}
}

func WithReadTimeout(readTimeout time.Duration) Option {
	return func(c *Config) {
		c.readTimeout = readTimeout
	}
}

func WithWriteTimeout(writeTimeout time.Duration) Option {
	return func(c *Config) {
		c.writeTimeout = writeTimeout
	}
}

func WithPoolSize(poolSize int) Option {
	return func(c *Config) {
		c.poolSize = poolSize
	}
}

func WithMinIdleConns(minIdleConns int) Option {
	return func(c *Config) {
		c.minIdleConns = minIdleConns
	}
}
