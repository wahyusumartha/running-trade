package streaming

//go:generate mockgen -destination=mock_kafka_client_test.go -package=streaming . kafkaClient

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

type Option func(*config)

type config struct {
	brokers  []string
	clientID string
	franzOpt []kgo.Opt
}

func WithBrokers(brokers ...string) Option {
	return func(c *config) {
		c.brokers = brokers
	}
}

func WithClientID(clientID string) Option {
	return func(c *config) {
		c.clientID = clientID
	}
}

func WithFranzOpt(franzOpt kgo.Opt) Option {
	return func(c *config) {
		c.franzOpt = append(c.franzOpt, franzOpt)
	}
}

func NewClient(opts ...Option) (*Client, error) {
	cfg := &config{}

	for _, opt := range opts {
		opt(cfg)
	}

	franzOpts := []kgo.Opt{
		kgo.SeedBrokers(cfg.brokers...),
		kgo.ClientID(cfg.clientID),
	}

	franzOpts = append(franzOpts, cfg.franzOpt...)

	franz, err := kgo.NewClient(franzOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return &Client{client: franz}, nil
}

type kafkaClient interface {
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
	Produce(ctx context.Context, record *kgo.Record, callback func(*kgo.Record, error))
	Close()
}

type Client struct {
	client kafkaClient
}

func (c *Client) ProduceSync(
	ctx context.Context,
	msg *Message,
) error {
	record := c.messageToRecord(msg)
	return c.client.ProduceSync(ctx, record).FirstErr()
}

func (c *Client) ProduceAsync(ctx context.Context, msg *Message, callback func(error)) {
	record := c.messageToRecord(msg)
	c.client.Produce(ctx, record, func(record *kgo.Record, err error) {
		if callback != nil {
			callback(err)
		}
	})
}

func (c *Client) messageToRecord(msg *Message) *kgo.Record {
	record := &kgo.Record{
		Topic: msg.Topic,
		Key:   msg.Key,
		Value: msg.Value,
	}

	if len(msg.Headers) > 0 {
		record.Headers = make([]kgo.RecordHeader, 0, len(msg.Headers))
		for k, v := range msg.Headers {
			record.Headers = append(
				record.Headers,
				kgo.RecordHeader{
					Key:   k,
					Value: v,
				},
			)
		}
	}

	return record
}

func (c *Client) Close() {
	c.client.Close()
}
