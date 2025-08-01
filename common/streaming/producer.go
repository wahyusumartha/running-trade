package streaming

//go:generate mockgen -destination=mock_kafka_client_test.go -package=streaming . kafkaClient

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Message struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers map[string][]byte
}

type ProducerOption func(*producerConfig)

type producerConfig struct {
	clientID string
	franzOpt []kgo.Opt
}

func WithBrokers(brokers ...string) ProducerOption {
	return func(p *producerConfig) {
		p.franzOpt = append(p.franzOpt, kgo.SeedBrokers(brokers...))
	}
}

func WithClientID(clientID string) ProducerOption {
	return func(p *producerConfig) {
		p.franzOpt = append(p.franzOpt, kgo.ClientID(clientID))
	}
}

func WithRetries(maxRetries int) ProducerOption {
	return func(p *producerConfig) {
		p.franzOpt = append(p.franzOpt, kgo.RequestRetries(maxRetries))
	}
}

func WithRetryBackOff(min, max time.Duration) ProducerOption {
	return func(p *producerConfig) {
		p.franzOpt = append(
			p.franzOpt,
			kgo.RetryBackoffFn(func(tries int) time.Duration {
				backOff := time.Duration(tries) * min
				if backOff > max {
					return max
				}
				return backOff
			}),
		)
	}
}

func NewProducer(opts ...ProducerOption) (*Producer, error) {
	cfg := &producerConfig{}

	for _, opt := range opts {
		opt(cfg)
	}

	franz, err := kgo.NewClient(cfg.franzOpt...)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	return &Producer{client: franz}, nil
}

type kafkaClient interface {
	ProduceSync(ctx context.Context, records ...*kgo.Record) kgo.ProduceResults
	Produce(ctx context.Context, record *kgo.Record, callback func(*kgo.Record, error))
	Close()
}

type Producer struct {
	client kafkaClient
}

func (p *Producer) ProduceSync(
	ctx context.Context,
	msg *Message,
) error {
	record := p.messageToRecord(msg)
	return p.client.ProduceSync(ctx, record).FirstErr()
}

func (p *Producer) ProduceAsync(
	ctx context.Context,
	msg *Message,
	callback func(error),
) {
	record := p.messageToRecord(msg)
	p.client.Produce(ctx, record, func(record *kgo.Record, err error) {
		if callback != nil {
			callback(err)
		}
	})
}

func (p *Producer) messageToRecord(msg *Message) *kgo.Record {
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

func (p *Producer) Close() {
	p.client.Close()
}
