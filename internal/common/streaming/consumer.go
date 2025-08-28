package streaming

//go:generate mockgen -destination=mock_kgo_consumer_test.go -package=streaming . kgoConsumer
//go:generate mockgen -destination=mock_processing_strategy_test.go -package=streaming . ProcessingStrategy

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type PartitionInfo struct {
	Topic     string
	Partition int32
}

type RebalanceEvent struct {
	Assigned []PartitionInfo
	Revoked  []PartitionInfo
}

type ConsumerMessage struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Timestamp time.Time
}

type MessageHandler func(context.Context, ConsumerMessage) error
type RebalanceHandler func(RebalanceEvent)
type ConsumerOption func(*consumerConfig)

type consumerConfig struct {
	franzOpt         []kgo.Opt
	rebalanceHandler RebalanceHandler
}

type ProcessingStrategy interface {
	ProcessRecords(
		ctx context.Context,
		records []ConsumerMessage,
		handler MessageHandler,
	) error
	Stop() error
}

type SequentialProcessor struct{}

func (p *SequentialProcessor) ProcessRecords(
	ctx context.Context,
	records []ConsumerMessage,
	handler MessageHandler,
) error {
	for _, record := range records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := handler(ctx, record); err != nil {
			return fmt.Errorf("error processing record %v: %w", record, err)
		}
	}
	return nil
}

func (p *SequentialProcessor) Stop() error {
	return nil
}

type keyProcessingJob struct {
	key      string
	messages []ConsumerMessage
	handler  MessageHandler
}

func (k *keyProcessingJob) Key() string {
	return k.key
}

func (k *keyProcessingJob) Execute(ctx context.Context) error {
	for _, message := range k.messages {
		if err := k.handler(ctx, message); err != nil {
			return fmt.Errorf("error processing message %v: %w", message, err)
		}
	}
	return nil
}

type KeyOrderedProcessor struct {
	MaxWorkers int
	pool       *hashBasedWorkerPool
	once       sync.Once
	startErr   error
}

func (p *KeyOrderedProcessor) startUnsafe(ctx context.Context) error {
	if p.MaxWorkers <= 0 {
		p.MaxWorkers = 1
	}

	p.pool = newHashBasedWorkerPool(p.MaxWorkers)
	if err := p.pool.Start(ctx); err != nil {
		return err
	}

	return nil
}

func (p *KeyOrderedProcessor) ProcessRecords(
	ctx context.Context,
	records []ConsumerMessage,
	handler MessageHandler,
) error {
	p.once.Do(func() {
		p.startErr = p.startUnsafe(ctx)
	})

	if p.startErr != nil {
		return p.startErr
	}

	messagesByKey := p.groupMessagesByKey(records)
	if len(messagesByKey) == 0 {
		return nil
	}

	jobs := p.createJobs(messagesByKey, handler)
	return p.pool.SubmitBatch(ctx, jobs)
}

func (p *KeyOrderedProcessor) createJobs(messagesByKey map[string][]ConsumerMessage, handler MessageHandler) []Job {
	jobs := make([]Job, 0, len(messagesByKey))
	for key, messages := range messagesByKey {
		jobs = append(jobs, &keyProcessingJob{
			key:      key,
			messages: messages,
			handler:  handler,
		})
	}
	return jobs
}

func (p *KeyOrderedProcessor) groupMessagesByKey(records []ConsumerMessage) map[string][]ConsumerMessage {
	sort.Slice(
		records,
		func(i, j int) bool {
			return records[i].Offset < records[j].Offset
		},
	)

	groups := make(map[string][]ConsumerMessage)
	for _, record := range records {
		key := p.getKey(record)
		groups[key] = append(groups[key], record)
	}

	return groups
}

func (p *KeyOrderedProcessor) getKey(msg ConsumerMessage) string {
	if len(msg.Key) > 0 {
		return string(msg.Key)
	}

	return fmt.Sprintf("%s-%d", msg.Topic, msg.Partition)
}

func (p *KeyOrderedProcessor) Stop() error {
	if p.pool != nil {
		err := p.pool.Stop()
		p.pool = nil
		return err
	}
	return nil
}

type kgoConsumer interface {
	PollFetches(ctx context.Context) kgo.Fetches
	Close()
}

func WithConsumerBrokers(brokers ...string) ConsumerOption {
	return func(c *consumerConfig) {
		c.franzOpt = append(c.franzOpt, kgo.SeedBrokers(brokers...))
	}
}

func WithConsumerGroup(group string) ConsumerOption {
	return func(c *consumerConfig) {
		c.franzOpt = append(c.franzOpt, kgo.ConsumerGroup(group))
	}
}

func WithTopic(topic string) ConsumerOption {
	return func(c *consumerConfig) {
		c.franzOpt = append(c.franzOpt, kgo.ConsumeTopics(topic))
	}
}

func NewConsumer(
	processor ProcessingStrategy,
	opts ...ConsumerOption,
) (*Consumer, error) {
	cfg := &consumerConfig{}

	for _, opt := range opts {
		opt(cfg)
	}

	franz, err := kgo.NewClient(cfg.franzOpt...)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer client: %w", err)
	}

	return &Consumer{
		client:    franz,
		processor: processor,
	}, nil
}

type Consumer struct {
	client    kgoConsumer
	processor ProcessingStrategy
}

func (c *Consumer) Consume(ctx context.Context, handler MessageHandler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // graceful shutdown - exit immediately
		default:
		}

		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); errs != nil {
			return fmt.Errorf("failed to poll fetches: %w", errs[0].Err)
		}

		if err := c.processRecords(ctx, fetches, handler); err != nil {
			return err
		}
	}
}

func (c *Consumer) processRecords(
	ctx context.Context,
	fetches kgo.Fetches,
	handler MessageHandler,
) error {
	var records []ConsumerMessage
	fetches.EachRecord(func(record *kgo.Record) {
		records = append(records, recordToConsumerMessage(record))
	})

	return c.processor.ProcessRecords(ctx, records, handler)
}

func (c *Consumer) Close() {
	err := c.processor.Stop()
	if err != nil {
		fmt.Printf("failed to stop processor: %v", err)
	}
	c.client.Close()
}

func recordToConsumerMessage(record *kgo.Record) ConsumerMessage {
	return ConsumerMessage{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Key:       record.Key,
		Value:     record.Value,
		Headers:   convertHeaders(record.Headers),
		Timestamp: record.Timestamp,
	}
}

func convertHeaders(kgoHeaders []kgo.RecordHeader) map[string][]byte {
	if len(kgoHeaders) == 0 {
		return nil
	}

	headers := make(map[string][]byte, len(kgoHeaders))
	for _, header := range kgoHeaders {
		headers[header.Key] = header.Value
	}
	return headers
}
