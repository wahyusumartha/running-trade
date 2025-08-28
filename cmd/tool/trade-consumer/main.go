package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"kolangkoding.com/experiment/internal/common/streaming"
	"kolangkoding.com/experiment/internal/trade"
)

type ConsumerStats struct {
	totalProcessed  int64
	totalErrors     int64
	tradesPerSymbol map[string]int64
	lastTradeOffset map[string]int64
	orderViolations int64
	startTime       time.Time
	mutex           sync.RWMutex
}

func (s *ConsumerStats) recordTrade(trade trade.Trade, offset int64) {
	atomic.AddInt64(&s.totalProcessed, 1)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if lastOffset, exists := s.lastTradeOffset[trade.Symbol]; exists {
		if offset <= lastOffset {
			atomic.AddInt64(&s.orderViolations, 1)
			log.Printf("ORDER VIOLATION: %s trade %s received with offset %d <= previous offset %d",
				trade.Symbol, trade.TradeID, offset, lastOffset)
			log.Printf("  Current offset: %d", offset)
			log.Printf("  Previous offset: %d", lastOffset)
		}
	}

	s.tradesPerSymbol[trade.Symbol]++
	s.lastTradeOffset[trade.Symbol] = offset
}

func (s *ConsumerStats) recordError() {
	atomic.AddInt64(&s.totalErrors, 1)
}

func (s *ConsumerStats) GetStats() (int64, int64, int64, float64) {
	processed := atomic.LoadInt64(&s.totalProcessed)
	errors := atomic.LoadInt64(&s.totalErrors)
	violations := atomic.LoadInt64(&s.orderViolations)
	duration := time.Since(s.startTime).Seconds()
	rate := float64(processed) / duration
	return processed, errors, violations, rate
}

func (s *ConsumerStats) getSymbolStats() map[string]int64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	result := make(map[string]int64, len(s.tradesPerSymbol))
	for k, v := range s.tradesPerSymbol {
		result[k] = v
	}
	return result
}

func NewConsumerStats() *ConsumerStats {
	return &ConsumerStats{
		tradesPerSymbol: make(map[string]int64),
		lastTradeOffset: make(map[string]int64),
		startTime:       time.Now(),
	}
}

func main() {
	var (
		brokers    = flag.String("brokers", "localhost:19092", "Kafka brokers")
		topic      = flag.String("topic", "trades", "Topic to consume from")
		groupID    = flag.String("group", "trade-consumer-test", "Consumer group ID")
		maxWorkers = flag.Int("workers", 10, "Max workers for KeyOrderedProcessor")
		processor  = flag.String("processor", "key-ordered", "Processor type: sequential or key-ordered")
		verbose    = flag.Bool("verbose", false, "Verbose logging of trades")
	)
	flag.Parse()

	stats := NewConsumerStats()

	fmt.Printf("📊 Starting Trade Consumer\n")
	fmt.Printf("   Topic: %s\n", *topic)
	fmt.Printf("   Group ID: %s\n", *groupID)
	fmt.Printf("   Processor: %s", *processor)
	if *processor == "key-ordered" {
		fmt.Printf(" (workers: %d)", *maxWorkers)
	}
	fmt.Printf("\n   Verbose: %t\n", *verbose)
	fmt.Println()

	var processingStrategy streaming.ProcessingStrategy
	switch *processor {
	case "sequential":
		processingStrategy = &streaming.SequentialProcessor{}
		fmt.Println("🔄 Using Sequential Processor")
	case "key-ordered":
		processingStrategy = &streaming.KeyOrderedProcessor{
			MaxWorkers: *maxWorkers,
		}
		fmt.Printf("⚡ Using KeyOrdered Processor with %d workers\n", *maxWorkers)
	default:
		log.Fatalf("Unknown processor type: %s", *processor)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go reportStats(ctx, stats)

	consumer, err := streaming.NewConsumer(
		processingStrategy,
		streaming.WithConsumerBrokers(*brokers),
		streaming.WithConsumerGroup(*groupID),
		streaming.WithTopic(*topic),
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	tradeValidatorConsumer := NewTradeValidatorConsumer(
		consumer,
		stats,
		*verbose,
	)
	defer tradeValidatorConsumer.Close()

	fmt.Println("Consumer started.....")
	done := make(chan error, 1)
	go func() {
		consumeErr := tradeValidatorConsumer.Consume(ctx)
		done <- consumeErr
	}()

	select {
	case sig := <-sigChan:
		fmt.Printf("Received signal %s, shutting down...\n", sig)
	case doneErr := <-done:
		if doneErr != nil && !errors.Is(doneErr, context.Canceled) {
			fmt.Printf("Consumer error: %v\n", doneErr)
			cancel()
			time.Sleep(1 * time.Second)
			printFinalStats(stats)
			os.Exit(1) // Pod restart
		}
		fmt.Println("Consumer done.")
	}

	cancel()
	time.Sleep(2 * time.Second)
	printFinalStats(stats)
	fmt.Println("✅ Graceful shutdown completed")
}

func reportStats(ctx context.Context, stats *ConsumerStats) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			processed, errs, violations, rate := stats.GetStats()
			if processed > 0 {
				fmt.Printf("📊 Processed: %d trades, Errors: %d, Violations: %d, Rate: %.1f trades/sec\n",
					processed, errs, violations, rate)

				// Show top symbols
				symbolStats := stats.getSymbolStats()
				if len(symbolStats) > 0 {
					fmt.Printf("   Top symbols: ")
					count := 0
					for symbol, trades := range symbolStats {
						if count >= 5 {
							break
						}
						fmt.Printf("%s:%d ", symbol, trades)
						count++
					}
					fmt.Println()
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func printFinalStats(stats *ConsumerStats) {
	processed, errors, violations, avgRate := stats.GetStats()
	duration := time.Since(stats.startTime)

	fmt.Printf("\n🎉 Final Consumer Statistics:\n")
	fmt.Printf("   Total Processed: %d trades\n", processed)
	fmt.Printf("   Total Errors: %d\n", errors)
	fmt.Printf("   Order Violations: %d\n", violations)
	fmt.Printf("   Duration: %v\n", duration)
	fmt.Printf("   Average Rate: %.1f trades/sec\n", avgRate)

	if processed > 0 {
		fmt.Printf("   Success Rate: %.2f%%\n", float64(processed)*100/float64(processed+errors))
		fmt.Printf("   Order Accuracy: %.2f%%\n", float64(processed-violations)*100/float64(processed))
	}

	fmt.Printf("\n📈 Trades per Symbol:\n")
	for symbol, count := range stats.tradesPerSymbol {
		fmt.Printf("   %s: %d trades\n", symbol, count)
	}

	if violations == 0 {
		fmt.Printf("\n✅ SUCCESS: No ordering violations detected!\n")
		fmt.Printf("   KeyOrderedProcessor is working correctly!\n")
	} else {
		fmt.Printf("\n❌ WARNING: %d ordering violations detected!\n", violations)
		fmt.Printf("   KeyOrderedProcessor may have issues!\n")
	}
}

type tradeRecorder interface {
	recordTrade(trade trade.Trade, offset int64)
	recordError()
}

type kafkaConsumer interface {
	Consume(ctx context.Context, handler streaming.MessageHandler) error
	Close()
}

func NewTradeValidatorConsumer(
	consumer kafkaConsumer,
	recorder tradeRecorder,
	isVerbose bool,
) *TradeValidatorConsumer {
	return &TradeValidatorConsumer{
		consumer:      consumer,
		tradeRecorder: recorder,
		verbose:       isVerbose,
		wg:            &sync.WaitGroup{},
		stopChan:      make(chan struct{}),
	}
}

type TradeValidatorConsumer struct {
	consumer      kafkaConsumer
	tradeRecorder tradeRecorder
	verbose       bool
	wg            *sync.WaitGroup
	stopChan      chan struct{}
	stopOnce      sync.Once
}

func (c *TradeValidatorConsumer) Consume(ctx context.Context) error {
	return c.consumer.Consume(ctx, c.handler)
}

func (c *TradeValidatorConsumer) handler(ctx context.Context, message streaming.ConsumerMessage) error {
	c.wg.Add(1)
	defer c.wg.Done()

	select {
	case <-c.stopChan:
		return nil
	default:
	}

	var t trade.Trade
	if err := json.Unmarshal(message.Value, &t); err != nil {
		c.tradeRecorder.recordError()
		return fmt.Errorf("failed to unmarshal trade: %w", err)
	}

	// Verify the key matches the symbol
	expectedKey := t.Symbol
	actualKey := string(message.Key)
	if actualKey != expectedKey {
		log.Printf("⚠️  KEY MISMATCH: Expected key '%s', got '%s' for trade %s",
			expectedKey, actualKey, t.TradeID)
	}

	c.tradeRecorder.recordTrade(t, message.Offset)

	if c.verbose {
		fmt.Printf("📈 %s: %s %d @ $%.2f [%s] (partition: %d, offset: %d)\n",
			t.Symbol, t.Side, t.Volume, t.Price,
			t.TradeID, message.Partition, message.Offset)
	}

	return nil
}

func (c *TradeValidatorConsumer) Close() error {
	c.stopOnce.Do(func() {
		c.consumer.Close()
		close(c.stopChan)
		c.wg.Wait()
	})

	return nil
}
