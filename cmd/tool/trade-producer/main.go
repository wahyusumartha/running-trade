package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"kolangkoding.com/experiment/internal/common/streaming"
	"kolangkoding.com/experiment/internal/tool"
	"kolangkoding.com/experiment/internal/trade"
)

func main() {
	var (
		brokers  = flag.String("brokers", "localhost:19092", "Kafka brokers")
		topic    = flag.String("topic", "trades", "Topic to produce to")
		count    = flag.Int("count", 1000, "Number of trades to produce")
		rate     = flag.Int("rate", 100, "Trades per second (0 = unlimited)")
		duration = flag.Duration("duration", 0, "Run for duration instead of count (e.g., 5m)")
	)
	flag.Parse()

	// Create producer using your common package
	producer, err := streaming.NewProducer(
		streaming.WithBrokers(*brokers),
		streaming.WithClientID("trade-producer"),
		streaming.WithRetries(2),
		streaming.WithRequestRetries(2),
		streaming.WithConnectionTimeout(5*time.Second),
		streaming.WithRetryBackOff(100*time.Millisecond, 1*time.Second),
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	generator := tool.NewTradeGenerator()

	fmt.Printf("📈 Starting Trade Producer\n")
	fmt.Printf("   Topic: %s\n", *topic)
	if *duration > 0 {
		fmt.Printf("   Duration: %v\n", *duration)
	} else {
		fmt.Printf("   Count: %d trades\n", *count)
	}
	fmt.Printf("   Rate: %d trades/sec%s\n", *rate, func() string {
		if *rate == 0 {
			return " (unlimited)"
		}
		return ""
	}())
	fmt.Println()

	ctx := context.Background()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	var sent int64
	var errors int64
	start := time.Now()

	// Rate limiter
	var ticker *time.Ticker
	if *rate > 0 {
		ticker = time.NewTicker(time.Second / time.Duration(*rate))
		defer ticker.Stop()
	}

	// Progress reporter
	done := make(chan bool)
	go func() {
		reportTicker := time.NewTicker(5 * time.Second)
		defer reportTicker.Stop()

		for {
			select {
			case <-reportTicker.C:
				currentSent := atomic.LoadInt64(&sent)
				currentErrors := atomic.LoadInt64(&errors)
				if currentSent > 0 {
					elapsed := time.Since(start).Seconds()
					rate := float64(currentSent) / elapsed
					fmt.Printf("📊 Progress: %d sent, %d errors, %.1f trades/sec\n",
						currentSent, currentErrors, rate)
				}
			case <-done:
				return
			}
		}
	}()

	// Generate and send trades
	for i := 0; i < *count || *duration > 0; i++ {
		select {
		case <-ctx.Done():
			break
		default:
		}

		// Rate limiting
		if ticker != nil {
			select {
			case <-ticker.C:
			case <-ctx.Done():
				break
			}
		}

		trade := generator.Generate()

		if err := sendTrade(ctx, producer, *topic, trade); err != nil {
			atomic.AddInt64(&errors, 1)
			if errors%100 == 1 { // Log every 100th error
				log.Printf("Error sending trade: %v", err)
			}
		} else {
			fmt.Println("sent")
			atomic.AddInt64(&sent, 1)
		}

		// Break if using duration and context is done
		if *duration > 0 {
			select {
			case <-ctx.Done():
				goto finish
			default:
			}
		}
	}

finish:
	done <- true

	finalSent := atomic.LoadInt64(&sent)
	finalErrors := atomic.LoadInt64(&errors)
	totalDuration := time.Since(start)

	fmt.Printf("\n🎉 Trade Production Complete!\n")
	fmt.Printf("   Total Trades: %d\n", finalSent)
	fmt.Printf("   Errors: %d\n", finalErrors)
	fmt.Printf("   Duration: %v\n", totalDuration)
	fmt.Printf("   Average Rate: %.1f trades/sec\n", float64(finalSent)/totalDuration.Seconds())
	if finalSent > 0 {
		fmt.Printf("   Success Rate: %.2f%%\n", float64(finalSent)*100/float64(finalSent+finalErrors))
	}
}

func sendTrade(ctx context.Context, producer *streaming.Producer, topic string, trade trade.Trade) error {
	data, err := trade.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal trade: %w", err)
	}

	msg := &streaming.Message{
		Topic: topic,
		Key:   []byte(trade.Symbol), // Key by symbol for ordered processing
		Value: data,
		Headers: map[string][]byte{
			"symbol":   []byte(trade.Symbol),
			"side":     []byte(trade.Side),
			"trade_id": []byte(trade.TradeID),
		},
	}
	fmt.Printf("send trade to kafka: %s", trade.TradeID)
	return producer.ProduceSync(ctx, msg)
}
