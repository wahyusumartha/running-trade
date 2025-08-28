package tool

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"kolangkoding.com/experiment/internal/trade"
)

var symbols = []string{
	"AAPL", "MSFT", "GOOGL", "AMZN", "TSLA",
	"NVDA", "META", "NFLX", "AMD", "INTC",
}

var basePrices = map[string]float64{
	"AAPL":  185.50,
	"MSFT":  378.85,
	"GOOGL": 140.25,
	"AMZN":  145.75,
	"TSLA":  248.50,
	"NVDA":  875.25,
	"META":  515.75,
	"NFLX":  485.25,
	"AMD":   142.85,
	"INTC":  23.45,
}

func NewTradeGenerator() *TradeGenerator {
	symbolCounter := make(map[string]int64)
	for _, symbol := range symbols {
		symbolCounter[symbol] = 0
	}
	return &TradeGenerator{
		symbols:       symbols,
		prices:        basePrices,
		counter:       0,
		symbolCounter: symbolCounter,
	}
}

type TradeGenerator struct {
	symbols       []string
	prices        map[string]float64
	counter       int64
	symbolCounter map[string]int64
	mutex         sync.Mutex
}

func (g *TradeGenerator) Generate() trade.Trade {
	g.counter++

	symbol := g.getRandomSymbol()

	g.mutex.Lock()
	g.symbolCounter[symbol]++
	symbolSeq := g.symbolCounter[symbol]
	g.mutex.Unlock()

	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	monotonicTime := baseTime.Add(time.Duration(symbolSeq) * time.Millisecond)

	price := g.generatePrice(symbol)
	volume := g.generateVolume()
	side := g.generateSide()

	return trade.Trade{
		Symbol:    symbol,
		Price:     price,
		Volume:    volume,
		Side:      side,
		Timestamp: monotonicTime,
		TradeID:   fmt.Sprintf("Trade-%s-%d", symbol, symbolSeq),
	}
}

func (g *TradeGenerator) getRandomSymbol() string {
	return g.symbols[rand.Intn(len(g.symbols))]
}

func (g *TradeGenerator) generatePrice(symbol string) float64 {
	basePrice := g.prices[symbol]

	// Random price movement: ±2% of base price
	variation := (rand.Float64() - 0.5) * 0.04 * basePrice
	newPrice := basePrice + variation

	// Round to 2 decimal places
	newPrice = float64(int(newPrice*100)) / 100

	// Update stored price for next trade (creates trending)
	g.prices[symbol] = newPrice

	return newPrice
}

func (g *TradeGenerator) generateVolume() int {
	// Generate volume between 100 and 10,000 shares
	// Most trades are small, some are large
	if rand.Float64() < 0.8 {
		return 100 + rand.Intn(900)
	}

	return 1000 + rand.Intn(9000)
}

func (g *TradeGenerator) generateSide() trade.Side {
	if rand.Float64() < 0.8 {
		return trade.SideBuy
	}

	return trade.SideSell
}
