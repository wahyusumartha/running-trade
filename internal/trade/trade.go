package trade

import (
	"encoding/json"
	"time"
)

type Side string

const (
	SideBuy  Side = "BUY"
	SideSell Side = "SELL"
)

type Trade struct {
	Symbol    string    `json:"symbol"`
	Price     float64   `json:"price"`
	Volume    int       `json:"volume"`
	Side      Side      `json:"side"`
	Timestamp time.Time `json:"timestamp"`
	TradeID   string    `json:"trade_id"`
}

func (t Trade) ToJSON() ([]byte, error) {
	return json.Marshal(t)
}
