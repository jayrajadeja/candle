// Package candle aggregates a non-decreasing stream of ticks into fixed-width
// OHLCV candles bucketed by logical timestamp. It is a pure, deterministic,
// integer core with no IO.
package candle

import (
	"errors"

	"github.com/jayrajadeja/candle/tick"
)

// ErrBadWidth is returned when the bucket width is not positive.
var ErrBadWidth = errors.New("candle: width must be positive")

// Source is a transport-agnostic producer of candles for one symbol over a
// logical-time window, letting an HTTP server hold any candle backend.
type Source interface {
	Candles(symbol string, width, from, to int64) ([]Candle, error)
}

// Candle is one aggregated bar. Start is the bucket's logical-time start
// (bucketIndex * width). Prices are in integer ticks; VWAP is integer
// (volume-weighted mean price, floored).
type Candle struct {
	Start   int64
	Open    int64
	High    int64
	Low     int64
	Close   int64
	Volume  uint64
	VWAP    int64
	Trades  int
	BuyVol  uint64
	SellVol uint64
}

// Aggregate groups ticks into candles of the given logical-TS width. ticks are
// assumed non-decreasing in TS (tickstore's guarantee). Only buckets that
// contain at least one trade are emitted, in TS order. width must be positive.
// It is a thin one-shot wrapper over Aggregator.
func Aggregate(ticks []tick.Tick, width int64) ([]Candle, error) {
	a, err := NewAggregator(width)
	if err != nil {
		return nil, err
	}
	a.Add(ticks)
	return a.Candles(), nil
}
