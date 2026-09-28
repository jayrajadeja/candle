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
func Aggregate(ticks []tick.Tick, width int64) ([]Candle, error) {
	if width <= 0 {
		return nil, ErrBadWidth
	}
	out := make([]Candle, 0)
	var (
		cur    Candle
		num    int64 // Σ(price*qty) for the open bucket
		bucket int64
		open   bool
	)
	flush := func() {
		if !open {
			return
		}
		if cur.Volume > 0 {
			cur.VWAP = num / int64(cur.Volume)
		}
		out = append(out, cur)
	}
	for _, tk := range ticks {
		b := tk.TS / width // TS assumed non-negative (logical counter); / truncates toward zero
		if !open || b != bucket {
			flush()
			bucket = b
			cur = Candle{Start: b * width, Open: tk.Price, High: tk.Price, Low: tk.Price}
			num = 0
			open = true
		}
		if tk.Price > cur.High {
			cur.High = tk.Price
		}
		if tk.Price < cur.Low {
			cur.Low = tk.Price
		}
		cur.Close = tk.Price
		cur.Volume += tk.Qty
		if tk.Side == tick.Sell {
			cur.SellVol += tk.Qty
		} else {
			cur.BuyVol += tk.Qty
		}
		cur.Trades++
		num += tk.Price * int64(tk.Qty)
	}
	flush()
	return out, nil
}
