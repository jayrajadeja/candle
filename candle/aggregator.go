package candle

import "github.com/jayrajadeja/candle/tick"

// Aggregator is the resumable form of Aggregate: it folds a non-decreasing tick
// stream into fixed-width OHLCV candles across successive Add calls, doing work
// proportional to the ticks it is given rather than the whole history. Because
// tickstore's log is append-only, later Add calls only extend the open bucket or
// open newer ones — a finalized bucket is never revised.
type Aggregator struct {
	width  int64
	done   []Candle // finalized buckets, in TS order
	cur    Candle   // the open (still-growing) bucket
	num    int64    // Σ(price*qty) for the open bucket, for VWAP
	bucket int64    // open bucket index
	open   bool
}

// NewAggregator returns an aggregator for the given logical-TS width. width must
// be positive, else ErrBadWidth.
func NewAggregator(width int64) (*Aggregator, error) {
	if width <= 0 {
		return nil, ErrBadWidth
	}
	return &Aggregator{width: width, done: make([]Candle, 0)}, nil
}

// Add folds more ticks into the running candles. ticks are assumed non-decreasing
// in TS and to not precede the open bucket (tickstore's guarantee).
func (a *Aggregator) Add(ticks []tick.Tick) {
	for _, tk := range ticks {
		b := tk.TS / a.width // TS assumed non-negative; / truncates toward zero
		if !a.open || b != a.bucket {
			a.flush()
			a.bucket = b
			a.cur = Candle{Start: b * a.width, Open: tk.Price, High: tk.Price, Low: tk.Price}
			a.num = 0
			a.open = true
		}
		if tk.Price > a.cur.High {
			a.cur.High = tk.Price
		}
		if tk.Price < a.cur.Low {
			a.cur.Low = tk.Price
		}
		a.cur.Close = tk.Price
		a.cur.Volume += tk.Qty
		if tk.Side == tick.Sell {
			a.cur.SellVol += tk.Qty
		} else {
			a.cur.BuyVol += tk.Qty
		}
		a.cur.Trades++
		a.num += tk.Price * int64(tk.Qty)
	}
}

// flush finalizes the open bucket into done. It is only called on a bucket change,
// so the open bucket is complete and never revised afterward.
func (a *Aggregator) flush() {
	if !a.open {
		return
	}
	a.cur.VWAP = a.vwap()
	a.done = append(a.done, a.cur)
	a.open = false
}

func (a *Aggregator) vwap() int64 {
	if a.cur.Volume > 0 {
		return a.num / int64(a.cur.Volume)
	}
	return a.cur.VWAP
}

// Candles returns the candles so far as a fresh slice: the finalized buckets plus,
// if a bucket is open, a finalized copy of it. It does not mutate internal state, so
// it is safe to call repeatedly and the result never shares a backing array with a
// later call.
func (a *Aggregator) Candles() []Candle {
	out := make([]Candle, len(a.done), len(a.done)+1)
	copy(out, a.done)
	if a.open {
		c := a.cur
		c.VWAP = a.vwap()
		out = append(out, c)
	}
	return out
}
