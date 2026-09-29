package candle

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"

	"github.com/jayrajadeja/candle/tick"
)

func genTicks(n int, width int64, seed int64) []tick.Tick {
	r := rand.New(rand.NewSource(seed))
	out := make([]tick.Tick, n)
	var ts int64
	for i := range out {
		ts += int64(r.Intn(int(width/2) + 1)) // non-decreasing, sometimes same bucket
		side := tick.Buy
		if r.Intn(2) == 1 {
			side = tick.Sell
		}
		out[i] = tick.Tick{TS: ts, Price: 100 + int64(r.Intn(50)), Qty: uint64(1 + r.Intn(5)), Side: side}
	}
	return out
}

func TestNewAggregatorBadWidth(t *testing.T) {
	for _, w := range []int64{0, -1, -100} {
		if _, err := NewAggregator(w); !errors.Is(err, ErrBadWidth) {
			t.Fatalf("NewAggregator(%d) err = %v, want ErrBadWidth", w, err)
		}
	}
}

func TestAggregatorEmpty(t *testing.T) {
	a, err := NewAggregator(100)
	if err != nil {
		t.Fatal(err)
	}
	got := a.Candles()
	if got == nil || len(got) != 0 {
		t.Fatalf("Candles() = %v, want non-nil empty", got)
	}
}

// One-shot Add must equal the pure Aggregate over the same ticks and width.
func TestAggregatorOneShotEqualsAggregate(t *testing.T) {
	for _, width := range []int64{1, 7, 100, 1000} {
		ticks := genTicks(500, width, width*3+1)
		want, err := Aggregate(ticks, width)
		if err != nil {
			t.Fatal(err)
		}
		a, err := NewAggregator(width)
		if err != nil {
			t.Fatal(err)
		}
		a.Add(ticks)
		if got := a.Candles(); !reflect.DeepEqual(got, want) {
			t.Fatalf("width=%d one-shot mismatch\n got=%v\nwant=%v", width, got, want)
		}
	}
}

// Splitting the stream across arbitrary Add boundaries must equal one-shot.
func TestAggregatorSplitEqualsOneShot(t *testing.T) {
	width := int64(100)
	ticks := genTicks(600, width, 42)
	want, _ := Aggregate(ticks, width)
	for _, splits := range [][]int{{0}, {1}, {300}, {1, 2, 599}, {100, 200, 300, 400, 500}} {
		a, _ := NewAggregator(width)
		prev := 0
		for _, s := range splits {
			if s < prev || s > len(ticks) {
				continue
			}
			a.Add(ticks[prev:s])
			prev = s
		}
		a.Add(ticks[prev:])
		if got := a.Candles(); !reflect.DeepEqual(got, want) {
			t.Fatalf("splits=%v mismatch\n got=%v\nwant=%v", splits, got, want)
		}
	}
}

// Candles() must be repeatable and must not mutate internal state.
func TestAggregatorCandlesRepeatableNonMutating(t *testing.T) {
	width := int64(50)
	ticks := genTicks(200, width, 7)
	a, _ := NewAggregator(width)
	a.Add(ticks)
	first := a.Candles()
	firstCopy := append([]Candle(nil), first...)
	second := a.Candles()
	if !reflect.DeepEqual(firstCopy, second) {
		t.Fatalf("Candles() not repeatable")
	}
	// mutating the returned slice must not affect a later call
	if len(first) > 0 {
		first[0].Close = -999
	}
	third := a.Candles()
	if !reflect.DeepEqual(firstCopy, third) {
		t.Fatalf("Candles() shares backing array with caller")
	}
}

// Adding more ticks after Candles() finalizes correctly (open bucket keeps growing).
func TestAggregatorOpenBucketContinues(t *testing.T) {
	width := int64(100)
	a, _ := NewAggregator(width)
	a.Add([]tick.Tick{{TS: 10, Price: 100, Qty: 1, Side: tick.Buy}})
	if c := a.Candles(); len(c) != 1 || c[0].Close != 100 || c[0].Volume != 1 {
		t.Fatalf("after first add: %+v", c)
	}
	a.Add([]tick.Tick{{TS: 20, Price: 200, Qty: 2, Side: tick.Sell}}) // same bucket
	c := a.Candles()
	if len(c) != 1 {
		t.Fatalf("len=%d want 1", len(c))
	}
	want, _ := Aggregate([]tick.Tick{
		{TS: 10, Price: 100, Qty: 1, Side: tick.Buy},
		{TS: 20, Price: 200, Qty: 2, Side: tick.Sell},
	}, width)
	if !reflect.DeepEqual(c, want) {
		t.Fatalf("open-bucket continue mismatch\n got=%v\nwant=%v", c, want)
	}
}
