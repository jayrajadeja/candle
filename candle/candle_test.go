package candle

import (
	"errors"
	"testing"

	"github.com/jayrajadeja/candle/tick"
)

func TestAggregateEmpty(t *testing.T) {
	got, err := Aggregate(nil, 100)
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("got = %v, want non-nil empty slice", got)
	}
}

func TestAggregateBadWidth(t *testing.T) {
	for _, w := range []int64{0, -1, -100} {
		if _, err := Aggregate([]tick.Tick{{TS: 1, Price: 10, Qty: 1}}, w); !errors.Is(err, ErrBadWidth) {
			t.Fatalf("Aggregate width=%d err = %v, want ErrBadWidth", w, err)
		}
	}
}

func TestAggregateSingle(t *testing.T) {
	got, err := Aggregate([]tick.Tick{{TS: 5, Price: 100, Qty: 3, Side: tick.Buy}}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	c := got[0]
	want := Candle{Start: 0, Open: 100, High: 100, Low: 100, Close: 100, Volume: 3, VWAP: 100, Trades: 1, BuyVol: 3, SellVol: 0}
	if c != want {
		t.Fatalf("candle = %+v, want %+v", c, want)
	}
}

func TestAggregateMultiOneBucket(t *testing.T) {
	ticks := []tick.Tick{
		{TS: 0, Price: 100, Qty: 1, Side: tick.Buy},
		{TS: 10, Price: 120, Qty: 2, Side: tick.Sell},
		{TS: 20, Price: 90, Qty: 3, Side: tick.Buy},
	}
	got, err := Aggregate(ticks, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	c := got[0]
	// VWAP = (100*1 + 120*2 + 90*3) / (1+2+3) = (100+240+270)/6 = 610/6 = 101 (floor)
	want := Candle{Start: 0, Open: 100, High: 120, Low: 90, Close: 90, Volume: 6, VWAP: 101, Trades: 3, BuyVol: 4, SellVol: 2}
	if c != want {
		t.Fatalf("candle = %+v, want %+v", c, want)
	}
}

func TestAggregateBucketBoundary(t *testing.T) {
	ticks := []tick.Tick{
		{TS: 99, Price: 100, Qty: 1, Side: tick.Buy},
		{TS: 100, Price: 200, Qty: 1, Side: tick.Sell},
	}
	got, err := Aggregate(ticks, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Start != 0 || got[1].Start != 100 {
		t.Fatalf("starts = %d,%d, want 0,100", got[0].Start, got[1].Start)
	}
	if got[0].Close != 100 || got[1].Open != 200 {
		t.Fatalf("boundary split wrong: %+v %+v", got[0], got[1])
	}
}

func TestAggregateSparseNoGapFill(t *testing.T) {
	ticks := []tick.Tick{
		{TS: 0, Price: 100, Qty: 1, Side: tick.Buy},
		{TS: 500, Price: 200, Qty: 1, Side: tick.Buy},
	}
	got, err := Aggregate(ticks, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2 (no empty buckets 100..400)", len(got))
	}
	if got[0].Start != 0 || got[1].Start != 500 {
		t.Fatalf("starts = %d,%d, want 0,500", got[0].Start, got[1].Start)
	}
}

func TestAggregateBuySellSplit(t *testing.T) {
	ticks := []tick.Tick{
		{TS: 0, Price: 10, Qty: 4, Side: tick.Buy},
		{TS: 1, Price: 10, Qty: 6, Side: tick.Sell},
		{TS: 2, Price: 10, Qty: 5, Side: tick.Buy},
	}
	got, _ := Aggregate(ticks, 100)
	c := got[0]
	if c.BuyVol != 9 || c.SellVol != 6 || c.Volume != 15 {
		t.Fatalf("split: buy=%d sell=%d vol=%d, want 9/6/15", c.BuyVol, c.SellVol, c.Volume)
	}
	if c.BuyVol+c.SellVol != c.Volume {
		t.Fatalf("buy+sell != volume")
	}
}

func TestAggregateVWAPFloor(t *testing.T) {
	// (10*1 + 21*1) / 2 = 31/2 = 15 (floor, not 15.5)
	ticks := []tick.Tick{
		{TS: 0, Price: 10, Qty: 1, Side: tick.Buy},
		{TS: 1, Price: 21, Qty: 1, Side: tick.Buy},
	}
	got, _ := Aggregate(ticks, 100)
	if got[0].VWAP != 15 {
		t.Fatalf("VWAP = %d, want 15 (floored)", got[0].VWAP)
	}
}
