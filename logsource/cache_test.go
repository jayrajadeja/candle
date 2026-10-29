package logsource

import (
	"errors"
	"sync"
	"testing"

	"github.com/jayrajadeja/candle/candle"
	"github.com/jayrajadeja/candle/tick"
)

const (
	minTS = int64(-1) << 62
	maxTS = int64(1) << 62
)

var errMismatch = errors.New("cache result did not match logsource")

// assertParity checks Cache and LogSource agree for one query over dir.
func assertParity(t *testing.T, dir, symbol string, width, from, to int64) {
	t.Helper()
	got, gErr := NewCached(dir).Candles(symbol, width, from, to)
	want, wErr := New(dir).Candles(symbol, width, from, to)
	if (gErr == nil) != (wErr == nil) {
		t.Fatalf("error mismatch w=%d [%d,%d]: cache=%v logsource=%v", width, from, to, gErr, wErr)
	}
	if gErr != nil {
		return
	}
	if !candlesEqual(got, want) {
		t.Fatalf("parity mismatch w=%d [%d,%d]: cache=%d candles, logsource=%d", width, from, to, len(got), len(want))
	}
}

func TestCacheParityWithLogSource(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "SYNTH", seedTicks(1000))
	cases := []struct {
		width, from, to int64
	}{
		{1, minTS, maxTS},
		{20, minTS, maxTS},
		{20, 100, 300},
		{100000, minTS, maxTS},
		{20, 300, 100},    // from > to -> empty
		{20, 500000, 9e5}, // window past the data -> empty
	}
	for _, tc := range cases {
		assertParity(t, dir, "SYNTH", tc.width, tc.from, tc.to)
	}
	assertParity(t, dir, "NOPE", 20, minTS, maxTS) // unknown symbol
}

func TestCacheBadWidth(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "SYNTH", seedTicks(10))
	c := NewCached(dir)
	if _, err := c.Candles("SYNTH", 0, minTS, maxTS); err != candle.ErrBadWidth {
		t.Fatalf("width 0: got %v, want ErrBadWidth", err)
	}
	if _, err := c.Candles("SYNTH", -5, minTS, maxTS); err != candle.ErrBadWidth {
		t.Fatalf("width -5: got %v, want ErrBadWidth", err)
	}
}

func TestCacheMissingSymbolIsEmptyNotError(t *testing.T) {
	got, err := NewCached(t.TempDir()).Candles("GONE", 20, minTS, maxTS)
	if err != nil {
		t.Fatalf("missing symbol: unexpected error %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("missing symbol: want empty, got %d", len(got))
	}
}

func TestCacheInvalidatesOnRewrite(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "SYNTH", seedTicks(100))
	c := NewCached(dir)

	first, err := c.Candles("SYNTH", 1000, minTS, maxTS)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	// Rewrite with more ticks: byte size changes, so the cache must reload.
	writeLog(t, dir, "SYNTH", seedTicks(400))

	second, err := c.Candles("SYNTH", 1000, minTS, maxTS)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	want, err := New(dir).Candles("SYNTH", 1000, minTS, maxTS)
	if err != nil {
		t.Fatalf("oracle: %v", err)
	}
	if !candlesEqual(second, want) {
		t.Fatalf("after rewrite: cache and logsource disagree")
	}
	if candlesEqual(first, second) {
		t.Fatalf("cache did not reflect rewritten data")
	}
}

// A trailing partial record must be tolerated (served as "not yet written"),
// and growing the file past the completed tail must invalidate the cached
// entry so the next load sees every record, with no duplicates or gaps.
func TestCacheTrailingPartialRecordTolerated(t *testing.T) {
	dir := t.TempDir()
	full := strictTicks(5, 0) // N=5
	writeLog(t, dir, "SYNTH", full)

	next := tick.Tick{TS: 5, Price: 105, Qty: 2, Side: tick.Buy}
	nextBytes := recordBytes(next)
	appendRaw(t, dir, "SYNTH", nextBytes[:10]) // partial tail: 10 of 25 bytes

	c := NewCached(dir)
	got, err := c.Candles("SYNTH", 1, minTS, maxTS)
	if err != nil {
		t.Fatalf("partial tail: unexpected error %v", err)
	}
	if len(got) != len(full) || totalVolume(got) != totalQty(full) {
		t.Fatalf("partial tail: want %d candles vol %d, got %d candles vol %d",
			len(full), totalQty(full), len(got), totalVolume(got))
	}

	// Complete the partial record, then append one more full record.
	appendRaw(t, dir, "SYNTH", nextBytes[10:])
	more := tick.Tick{TS: 6, Price: 106, Qty: 3, Side: tick.Sell}
	appendRaw(t, dir, "SYNTH", recordBytes(more))

	want := append(append([]tick.Tick{}, full...), next, more)
	got2, err := c.Candles("SYNTH", 1, minTS, maxTS)
	if err != nil {
		t.Fatalf("after completion: unexpected error %v", err)
	}
	if len(got2) != len(want) || totalVolume(got2) != totalQty(want) {
		t.Fatalf("after completion: want %d candles vol %d (no dup/skip), got %d candles vol %d",
			len(want), totalQty(want), len(got2), totalVolume(got2))
	}
}

func TestCacheConcurrentReadsMatchLogSource(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "SYNTH", seedTicks(2000))
	c := NewCached(dir)
	want, err := New(dir).Candles("SYNTH", 20, minTS, maxTS)
	if err != nil {
		t.Fatalf("oracle: %v", err)
	}

	const goroutines = 32
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if i%2 == 0 {
					got, err := c.Candles("SYNTH", 20, minTS, maxTS)
					if err != nil {
						errs <- err
						return
					}
					if !candlesEqual(got, want) {
						errs <- errMismatch
						return
					}
				} else if _, err := c.Candles("SYNTH", 20, int64(g*10), int64(g*10+500)); err != nil {
					errs <- err
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent: %v", err)
	}
}
