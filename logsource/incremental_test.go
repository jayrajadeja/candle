package logsource

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jayrajadeja/candle/candle"
	"github.com/jayrajadeja/candle/tick"
)

// appendRecords appends raw 25-byte tick records (no header) to an existing log,
// simulating tickstore's append-only growth.
func appendRecords(tb testing.TB, dir, symbol string, ticks []tick.Tick) {
	tb.Helper()
	f, err := os.OpenFile(filepath.Join(dir, symbol+".log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		tb.Fatalf("append open: %v", err)
	}
	defer f.Close()
	buf := make([]byte, 0, len(ticks)*tick.RecordSize)
	for _, tk := range ticks {
		var b [tick.RecordSize]byte
		binary.LittleEndian.PutUint64(b[0:8], uint64(tk.TS))
		binary.LittleEndian.PutUint64(b[8:16], uint64(tk.Price))
		binary.LittleEndian.PutUint64(b[16:24], tk.Qty)
		b[24] = byte(tk.Side)
		buf = append(buf, b[:]...)
	}
	if _, err := f.Write(buf); err != nil {
		tb.Fatalf("append write: %v", err)
	}
}

// incParity checks IncrementalCache against LogSource (the oracle) for one query.
func incParity(t *testing.T, c *IncrementalCache, dir, symbol string, width, from, to int64) {
	t.Helper()
	got, gErr := c.Candles(symbol, width, from, to)
	want, wErr := New(dir).Candles(symbol, width, from, to)
	if (gErr == nil) != (wErr == nil) {
		t.Fatalf("error mismatch w=%d [%d,%d]: inc=%v logsource=%v", width, from, to, gErr, wErr)
	}
	if gErr != nil {
		return
	}
	if !candlesEqual(got, want) {
		t.Fatalf("parity mismatch w=%d [%d,%d]: inc=%d candles, logsource=%d", width, from, to, len(got), len(want))
	}
}

func incCases() []struct{ width, from, to int64 } {
	return []struct{ width, from, to int64 }{
		{1, minInt64, maxInt64},      // full range, width 1
		{20, minInt64, maxInt64},     // full range
		{20, 100, 299},               // bucket-aligned window (fast path)
		{20, minInt64, 299},          // left-unbounded, right-aligned
		{20, 100, maxInt64},          // left-aligned, right-unbounded
		{20, 105, 287},               // non-aligned window (fallback)
		{100000, minInt64, maxInt64}, // one giant bucket
		{20, 300, 100},               // from > to -> empty
		{20, 500000, 900000},         // past the data -> empty
	}
}

func TestIncrementalParityWithLogSource(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "SYNTH", seedTicks(1000))
	c := NewIncremental(dir)
	for _, tc := range incCases() {
		incParity(t, c, dir, "SYNTH", tc.width, tc.from, tc.to)
	}
	incParity(t, c, dir, "NOPE", 20, minInt64, maxInt64) // unknown symbol
}

func TestIncrementalBadWidth(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "SYNTH", seedTicks(10))
	c := NewIncremental(dir)
	if _, err := c.Candles("SYNTH", 0, minInt64, maxInt64); err != candle.ErrBadWidth {
		t.Fatalf("width 0: got %v, want ErrBadWidth", err)
	}
	if _, err := c.Candles("SYNTH", -5, minInt64, maxInt64); err != candle.ErrBadWidth {
		t.Fatalf("width -5: got %v, want ErrBadWidth", err)
	}
}

func TestIncrementalMissingSymbolIsEmptyNotError(t *testing.T) {
	got, err := NewIncremental(t.TempDir()).Candles("GONE", 20, minInt64, maxInt64)
	if err != nil {
		t.Fatalf("missing symbol: unexpected error %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("missing symbol: want empty, got %d", len(got))
	}
}

// Appending (delta read) must keep parity with a cold LogSource over the grown file,
// across many appends and after new widths are first seen mid-stream.
func TestIncrementalAppendDeltaParity(t *testing.T) {
	dir := t.TempDir()
	all := seedTicks(2000)
	writeLog(t, dir, "SYNTH", all[:200])
	c := NewIncremental(dir)

	// Prime width 20 (and 1) so the appends flow through the incremental path.
	incParity(t, c, dir, "SYNTH", 20, minInt64, maxInt64)
	incParity(t, c, dir, "SYNTH", 1, minInt64, maxInt64)

	next := 200
	for _, batch := range []int{1, 50, 300, 700, 749} {
		appendRecords(t, dir, "SYNTH", all[next:next+batch])
		next += batch
		incParity(t, c, dir, "SYNTH", 20, minInt64, maxInt64)
		incParity(t, c, dir, "SYNTH", 1, minInt64, maxInt64)
		incParity(t, c, dir, "SYNTH", 20, 100, 299) // aligned window over growing data
		// A width first seen only now must still match (built from all resident ticks).
		incParity(t, c, dir, "SYNTH", 7, minInt64, maxInt64)
	}
}

// A shrink/replace (non-append growth or truncation) must rebuild and stay correct.
func TestIncrementalRebuildsOnReplace(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "SYNTH", seedTicks(400))
	c := NewIncremental(dir)
	first, err := c.Candles("SYNTH", 1000, minInt64, maxInt64)
	if err != nil {
		t.Fatal(err)
	}
	// Replace with a smaller, different log (shrink -> rebuild).
	writeLog(t, dir, "SYNTH", seedTicks(50))
	incParity(t, c, dir, "SYNTH", 1000, minInt64, maxInt64)
	second, err := c.Candles("SYNTH", 1000, minInt64, maxInt64)
	if err != nil {
		t.Fatal(err)
	}
	if candlesEqual(first, second) {
		t.Fatal("cache did not reflect the replaced (smaller) log")
	}
}

func TestIncrementalConcurrentReadsMatchLogSource(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "SYNTH", seedTicks(2000))
	c := NewIncremental(dir)
	want, err := New(dir).Candles("SYNTH", 20, minInt64, maxInt64)
	if err != nil {
		t.Fatal(err)
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
					got, err := c.Candles("SYNTH", 20, minInt64, maxInt64)
					if err != nil {
						errs <- err
						return
					}
					if !candlesEqual(got, want) {
						errs <- errMismatch
						return
					}
				} else if _, err := c.Candles("SYNTH", 20, int64(g*10), int64(g*10+499)); err != nil {
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
