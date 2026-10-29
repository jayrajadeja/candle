package logsource

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/jayrajadeja/candle/candle"
	"github.com/jayrajadeja/candle/feed"
	"github.com/jayrajadeja/candle/tick"
)

// writeLog writes a tickstore-format .log (8-byte header + 25-byte records) for
// symbol under dir. It accepts testing.TB so tests and benchmarks can share it.
func writeLog(tb testing.TB, dir, symbol string, ticks []tick.Tick) {
	tb.Helper()
	var buf []byte
	var h [tick.HeaderSize]byte
	copy(h[0:6], []byte("TCKLOG"))
	binary.LittleEndian.PutUint16(h[6:8], 1)
	buf = append(buf, h[:]...)
	for _, tk := range ticks {
		var b [tick.RecordSize]byte
		binary.LittleEndian.PutUint64(b[0:8], uint64(tk.TS))
		binary.LittleEndian.PutUint64(b[8:16], uint64(tk.Price))
		binary.LittleEndian.PutUint64(b[16:24], tk.Qty)
		b[24] = byte(tk.Side)
		buf = append(buf, b[:]...)
	}
	if err := os.WriteFile(filepath.Join(dir, symbol+".log"), buf, 0o644); err != nil {
		tb.Fatalf("writeLog: %v", err)
	}
}

// recordBytes encodes one tick as a raw 25-byte record (no header), matching
// writeLog's record encoding.
func recordBytes(tk tick.Tick) []byte {
	var b [tick.RecordSize]byte
	binary.LittleEndian.PutUint64(b[0:8], uint64(tk.TS))
	binary.LittleEndian.PutUint64(b[8:16], uint64(tk.Price))
	binary.LittleEndian.PutUint64(b[16:24], tk.Qty)
	b[24] = byte(tk.Side)
	return b[:]
}

// appendRaw appends arbitrary bytes (e.g. a partial record) to an existing log
// file, simulating a bufio flush that split a record across two writes.
func appendRaw(tb testing.TB, dir, symbol string, b []byte) {
	tb.Helper()
	f, err := os.OpenFile(filepath.Join(dir, symbol+".log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		tb.Fatalf("appendRaw open: %v", err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		tb.Fatalf("appendRaw write: %v", err)
	}
}

// strictTicks returns n ticks with strictly increasing TS starting at from, so
// each tick lands in its own width-1 candle: an exact per-tick count/volume
// check for the partial-tail tests.
func strictTicks(n int, from int64) []tick.Tick {
	out := make([]tick.Tick, n)
	for i := 0; i < n; i++ {
		out[i] = tick.Tick{TS: from + int64(i), Price: 100 + int64(i), Qty: uint64(i%5) + 1, Side: tick.Side(i % 2)}
	}
	return out
}

// totalVolume sums candle Volume across cs.
func totalVolume(cs []candle.Candle) uint64 {
	var sum uint64
	for _, c := range cs {
		sum += c.Volume
	}
	return sum
}

// totalQty sums tick Qty across ticks.
func totalQty(ticks []tick.Tick) uint64 {
	var sum uint64
	for _, tk := range ticks {
		sum += tk.Qty
	}
	return sum
}

func seedTicks(n int) []tick.Tick {
	out := make([]tick.Tick, n)
	ts := int64(0)
	for i := 0; i < n; i++ {
		if i%3 != 0 {
			ts++
		}
		out[i] = tick.Tick{TS: ts, Price: 100 + int64(i%20), Qty: uint64(i%5) + 1, Side: tick.Side(i % 2)}
	}
	return out
}

func candlesEqual(a, b []candle.Candle) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// wantCandles is the reference: read all ticks, keep from<=TS<=to, aggregate.
func wantCandles(t *testing.T, dir, symbol string, width, from, to int64) []candle.Candle {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, symbol+".log"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("open: %v", err)
	}
	defer f.Close()
	all, err := feed.ReadTicks(f, true)
	if err != nil {
		t.Fatalf("ReadTicks: %v", err)
	}
	kept := all[:0]
	for _, tk := range all {
		if tk.TS >= from && tk.TS <= to {
			kept = append(kept, tk)
		}
	}
	cs, err := candle.Aggregate(kept, width)
	if err != nil {
		t.Fatalf("Aggregate: %v", err)
	}
	return cs
}

func TestLogSourceParity(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "SYNTH", seedTicks(1000))
	src := New(dir)

	cases := []struct {
		width, from, to int64
	}{
		{10, 0, 1 << 62},
		{20, 100, 300},
		{5, 0, 50},
		{50, 200, 200},
		{7, 300, 100}, // from > to => empty
	}
	for _, c := range cases {
		want := wantCandles(t, dir, "SYNTH", c.width, c.from, c.to)
		got, err := src.Candles("SYNTH", c.width, c.from, c.to)
		if err != nil {
			t.Fatalf("Candles(%d,%d,%d): %v", c.width, c.from, c.to, err)
		}
		if !candlesEqual(want, got) {
			t.Fatalf("Candles(%d,%d,%d) mismatch: want %d candles, got %d", c.width, c.from, c.to, len(want), len(got))
		}
	}
}

func TestLogSourceMissingSymbolEmpty(t *testing.T) {
	src := New(t.TempDir())
	got, err := src.Candles("NOPE", 10, 0, 100)
	if err != nil {
		t.Fatalf("missing: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %+v", got)
	}
}

func TestLogSourceBadWidth(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "S", seedTicks(10))
	_, err := New(dir).Candles("S", 0, 0, 100)
	if err != candle.ErrBadWidth {
		t.Fatalf("want ErrBadWidth, got %v", err)
	}
}
