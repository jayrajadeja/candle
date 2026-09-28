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
// symbol under dir.
func writeLog(t *testing.T, dir, symbol string, ticks []tick.Tick) {
	t.Helper()
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
		t.Fatalf("writeLog: %v", err)
	}
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
