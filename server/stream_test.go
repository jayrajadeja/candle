package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jayrajadeja/candle/candle"
	"github.com/jayrajadeja/candle/logsource"
	"github.com/jayrajadeja/candle/tick"
)

// growingSource is a candle.Source whose returned series can be mutated between polls.
// Candles returns a copy so the handler's encode never races the test's mutation.
type growingSource struct {
	mu      sync.Mutex
	candles []candle.Candle
}

func (g *growingSource) set(cs []candle.Candle) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.candles = append([]candle.Candle(nil), cs...)
}

func (g *growingSource) final() []candle.Candle {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]candle.Candle(nil), g.candles...)
}

func (g *growingSource) Candles(symbol string, width, from, to int64) ([]candle.Candle, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]candle.Candle(nil), g.candles...), nil
}

// sseReader consumes an SSE body, upserting each `data:` candle by Start and ignoring
// `:` heartbeat comments. It stops when the body closes.
type sseReader struct {
	mu    sync.Mutex
	byKey map[int64]candleDTO
}

func newSSEReader() *sseReader { return &sseReader{byKey: map[int64]candleDTO{}} }

func (s *sseReader) consume(body *bufio.Scanner) {
	for body.Scan() {
		line := body.Text()
		if !strings.HasPrefix(line, "data:") {
			continue // event:/heartbeat/blank lines
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		var dto candleDTO
		if err := json.Unmarshal([]byte(payload), &dto); err != nil {
			continue // e.g. an error event's payload
		}
		s.mu.Lock()
		s.byKey[dto.Start] = dto
		s.mu.Unlock()
	}
}

func (s *sseReader) sorted() []candleDTO {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]candleDTO, 0, len(s.byKey))
	for _, d := range s.byKey {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Start < out[j].Start })
	return out
}

// streamAndCollect connects to the stream endpoint and collects upserted candles until
// the returned cancel is called and the body drains. It returns the reader and a
// function that cancels and waits for the read goroutine.
func streamAndCollect(t *testing.T, url string) (*sseReader, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stream status = %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("content-type = %q", ct)
	}
	r := newSSEReader()
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.consume(bufio.NewScanner(resp.Body))
	}()
	return r, func() {
		cancel()
		resp.Body.Close()
		<-done
	}
}

func dtosOf(cs []candle.Candle) []candleDTO { return toDTOs(cs) }

func TestStreamUpsertInvariant(t *testing.T) {
	src := &growingSource{}
	src.set([]candle.Candle{{Start: 0, Open: 10, High: 12, Low: 9, Close: 11, Volume: 5, VWAP: 10, Trades: 3, BuyVol: 3, SellVol: 2}})

	srv := httptest.NewServer(newStreamHandler(src, 5*time.Millisecond))
	defer srv.Close()

	r, stop := streamAndCollect(t, srv.URL+"/v1/stream?symbol=X&width=100")

	time.Sleep(40 * time.Millisecond) // initial snapshot delivered

	// Mutate the open bucket (must replace, not duplicate) and append a new bucket.
	src.set([]candle.Candle{
		{Start: 0, Open: 10, High: 15, Low: 9, Close: 14, Volume: 9, VWAP: 11, Trades: 5, BuyVol: 6, SellVol: 3},
		{Start: 100, Open: 14, High: 14, Low: 13, Close: 13, Volume: 2, VWAP: 13, Trades: 1, BuyVol: 0, SellVol: 2},
	})
	time.Sleep(40 * time.Millisecond)

	// Append another bucket.
	final := []candle.Candle{
		{Start: 0, Open: 10, High: 15, Low: 9, Close: 14, Volume: 9, VWAP: 11, Trades: 5, BuyVol: 6, SellVol: 3},
		{Start: 100, Open: 14, High: 14, Low: 13, Close: 13, Volume: 2, VWAP: 13, Trades: 1, BuyVol: 0, SellVol: 2},
		{Start: 200, Open: 13, High: 20, Low: 13, Close: 19, Volume: 7, VWAP: 16, Trades: 4, BuyVol: 5, SellVol: 2},
	}
	src.set(final)
	time.Sleep(40 * time.Millisecond)

	stop()

	got := r.sorted()
	want := dtosOf(final)
	if len(got) != len(want) {
		t.Fatalf("upserted %d candles, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candle %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestStreamValidation(t *testing.T) {
	h := Handler(&fakeSource{})
	for _, target := range []string{
		"/v1/stream",                    // missing symbol
		"/v1/stream?symbol=X",           // missing width
		"/v1/stream?symbol=X&width=0",   // non-positive width
		"/v1/stream?symbol=X&width=-3",  // negative width
		"/v1/stream?symbol=X&width=abc", // non-integer width
	} {
		rec := do(t, h, "GET", target)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s -> %d, want 400", target, rec.Code)
		}
	}
}

func TestStreamDisconnectReturns(t *testing.T) {
	src := &growingSource{}
	src.set([]candle.Candle{{Start: 0, Open: 1, High: 1, Low: 1, Close: 1, Volume: 1, VWAP: 1, Trades: 1, BuyVol: 1}})
	srv := httptest.NewServer(newStreamHandler(src, 5*time.Millisecond))
	defer srv.Close()

	_, stop := streamAndCollect(t, srv.URL+"/v1/stream?symbol=X&width=10")
	time.Sleep(20 * time.Millisecond)
	doneAt := make(chan struct{})
	go func() { stop(); close(doneAt) }()
	select {
	case <-doneAt:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return promptly after client disconnect")
	}
}

// --- end-to-end over a real IncrementalCache ------------------------------------

func writeStreamLog(t *testing.T, dir, symbol string, ticks []tick.Tick) {
	t.Helper()
	var buf []byte
	var h [tick.HeaderSize]byte
	copy(h[0:6], []byte("TCKLOG"))
	binary.LittleEndian.PutUint16(h[6:8], 1)
	buf = append(buf, h[:]...)
	buf = append(buf, encodeRecords(ticks)...)
	if err := os.WriteFile(filepath.Join(dir, symbol+".log"), buf, 0o644); err != nil {
		t.Fatal(err)
	}
}

func appendStreamRecords(t *testing.T, dir, symbol string, ticks []tick.Tick) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(dir, symbol+".log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(encodeRecords(ticks)); err != nil {
		t.Fatal(err)
	}
}

func encodeRecords(ticks []tick.Tick) []byte {
	buf := make([]byte, 0, len(ticks)*tick.RecordSize)
	for _, tk := range ticks {
		var b [tick.RecordSize]byte
		binary.LittleEndian.PutUint64(b[0:8], uint64(tk.TS))
		binary.LittleEndian.PutUint64(b[8:16], uint64(tk.Price))
		binary.LittleEndian.PutUint64(b[16:24], tk.Qty)
		b[24] = byte(tk.Side)
		buf = append(buf, b[:]...)
	}
	return buf
}

func streamTicks(n, base int) []tick.Tick {
	out := make([]tick.Tick, n)
	ts := int64(base)
	for i := 0; i < n; i++ {
		ts += int64(i%3) + 1
		out[i] = tick.Tick{TS: ts, Price: 100 + int64((base+i)%20), Qty: uint64(i%4) + 1, Side: tick.Side(i % 2)}
	}
	return out
}

func TestStreamEndToEndMatchesCandles(t *testing.T) {
	dir := t.TempDir()
	writeStreamLog(t, dir, "SYNTH", streamTicks(300, 0))
	src := logsource.NewIncremental(dir)

	srv := httptest.NewServer(Handler(src)) // default interval; wire route + read API
	defer srv.Close()
	// Drive the stream through a fast handler for the test by using newStreamHandler too:
	fast := httptest.NewServer(newStreamHandler(src, 5*time.Millisecond))
	defer fast.Close()

	r, stop := streamAndCollect(t, fast.URL+"/v1/stream?symbol=SYNTH&width=20")
	time.Sleep(40 * time.Millisecond)

	next := int64(300 * 4) // beyond the seeded TS range
	for _, batch := range []int{10, 40, 120} {
		appendStreamRecords(t, dir, "SYNTH", streamTicks(batch, int(next)))
		next += int64(batch) * 4
		time.Sleep(40 * time.Millisecond)
	}
	time.Sleep(40 * time.Millisecond)
	stop()

	// Oracle: a full-range /v1/candles snapshot after all appends.
	resp, err := http.Get(srv.URL + "/v1/candles?symbol=SYNTH&width=20")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var oracle candlesResponse
	if err := json.NewDecoder(resp.Body).Decode(&oracle); err != nil {
		t.Fatal(err)
	}

	got := r.sorted()
	if len(got) != len(oracle.Candles) {
		t.Fatalf("stream upserted %d candles, oracle has %d", len(got), len(oracle.Candles))
	}
	for i := range oracle.Candles {
		if got[i] != oracle.Candles[i] {
			t.Fatalf("candle %d: stream %+v, oracle %+v", i, got[i], oracle.Candles[i])
		}
	}
}
