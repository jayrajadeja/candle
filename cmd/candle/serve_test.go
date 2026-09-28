package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/jayrajadeja/candle/tick"
)

// writeLog writes a tickstore-format .log (8-byte header + 25-byte records).
func writeLog(t *testing.T, dir, symbol string, ticks []tick.Tick) {
	t.Helper()
	var h [tick.HeaderSize]byte
	copy(h[0:6], []byte("TCKLOG"))
	binary.LittleEndian.PutUint16(h[6:8], 1)
	buf := append([]byte{}, h[:]...)
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

// TestServeSmoke starts the real HTTP server on an ephemeral port, hits /healthz
// and /v1/candles, then cancels and confirms a clean shutdown.
func TestServeSmoke(t *testing.T) {
	dir := t.TempDir()
	writeLog(t, dir, "X", []tick.Tick{
		{TS: 0, Price: 100, Qty: 5, Side: tick.Buy},
		{TS: 1, Price: 102, Qty: 2, Side: tick.Sell},
		{TS: 2, Price: 101, Qty: 3, Side: tick.Buy},
	})

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan string, 1)
	errc := make(chan error, 1)
	go func() { errc <- runServe(dir, "127.0.0.1:0", ctx, ready) }()
	addr := <-ready
	base := "http://" + addr

	resp, err := http.Get(base + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || string(body) != "ok" {
		t.Fatalf("health = %d %q", resp.StatusCode, body)
	}

	resp, err = http.Get(base + "/v1/candles?symbol=X&width=10")
	if err != nil {
		t.Fatalf("GET /v1/candles: %v", err)
	}
	var got struct {
		Count   int `json:"count"`
		Candles []struct {
			Open   int64  `json:"open"`
			Close  int64  `json:"close"`
			Volume uint64 `json:"volume"`
		} `json:"candles"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if got.Count != 1 || got.Candles[0].Open != 100 || got.Candles[0].Close != 101 || got.Candles[0].Volume != 10 {
		t.Fatalf("candles response wrong: %+v", got)
	}

	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("runServe returned error: %v", err)
	}
}
