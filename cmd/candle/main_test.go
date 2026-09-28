package main

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/jayrajadeja/candle/tick"
)

// encodeRecord builds one 25-byte record (test-only producer).
func encodeRecord(tk tick.Tick) []byte {
	var b [tick.RecordSize]byte
	binary.LittleEndian.PutUint64(b[0:8], uint64(tk.TS))
	binary.LittleEndian.PutUint64(b[8:16], uint64(tk.Price))
	binary.LittleEndian.PutUint64(b[16:24], tk.Qty)
	b[24] = byte(tk.Side)
	return b[:]
}

func stream(ticks ...tick.Tick) []byte {
	var buf bytes.Buffer
	for _, tk := range ticks {
		buf.Write(encodeRecord(tk))
	}
	return buf.Bytes()
}

func TestRunCSVGolden(t *testing.T) {
	in := stream(
		tick.Tick{TS: 1, Price: 100, Qty: 5, Side: tick.Buy},
		tick.Tick{TS: 2, Price: 101, Qty: 2, Side: tick.Sell},
	)
	var out, errb bytes.Buffer
	code := run([]string{"--width", "100", "--csv"}, bytes.NewReader(in), &out, &errb)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, errb.String())
	}
	// Both TS 1,2 fall in bucket 0. VWAP = (100*5 + 101*2)/7 = 702/7 = 100.
	want := "start,open,high,low,close,volume,vwap,trades,buy_vol,sell_vol\n" +
		"0,100,101,100,101,7,100,2,5,2\n"
	if out.String() != want {
		t.Fatalf("csv:\n%q\nwant:\n%q", out.String(), want)
	}
}

func TestRunTableContains(t *testing.T) {
	in := stream(tick.Tick{TS: 0, Price: 50, Qty: 3, Side: tick.Buy})
	var out, errb bytes.Buffer
	code := run([]string{"--width", "10"}, bytes.NewReader(in), &out, &errb)
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	for _, tok := range []string{"start", "open", "vwap", "sellVol", "50", "3"} {
		if !strings.Contains(out.String(), tok) {
			t.Fatalf("table missing %q:\n%s", tok, out.String())
		}
	}
}

func TestRunLogMode(t *testing.T) {
	header := append([]byte("TCKLOG"), 0x01, 0x00)
	in := append(header, stream(tick.Tick{TS: 0, Price: 10, Qty: 1, Side: tick.Buy})...)
	var out, errb bytes.Buffer
	code := run([]string{"--width", "10", "--csv", "--log"}, bytes.NewReader(in), &out, &errb)
	if code != 0 {
		t.Fatalf("exit = %d, stderr=%s", code, errb.String())
	}
	if !strings.Contains(out.String(), "0,10,10,10,10,1,10,1,1,0") {
		t.Fatalf("log-mode csv wrong:\n%s", out.String())
	}
}

func TestRunBadMagic(t *testing.T) {
	in := append([]byte("XXXXXX"), 0x01, 0x00)
	var out, errb bytes.Buffer
	code := run([]string{"--width", "10", "--log"}, bytes.NewReader(in), &out, &errb)
	if code == 0 {
		t.Fatalf("expected non-zero exit on bad magic")
	}
}

func TestRunTruncated(t *testing.T) {
	in := stream(tick.Tick{TS: 0, Price: 10, Qty: 1, Side: tick.Buy})
	in = append(in, 0x01, 0x02, 0x03) // 3 stray bytes
	var out, errb bytes.Buffer
	code := run([]string{"--width", "10"}, bytes.NewReader(in), &out, &errb)
	if code == 0 {
		t.Fatalf("expected non-zero exit on truncated stream")
	}
	if !strings.Contains(errb.String(), "truncated") {
		t.Fatalf("stderr = %q, want 'truncated'", errb.String())
	}
}

func TestRunBadWidth(t *testing.T) {
	var out, errb bytes.Buffer
	if code := run([]string{"--width", "0"}, bytes.NewReader(nil), &out, &errb); code != 2 {
		t.Fatalf("width 0 exit = %d, want 2", code)
	}
	if code := run([]string{}, bytes.NewReader(nil), &out, &errb); code != 2 {
		t.Fatalf("missing width exit = %d, want 2", code)
	}
}

func TestRunEmpty(t *testing.T) {
	var out, errb bytes.Buffer
	code := run([]string{"--width", "10", "--csv"}, bytes.NewReader(nil), &out, &errb)
	if code != 0 {
		t.Fatalf("empty exit = %d", code)
	}
	// header only, no data rows
	if strings.Count(out.String(), "\n") != 1 {
		t.Fatalf("empty csv should be header only:\n%s", out.String())
	}
}
