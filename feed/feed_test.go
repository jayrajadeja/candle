package feed

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/jayrajadeja/candle/tick"
)

func rec(tk tick.Tick) []byte {
	var b [tick.RecordSize]byte
	binary.LittleEndian.PutUint64(b[0:8], uint64(tk.TS))
	binary.LittleEndian.PutUint64(b[8:16], uint64(tk.Price))
	binary.LittleEndian.PutUint64(b[16:24], tk.Qty)
	b[24] = byte(tk.Side)
	return b[:]
}

func header() []byte {
	var h [tick.HeaderSize]byte
	copy(h[0:6], []byte("TCKLOG"))
	binary.LittleEndian.PutUint16(h[6:8], 1)
	return h[:]
}

func TestReadTicksStream(t *testing.T) {
	in := append(rec(tick.Tick{TS: 1, Price: 10, Qty: 2, Side: tick.Buy}),
		rec(tick.Tick{TS: 2, Price: 11, Qty: 3, Side: tick.Sell})...)
	got, err := ReadTicks(bytes.NewReader(in), false)
	if err != nil {
		t.Fatalf("ReadTicks: %v", err)
	}
	if len(got) != 2 || got[0].TS != 1 || got[1].Price != 11 {
		t.Fatalf("got %+v", got)
	}
}

func TestReadTicksLogModeStripsHeader(t *testing.T) {
	in := append(header(), rec(tick.Tick{TS: 5, Price: 100, Qty: 1, Side: tick.Buy})...)
	got, err := ReadTicks(bytes.NewReader(in), true)
	if err != nil {
		t.Fatalf("ReadTicks log: %v", err)
	}
	if len(got) != 1 || got[0].TS != 5 {
		t.Fatalf("got %+v", got)
	}
}

func TestReadTicksEmpty(t *testing.T) {
	got, err := ReadTicks(bytes.NewReader(nil), false)
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("want empty, got %+v", got)
	}
}

func TestReadTicksTruncatedRecordErrors(t *testing.T) {
	full := rec(tick.Tick{TS: 1, Price: 10, Qty: 1, Side: tick.Buy})
	_, err := ReadTicks(bytes.NewReader(full[:len(full)-3]), false)
	if err == nil {
		t.Fatal("want truncated-record error, got nil")
	}
}

func TestReadTicksBadHeaderErrors(t *testing.T) {
	bad := make([]byte, tick.HeaderSize)
	copy(bad, []byte("XXXXXX"))
	_, err := ReadTicks(bytes.NewReader(bad), true)
	if err == nil {
		t.Fatal("want bad-header error, got nil")
	}
}
