// Package tick defines the read side of the fixed-width binary tick record —
// the 25-byte format produced by lob's `replay --emit` and persisted by
// tickstore, plus tickstore's 8-byte log header. candle re-declares this
// contract rather than importing either project: the three tools share only
// the byte layout, never Go code. Golden-byte tests guard against drift.
package tick

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// Side is the aggressor side of a trade.
type Side uint8

const (
	Buy  Side = 0
	Sell Side = 1
)

// String renders a Side for human/CSV output.
func (s Side) String() string {
	if s == Sell {
		return "sell"
	}
	return "buy"
}

// RecordSize is the fixed size of one encoded tick record, in bytes.
const RecordSize = 25

// HeaderSize is the size of tickstore's per-log header, in bytes.
const HeaderSize = 8

// logMagic is the first 6 bytes of a tickstore log header.
var logMagic = []byte("TCKLOG")

// logVersion is the only tickstore log version candle v1 understands.
const logVersion = uint16(1)

// Sentinel errors.
var (
	ErrBadSize    = errors.New("tick: buffer length invalid")
	ErrBadMagic   = errors.New("tick: bad log magic")
	ErrBadVersion = errors.New("tick: unsupported log version")
)

// Tick is a single execution. TS is a logical (non-decreasing) sequence
// timestamp; Price is in integer ticks.
type Tick struct {
	TS    int64
	Price int64
	Qty   uint64
	Side  Side
}

// Decode parses one little-endian record. len(buf) must equal RecordSize.
func Decode(buf []byte) (Tick, error) {
	if len(buf) != RecordSize {
		return Tick{}, ErrBadSize
	}
	return Tick{
		TS:    int64(binary.LittleEndian.Uint64(buf[0:8])),
		Price: int64(binary.LittleEndian.Uint64(buf[8:16])),
		Qty:   binary.LittleEndian.Uint64(buf[16:24]),
		Side:  Side(buf[24]),
	}, nil
}

// ValidateHeader checks the 8-byte tickstore log header at the start of buf.
func ValidateHeader(buf []byte) error {
	if len(buf) < HeaderSize {
		return ErrBadSize
	}
	if !bytes.Equal(buf[0:6], logMagic) {
		return ErrBadMagic
	}
	if binary.LittleEndian.Uint16(buf[6:8]) != logVersion {
		return ErrBadVersion
	}
	return nil
}
