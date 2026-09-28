// Package feed frames a byte stream into 25-byte tick records. It is the single
// reader used by both the candle CLI and the HTTP log source, so the on-the-wire
// record format (and tickstore's optional 8-byte log header) is parsed one way.
package feed

import (
	"bufio"
	"errors"
	"fmt"
	"io"

	"github.com/jayrajadeja/candle/tick"
)

// ReadTicks reads r to EOF, decoding consecutive fixed-width tick records. In log
// mode it first strips and validates tickstore's 8-byte log header. A trailing
// partial record is an error (the stream is truncated).
func ReadTicks(r io.Reader, logMode bool) ([]tick.Tick, error) {
	br := bufio.NewReader(r)
	if logMode {
		var h [tick.HeaderSize]byte
		if _, err := io.ReadFull(br, h[:]); err != nil {
			return nil, fmt.Errorf("reading log header: %w", err)
		}
		if err := tick.ValidateHeader(h[:]); err != nil {
			return nil, err
		}
	}
	out := make([]tick.Tick, 0)
	var buf [tick.RecordSize]byte
	for {
		_, err := io.ReadFull(br, buf[:])
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errors.New("truncated stream: trailing partial record")
		}
		if err != nil {
			return nil, err
		}
		tk, err := tick.Decode(buf[:])
		if err != nil {
			return nil, err
		}
		out = append(out, tk)
	}
}
