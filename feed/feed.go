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
// partial record is an error (the stream is truncated). Used by the candle CLI
// (stdin and --log), which reads a closed/finite stream and must stay strict.
func ReadTicks(r io.Reader, logMode bool) ([]tick.Tick, error) {
	return readTicks(r, logMode, false)
}

// ReadCompleteTicks is like ReadTicks but, on a trailing partial record, returns
// the complete ticks read so far with a nil error instead of an error — the
// partial tail is treated as "not written yet". Used by logsource, which polls a
// live, actively growing log where a bufio flush can split a record across two
// writes; the next poll picks the record up once it is complete.
func ReadCompleteTicks(r io.Reader, logMode bool) ([]tick.Tick, error) {
	return readTicks(r, logMode, true)
}

// readTicks is the shared framing loop. tolerateTail controls what happens on a
// trailing partial record: strict error (false) or return what was read so far
// (true).
func readTicks(r io.Reader, logMode, tolerateTail bool) ([]tick.Tick, error) {
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
			if tolerateTail {
				return out, nil
			}
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
