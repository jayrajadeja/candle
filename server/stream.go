package server

import (
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/jayrajadeja/candle/candle"
)

// defaultStreamInterval is how often the stream handler polls its source for new or
// changed candles. Small enough to feel live, large enough to be cheap (one Stat per
// symbol per tick on the IncrementalCache backend).
const defaultStreamInterval = 250 * time.Millisecond

// streamHandler pushes candles over Server-Sent Events. On each poll it fetches the
// full-range series from src and emits the suffix that changed since the last poll —
// which, because the tick log is append-only, is only the (possibly updated) open
// bucket plus any newly finalized buckets. A client that upserts events by Start holds
// exactly what GET /v1/candles (full range) would return.
type streamHandler struct {
	src      candle.Source
	interval time.Duration
}

func newStreamHandler(src candle.Source, interval time.Duration) *streamHandler {
	return &streamHandler{src: src, interval: interval}
}

func (s *streamHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	q := r.URL.Query()
	symbol := q.Get("symbol")
	if symbol == "" {
		writeErr(w, http.StatusBadRequest, "missing required param: symbol")
		return
	}
	width, err := parseInt64Required(q, "width")
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if width <= 0 {
		writeErr(w, http.StatusBadRequest, "width must be a positive integer")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, http.StatusInternalServerError, "streaming unsupported")
		return
	}

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	ctx := r.Context()

	var last []candle.Candle
	for {
		cs, err := s.src.Candles(symbol, width, minInt64, maxInt64)
		if err != nil {
			writeEvent(w, "error", errorResponse{Error: err.Error()})
			flusher.Flush()
			return
		}
		switch {
		case !continues(last, cs):
			// The series was rebuilt (shrank or its bucket boundaries changed), so a
			// suffix diff would strand now-absent candles in the client. Tell it to
			// clear and re-snapshot from the events that follow.
			writeEvent(w, "reset", resetEvent{Reset: true})
			for _, c := range cs {
				writeEvent(w, "candle", candleToDTO(c))
			}
		default:
			if d := firstDiff(last, cs); d < len(cs) {
				for _, c := range cs[d:] {
					writeEvent(w, "candle", candleToDTO(c))
				}
			} else {
				io.WriteString(w, ": ping\n\n") // heartbeat keeps idle connections open
			}
		}
		flusher.Flush()
		last = cs

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// continues reports whether cs is an append-continuation of last: no shorter, and every
// resident bucket keeps its Start (only the open bucket's values may change, and new
// buckets may follow). When false the series was rebuilt and the client must re-snapshot.
func continues(last, cs []candle.Candle) bool {
	if len(cs) < len(last) {
		return false
	}
	for i := range last {
		if last[i].Start != cs[i].Start {
			return false
		}
	}
	return true
}

// resetEvent is the payload of an `event: reset`, signalling the client to clear its
// candle set before applying the events that follow.
type resetEvent struct {
	Reset bool `json:"reset"`
}

// firstDiff returns the index of the first candle that differs between old and new (or
// the shorter length if one is a prefix of the other). Under append-only growth this is
// at or after len(old)-1, so new[firstDiff:] is the changed suffix to emit.
func firstDiff(old, new []candle.Candle) int {
	n := len(old)
	if len(new) < n {
		n = len(new)
	}
	for i := 0; i < n; i++ {
		if old[i] != new[i] {
			return i
		}
	}
	return n
}

// writeEvent writes one SSE event: a named event line plus a single JSON data line.
func writeEvent(w io.Writer, event string, payload any) {
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	io.WriteString(w, "event: "+event+"\ndata: ")
	w.Write(b)
	io.WriteString(w, "\n\n")
}
