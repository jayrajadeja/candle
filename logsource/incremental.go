package logsource

import (
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/jayrajadeja/candle/candle"
	"github.com/jayrajadeja/candle/feed"
	"github.com/jayrajadeja/candle/tick"
)

// Unbounded-window sentinels, matching the server's from/to defaults. A query with
// these edges asks for the whole logical-time range.
const (
	minInt64 = math.MinInt64
	maxInt64 = math.MaxInt64
)

// IncrementalCache is a resident candle.Source that does work proportional to the
// new ticks. Like Cache it keeps each symbol's ticks hot, but it never re-reads the
// whole log on growth (it reads only the appended bytes) and never re-aggregates
// from scratch on a full/aligned query (it maintains a resumable candle.Aggregator
// per width). Results are identical to LogSource.
//
// Growth is treated as tickstore's append-only contract: the delta bytes past the
// old size are framed as new records and folded in. A seam that is not a clean
// continuation (the first new tick precedes the last resident tick), or any shrink
// or replacement, forces a full rebuild — so correctness never depends on the
// contract holding.
//
// Safe for concurrent use, mirroring Cache's RWMutex discipline. Aggregator.Candles
// returns a fresh slice, so a snapshot handed to a reader is never mutated by a later
// refresh.
type IncrementalCache struct {
	dir string
	mu  sync.RWMutex
	m   map[string]*incHot
}

// incHot is the resident state for one symbol's log.
type incHot struct {
	ticks   []tick.Tick                  // all resident ticks, append-only
	aggs    map[int64]*candle.Aggregator // width -> full-range aggregator
	size    int64
	modUnix int64
}

// NewIncremental returns an IncrementalCache over the logs rooted at dir.
func NewIncremental(dir string) *IncrementalCache {
	return &IncrementalCache{dir: dir, m: make(map[string]*incHot)}
}

var _ candle.Source = (*IncrementalCache)(nil)

// Candles returns the OHLCV candles of width for symbol over the inclusive window
// [from, to]. Behaviour is identical to LogSource: a missing log yields no candles
// (not an error); width must be positive.
func (c *IncrementalCache) Candles(symbol string, width, from, to int64) ([]candle.Candle, error) {
	if width <= 0 {
		return nil, candle.ErrBadWidth
	}
	snap, ticks, err := c.resolve(symbol, width)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return []candle.Candle{}, nil
	}
	// Fast path: an unbounded or bucket-aligned window has no partial buckets, so it
	// is exactly the maintained candles whose Start lies in [from, to].
	if windowAligned(width, from, to) {
		return sliceByStart(snap, from, to), nil
	}
	// Fallback: an arbitrary window may split boundary buckets; reproduce LogSource's
	// filter-then-aggregate over the resident ticks.
	kept := make([]tick.Tick, 0, len(ticks))
	for _, tk := range ticks {
		if tk.TS >= from && tk.TS <= to {
			kept = append(kept, tk)
		}
	}
	return candle.Aggregate(kept, width)
}

// resolve refreshes symbol's resident state and returns (snapshot for width, resident
// ticks). A missing log returns (nil, nil, nil). The returned snapshot is a fresh copy
// and the tick slice is only appended to (never mutated in place), so both are safe to
// use after the lock is released.
func (c *IncrementalCache) resolve(symbol string, width int64) ([]candle.Candle, []tick.Tick, error) {
	path := filepath.Join(c.dir, symbol+".log")

	// Fast path: fresh entry whose width aggregator already exists.
	c.mu.RLock()
	if h := c.m[symbol]; h != nil {
		fresh, err := incFresh(path, h)
		if err != nil {
			c.mu.RUnlock()
			return nil, nil, err
		}
		if fresh {
			if a := h.aggs[width]; a != nil {
				snap, ticks := a.Candles(), h.ticks
				c.mu.RUnlock()
				return snap, ticks, nil
			}
		}
	}
	c.mu.RUnlock()

	// Slow path: refresh and/or build the width aggregator under the write lock.
	c.mu.Lock()
	defer c.mu.Unlock()
	h, err := c.refresh(symbol, path)
	if err != nil {
		return nil, nil, err
	}
	if h == nil {
		return nil, nil, nil
	}
	a := h.aggs[width]
	if a == nil {
		a, err = candle.NewAggregator(width)
		if err != nil {
			return nil, nil, err
		}
		a.Add(h.ticks)
		h.aggs[width] = a
	}
	return a.Candles(), h.ticks, nil
}

// refresh brings symbol's resident entry up to date with its log and returns it (nil
// if the log is missing). The caller holds the write lock. A read/framing error leaves
// the cache unchanged (not poisoned). Growth past the old size is folded in as a delta;
// a shrink, replacement, or non-continuing seam triggers a full rebuild.
func (c *IncrementalCache) refresh(symbol, path string) (*incHot, error) {
	// Re-check under the write lock: another goroutine may have refreshed already.
	if h := c.m[symbol]; h != nil {
		fresh, err := incFresh(path, h)
		if err != nil {
			return nil, err
		}
		if fresh {
			return h, nil
		}
	}

	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			delete(c.m, symbol)
			return nil, nil
		}
		return nil, err
	}
	size, modUnix := info.Size(), info.ModTime().Unix()

	h := c.m[symbol]
	if h != nil && size > h.size {
		// Growth: read only the appended bytes as new records.
		delta, err := readDelta(path, h.size)
		if err != nil {
			return nil, err
		}
		if incContinues(h.ticks, delta) {
			h.ticks = append(h.ticks, delta...)
			for _, a := range h.aggs {
				a.Add(delta)
			}
			// Anchor size to the bytes actually framed, not the pre-read Stat: a
			// writer may append between Stat and read, and the next delta must seek to
			// exactly where framing stopped or it would re-read (and duplicate) records.
			h.size, h.modUnix = framedSize(len(h.ticks)), modUnix
			return h, nil
		}
		// Not a clean append -> fall through to a full rebuild.
	}
	return c.rebuild(symbol, path, modUnix)
}

// rebuild reads and frames the whole log, replacing symbol's resident state with a
// fresh entry (no aggregators yet; they are built lazily per width). The caller holds
// the write lock.
func (c *IncrementalCache) rebuild(symbol, path string, modUnix int64) (*incHot, error) {
	ticks, err := readAll(path)
	if err != nil {
		return nil, err
	}
	h := &incHot{
		ticks:   ticks,
		aggs:    make(map[int64]*candle.Aggregator),
		size:    framedSize(len(ticks)),
		modUnix: modUnix,
	}
	c.m[symbol] = h
	return h, nil
}

// framedSize is the byte offset just past n framed records: the 8-byte header plus n
// fixed-width records. It is the seek offset for the next delta read.
func framedSize(n int) int64 {
	return tick.HeaderSize + int64(n)*tick.RecordSize
}

// incFresh reports whether the log at path is unchanged from the cached entry (same
// size and mtime). A vanished file is not fresh.
func incFresh(path string, h *incHot) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.Size() == h.size && info.ModTime().Unix() == h.modUnix, nil
}

// incContinues reports whether delta is a valid append after resident: its first tick
// does not precede the last resident tick (non-decreasing TS). An empty delta or empty
// resident set trivially continues.
func incContinues(resident, delta []tick.Tick) bool {
	if len(delta) == 0 || len(resident) == 0 {
		return true
	}
	return delta[0].TS >= resident[len(resident)-1].TS
}

// readAll opens and frames the whole log (stripping the 8-byte header).
func readAll(path string) ([]tick.Tick, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return feed.ReadTicks(f, true)
}

// readDelta frames the records appended past offset. The header lives only at offset 0
// (already consumed by the initial read), so the delta is framed without it.
func readDelta(path string, offset int64) ([]tick.Tick, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if _, err := f.Seek(offset, 0); err != nil {
		return nil, err
	}
	return feed.ReadTicks(f, false)
}

// windowAligned reports whether [from, to] has no partial boundary buckets at width:
// each edge is either unbounded or falls on a bucket boundary. Such a window equals a
// slice of the maintained full-range candles.
func windowAligned(width, from, to int64) bool {
	leftOK := from == minInt64 || from%width == 0
	rightOK := to == maxInt64 || (to+1)%width == 0
	return leftOK && rightOK
}

// sliceByStart returns the candles of snap whose Start lies in [from, to].
func sliceByStart(snap []candle.Candle, from, to int64) []candle.Candle {
	out := make([]candle.Candle, 0, len(snap))
	for _, cd := range snap {
		if cd.Start >= from && cd.Start <= to {
			out = append(out, cd)
		}
	}
	return out
}
