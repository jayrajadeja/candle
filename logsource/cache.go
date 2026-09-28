package logsource

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/jayrajadeja/candle/candle"
	"github.com/jayrajadeja/candle/feed"
	"github.com/jayrajadeja/candle/tick"
)

// Cache is a resident, read-only view over a directory of per-symbol tickstore
// logs. It keeps each symbol's fully parsed tick slice hot in memory, so a repeat
// request skips the per-request open + full read + framing that LogSource pays and
// runs only the window filter + aggregation. It is validated on every call by a
// cheap Stat: if the log grew, shrank, or was replaced, the ticks are reloaded.
// Results are identical to LogSource.
//
// Safe for concurrent use. The filter + aggregate runs while the RWMutex is held
// (a read lock on the fast path, the write lock on a reload), so a reload can never
// swap the slice a reader is iterating. The cached slice is shared and treated as
// immutable: the read path filters into a fresh slice, never in place.
type Cache struct {
	dir string
	mu  sync.RWMutex
	m   map[string]*hot
}

// hot is the cached state for one symbol's log.
type hot struct {
	ticks   []tick.Tick
	size    int64
	modUnix int64
}

// NewCached returns a Cache over the logs rooted at dir.
func NewCached(dir string) *Cache {
	return &Cache{dir: dir, m: make(map[string]*hot)}
}

var _ candle.Source = (*Cache)(nil)

// Candles returns the OHLCV candles of width for symbol over the inclusive window
// [from, to], served from the resident tick slice. Behaviour is identical to
// LogSource: a missing log yields no candles (not an error); width must be positive.
func (c *Cache) Candles(symbol string, width, from, to int64) ([]candle.Candle, error) {
	if width <= 0 {
		return nil, candle.ErrBadWidth
	}
	ticks, err := c.resolve(symbol)
	if err != nil {
		return nil, err
	}
	if ticks == nil {
		return []candle.Candle{}, nil
	}
	// Fresh slice: the cached ticks are shared across readers and must not be
	// filtered in place.
	kept := make([]tick.Tick, 0, len(ticks))
	for _, tk := range ticks {
		if tk.TS >= from && tk.TS <= to {
			kept = append(kept, tk)
		}
	}
	return candle.Aggregate(kept, width)
}

// resolve returns the resident tick slice for symbol, loading or reloading it if
// missing or stale. A missing log returns (nil, nil). The returned slice is shared
// and must be treated as read-only.
func (c *Cache) resolve(symbol string) ([]tick.Tick, error) {
	path := filepath.Join(c.dir, symbol+".log")

	// Fast path: hot entry that is still fresh.
	c.mu.RLock()
	if h := c.m[symbol]; h != nil {
		fresh, err := stillFresh(path, h)
		if err != nil {
			c.mu.RUnlock()
			return nil, err
		}
		if fresh {
			ticks := h.ticks
			c.mu.RUnlock()
			return ticks, nil
		}
	}
	c.mu.RUnlock()

	// Slow path: (re)load under the write lock.
	c.mu.Lock()
	defer c.mu.Unlock()
	// Another goroutine may have loaded a fresh entry between the locks.
	if h := c.m[symbol]; h != nil {
		fresh, err := stillFresh(path, h)
		if err != nil {
			return nil, err
		}
		if fresh {
			return h.ticks, nil
		}
	}
	return c.load(symbol, path)
}

// stillFresh reports whether the log at path is unchanged from the cached entry
// (same size and mtime). A vanished file is not fresh so resolve reloads it.
func stillFresh(path string, h *hot) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.Size() == h.size && info.ModTime().Unix() == h.modUnix, nil
}

// load reads and frames the whole log, caches it, and returns the ticks. The
// caller holds the write lock. A missing log drops any stale entry and returns
// (nil, nil). A read/framing error leaves the cache unchanged (not poisoned).
func (c *Cache) load(symbol, path string) ([]tick.Tick, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			delete(c.m, symbol)
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	ticks, err := feed.ReadTicks(f, true)
	if err != nil {
		return nil, err
	}
	h := &hot{ticks: ticks, size: info.Size(), modUnix: info.ModTime().Unix()}
	c.m[symbol] = h
	return ticks, nil
}
