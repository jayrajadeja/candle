// Package logsource reads tickstore .log files and aggregates them into candles,
// implementing candle.Source. It depends on tickstore only through the shared
// 25-byte record format (via candle/feed) — never its store or index packages.
package logsource

import (
	"os"
	"path/filepath"

	"github.com/jayrajadeja/candle/candle"
	"github.com/jayrajadeja/candle/feed"
)

// LogSource serves candles from a directory of per-symbol tickstore logs
// (dir/SYMBOL.log). Each request reads the log, keeps ticks in [from, to], and
// aggregates them at the requested width — the same work as
// `tickstore query --from --to --symbol S | candle --width W`.
type LogSource struct {
	dir string
}

// New returns a LogSource rooted at dir.
func New(dir string) *LogSource { return &LogSource{dir: dir} }

var _ candle.Source = (*LogSource)(nil)

// Candles returns the OHLCV candles of width for symbol over the inclusive
// logical-time window [from, to]. A missing log yields no candles (not an error),
// mirroring tickstore's missing-symbol read semantics. width must be positive.
func (ls *LogSource) Candles(symbol string, width, from, to int64) ([]candle.Candle, error) {
	if width <= 0 {
		return nil, candle.ErrBadWidth
	}
	f, err := os.Open(filepath.Join(ls.dir, symbol+".log"))
	if err != nil {
		if os.IsNotExist(err) {
			return []candle.Candle{}, nil
		}
		return nil, err
	}
	defer f.Close()

	all, err := feed.ReadTicks(f, true)
	if err != nil {
		return nil, err
	}
	kept := all[:0] // reuse backing array; ticks are non-decreasing in TS
	for _, tk := range all {
		if tk.TS >= from && tk.TS <= to {
			kept = append(kept, tk)
		}
	}
	return candle.Aggregate(kept, width)
}
