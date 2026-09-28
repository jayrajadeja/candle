# candle serve — resident read cache (design)

Project **F.1**, an increment on candle serve
([`2026-09-28-serve-design.md`](2026-09-28-serve-design.md)). Mirrors tickstore's
serve read cache, adapted to candle's very different read shape.

## Problem

`candle serve` answers `/v1/candles` through `logsource.LogSource`, which on **every
request** opens `dir/SYMBOL.log`, reads the whole file, frames every 25-byte record
into a `[]tick.Tick`, filters to `[from, to]`, then aggregates. For a hot symbol
queried repeatedly, the open + full read + framing is pure waste: the bytes on disk
did not change between requests.

This was deferred on purpose in the serve PR (YAGNI, and to keep the "we added a
server" change unmeasurable-free). Now we add the cache as its own measured
increment, benchmarked against that exact baseline.

## Goal

A resident, read-only cache that keeps each symbol's **parsed tick slice** hot in
memory, so a repeat request skips disk I/O and framing entirely and pays only the
filter + aggregate. Correctness must be identical to `LogSource`, and staleness must
be impossible: if the log changed on disk, the cache reloads before serving.

Non-goals (cut hard): no eviction/LRU (symbol count is tiny and bounded by files on
disk), no TTL, no cross-symbol index, no HTTP-level response cache, no change to the
write path (candle never writes logs).

## Design

### Where it lives

A new `logsource.Cache` type in the existing `logsource` package, alongside
`LogSource`, both implementing `candle.Source`. Constructor `NewCached(dir)`, mirroring
tickstore's `store.NewCached`. `serve` switches to `NewCached`; the stateless
`LogSource` stays as the semantic reference and the parity oracle in tests.

### What it caches

Per symbol, a `hot` entry:

```
hot struct {
    ticks   []tick.Tick // fully parsed, non-decreasing in TS
    size    int64       // os.Stat size at load
    modUnix int64       // os.Stat mtime (unix seconds) at load
}
```

Unlike tickstore's cache, candle keeps **no open fd and no index** — it reads the
whole log into `ticks` at load time and closes the file. candle's access pattern is a
full scan anyway (it must see every tick to aggregate), so there is nothing an fd +
index would buy; the parsed slice is the natural unit to memoize.

### Freshness

Validated on every call by a cheap `os.Stat(path)` comparing `size` and `mtime`:

- Same size and mtime -> serve from the resident slice.
- Different, or the file grew/shrank/was replaced -> reload (re-read + re-frame).
- File vanished -> drop the entry, return an empty result (not an error), matching
  `LogSource`'s missing-symbol semantics.

Logs are append-only in practice, but the check is a plain equality on (size, mtime),
so any change triggers a reload. Second resolution mtime is sufficient here: a change
that keeps byte size identical **and** lands within the same second is not a real
scenario for these logs, and tickstore's cache makes the same second-resolution
choice.

### Concurrency

`sync.RWMutex` + `map[string]*hot`, same structure as tickstore's cache:

- **Fast path:** `RLock`, look up the entry, `stillFresh`; if fresh, run the
  filter + aggregate while still holding the `RLock`, then unlock.
- **Slow path:** `RUnlock`, take the `Lock`, re-check freshness (another goroutine
  may have reloaded), reload if needed, then serve while still holding the `Lock`.

The cached `ticks` slice is **shared** across concurrent readers, so — critically —
the read path must treat it as immutable. `LogSource` filters in place with
`kept := all[:0]` (safe there because the slice is freshly read per request); the
cache MUST NOT do that. It allocates a **fresh** filtered slice each call. This is the
one load-bearing difference from `LogSource` and the easiest place to introduce a
data race, so it gets an explicit concurrent test.

### Read path

```
Candles(symbol, width, from, to):
    if width <= 0: return ErrBadWidth
    resolve hot (fast/slow path above); missing file -> return empty
    kept := make([]tick.Tick, 0, len(hot.ticks))   // FRESH, never hot.ticks[:0]
    for tk in hot.ticks: if from <= tk.TS <= to: append
    return candle.Aggregate(kept, width)
```

`width <= 0` is checked before any lookup, so a bad width never loads a file — same as
`LogSource`.

## Parity invariant

For every `(symbol, width, from, to)`, `Cache.Candles == LogSource.Candles`, byte for
byte. That is the core test: run both over the same directory and assert equal
results, including the empty and missing-symbol cases. The existing serve parity
(HTTP == `candle --width W --log X.log`) then transitively holds for the cached server.

## Error handling

- `width <= 0` -> `candle.ErrBadWidth` (HTTP 400 upstream), no I/O.
- missing log -> empty slice, nil error (HTTP 200 with `[]`).
- truncated / bad-header log -> the `feed.ReadTicks` error propagates (HTTP 500),
  same as `LogSource`. A load error does not poison the cache: the entry is simply not
  stored, so a later fixed file loads cleanly.
- `os.Stat` error other than "not exist" -> propagated.

## Testing

- `logsource/cache_test.go`
  - **Parity** vs `LogSource` across widths and windows (full, sub-range, empty,
    `from > to`, unknown symbol).
  - **Invalidation:** serve, append more ticks to the log, serve again -> new candles
    reflect the appended data (cache reloaded on size/mtime change).
  - **Bad width** -> `ErrBadWidth`, no panic.
  - **Concurrency:** many goroutines calling `Candles` on the same hot symbol with
    overlapping windows, run under `-race`, all results equal the `LogSource` answer
    (proves the fresh-slice filter and the lock discipline).
- `logsource/cache_bench_test.go`
  - `BenchmarkCacheCandles` vs `BenchmarkLogSourceCandles` over a warm multi-thousand
    tick log, to quantify the disk+framing saving.

## Benchmark expectation

The win is the skipped open + full `ReadFile` + framing loop; the aggregate cost is
unchanged (still O(N) over the window). So the speedup is large for I/O-bound small
windows and shrinks toward 1x as the aggregate dominates. We report the measured
number in the PR and the write-up rather than predicting it.
