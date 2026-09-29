# Incremental candle aggregation (design)

Project **C.2**, an increment on candle. Turns the resident-cache read path from a
per-request full recompute into an incremental fold, and turns the per-append full
log re-read into a delta read. Results stay byte-identical to the current
`Cache`/`LogSource`.

## Problem

Today the serve path (`logsource.Cache`) is resident but not incremental:

- **Every append re-reads the whole log.** `Cache.load` reopens the file and
  `feed.ReadTicks` re-frames *all* records whenever size/mtime changes — O(total
  ticks) per append, even when one record was added.
- **Every query re-aggregates every bucket.** `Cache.Candles` filters the hot ticks
  to `[from,to]` and calls `candle.Aggregate` from scratch — O(ticks in window) on
  every call, even for the same full-range query repeated.

For an ingest-heavy, dashboard-polled workload (exactly the demo: a growing log,
repeated full-range chart pulls) both costs are paid again and again.

## Goal

A resident source that, on append, does work proportional to the **new** ticks, and
serves a full-range or bucket-aligned query straight from a maintained candle series
with **no** re-aggregation — while returning results byte-identical to `LogSource`
for every `(width, from, to)`.

Non-goals (YAGNI): no persistence of aggregates to disk; no eviction/LRU (resident,
like `Cache`); no cross-width sharing beyond the shared tick slice; no change to the
wire format or the pure `Aggregate` behavior.

## Design

### 1. Resumable `Aggregator` (pure core, `candle` package)

The algorithm piece: the `Aggregate` loop made resumable.

```go
type Aggregator struct { /* width, done []Candle, cur Candle, num, bucket, open */ }

func NewAggregator(width int64) (*Aggregator, error) // ErrBadWidth if width <= 0
func (a *Aggregator) Add(ticks []tick.Tick)          // fold more ticks (each >= last seen)
func (a *Aggregator) Candles() []Candle              // finalized snapshot (fresh slice)
```

- `Add` runs the exact bucketing loop of `Aggregate`: a bucket change finalizes the
  open bucket (compute integer VWAP `num/Volume`), appends it to `done`, and opens the
  next. The open bucket is held in `cur` with its running `num`.
- Ticks handed to successive `Add` calls are non-decreasing in TS and never precede
  the open bucket (tickstore's append-only guarantee), so a later `Add` can only
  extend the open bucket or open newer ones — never mutate a finalized bucket.
- `Candles()` returns a fresh slice: `done` plus, if a bucket is open, a finalized
  copy of `cur`. It never mutates internal state, so it is safe to call repeatedly and
  to hand the result to a caller.
- `Aggregate(ticks, width)` is reimplemented as
  `a,_ := NewAggregator(width); a.Add(ticks); return a.Candles()`, so the existing
  `candle` tests pin equivalence and duplication is removed.

### 2. `IncrementalCache` source (`logsource` package)

Implements `candle.Source`. Per symbol it holds:

```go
type incHot struct {
    size, modUnix int64
    ticks         []tick.Tick               // resident, incrementally appended
    aggs          map[int64]*candle.Aggregator // width -> full-range aggregator
}
```

Refresh (per request, under the same RWMutex discipline as `Cache`):

- **unchanged** (size+mtime match) -> serve from the hot entry.
- **grew** -> `Seek` to the old size, `feed.ReadTicks(f, logMode=false)` the appended
  bytes only (records are 25 bytes, the 8-byte header is only at offset 0 which the
  first load already consumed), append to `ticks`, and `Add` the delta to **every**
  width aggregator. O(new ticks).
- **shrank / replaced / vanished** -> drop the entry and rebuild from scratch
  (rewind semantics; rare — logs are append-only).

Width aggregators are created lazily: the first query at a new width builds its
aggregator by `Add`-ing the whole resident tick slice, then rides the incremental
path thereafter.

### Query serving (byte-identical)

`Candles(symbol, width, from, to)`:

1. refresh the symbol; get/create the width aggregator; `snap := agg.Candles()`.
2. **Fast path** when the window has no partial buckets — i.e. the left edge is
   unbounded (`from == MinInt64`) or bucket-aligned (`from % width == 0`), and the
   right edge is unbounded (`to == MaxInt64`) or bucket-aligned (`(to+1) % width ==
   0`): return the candles of `snap` whose `Start` lies in `[from, to]`. No
   re-aggregation.
3. **Fallback** for a non-aligned window: filter the hot `ticks` to `[from,to]` and
   `candle.Aggregate` — the current behavior, preserving partial-bucket edge
   semantics exactly.

`serve` switches its default backend to `NewIncremental(dir)`. `LogSource` (stateless)
and `Cache` (resident, non-incremental) remain as simpler `candle.Source`
implementations.

### Concurrency

Mirror `Cache`: an RWMutex guards the per-symbol map. The fast read path takes the
read lock only when the entry is fresh *and* the width aggregator already exists, and
returns `agg.Candles()` (a copy) so the slice escaping the lock is never mutated by a
later reload. Any refresh or first-time width build takes the write lock. A read/frame
error leaves the cache unpoisoned (as in `Cache`).

## Testing

- `candle/aggregator_test.go` — `Add` in one shot equals `Aggregate`; split across
  several `Add` calls at arbitrary boundaries equals one-shot; `Candles()` is
  repeatable and does not mutate; `NewAggregator(0)` -> `ErrBadWidth`; empty -> `[]`.
- `logsource/incremental_test.go` — for a battery of `(width, from, to)` incl.
  full-range, bucket-aligned, non-aligned, empty, and single-bucket:
  `IncrementalCache` equals `LogSource` (the oracle). Plus: append-then-query returns
  the same as a cold `LogSource` over the grown file (delta path correctness);
  shrink/replace rebuilds; missing symbol -> `[]`; concurrent readers under `-race`.
- `logsource/incremental_bench_test.go` — an append-then-query-full-range loop, `Cache`
  vs `IncrementalCache`, to quantify the win.

## Layout additions

| unit | job |
|------|-----|
| `candle.Aggregator` | resumable OHLCV fold; `Aggregate` reuses it |
| `logsource.IncrementalCache` | resident source: delta log read + incremental per-width candles + byte-identical windowing |
