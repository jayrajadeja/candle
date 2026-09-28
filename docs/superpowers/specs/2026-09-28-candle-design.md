# candle — v1 OHLCV aggregation over a tick stream (Project C)

**Status:** approved design (brainstormed autonomously under autopilot; author away for review)
**Date:** 2026-09-28
**Series:** A `lob` (generate) → B `tickstore` (store) → **C `candle` (query/aggregate)**

## Purpose

`candle` is the third project in the series: it turns a stream of executions into
**OHLCV candles** — the aggregated bars a human actually charts. It reads the same
25-byte trade records that `lob --emit` produces and `tickstore` persists, groups
them into fixed-width buckets over the logical timestamp, and prints one row per
bucket (table or CSV).

It completes the pipeline story: **generate → store → aggregate.**

```
lob-replay --emit | candle --width 10          # live: A → C
candle --width 10 --log data/SYNTH.log         # stored: B → C
```

## Non-goals (v1, deliberately deferred)

- **Wall-clock time bars.** B's `TS` is a *logical* sequence counter, not a clock,
  so bucketing is by logical-TS width. Real minute/hour bars need a wall clock the
  data doesn't carry.
- **Tick-count and volume bars** (N-trades-per-bar, V-volume-per-bar). Natural
  follow-ups; v1 ships exactly one bucketing strategy (TS-width).
- **Gap filling.** Only buckets that contain ≥1 trade are emitted; empty buckets in
  a sparse logical-TS range are skipped (gap-fill is meaningless for logical time).
- **Multi-symbol.** One stream = one symbol, matching B's symbol-per-file model.
- **JSON output, live/streaming tail, plotting.** CSV is the charting hand-off.

## The shared contract (duplicated, never imported)

`candle` shares with `lob`/`tickstore` **only the on-disk / on-wire byte format** —
no Go package is imported across projects. `candle` re-declares:

1. **Record — 25 bytes, little-endian** (identical to `lob`'s emit and `tickstore`'s
   `tick`):
   `{ TS int64 [0:8] | Price int64 [8:16] | Qty uint64 [16:24] | Side uint8 [24] }`,
   `Side` = `Buy=0 / Sell=1`.
2. **Log header — 8 bytes** (only needed in `--log` mode): `"TCKLOG"` (6 bytes) +
   `version uint16` LE (must equal `1`).

Both are pinned by **golden-byte tests** so the duplicated copies cannot silently
drift from B. If B bumps its format, candle's golden test fails loudly.

## Input modes

`candle` reads from a positional file argument, or from **stdin** when none is given.

- **Bare record stream (default).** Input is a raw sequence of 25-byte records with
  **no header** — exactly what `lob-replay --emit` writes. Use for the live pipe.
- **`--log` mode.** Input is a `tickstore` log file: validate the 8-byte header
  (`ErrBadMagic` / `ErrBadVersion` on mismatch), then stream the records after it.

A trailing partial record (fewer than 25 bytes at EOF) is a hard error
(`truncated stream`), matching `tickstore ingest`'s treatment of a truncated tail.

## Architecture

Three packages, mirroring B's clean split (pure core, thin IO shell):

### `tick/` — the byte contract (pure)
- `type Side uint8` (`Buy=0`, `Sell=1`); `type Tick struct { TS, Price int64; Qty uint64; Side Side }`.
- `const RecordSize = 25`.
- `func Decode(buf []byte) (Tick, error)` — little-endian, `ErrBadSize` unless `len==25`.
- `const HeaderSize = 8`; `func ValidateHeader(buf []byte) error` — `ErrBadMagic`,
  `ErrBadVersion`. (Encode is **not** needed — candle only reads.)
- Golden-byte tests pin the record layout and the header bytes.

### `candle/` — aggregation core (pure, deterministic, no IO)
- `type Candle struct { Start int64; Open, High, Low, Close int64; Volume uint64; VWAP int64; Trades int; BuyVol, SellVol uint64 }`
  - `Start` = `bucket * width` (the bucket's logical-time start).
  - `Open` = first trade's price in the bucket; `Close` = last; `High`/`Low` = max/min.
  - `Volume` = Σ qty; `BuyVol`/`SellVol` = Σ qty split by aggressor `Side`.
  - `VWAP` = Σ(price·qty) / Σ(qty), **integer** division (deterministic; 0 if Volume 0).
  - `Trades` = record count in the bucket.
- `func Aggregate(ticks []tick.Tick, width int64) ([]Candle, error)`
  - `width <= 0` → `ErrBadWidth`.
  - Single pass; ticks are assumed **non-decreasing in `TS`** (B's guarantee). Bucket
    index = `TS / width` (floor for `TS >= 0`). Flush the open candle whenever the
    bucket index advances; append. Only non-empty buckets are emitted, in `TS` order.
  - Empty input → empty (non-nil) slice, no error.
- **VWAP accumulator:** numerator `Σ(price·qty)` is an `int64`. v1 documents the
  scale assumption that a bucket's `Σ(price·qty)` fits in `int64` (true at demo
  scale); a production build would widen the accumulator. Same "boring, documented,
  deliberate-v1" stance B took with its O(n) reads.

### `cmd/candle/` — CLI (thin shell)
- Flags: `--width int` (required, >0), `--csv` (bool), `--log` (bool).
- Reads records from the file arg or stdin via a 25-byte framing reader (`--log`
  strips+validates the header first), decodes each with `tick.Decode`, calls
  `candle.Aggregate`, renders.
- **Render:**
  - default → aligned text table with a header line.
  - `--csv` → `start,open,high,low,close,volume,vwap,trades,buy_vol,sell_vol` + rows.
- Exit non-zero with a stderr message on: bad flags, truncated stream, bad header,
  read error.

## Data flow

```
[file | stdin]
   │  (25-byte frames; --log strips 8-byte header first)
   ▼
tick.Decode  ──►  []tick.Tick
   ▼
candle.Aggregate(ticks, width)  ──►  []candle.Candle   (pure)
   ▼
render (table | csv)  ──►  stdout
```

## Error handling

| Condition | Behavior |
|---|---|
| `--width <= 0` (or missing) | `ErrBadWidth` / usage error, exit 2 |
| bad log magic / version (`--log`) | `ErrBadMagic` / `ErrBadVersion`, exit 1 |
| trailing bytes `1..24` at EOF | `truncated stream`, exit 1 |
| `len(buf) != 25` into `Decode` | `ErrBadSize` |
| empty input | zero candles, exit 0 |
| bucket with `Volume == 0` | `VWAP = 0` (no divide-by-zero) |

## Testing

- **Unit (`tick/`):** golden record bytes; golden header bytes; `Decode` round-trip
  vs known hex (the cross-repo drift guard); `ValidateHeader` bad-magic/bad-version.
- **Unit (`candle/`):** single trade → O=H=L=C; multi-trade bucket OHLC; bucket
  boundary (TS at exact multiple of width opens a new bucket); VWAP integer value;
  buy/sell volume split; sparse buckets (gap skipped, not filled); `width<=0` error;
  empty input.
- **CLI (`cmd/candle`):** table render golden; CSV render golden; `--log` header
  strip; truncated-tail error; stdin path.
- **End-to-end:** `lob-replay --emit --seed 99 --orders 200 | candle --width 20`
  produces candles whose total `Trades` equals lob's emitted trade count and whose
  per-bucket `Volume` sums to the store's total — cross-checked against
  `tickstore dump`.

## Success criteria

- `go build ./... && go vet ./... && go test ./...` green.
- Both demos work: live (`lob-replay --emit | candle`) and stored (`candle --log`).
- Candle totals reconcile with the tick stream (Σ Trades and Σ Volume match B).
- Deterministic: same seed → identical candles (byte-identical CSV).
