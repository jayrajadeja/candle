# candle — OHLCV aggregation over a tick stream (Go)

Turns a stream of executions into **OHLCV candles** — the aggregated bars a human
actually charts. It reads the same 25-byte trade records that `lob --emit` produces
and `tickstore` persists, groups them into fixed-width buckets over the logical
timestamp, and prints one row per bucket (table or CSV). Pure Go, standard library
only, no floats.

This is **project C** in a four-part series:

```
A  lob        generate   — match orders, emit a trade stream
B  tickstore  store      — append-only, indexed tick store
C  candle     aggregate  — OHLCV candles over the tick stream    (this repo)
```

It completes the pipeline story: **generate → store → aggregate.**

## Build

```bash
go build ./...
go build -o candle ./cmd/candle
```

Requires Go 1.26+ (see `go.mod`). No third-party dependencies.

## Usage

```bash
# live: aggregate straight off the generator (A → C)
lob-replay --emit | candle --width 20

# stored: aggregate a tickstore .log file (B → C)
candle --width 20 --log data/SYNTH.log

# CSV instead of a table
lob-replay --emit | candle --width 20 --csv
```

Flags:

| flag | default | meaning |
|------|---------|---------|
| `--width` | — | logical-TS bucket width (**required**, > 0) |
| `--csv` | off | emit CSV instead of an aligned table |
| `--log` | off | input is a tickstore `.log` file (strip the 8-byte header) |

Input is read from the file named as the first positional argument, or from stdin
when none is given.

## How it works

- **One bucket per `width` of logical time.** B's `TS` is a *logical* sequence
  counter, not a wall clock, so bars are bucketed by logical-TS width (real
  minute/hour bars need a clock the series doesn't yet have).
- **Each candle carries O, H, L, C, Volume, VWAP, and a buy/sell split.**
- **VWAP without floats.** Volume-weighted average price is kept exact using integer
  running sums (`Σ price·qty` and `Σ qty`); the ratio is formed only at render time.
- **Two input shapes, one core.** A bare 25-byte record stream (from `--emit`) or a
  tickstore `.log` (with `--log`, which strips the 8-byte header). Aggregation itself
  is pure and identical for both.
- **I/O in one package.** The `tick`/`candle` cores are pure; only `cmd/candle`
  touches stdin/stdout/argv.

## Layout

| package | job |
|---------|-----|
| `tick/`   | the 25-byte record + `Decode` (pure) |
| `candle/` | `Aggregate` — ticks → OHLCV+VWAP+buy/sell candles (pure) |
| `cmd/candle/` | table/CSV rendering + file/stdin plumbing — the only I/O layer |

## Development

```bash
go build ./... && go vet ./... && go test ./...
```

Design specs and implementation plans live under `docs/superpowers/`.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Agent contributors: read
[`AGENTS.md`](AGENTS.md).

## License

MIT — see [`LICENSE`](LICENSE).
