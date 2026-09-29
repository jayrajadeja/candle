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

# serve: read-only HTTP/JSON candles over a directory of .log files
candle serve --dir data --addr 127.0.0.1:8138
```

Flags:

| flag | default | meaning |
|------|---------|---------|
| `--width` | — | logical-TS bucket width (**required**, > 0) |
| `--csv` | off | emit CSV instead of an aligned table |
| `--log` | off | input is a tickstore `.log` file (strip the 8-byte header) |

Input is read from the file named as the first positional argument, or from stdin
when none is given.

## Serve (HTTP)

`candle serve` exposes the same aggregation read-only over HTTP/JSON, reading
`tickstore` `.log` files directly from a directory (one `SYMBOL.log` per symbol).
It shares nothing with `tickstore` but the 25-byte record: no store or index
dependency, just a per-request scan of the log.

```bash
candle serve --dir data --addr 127.0.0.1:8138
curl 'localhost:8138/v1/candles?symbol=SYNTH&width=250&from=0&to=1000'
```

Serve flags:

| flag | default | meaning |
|------|---------|---------|
| `--dir` | `data` | directory holding `SYMBOL.log` files |
| `--addr` | `127.0.0.1:8138` | listen address |

Endpoints:

| method | path | meaning |
|--------|------|---------|
| `GET` | `/healthz` | liveness — plain `ok` |
| `GET` | `/v1/candles?symbol=&width=&from=&to=` | OHLCV candles as JSON |
| `GET` | `/v1/stream?symbol=&width=` | live candles over Server-Sent Events |

`symbol` and `width` (> 0) are required; `from`/`to` are inclusive logical-TS bounds
and default to the full range. An unknown symbol returns an empty `candles` array.

### Live stream (SSE)

`/v1/stream` pushes candles as the log grows, instead of forcing the client to poll
`/v1/candles`. On connect it emits the current full-range series, then one
`event: candle` per candle that appears or changes (`:` comment lines are idle
heartbeats):

```bash
curl -N 'localhost:8138/v1/stream?symbol=SYNTH&width=250'
```
```
event: candle
data: {"start":0,"open":100,"high":104,...}

: ping
event: candle
data: {"start":250,"open":104,"high":109,...}
```

**Streaming invariant.** A client that upserts each event by `start` holds, at any
moment, exactly what `GET /v1/candles` (full range) would return. Because ticks are
append-only, each poll emits only the changed suffix — the updated open bucket plus
any newly finalized buckets. If the underlying log is ever rebuilt (shrunk or
replaced), the server sends an `event: reset` telling the client to clear and
re-snapshot from the events that follow. The server polls its source every 250 ms.

`serve` keeps each symbol's parsed ticks hot in a resident, read-only source and
aggregates *incrementally*: on append it reads only the new bytes past the old file
size (not the whole log), folds them into a resumable per-width aggregator, and
answers a full-range or bucket-aligned query straight from the maintained candles —
no re-read, no re-aggregation. In the package benchmark a repeated full-range query
over a 200k-tick log drops from ~760µs to ~8µs (about 90x) with ~65x fewer bytes
allocated. Growth is validated per request by a cheap `Stat`; a shrink, replacement,
or non-append seam triggers a full rebuild, so results always match the file.

**Parity invariant.** For any window, the HTTP result equals the CLI pipe over the
same ticks:

```
GET /v1/candles?symbol=X&width=W&from=F&to=T
  ==  ticks of X.log with F <= TS <= T  |  candle --width W
```

With no `from`/`to`, that is exactly `candle --width W --log X.log`.

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
| `candle/` | `Aggregate` and the resumable `Aggregator` — ticks → OHLCV+VWAP+buy/sell candles (pure) |
| `feed/`   | frame a 25-byte record stream (or a `.log` with header) into ticks |
| `logsource/` | read `dir/SYMBOL.log`, TS-filter, aggregate — `LogSource` (stateless), `Cache` (resident, full re-read), and `IncrementalCache` (`NewIncremental`, resident + delta read + maintained candles), the `serve` data source |
| `server/` | transport-only HTTP/JSON handler over a `candle.Source`; `/v1/candles` (read) and `/v1/stream` (SSE live tail) |
| `cmd/candle/` | table/CSV rendering, `serve` subcommand, file/stdin plumbing — the I/O layer |

```bash
go build ./... && go vet ./... && go test ./...
```

Design specs and implementation plans live under `docs/superpowers/`.

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Agent contributors: read
[`AGENTS.md`](AGENTS.md).

## License

MIT — see [`LICENSE`](LICENSE).
