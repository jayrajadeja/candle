# candle serve — design (Project F)

> A read-only HTTP/JSON server that returns OHLCV candles, mirroring
> `tickstore serve`. Go, stdlib only. Keeps candle's core contract: it consumes a
> stream of 25-byte tick records and **depends on nothing from tickstore but that
> record format** — no import of tickstore's store/index packages.

## Problem

`candle` today is a one-shot CLI: it frames a tick stream (stdin or a `.log` file),
aggregates into candles, and prints a table/CSV. To make candles queryable — the
same way `tickstore serve` made ticks queryable — we add an HTTP face that returns
JSON candles for a symbol at a requested bucket width and time range.

The governing invariant: **`GET /v1/candles?symbol=X&width=W&from=F&to=T` returns
exactly what `tickstore query --symbol X --from F --to T | candle --width W`
produces.** serve is just "range then aggregate" behind HTTP.

Scope, cut hard (YAGNI): read-only; no cache/index (per-request O(N) log scan — the
same work the CLI does today); no websocket/streaming; no multi-width in one call;
no auth/TLS. One transport, added honestly, decoupled from tickstore internals.

## Architecture

Mirror the tickstore serve seam exactly.

- **`candle/feed`** (new) — extract the 25-byte record framing currently inside
  `cmd/candle` into a reusable package: `feed.ReadTicks(r io.Reader, logMode bool)
  ([]tick.Tick, error)`. Both the CLI and the HTTP log source use it, so framing
  behavior (header strip/validate, truncated-record error) stays identical.

- **`candle.Source`** (interface, transport-agnostic) —
  `Candles(symbol string, width, from, to int64) ([]Candle, error)`. Lets the
  server hold any candle producer.

- **`candle/logsource`** (new) — `LogSource{dir}` implements `Source`: opens
  `dir/SYMBOL.log`, `feed.ReadTicks(logMode=true)`, filters ticks to
  `from <= TS <= to`, then `candle.Aggregate(filtered, width)`. A missing log =>
  no ticks => empty candles (not an error), matching tickstore's missing-symbol
  semantics.

- **`candle/server`** (new) — transport-only `Handler(src candle.Source)
  http.Handler`. Routes `/healthz`, `/v1/candles`, catch-all `/` => JSON 404. No
  socket, no os.Exit, no argv: fully httptest-able.

- **`cmd/candle`** — add a `serve` subcommand (`serve --dir --addr`) that builds a
  `LogSource` + `server.Handler`, listens, and shuts down gracefully on
  SIGINT/SIGTERM (same `runServe` shape as tickstore). The existing flag-based
  aggregation path is preserved: `serve` is dispatched only when `args[0] ==
  "serve"`.

## Endpoint

`GET /v1/candles?symbol=&width=&from=&to=`

- `symbol` required, non-blank.
- `width` required, integer `> 0`.
- `from` defaults to MinInt64, `to` to MaxInt64; `from > to` => `200` empty.
- Response:
  ```json
  {"symbol":"X","width":100,"from":0,"to":500,"count":2,"candles":[
    {"start":0,"open":..,"high":..,"low":..,"close":..,"volume":..,"vwap":..,"trades":..,"buyVol":..,"sellVol":..}
  ]}
  ```
- `candles` is always a non-nil slice (empty array, never `null`).

`GET /healthz` => `ok`.

Errors (all JSON): missing/blank symbol, missing/`<=0`/non-integer width, bad
`from`/`to` => `400`; unknown path => `404`; non-GET => `405`; a read/decode error
(e.g. a corrupt/truncated log) => `500`.

## Data flow (one request)

`/v1/candles` -> parse+validate params -> `src.Candles(symbol,width,from,to)` ->
`LogSource` opens `dir/SYMBOL.log`, frames all records, keeps `from<=TS<=to`,
`candle.Aggregate(width)` -> JSON DTOs. Because ticks are non-decreasing in TS
(tickstore's guarantee) an inclusive TS filter yields the same buckets the pipe
would, so parity holds by construction.

## Error handling

Missing log => empty candles, `200` (mirrors tickstore serve). `width<=0` => `400`
(mirrors the CLI's own guard). Truncated/corrupt log => `500` with a JSON error.
`from>to` => `200` empty. All handler errors are JSON.

## Testing

- `feed` — a framing test moved/added: header strip+validate, clean EOF, truncated
  trailing record error. (The existing cmd golden tests keep passing unchanged.)
- `logsource` — parity: for random `(width,from,to)`, `LogSource.Candles` equals
  `candle.Aggregate(feed.ReadTicks(log) filtered to [from,to], width)`; missing log
  => empty; a `from/to` window matches a hand-filtered slice.
- `server` — httptest table tests: routing, `/healthz`, `/v1/candles` happy path,
  every validation branch, status codes, null-vs-`[]`, non-GET 405, unknown 404.
- `cmd/candle` — a `serve` smoke test on `127.0.0.1:0` (ephemeral port) asserting
  clean graceful shutdown, mirroring tickstore's `serve_test.go`.
- Live e2e: `tickstore query --from F --to T | candle --width W` vs
  `curl /v1/candles?...` — byte-for-byte equal candle rows.

## Out of scope

Caching/index-accelerated range (candle intentionally doesn't own the index),
streaming/live updates, multiple widths per request, auth, TLS, pagination. Each is
a separate increment if justified.
