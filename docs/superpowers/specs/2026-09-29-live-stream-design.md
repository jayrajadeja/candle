# Live candle streaming over SSE (design)

Project **C.3**, an increment on candle `serve`. Adds a push endpoint so a client sees
candles update *as ticks are appended*, instead of polling `/v1/candles`. It is the
payoff of the incremental aggregator: the server already maintains the full-range candle
series in memory and detects log growth on a cheap `Stat`, so streaming is a thin diff
loop over that state.

## Problem

`serve` is request/response only. A live chart must poll `/v1/candles` on a timer,
re-fetching the whole series each time and diffing client-side. The server already knows
what changed on each append (a maintained candle suffix), so the client shouldn't have to
re-derive it.

## Goal

A `GET /v1/stream?symbol=X&width=W` endpoint that emits Server-Sent Events: the current
full-range candle series on connect, then one event per candle that appears or changes as
the log grows. A client that upserts each event by `Start` holds, at any moment, exactly
what `GET /v1/candles` (full range) would return — the streaming invariant, and the test
oracle.

Non-goals (YAGNI): no windowed streaming (full-range tail only); no websockets; no
per-client backpressure tuning; no auth; no replay-from-offset cursor. Poll interval is a
server constant, not a query param.

## Design

### Transport: Server-Sent Events

SSE, not websockets: one-way server→client, plain HTTP, auto-reconnecting in browsers,
trivially testable with `net/http/httptest`. Response headers: `Content-Type:
text/event-stream`, `Cache-Control: no-cache`, `Connection: keep-alive`. Each candle is
one event:

```
event: candle
data: {"start":0,"open":10,...}

```

A `:` comment line is sent as a heartbeat when a poll finds no change, so idle
connections and intermediary proxies stay open. SSE comments are ignored by clients.

### The loop (`server` package)

The stream handler holds the same `candle.Source` as the read API plus a poll interval:

```go
type streamHandler struct { src candle.Source; interval time.Duration }
```

On a request:

1. Validate `symbol` (required) and `width` (required, > 0) exactly as `/v1/candles`.
2. Assert the `ResponseWriter` is an `http.Flusher` (else 500 — the test recorder in unit
   tests uses a real `httptest.Server`).
3. Write headers, then loop on a `time.Ticker(interval)` until `r.Context().Done()`:
   - `cs, err := src.Candles(symbol, width, minInt64, maxInt64)`. On error, emit an
     `event: error` and return.
   - Diff `cs` against the last-sent slice: because ticks are append-only, history never
     rewrites, so the difference is always a **suffix** — the first index where the
     slices differ is at or after `len(last)-1` (only the open bucket mutates, then new
     buckets append). Emit `cs[firstDiff:]`, one event each, then flush.
   - If nothing changed, emit a heartbeat comment.
   - Set `last = cs`.
   The first iteration runs immediately (last is nil), so the client receives the whole
   current series up front, then live deltas.

`firstDiff` compares candles with `==` (they are comparable structs). The emitted suffix
is tiny in steady state (one updated open bucket, plus any newly finalized buckets).

### Wiring

`Handler(src)` gains a `/v1/stream` route using a default interval
(`defaultStreamInterval = 250ms`). `cmd/candle serve` is unchanged beyond that — it already
passes `IncrementalCache`, whose `Stat`-gated growth detection is what makes each poll
observe new ticks.

## Testing

- `server/stream_test.go`:
  - **Diff/emit invariant** with a `growingSource` (a `candle.Source` whose returned slice
    is mutated between polls under a lock): drive an `httptest.Server`, read events in a
    goroutine, upsert data payloads by `Start`, grow the source (mutate the open bucket,
    then append new buckets) across several steps, then assert the reduced upsert map,
    sorted by `Start`, equals the source's final slice. Covers: initial snapshot, open
    bucket mutation (replace, not duplicate), new bucket append, heartbeat lines ignored.
  - **Validation**: missing symbol / missing or non-positive width → 400 before streaming.
  - **Client disconnect**: cancel the request context; the handler returns promptly
    (goroutine leak check via a done signal).
  - **End-to-end** over a real `logsource.IncrementalCache` on a temp `.log`: append
    records between reads; the final upserted series equals `GET /v1/candles` full-range.

## Layout addition

| unit | job |
|------|-----|
| `server.streamHandler` | SSE loop: diff the maintained full-range series per poll, emit the changed suffix; heartbeat when idle |
