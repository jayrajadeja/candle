# Live candle streaming over SSE — implementation plan

Spec: [`../specs/2026-09-29-live-stream-design.md`](../specs/2026-09-29-live-stream-design.md).
TDD, minimal surgical diffs, gate green after every task. Standard library only. The
streaming invariant (upsert-by-Start equals `/v1/candles` full-range) is the oracle.

## Task 1 — SSE stream handler
- **Test first** `server/stream_test.go`:
  - `growingSource` whose slice mutates between polls under a mutex; `httptest.Server`;
    an event-reading goroutine that upserts `data` payloads by `Start`, ignoring `:`
    heartbeats. Drive: initial snapshot, mutate open bucket, append new buckets; assert
    the reduced map (sorted by Start) equals the source's final slice.
  - validation: missing symbol / missing / non-positive width → 400.
  - disconnect: cancel the request context → handler returns promptly.
- **Implement** `server/stream.go`: `streamHandler{src, interval}`, `defaultStreamInterval`,
  `firstDiff` suffix diff, SSE writes (`event: candle` / heartbeat comment / `event: error`),
  `http.Flusher` assertion, ticker loop bounded by `r.Context()`.
- Register `/v1/stream` in `Handler` (default interval). Gate (`go test ./server/...`).

## Task 2 — end-to-end over IncrementalCache
- **Test** in `server/stream_test.go`: build `logsource.NewIncremental(tempdir)`, write a
  seed `.log`, stream, append records between reads (a tiny local writeLog/appendRecords
  helper), then assert the upserted series equals `GET /v1/candles` full-range.
- No new production code expected; fix any integration gap. Gate `-race`.

## Task 3 — docs
- README: a "Live stream" subsection under Serve (SSE endpoint, event shape, upsert-by-Start
  invariant, default interval) and a layout row for `streamHandler`.
- AGENTS: one line — `/v1/stream` is a diff-the-maintained-series SSE loop; the streaming
  invariant equals `/v1/candles` full-range.

## Task 4 — gate + review + PR
- Full gate (`go build/vet ./...`, `go test -race ./...`, gofmt).
- Fresh code-review agent (focus: suffix-diff correctness under open-bucket mutation +
  append, flush/heartbeat, context-cancel/goroutine leak, header correctness, error event).
- Fix Critical/Important. Open PR base main via `as-personal gh pr create`, disclose model
  + plugins, state the streaming invariant. Never self-merge.
