# Incremental candle aggregation — implementation plan

Spec: [`../specs/2026-09-29-incremental-aggregation-design.md`](../specs/2026-09-29-incremental-aggregation-design.md).
TDD, minimal surgical diffs, gate green after every task. Standard library only.
Byte-identical results vs `LogSource` are the invariant — `LogSource` is the test
oracle.

## Task 1 — resumable `candle.Aggregator`
- **Test first** `candle/aggregator_test.go`: one-shot `Add` == `Aggregate`; split
  `Add` at arbitrary boundaries == one-shot; `Candles()` repeatable + non-mutating;
  `NewAggregator(0)` -> `ErrBadWidth`; empty -> `[]`.
- **Implement** `candle/aggregator.go`: struct + `NewAggregator`/`Add`/`Candles`.
  Reimplement `Aggregate` in `candle.go` on top of it (keep the doc comment).
- Gate (`go test ./candle/...`).

## Task 2 — `logsource.IncrementalCache`
- **Test first** `logsource/incremental_test.go`: equality vs `LogSource` over a
  matrix of `(width, from, to)` (full, aligned, non-aligned, empty, single bucket);
  append-then-query delta correctness; shrink/replace rebuild; missing symbol -> `[]`;
  `-race` concurrent readers.
- **Implement** `logsource/incremental.go`: `IncrementalCache`, `NewIncremental(dir)`,
  `incHot`, refresh (unchanged / grew=delta read + `Add` to all width aggs /
  shrank=rebuild), lazy per-width aggregator, fast-path windowing + fallback. Mirror
  `Cache`'s RWMutex pattern.
- Gate.

## Task 3 — serve wiring
- **Implement** `cmd/candle/serve.go`: swap `logsource.NewCached(*dir)` ->
  `logsource.NewIncremental(*dir)`. Adjust the one serve test/log line if needed.
- Gate + ephemeral-port smoke still green.

## Task 4 — benchmark
- `logsource/incremental_bench_test.go`: build a growing log; loop { append a batch;
  query full-range }; benchmark `Cache` vs `IncrementalCache`. Run `go test -bench`
  and record numbers for the PR/write-up.

## Task 5 — docs
- README: a short "Incremental serve" note (delta read + incremental candles; identical
  results; fast path vs fallback) and a layout row for `IncrementalCache`.
- AGENTS: one line — serve is incremental; `Aggregator` is the resumable core; results
  stay byte-identical to `LogSource` (the oracle).

## Task 6 — gate + review + PR
- Full gate (`go build/vet ./...`, `go test -race ./...`, gofmt) + capture bench.
- Fresh code-review agent (focus: delta-read offset/framing correctness, aggregator
  open-bucket finalization + VWAP, fast-path alignment predicate incl. MinInt64/
  MaxInt64 edges, fallback equivalence, concurrency/races, shrink-rebuild).
- Fix Critical/Important. Open PR base main via `as-personal gh pr create`, disclose
  model + plugins, state the byte-identical invariant + bench numbers. Never
  self-merge.
