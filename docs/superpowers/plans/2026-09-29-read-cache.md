# candle serve read cache — implementation plan

Spec: [`../specs/2026-09-29-read-cache-design.md`](../specs/2026-09-29-read-cache-design.md).
TDD, minimal surgical diffs, gate green after every task.

## Task 1 — `logsource.Cache` (resident parsed-tick cache)
- **Test first** `logsource/cache_test.go`:
  - parity vs `LogSource` over widths {1, 250, huge} and windows {full, sub-range,
    empty, from>to, unknown symbol};
  - bad width -> `candle.ErrBadWidth`;
  - invalidation: serve, append ticks to the log, serve again -> reflects new data.
- **Implement** `logsource/cache.go`: `Cache{dir, mu sync.RWMutex, m map[string]*hot}`,
  `hot{ticks, size, modUnix}`, `NewCached(dir)`, `var _ candle.Source`. `Candles`
  checks width, resolves hot via fast/slow path, filters into a **fresh** slice,
  `candle.Aggregate`. `stillFresh` = `os.Stat` size+mtime; missing -> drop+empty;
  load reads whole file via `feed.ReadTicks(f, true)`, stats, stores.
- Gate: `go build/vet/test ./...`.

## Task 2 — concurrency test (race)
- Add `TestCacheConcurrentReadsMatchLogSource` to `cache_test.go`: N goroutines,
  overlapping windows on one hot symbol, every result deep-equals the `LogSource`
  answer. Run `go test -race ./logsource/`. Proves the fresh-slice filter + locks.

## Task 3 — wire into serve
- `cmd/candle/serve.go`: `server.Handler(logsource.NewCached(dir))` (was `New`).
- Serve smoke test still green (behavior identical, now cached).

## Task 4 — benchmark
- `logsource/cache_bench_test.go`: build a warm N-tick log in a temp dir;
  `BenchmarkCacheCandles` and `BenchmarkLogSourceCandles` over the same window.
  Record the ratio for the PR + write-up.

## Task 5 — docs
- README: note that `serve` uses a resident, `Stat`-validated read cache (skips
  open+read+framing on repeat requests); add `Cache`/`NewCached` to the layout note
  for `logsource/`.
- AGENTS: one line — the cache slice is shared and immutable on the read path; never
  filter it in place.

## Task 6 — gate + review + PR
- Full gate + `go test -race ./...`.
- Fresh code-review agent on `main...HEAD` (focus: fresh-slice vs in-place filter,
  lock discipline / double-check after upgrade, Stat freshness edge cases,
  load-error does not poison cache, parity with `LogSource`).
- Fix Critical/Important (3x reassess), open PR base main via `as-personal gh pr
  create`, disclose model + plugins + measured speedup. Never self-merge.
