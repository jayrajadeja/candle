# AGENTS.md — candle

Guidance for AI coding agents working in this repository. Human contributors: see
[`CONTRIBUTING.md`](CONTRIBUTING.md).

**What this is:** OHLCV candle aggregation over a 25-byte tick stream — one bar per
fixed logical-TS width, with Open/High/Low/Close/Volume/VWAP and a buy/sell split.
Pure Go, standard library only, deterministic. **Project C** in the
`lob → tickstore → candle` series.

## Commands you must run

```bash
go build ./...            # compile
go build -o candle ./cmd/candle
go vet ./...              # static checks
go test ./...             # full test suite
```

**Never claim a change is done until `go build ./... && go vet ./... && go test ./...`
passes.** Show the output; don't assert.

## Non-negotiable conventions

- **No floats in aggregation.** VWAP is exact: accumulate integer `Σ price·qty` and
  `Σ qty`, and form the ratio only when rendering. Never accumulate prices as
  floating-point.
- **Bucket by logical TS width.** `TS` is a logical sequence counter, not a wall
  clock. Bars are `[k*width, (k+1)*width)`. Do not add wall-clock time.
- **The 25-byte record is a cross-repo contract.** Input is the bare little-endian
  `TS int64 | Price int64 | Qty uint64 | Side uint8` stream from `lob --emit` /
  `tickstore`; `--log` only strips the 8-byte tickstore log header. Don't change the
  record layout in isolation.
- **I/O is isolated.** `tick` and `candle` are pure (return errors, no `os.Exit`/
  print/panic). Only `cmd/candle` touches files/stdin/stdout/argv, and it uses a
  testable `run(args, stdin, stdout, stderr) int` seam.
- **Determinism.** Same input ⇒ byte-identical output (table and CSV).
- **Minimal, surgical diffs.** Keep every safety guard; write the failing test first.

## Workflow

- Design specs and implementation plans live under `docs/superpowers/`. Read the
  relevant spec before a non-trivial change; add one for new work.
- **Commits:** include the trailer
  `Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>`.
  Git identity is machine-level — do not hard-code author info.
- **Pull requests:** open a PR and let a human merge it. **Never self-merge.**

## Layout

| package | job |
|---------|-----|
| `tick/`   | the 25-byte record + `Decode` (pure) |
| `candle/` | `Aggregate` — ticks → OHLCV+VWAP+buy/sell candles (pure) |
| `cmd/candle/` | table/CSV rendering + file/stdin plumbing — the only I/O layer |
