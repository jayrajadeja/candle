# Contributing to candle

Thanks for helping improve **candle** — OHLCV candle aggregation over a stream of
trade ticks.

## Prerequisites

- Go 1.26 or newer (see `go.mod`).
- Standard library only. **Do not add third-party dependencies** without discussion;
  keeping the module dependency-free is a design goal of this series.

## Local checks

Run the full gate from the repository root before opening a pull request:

```bash
go build ./... && go vet ./... && go test ./...
```

## Coding expectations

- **Test-first.** Add or update a failing test before the change that makes it pass.
- **No floats in aggregation.** VWAP is kept exact with integer running sums
  (`Σ price·qty`, `Σ qty`); form the ratio only at render time. Never accumulate
  prices as floating-point.
- **Keep the core pure.** `tick` and `candle` return errors and never `os.Exit`,
  print, or panic on bad input. All I/O (files/stdin/stdout/argv) lives in
  `cmd/candle`.
- **Respect the 25-byte record contract.** Input is the same little-endian
  `TS int64 | Price int64 | Qty uint64 | Side uint8` record `lob --emit` and
  `tickstore` use; `--log` only strips the 8-byte tickstore header.
- **Bucketing is by logical TS width**, not wall-clock time — don't introduce a clock.
- **Minimal, surgical changes.** No speculative features, no unrelated refactors.
- Update documentation when behavior or flags change.
- Never commit secrets.

## Pull-request checklist

- `go build ./... && go vet ./... && go test ./...` is green.
- New behavior is covered by tests.
- Docs (README, specs) reflect the change.
- No unrelated files are modified.

## Design docs

Specs and implementation plans live under `docs/superpowers/`. For a non-trivial
change, add or update the relevant spec before implementing.
