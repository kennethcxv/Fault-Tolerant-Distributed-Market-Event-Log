# Status

Resume point for anyone (human or agent) continuing the work. Updated at each
milestone.

## Completed

- M1 Requirements, invariants, architecture: `docs/design.md`, `docs/decisions.md`.
- M2 Single-node append-only storage:
  - `internal/event`: v1 envelope, CRC32C, fuzz and bit-flip tests.
  - `internal/storage`: Pebble keyspace, verified range reads, atomic batches.
  - `internal/fsm`: producer dedup, dense offsets, checkpoints, crash-atomicity test.
  - `internal/server` (standalone mode), `internal/client`, `cmd/mlogd`, `cmd/mlogctl`.
  - CI: gofmt, vet, staticcheck, `go test -race`.

## Remaining acceptance criteria

- [ ] M3 Order-book fixture (schema v1), deterministic replay engine, golden hash
- [ ] M4 Raft replication (`internal/replica`, peer transport, 3- and 5-node clusters)
- [ ] M5 Restart recovery; crash tests on the crash-simulating filesystem
- [ ] M6 Snapshots, raft-log compaction, snapshot transfer and install
- [ ] M7 Deterministic simulator, Porcupine checks, the eight required fault scenarios
- [ ] M8 Consumer checkpoint/restart flow, dedup under duplicated requests
- [ ] M9 C++ spool writer, publisher sidecar, CI build of the C++ side
- [ ] M10 Benchmarks, metrics, multiprocess demo scripts, final docs, résumé bullets
- [ ] Membership change (add/remove node), needed to replace a node whose disk is lost (D10)

## Latest verification

`go test -race -count=3 ./...`: event, storage, fsm and server packages pass
(2026-11-02, Apple M4 Max, macOS 27.0.1, Go 1.27.3).

## Active issues

None open.

## Next steps

Start M3: `internal/orderbook` (schema v1 event codecs, deterministic book,
canonical state hash, snapshot/restore) and `internal/replay` (engine over a
store or HTTP source, checkpoint files).
