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
- M3 Versioned events and deterministic replay: `internal/orderbook` (schema v1,
  book, generator), `internal/replay` (engine, checkpoint files),
  `fixtures/orderbook-v1`, `mlogctl replay`.
- M4 Raft replication: `internal/replica`, Raft persistence, `internal/sim`
  (simulator, Porcupine model), Raft engine and HTTP transport in
  `internal/server`, metrics, fault-injection endpoint, `scripts/cluster.sh`.
- M5 Durable restart and recovery: simulator crash tests (truncated
  uncommitted entries, whole-cluster power loss, torn writes, fsync=never
  loss), startup log-truncation check, raft panics become node failure,
  real-disk corruption and WAL truncation tests, `mlogctl verify`.
- M6 Snapshots and log compaction: periodic snapshot + compaction, state
  machine streaming, HTTP transfer, crash-safe install, simulator and HTTP
  tests, `--snapshot-entries`/`--snapshot-keep`.
- M7 Fault-injection harness: randomized combined-fault simulator test,
  named scenarios, multi-process e2e test with Porcupine, `mlogctl fault`,
  `load`, `leader`, demo scripts, `docs/testing.md` (mutation checks).
- M8 Consumer checkpointing: `replay.Consumer`, `mlogctl consume`, consumer
  backlog in status, restart tests in-process and with SIGKILL.

## Remaining acceptance criteria

- [ ] M9 C++ spool writer, publisher sidecar, CI build of the C++ side
- [ ] M10 Benchmarks, metrics, multiprocess demo scripts, final docs, résumé bullets
- [ ] Membership change (add/remove node), needed to replace a node whose disk is lost (D10)

## Latest verification

`go test -race -count=1 ./...` passes; server cluster tests passed 5 runs in a
row (2026-11-02, Apple M4 Max, macOS 27.0.1, Go 1.27.3).

## Active issues

None open.

## Next steps

Start M9: C++ header-only spool writer (CRC32C, envelope v1, SPSC ring,
background writer thread, GAP records), a C++ example and test built in CI,
a Go spool reader, and `mlog-publisher` (tail spool, resume from the
producer's last committed sequence). Then docs/integration.md for Project 1.
