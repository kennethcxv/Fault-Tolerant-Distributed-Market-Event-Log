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
- M9 Project 1 integration: C++ spool writer (TSan/ASan clean), example
  engine, Go spool reader and publisher, `mlog-publisher`, cross-language
  tests, `scripts/demo-project1.sh`, `docs/integration.md`.

## Remaining acceptance criteria

- [ ] M10 Benchmarks, metrics, multiprocess demo scripts, final docs, résumé bullets
- [ ] Membership change (add/remove node), needed to replace a node whose disk is lost (D10)

## Latest verification

`go test -race -count=1 ./...` passes; server cluster tests passed 5 runs in a
row (2026-11-02, Apple M4 Max, macOS 27.0.1, Go 1.27.3).

## Active issues

- M10 in progress.
  - Measured so far (bench/results/):
    - storage microbenchmarks on macOS and on a Linux GitHub runner;
    - a baseline cluster matrix (`bench/cluster.sh`).
  - Findings to act on:
    1. Group-commit convoy: 8 closed-loop clients averaged 3.9 commands per
       fsync instead of 8.
    2. The leader persists before replicating, so a commit costs two serial
       fsyncs. Fix in progress: the leader sends before persisting
       (replica.Ready takes a send callback).
  - The Linux batch=1 synced result (6.8 ms/op, slower than batch=10) looks
    anomalous; a second workflow run was triggered to check it.

## Next steps

1. Finish the baseline matrix. Then run `go test -race ./...` on the
   leader-send-first change, re-run `bench/cluster.sh` into
   bench/results/leader-parallel, and compare 3-node latency and throughput.
2. Decide on a group-commit linger based on the `mlog_commit_group_size`
   histogram.
3. Run bench/failover.sh, bench/recovery.sh and bench/publisher.sh.
4. Write docs/benchmarks.md with every parameter and the raw results.
5. Final review, fresh-clone check, README rewrite, résumé bullets.
