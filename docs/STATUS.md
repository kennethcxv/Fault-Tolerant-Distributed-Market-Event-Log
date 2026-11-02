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

## Completed in milestone 10

- Benchmarks (`bench/`, `docs/benchmarks.md`); fsync probe; Linux benchmark
  workflow; leader-sends-first and drain-before-Ready changes; publisher batch
  default.
- Independent code review; all findings fixed and regression-tested.
- Final README; fresh-clone check.

## Acceptance criteria

| Criterion | Evidence |
|---|---|
| Real 3-node cluster operates reliably | `scripts/cluster.sh`, `internal/e2e`, demo scripts |
| Consensus and acknowledgment semantics correct | simulator invariants I1–I7, Porcupine (sim and e2e) |
| Log survives process failures | SIGKILL e2e tests, simulator power-loss and crash-at-send tests |
| Partitions preserve guarantees | minority-leader, one-way and randomized partition tests; `demo-partition.sh` |
| Snapshots and recovery work | snapshot catch-up (sim, HTTP, multi-process), crash mid-install |
| Replay deterministic | golden fixture, replay-hash agreement on every replica |
| Consumer checkpointing works | in-process and SIGKILL consumer restart tests |
| Project 1 integration | C++ spool writer and publisher, cross-language tests, `docs/integration.md` |
| Tests, race checks, CI pass | `.github/workflows/ci.yml` (race, staticcheck, C++ under TSan) |
| Benchmarks real and reproducible | `bench/` scripts, raw results, `docs/benchmarks.md` |
| Fresh clone works | verified 2026-11-02 at 8663d34 |

## Added after milestone 10

- Membership changes (D10): `mlogctl member list/add/remove`, `--join`,
  `--cluster-id`, stored cluster ID, address book from the replicated log,
  removed nodes stop. Tests: simulator (20 seeds), HTTP, multi-process,
  `scripts/demo-replace-node.sh`.

## Open items (not required, known limitations)

- Learners and joint consensus. Membership changes are single-voter, which
  is enough to replace a failed node (D10).
- Log retention and truncation; tiered snapshots instead of shipping the whole
  event store (D6).
- Spool file rotation on the engine host.
- Profiling of the HTTP/JSON append path with fsync off; a binary append
  endpoint if the profile justifies one.
- Benchmarks across separate machines.

## Résumé bullets (based on completed, measured work)

- Built a replicated market-event log in Go on etcd-io/raft and Pebble. It
  has idempotent producers, dense offsets, snapshot catch-up and
  deterministic order-book replay. It was validated with a fault-injection
  simulator (power loss with torn writes, partitions, message
  loss/duplication/delay, crashes at send time), with Porcupine
  linearizability checks over 200+ randomized runs, and with multi-process
  SIGKILL and partition tests.
- Benchmarked on macOS and Linux:
  - 152k events/s at 4.5 ms p50 on a 3-node Linux cluster (1.3 ms p50
    single-producer commits);
  - traced the laptop's 9 ms commit floor to SSD-serialized `F_FULLFSYNC`
    with a purpose-built probe;
  - shipped a lock-free C++ spool hand-off (p99 under 400 ns, 5M events/s)
    that keeps consensus off the market-data hot path.

## Latest verification

At commit 4036d11 (2026-11-02):

- CI green: gofmt, vet, staticcheck, `go test -race ./...` including the
  multi-process e2e tests, and the C++ spool test under ThreadSanitizer.
- Locally: the simulator with 100 randomized seeds.
- A fresh clone of the public repo builds, passes `go test -short ./...`,
  and runs `scripts/demo-replace-node.sh` successfully.
