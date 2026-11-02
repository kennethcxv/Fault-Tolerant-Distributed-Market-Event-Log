# Fault-Tolerant Distributed Market Event Log

A replicated, append-only log for market events, with deterministic replay into an order
book. Consensus comes from [etcd-io/raft](https://github.com/etcd-io/raft) and storage from
[Pebble](https://github.com/cockroachdb/pebble). This repository adds everything around
them:

- the storage layout and its crash semantics;
- the replicated state machine (dense offsets, idempotent producers, consumer checkpoints);
- Raft persistence and the Ready loop;
- snapshot streaming and installation;
- the HTTP transport;
- a deterministic fault-injection simulator;
- the replay engine;
- a C++ hand-off for the market-data engine.

All ten milestones are done; [STATUS](docs/STATUS.md) lists what remains open.

## What it guarantees

These hold with `--fsync always` under the failure model in [design.md](docs/design.md) §4:
crash-recovery nodes, an asynchronous network, and disks that honor fsync.

- **An acknowledged append is never lost or reordered** while a majority of nodes keep their
  storage. A write is acknowledged only after it is committed on a majority and applied on
  the leader.
- **Every replica holds byte-identical records at the same offsets.** Offsets are dense
  (1, 2, 3, …) and assigned deterministically when entries are applied.
- **Exactly once per producer sequence.** Producers number their events. A retry after a
  timeout is recognized as a duplicate, and a skipped sequence is rejected as a gap.
- **Appends and opt-in reads are linearizable.** Default reads come from any replica and
  return a committed prefix.
- **Corrupt data is never served.** Every record carries a CRC32C, and a node that finds
  corruption stops serving.
- **Deterministic replay.** The same committed prefix always produces the same order-book
  state hash, including when replay resumes from a checkpoint.

Not guaranteed:

- Exactly-once delivery to consumers. Delivery is at least once; the replay engine dedupes
  by offset.
- Any order between producers beyond the log's own order.
- Anything about durability under `--fsync never`. A test shows that mode losing every
  acknowledged write when all nodes lose power.

## Architecture

```
 C++ engine ──try_publish (never blocks)──► spool file ──► mlog-publisher ──┐
                                                                            │ HTTP
 producers / mlogctl ───────────────────────────────────────────────────────┤
                                                                            ▼
     ┌─────────────── mlogd (3 or 5 nodes) ────────────────┐     consumers / replay
     │ HTTP API ──► core loop (one goroutine owns RawNode) │ ◄── read any node;
     │              persist → send → apply → Advance       │     CRC-verified records
     │ peer transport ◄──────────────────► other nodes     │
     │ Pebble: raft log │ event store │ metadata           │
     └─────────────────────────────────────────────────────┘
```

| Package | Role |
|---|---|
| `internal/event` | v1 record envelope: fixed 48-byte header, CRC32C |
| `internal/storage` | All Pebble access: keyspace, verified reads, Raft persistence, snapshot export, receive and install |
| `internal/fsm` | Deterministic apply: offsets, producer dedup, checkpoints |
| `internal/replica` | Drives `RawNode`; shared by the server and the simulator |
| `internal/server` | Raft engine goroutine, HTTP API and transport, metrics, fault injection |
| `internal/sim` | Deterministic cluster simulator, Porcupine model, invariant checks |
| `internal/orderbook`, `internal/replay` | Schema-v1 order book, replay engine, checkpointing consumer |
| `internal/spool`, `integration/cpp` | Project 1 hand-off: C++ spool writer, Go reader and publisher |
| `cmd/mlogd`, `cmd/mlogctl`, `cmd/mlog-publisher` | Server, operator CLI, spool publisher |

## Quick start

Requires Go 1.27 (and a C++17 compiler for the integration demo).

```sh
scripts/cluster.sh start 3                      # build bin/, start 3 mlogd processes, wait for a leader
export MLOG_ENDPOINTS=$(scripts/cluster.sh endpoints)
bin/mlogctl append --producer 1 "AAPL bid 189.50 x 100"
bin/mlogctl read --linearizable
bin/mlogctl load --producers 4 --events 50000 --batch 500    # synthetic order-book flow
bin/mlogctl status                              # roles, terms, commit/applied, per-peer lag, consumer backlog
bin/mlogctl verify                              # checksum every node's log over a common prefix
bin/mlogctl replay                              # rebuild the order book and print its SHA-256
bin/mlogctl consume --name risk --to 50000      # checkpointing consumer
scripts/cluster.sh kill 1; scripts/cluster.sh restart 1     # SIGKILL a node and bring it back
bin/mlogctl fault isolate 2; bin/mlogctl fault heal          # partition a node via /debug/fault
scripts/cluster.sh wipe
```

Demonstrations:

- `scripts/demo-failover.sh`: SIGKILL the leader under load. The final count is exact, with
  duplicates resolved and nothing lost.
- `scripts/demo-partition.sh`: an isolated leader refuses writes while the majority
  continues.
- `scripts/demo-project1.sh`: a C++ engine writes 1M events through the spool while the
  leader is killed.

Metrics are served at `/metrics` (Prometheus). They include:

- commit and WAL-sync latency;
- elections and leader changes;
- per-peer replication lag;
- consumer backlog;
- snapshot counts;
- group-commit size.

## Results

The full report, with every parameter and the raw outputs, is in
[docs/benchmarks.md](docs/benchmarks.md). A few numbers with their conditions:

| Measurement | Result |
|---|---|
| 3-node cluster, batch 100, fsync on, Linux CI runner (4 vCPU) | 152k events/s, p50 4.5 ms |
| 3-node, single producer, same runner | p50 commit 1.3 ms |
| 3-node, batch 100, fsync on, M4 Max laptop | 23k events/s, p50 34 ms |
| Leader SIGKILL → new leader (laptop) | 1.1–3.1 s at a 100 ms tick; 0.24–0.38 s at 20 ms |
| Follower catch-up after missing 1M events | 1.1 s via snapshot, 1.9 s via log |
| Replay over HTTP | ~3 M events/s; order book alone 12 M events/s |
| C++ `try_publish` | p50 ≤ 42 ns (clock resolution), p99 200–375 ns |

The laptop numbers are an fsync story, not a Raft story. macOS issues `F_FULLFSYNC` (~4.5 ms),
and the SSD serializes flushes from all three nodes, so a commit pays two flushes back to back.
`bench/fsyncprobe` demonstrates this. Two changes made for replicated latency are described in
benchmarks §7:

- the leader sends before persisting;
- queued input is drained before each Ready.

Together they gave 6–21% on Linux and nothing measurable on the single Mac disk.

## Testing

[docs/testing.md](docs/testing.md) maps every required failure scenario to its tests. The layers:

- **Simulator.** Real replica, raft and storage code on virtual time, with a seeded network
  that drops, delays, duplicates and partitions. Crashes use Pebble's crash-simulating
  filesystem, so they lose exactly the unsynced data, sometimes leave torn writes, and can
  land at the moment a node sends. Seven invariants are checked after every run, including
  linearizability with [Porcupine](https://github.com/anishathalye/porcupine).
  `TestRandomizedFaults` passes 200 seeds.
- **Multi-process.** Real `mlogd` binaries under SIGKILL and partitions, with concurrent
  producers and linearizable readers. Porcupine checks about 1,500 operations per run.
- **Mutation checks.** Seven deliberate bugs were run by hand: lost fsync, lost dedup, lost
  term/vote, stale reads, nodes acknowledging before persisting, unsynchronized reads during
  snapshot install, and the old spool partial-write handling. Each is now caught. Two were
  missed at first, and the harness was improved until it caught them.
- **CI.** `gofmt`, `vet`, staticcheck, `go test -race` across the whole repository, and the
  C++ spool test under ThreadSanitizer.

This is testing, not formal verification.

## Limitations

- **Static membership.** Clusters are 3 or 5 nodes, fixed at bootstrap. A node whose disk
  lost acknowledged data is detected and stopped, but it can't be replaced by a new member
  ([D10](docs/decisions.md)).
- **One Raft group, and the log is never truncated.** Throughput is bounded by one leader.
  A snapshot ships the whole event store, so install cost grows with the log (D6).
- **The append path is HTTP/JSON with base64 payloads.** It costs CPU that hasn't been
  profiled in detail. There is no TLS or authentication, so run it on a trusted network.
- **Benchmarks run on one machine.** Nothing has been measured across machines with real
  network latency.
- **The spool is one file that is never truncated.** Until the backlog drains, the engine
  host's disk is a single point of failure (see [integration.md](docs/integration.md)).
- **Simulator seeds aren't fully reproducible.** A seed reproduces the fault schedule, not
  raft's election jitter, which etcd-io/raft draws from `crypto/rand`. Failing runs save
  full traces instead.

## Documents

[Design and guarantees](docs/design.md) ·
[Decision log](docs/decisions.md) ·
[Testing](docs/testing.md) ·
[Benchmarks](docs/benchmarks.md) ·
[Project 1 integration](docs/integration.md) ·
[Engineering journal](docs/journal.md) ·
[Status](docs/STATUS.md)

## How this was built

The code and documents were written with Claude Code, an AI coding assistant, working
milestone by milestone in this repository. The [journal](docs/journal.md) records what was done
at each step, including the bugs found along the way and their root causes. Commit history
reflects the real order of work.

MIT licensed.
