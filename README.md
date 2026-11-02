# Fault-Tolerant Distributed Market Event Log

A replicated, append-only event log for market data, with deterministic replay.
Written in Go on top of [etcd-io/raft](https://github.com/etcd-io/raft) (consensus) and
[Pebble](https://github.com/cockroachdb/pebble) (storage). The consensus algorithm and the
storage engine come from those libraries. This repository adds everything around them: the
storage layout, Raft integration, state machine, snapshot transfer, transport, fault-injection
harness, and replay engine.

**Status: milestones 1–4 of 10 done** (see [STATUS](docs/STATUS.md)). Nothing here is claimed to
work unless a test or benchmark in this repository shows it.

- [Design: guarantees, failure model, architecture](docs/design.md)
- [Decision log](docs/decisions.md)
- [Engineering journal](docs/journal.md)

## Quick start

Requires Go 1.27.

```sh
scripts/cluster.sh start 3                    # builds bin/ and starts three mlogd processes
export MLOG_ENDPOINTS=$(scripts/cluster.sh endpoints)
bin/mlogctl append --producer 1 "AAPL bid 189.50 x 100"
bin/mlogctl read
bin/mlogctl status                            # roles, terms, commit/applied, per-peer lag
scripts/cluster.sh kill 1                     # SIGKILL a node; the others elect a leader
scripts/cluster.sh restart 1                  # it rejoins and catches up
scripts/cluster.sh wipe
```

## Milestones

| # | Milestone | Done when |
|---|---|---|
| 1 | Requirements, invariants, architecture | ✔ design.md, decisions.md |
| 2 | Single-node append-only storage | ✔ storage, fsm, standalone server tests |
| 3 | Versioned events, deterministic replay | ✔ golden fixture, replay tests |
| 4 | Raft replication | ✔ simulator + HTTP cluster tests |
| 5 | Durable restart and recovery | crash tests on the crash-simulating filesystem |
| 6 | Snapshots and log compaction | a lagging follower recovers through snapshot install |
| 7 | Fault-injection harness | randomized runs pass I1–I7 and Porcupine |
| 8 | Consumer checkpoints, dedup | consumer restart reaches the same replay hash |
| 9 | Project 1 integration | C++ spool writer → publisher → cluster, tested in CI |
| 10 | Performance analysis, final docs | benchmarks with stated conditions |
