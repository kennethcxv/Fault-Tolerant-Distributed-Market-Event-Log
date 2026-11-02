# Fault-Tolerant Distributed Market Event Log

A replicated, append-only event log for market data, with deterministic replay.
Written in Go on top of [etcd-io/raft](https://github.com/etcd-io/raft) (consensus) and
[Pebble](https://github.com/cockroachdb/pebble) (storage). The consensus algorithm and the
storage engine come from those libraries. This repository adds everything around them: the
storage layout, Raft integration, state machine, snapshot transfer, transport, fault-injection
harness, and replay engine.

**Status: milestone 1 of 10. Design only, no code yet.** Nothing below is claimed to work
until a test or benchmark in this repository shows it.

- [Design: guarantees, failure model, architecture](docs/design.md)
- [Decision log](docs/decisions.md)
- [Engineering journal](docs/journal.md)

## Milestones

| # | Milestone | Done when |
|---|---|---|
| 1 | Requirements, invariants, architecture | design reviewed ✔ |
| 2 | Single-node append-only storage | append/read/checkpoint tests pass under `-race` |
| 3 | Versioned events, deterministic replay | golden replay hash; corruption and duplicate tests |
| 4 | Raft replication | 3-node cluster elects, replicates, survives leader change |
| 5 | Durable restart and recovery | crash tests on the crash-simulating filesystem |
| 6 | Snapshots and log compaction | a lagging follower recovers through snapshot install |
| 7 | Fault-injection harness | randomized runs pass I1–I7 and Porcupine |
| 8 | Consumer checkpoints, dedup | consumer restart reaches the same replay hash |
| 9 | Project 1 integration | C++ spool writer → publisher → cluster, tested in CI |
| 10 | Performance analysis, final docs | benchmarks with stated conditions |
