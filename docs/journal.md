# Engineering journal

Records what was done, what was found, and what is still open, one entry per
milestone. Code and docs are written with Claude Code (an AI coding assistant),
working under review by the repository owner. This journal says so rather than
presenting the work as hand-written.

## 2026-11-02 — Milestone 1: requirements, invariants, architecture

Done:

- Set up the toolchain (Go 1.27.3) and picked the dependency versions to pin in
  milestone 2: etcd-io/raft v3.7.0, Pebble v2.1.7, Porcupine v1.3.1, and
  Prometheus client_golang v1.25.0.
- Read the parts of the raft and Pebble source the design relies on, instead of
  trusting the documentation alone. Findings that changed the design:
  - `raft.Config.Applied` panics if it exceeds the persisted commit index
    (`raftLog.appliedTo`). After a snapshot install the state machine can be
    ahead of the local commit index, so restart has to pass
    `min(applied, commit)` and apply has to be idempotent by index.
  - `MemoryStorage.InitialState` returns membership only from the last
    snapshot. Membership established by the bootstrap entries would be lost on
    restart, so it is stored with the state machine (`m/confstate`).
  - `RawNode.Bootstrap` hands the initial membership entries to the
    application through `CommittedEntries`. Applying them twice is expected and
    harmless.
  - Election jitter comes from `crypto/rand` (`raft.go`, `globalRand`), so a
    seeded simulator cannot reproduce Raft's internal timing. Failing runs must
    save their traces.
  - Pebble v2's `vfs.NewCrashableMem` with `CrashClone` produces a filesystem
    holding only fsynced data, which is exactly the crash model in design §4.
- Checked Project 1 (github.com/kennethcxv/Low-Latency-Market-Data-Engine-and-Trading-Simulator).
  It has no code yet. As the spec allows, replay will use a versioned
  order-book fixture defined here, and design §8 states the contract Project 1
  should implement.
- Wrote [design.md](design.md) (lifecycle states, failure model, guarantees
  G1–G8, invariants I1–I7, architecture) and [decisions.md](decisions.md)
  (D1–D14).

Not done: no code exists yet, so none of the guarantees has been tested.

Open questions for review: D10 (static membership and replacing a node with a
corrupted disk), and the license.

## 2026-11-02 — Milestone 2: single-node append-only storage

Done:

- Event envelope (`internal/event`). Tests: round-trip property, every
  single-bit flip of a record is rejected, every truncated prefix is reported
  as truncated rather than corrupt, and a fuzz target.
- Storage layer (`internal/storage`). It reads only verified records: a gap, a
  misplaced record or a CRC failure below the applied offset is `ErrCorrupt`.
- State machine (`internal/fsm`): producer dedup with strict contiguity, dense
  offsets, monotonic checkpoints. Malformed commands are rejected the same way
  on every replica instead of crashing it.
- Standalone server with group commit (one fsync per drained queue), HTTP
  client and CLI. On a corrupt read the node fails stop; it then answers
  `node_failed` to everything except `/v1/status`.

Bug found by the crash test: `TestCrashLeavesAtomicPrefixOfBatches` first
failed on several seeds, and each failure lost the entire database, including
batches committed with `Sync`. Root cause: `storage.Open` created the node
directory with a plain `MkdirAll` and then opened Pebble in a subdirectory.
Pebble creates its directory and fsyncs the parents of whatever it creates, but
it stops at the nearest directory that already exists. Our node directory
already existed, so its own entry in its parent was never synced, and a crash
could lose the whole tree. A first fix fsynced every ancestor ourselves. After
reading Pebble's `mkdirAllAndSyncParents`, the simpler fix was to remove our
`MkdirAll` and let Pebble create the path. Real filesystems behave the same way
after power loss; this isn't specific to the simulator.

Not yet measured: throughput and latency. Nothing about performance is claimed.
