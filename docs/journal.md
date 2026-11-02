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

## 2026-11-02 — Milestone 3: versioned events and deterministic replay

Done:

- `internal/orderbook`: schema v1 (add, cancel, execute, replace; ITCH-style)
  and a price-time priority book. Invalid messages are counted, not fatal, and
  the counts are part of the hash. An unknown schema version stops replay
  rather than being skipped. The canonical binary encoding serves as both the
  snapshot format and the hash input.
- A seeded feed generator and a golden fixture
  (`fixtures/orderbook-v1/events.bin`, 5,000 records, expected SHA-256 in
  `expected.txt`). The test checks that the generator still produces the same
  bytes and that replaying them still produces the same state.
- `internal/replay`: strict offset order. Repeated deliveries are skipped;
  gaps, corrupt records and unsupported schemas stop replay at the last good
  offset. Checkpoint files are written atomically and verified on load.
- `mlogctl replay`.
- Tests:
  - snapshot at any point plus the rest of the feed equals a full replay;
  - interrupted replay resumes to the same hash;
  - overlapping deliveries;
  - missing record;
  - corrupt record;
  - damaged checkpoint file;
  - fuzzing of snapshot decoding (5.6M inputs, no accepted non-canonical
    snapshot).

Bugs found:

- The first fixture showed crossed books: bid 9996 above ask 9995 on
  instrument 0. The generator moved each instrument's mid price while old
  orders kept resting, so bids ended up above asks. No real venue feed looks
  like that, so the generator now keeps bids strictly below a fixed mid and
  asks strictly above, and a test asserts that every book in the generated
  feed is uncrossed.
- `Engine.Run` looped forever when the source re-sent only events it had
  already applied (it took "non-empty batch" to mean "progress"). An
  at-least-once source does exactly that at the end of the log. Run now
  treats a batch with nothing at or past the next offset as caught up. Found
  by `TestRepeatedDeliveryIsIgnored`, which hung.

## 2026-11-02 — Milestone 4: Raft replication

Done:

- Raft persistence in `internal/storage/raftlog.go`. Hard state, entries and
  received snapshot metadata go in one batch, and entries overwritten by a
  leader are truncated.
- `internal/replica`: drives `RawNode` with the library's ordering (persist →
  send → apply → Advance). It has no goroutines, so the server and the
  simulator share it.
- `internal/sim`: virtual time, seeded network faults, crash via
  `CrashClone`, a workload with retries, a Porcupine model of the log
  (including the dedup semantics), and checks I1–I7.
- Server Raft engine:
  - one goroutine owns the RawNode;
  - leader-only writes with redirects;
  - ReadIndex reads, served by followers too;
  - pending writes fail as `outcome_unknown` on leadership loss;
  - HTTP peer transport with per-peer ordered queues;
  - fault-injection endpoint and Prometheus metrics.
- `scripts/cluster.sh` runs a real multi-process cluster.

Verified:

- Simulator: 3- and 5-node replication, and an isolated leader stepping down,
  all with linearizable histories.
- In-process HTTP clusters: 3 and 5 nodes, follower redirect, linearizable
  follower read, leader stop and catch-up, quorum loss (nothing
  acknowledged) and recovery. Five consecutive `-race` runs passed.
- Manually, with three `mlogd` processes: `kill -9` of the leader, a new
  leader elected about a second later in term 3, a write accepted, and the
  killed node rejoining as a follower with all three events.

Bugs found:

- Simulator bootstrap. `sim.New` opened each node as it created it, so the
  nodes bootstrapped with peer sets {1}, {1,2} and {1,2,3}. Raft panicked with
  "need non-empty snapshot". The fix opens nodes only after all exist. The
  same mistake is possible in production with inconsistent `--peers` flags,
  which led to D15.
- Client error semantics. The client reported `outcome_unknown` after
  connection-refused errors, even though a failed dial proves the request
  never left. Only non-dial transport errors now count as possibly applied.

## 2026-11-02 — Milestone 5: durable restart and recovery

Done:

- Simulator crash tests:
  - a leader crashes holding uncommitted entries, and they are truncated
    when it rejoins;
  - the whole cluster loses power (5 seeds), and every acknowledged write
    survives;
  - random crash/restart cycles that keep a random fraction of unsynced data
    (10 seeds);
  - `--fsync=never` with a power loss: 50 acknowledged, 0 survived, which
    documents that the mode voids G1.
- Mutation check of the crash tests. With fsync disabled in the replica's
  persist path, the power-loss test fails: raft panics on restart with
  `tocommit(61) is out of range [lastIndex(3)]`. So the test does depend on
  fsync.
- Startup refuses a hard state whose commit index is past the end of the log.
- The Raft engine recovers raft's panics and turns them into node failure with
  an explanation, instead of crashing the process.
- Real-disk tests:
  - a corrupt record on a follower stops only that node;
  - truncating a follower's newest WAL file: Pebble recovers a consistent
    prefix and the node starts, but the leader still remembers its old match
    index. Raft then stops the node with `tocommit(84) is out of range
    [lastIndex(45)]`. Same result on all three runs.
- `GET /v1/verify` and `mlogctl verify`: checksum every node's log over a
  common prefix and compare.

What this means: a node whose disk lost acknowledged data can't rejoin under
its old identity. It's detected and stopped. Bringing back a replacement needs
a membership change, which isn't implemented yet (D10). Until it is, the
cluster runs with one less failure of headroom.
