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

## 2026-11-02 — Milestone 6: snapshots and log compaction

Done:

- Periodic snapshots and log compaction in the replica.
- State-machine snapshot streaming (`storage/snapshot.go`): export from a
  consistent Pebble snapshot, durable receive into a CRC-checked file, and a
  chunked install that writes the applied index last.
- HTTP snapshot transfer.
- `replica.Open` finishes an install that a crash interrupted.
- Tests:
  - storage round trip, damaged and mismatched streams, and a crash after each
    of the first five install batches;
  - simulator: a lagging follower catches up via snapshot, and a crash during
    install (the test asserts that the crash point left applied < snapshot
    index, so the finish-at-startup path really ran);
  - 10 seeds of random crashes with compaction on;
  - an HTTP cluster follower catching up via snapshot.
- Manually, with three processes and `--snapshot-entries 20`: a killed
  follower restored a snapshot at index 64 and `mlogctl verify` reported all
  three nodes agree on offsets 1..120.

Design change: the leader's state is at its applied index A, but raft's
snapshot may be at an older S. Installing state at A under metadata at S
would make raft re-deliver S+1..A on top of state that already contains them.
For events that's harmless, since apply is idempotent by index. For membership
changes it would apply them to raft out of order. The leader now rewrites the
outgoing snapshot metadata to (A, term(A), membership at A), and the follower
checks that the installed state is at exactly that index.

Findings:

- Under `-race`, Pebble's in-memory filesystem inverts every byte of a buffer
  after `Write`. This flushes out code that assumes the buffer is unchanged.
  `io.Copy` from a `bytes.Reader` passes the reader's backing array to
  `Write`, so receiving a snapshot destroyed the sender's copy. The test passed
  without `-race` and failed with it. Real files never do this, but the
  simulator could deliver the same bytes twice. `ReceiveSnapshot` now copies
  through its own buffer.
- I had fsynced the snapshot-and-compact batch, with a comment saying the
  fsync made earlier state-machine batches durable. A mutation run with that
  fsync removed broke no crash test. The comment had the reasoning wrong: WAL
  order alone guarantees the compaction can't outlive the batches it depends
  on. The fsync was removed and D4 was corrected.

## 2026-11-02 — Milestone 7: fault-injection harness

Done:

- `sim.TestRandomizedFaults`: drops, duplicates, delays, symmetric and one-way
  partitions, crashes with torn writes, and snapshots on 3- and 5-node
  clusters, with I1–I7 checked on every seed. Across the default 30 seeds:
  53 crashes, 99 partitions, 79 one-way blocks, 40 snapshot installs and 122
  timed-out operations. 200 seeds passed.
- Named scenario tests for:
  - a minority leader, which accepted 5 proposals and acknowledged none;
  - duplicated, delayed, gapped and overlapping client requests;
  - a one-way partition.
- `internal/e2e`: builds `mlogd`, runs three processes, and keeps three
  producers and two linearizable readers busy through:
  - SIGKILL of the leader;
  - an isolated leader;
  - SIGKILL of a follower.

  Every run checks Porcupine, identical logs, identical replay hashes and
  that every acknowledged write is present. Three consecutive `-race` runs
  passed, each with about 1,050 events and 1,500 checked operations.
- `mlogctl fault`, `load` and `leader`; `scripts/demo-failover.sh` and
  `scripts/demo-partition.sh`.
- `docs/testing.md`: scenario-to-test map, invariants, mutation checks.

Findings:

- Porcupine timed out on one of the first 30 randomized seeds (248
  operations, 60 s). Timed-out appends with an unbounded return time make the
  search explode. `BoundRetries` bounds each by the return of the retry that
  settled it. That's sound because the dedup semantics leave the earlier
  attempt either before that point or with no effect at all. Afterwards, 200
  seeds checked in about 5 s.
- Mutation checks showed the read path was effectively untested. With
  ReadIndex reads answered before applying, the simulator still passed: its
  reads went only to the leader, which applies before answering, and hit
  random offsets far from the tail. Reads now go to random nodes, and half of
  them target the newest offsets. The same mutation then fails Porcupine.
- During the failover demo the leader was killed while batches were in
  flight. The producers retried, and 100 events came back as `duplicate`
  (committed, but the ack was lost). Final count: exactly 40,000, nothing
  missing and nothing doubled.
- The first replay of demo traffic showed 27,000 of 40,000 events rejected.
  Every producer's generator numbered orders from 1, so the producers
  collided on order IDs. Each producer now has its own ID range, and rejects
  are back to the generator's intended ~2%.
- `mlogctl load` failed when started right after the cluster: the
  linearizable producer lookup returned `no_leader` and wasn't retried. The
  client now retries transient read failures, and `cluster.sh start` waits
  for a leader.

## 2026-11-02 — Milestone 8: consumer checkpointing and deduplication

Done:

- `replay.Consumer` follows the log into an order book. It writes a local
  checkpoint file, then commits the offset to the replicated consumer
  checkpoint. On restart it resumes from the local file. Re-delivered offsets
  are skipped, so the state matches exactly-once application.
- `mlogctl consume`. `GET /v1/consumers` and a consumers section in
  `mlogctl status`, showing each consumer's checkpoint and backlog. `replay`,
  `consume` and `load` no longer stop at the CLI's 10 s default timeout.
- Tests:
  - an in-process consumer cancelled at random points 100–130 times per run,
    sometimes with a torn `.tmp` file left behind (5 runs);
  - a lost local state file rebuilds from offset 1;
  - a damaged state file is refused;
  - `mlogctl consume` SIGKILLed 30 times over a 20,000-event log. Every run
    ends with the same hash as a clean replay, and the cluster checkpoint at
    the end of the log.

Bug found: the restart test reached the right state but left the cluster
checkpoint at 5,238 instead of 6,000. A previous run had been killed after
writing the local file for offset 6,000 and before committing it. On restart
the consumer was already at its target, so it exited without committing. The
crash window was described in the design but not handled. The consumer now
commits its local offset at startup whenever the cluster checkpoint is behind.

## 2026-11-02 — Milestone 9: Project 1 integration

Done:

- `integration/cpp/mlog_spool.h`, header-only C++17:
  - CRC-32C and v1 envelope encoding;
  - `SpoolWriter`, which recovers a torn tail and refuses mid-file corruption;
  - `AsyncSpool`, a lock-free SPSC ring with a writer thread that records
    drops as GAP records.
- `spool_test.cpp` passes under ThreadSanitizer, AddressSanitizer and UBSan.
- `example_engine.cpp`, a paced stand-in for the engine.
- `internal/spool`: a Go reader that tails a growing file, and a `Publisher`
  that resumes from the producer's last committed sequence. Because the spool
  position is the producer sequence, spool to log is exactly once.
  `cmd/mlog-publisher` serves backlog metrics.
- Cross-language tests: the C++ engine's spool goes through a publisher
  restarted midway, and the log matches the spool record for record with the
  same replay hash. GAP counts in the log add up to the engine's drop count.
- `scripts/demo-project1.sh`: 1M events at 100k/s from the C++ engine, with
  the leader SIGKILLed midway. The log ended at producer sequence 1,000,000 =
  offset 1,000,000, all three nodes agree, and the publisher drained 26 s
  after the engine finished.
- `docs/integration.md`, the contract for Project 1.

Findings:

- The first writer made one `write()` per record. An unpaced 200,000-event
  burst dropped 132,396 events (reported correctly as GAP records). The
  writer now encodes everything in a drain pass and makes one `write()`. A
  failed write truncates the file back to the last good size and reports the
  batch as a gap. With paced input it sustains 5 M events/s with no drops on
  this machine (page cache, no fsync); drops start around 10 M/s.
- `try_publish`: p50 42 ns, which is the `steady_clock` resolution here; p99
  about 200–375 ns. Rare maxima up to 0.5 ms were not investigated.
- `mlog_publisher_spool_unread_bytes` read 0 while the publisher was hundreds
  of thousands of records behind. It only updated at end of file, which a
  lagging reader never reaches. It now updates after every batch.
- In the demo, the publisher (one producer, sequential batches of 500 into a
  3-node cluster with fsync on macOS) ran at roughly 22–28k events/s, below
  the engine's 100k/s. The spool absorbed the difference, as designed.
  Milestone 10 measures how batch size affects this.
- The demo script compared Prometheus's `1e+06` against `1000000` as strings
  and waited forever.

## 2026-11-02 — Independent review findings and fixes

A review agent with no prior context read the core packages and the raft
library source. It found no lost acknowledged writes and no divergence, and it
independently confirmed the leader-sends-first safety argument. It did find
these:

1. **High: a healthy follower could fail-stop as "corrupt".** A snapshot
   larger than one install batch (16 MiB) is installed in several batches,
   and each one is visible to readers. An HTTP read during the install saw
   the old records deleted and the new ones only partly written, reported
   "offset N missing", and the node shut itself down. Fixed with a seqlock in
   `Store`: installs bump a generation counter at start and end, and reads on
   other goroutines that overlap an install return `ErrUnavailable` (HTTP
   503 `unavailable`, retried by the client) instead of judging the data.
   `TestReadsDuringInstallAreNeverCorrupt` fails without the fix ("offset 113
   missing") and passes with it.
2. **Medium: the C++ spool writer reused sequence numbers after a partial
   write.** On a failed batch write it truncated the whole batch, including
   records written completely, which a tailing publisher could already have
   sent. It now keeps complete records, cuts only the torn one, and counts
   only the unwritten records as lost. The test uses `RLIMIT_FSIZE` to force
   a short write.
3. **Lower:**
   - concurrent receives for the same snapshot index shared one temp file;
     temp names are now unique;
   - snapshot sends had no timeout; they now have 10 minutes;
   - snapshot files raft discarded lingered until restart; they are now
     removed once they are at or below the applied index;
   - timed-out linearizable reads left map entries behind; waiters now
     expire.
4. **G5 gap:** snapshot transfer didn't re-verify each record's CRC, so a
   record that rotted on the leader's disk would have been shipped. The
   sender now verifies every record it streams.
   `TestSnapshotRefusesToStreamCorruptRecords` covers it.
5. **D4 open question:** does WAL rotation keep the prefix property? Checked
   in Pebble's source: `rotateWAL` closes, and so syncs, the old WAL before
   creating the new one. Recorded in D4.

## 2026-11-02 — Milestone 10: performance analysis and final documentation

Done:

- Benchmarks:
  - storage microbenchmarks (batch size × fsync);
  - `bench/fsyncprobe`;
  - the configuration × batch matrix (standalone, 1/3/5-node Raft, fsync on and
    off);
  - a concurrency sweep;
  - failover time;
  - catch-up via log and via snapshot;
  - storage growth and write amplification;
  - replay and consumer throughput;
  - publisher throughput by batch size.

  All of these run on macOS. A manual workflow runs the microbenchmarks, the
  probe and the replicated matrix on GitHub's Linux runners.
- `docs/benchmarks.md` with every parameter and the raw outputs in
  `bench/results/`. Final README.
- Fresh clone of the public repo: builds, `go test -short` passes, a 3-node
  cluster starts and verifies.

Findings, in the order they happened:

- A synced commit costs ~4.2 ms on this Mac regardless of size: Pebble →
  `File.Sync` → `F_FULLFSYNC`. On Linux runners it costs ~0.2–0.28 ms
  (`fdatasync`).
- A single-producer 3-node commit took ~9 ms. The fsync probe showed why: this
  SSD serializes cache flushes across processes (total ~250/s for 1–5 writers),
  so the leader's and followers' flushes can't overlap.
- Standalone group commit averaged 3.9 commands per fsync with 8 closed-loop
  clients (measured with a new `mlog_commit_group_size` histogram). Left as is
  and documented; tuning a wait for a laptop's 4 ms flush would be tuning for
  the wrong machine.
- Two changes for replicated latency:
  - the leader sends before persisting;
  - all queued input is drained before each Ready.

  On macOS they made no measurable difference, as the probe predicts. On
  Linux, on the one pair of runs with the same CPU model, they gave +6% to +21%.
  Runner hardware alone varied the same code's throughput by up to 2.2×, so
  the comparison is indicative only.
- The first attempt at a clean macOS microbenchmark ran while the cluster
  matrix was still running, contending for CPU and fsyncs. Those results were
  discarded and both were rerun alone.
- Letting every node send before persisting passed every test. The
  simulator's crashes happened between steps, never between "sent" and
  "persisted". The new `CrashAtSendRate` fault cuts power at the moment of
  sending. The unsafe mutation now fails, and the correct code survives 1,064
  such cuts across 40 seeds.
- Publisher: batches of 500 drained at 42k events/s; 2,000 or more reached
  110k+. Default raised to 5,000.
- A Porcupine check once timed out (60 s) under `-race` on a crash-at-send
  history. Typical checks take under 1 s. The budget was raised to 3 minutes;
  a timeout still fails the test.

## 2026-11-02 — After milestone 10: membership changes

Why: D10 left no way to replace a node whose disk lost data. Restarting the
old ID with an empty disk is unsafe (it has forgotten its vote), so a cluster
that lost one node's disk was stuck one failure closer to unavailability.

Done:

- Single-voter add and remove through the leader. Member URLs ride in the
  conf change and are stored atomically with the applied index.
- `--join` for a node with an empty data directory. The cluster ID is stored
  at bootstrap and given to joining nodes.
- The transport adds and removes peers at runtime. A removed node stops.
- `mlogctl member list/add/remove`. `add` prints the command to start the new
  node.
- Tests:
  - simulator replacement (20 seeds, half with loss/dup/delay) and leader
    removal;
  - in-process HTTP replacement;
  - the pending-change refusal;
  - multi-process replacement with real `--join`;
  - a demo script.

Found along the way:

- Raft silently turns a second conf change into a no-op while one is pending,
  but the API was answering "proposed". It now refuses with an explicit
  error.
- My first HTTP test tried to show that refusal by proposing a second change
  right after the first. The first had already applied, so the test added a
  real bogus member and then failed on the member count. The refusal is now
  tested directly against the engine with back-to-back calls.
- The demo script died silently on `mlogctl status | head -4`. `head` closing
  the pipe is fatal under `pipefail`, the same mistake as in the benchmark
  script.
