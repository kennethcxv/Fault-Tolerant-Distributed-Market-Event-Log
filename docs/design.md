# Design: guarantees, failure model, architecture

Status: written in milestone 1, before any implementation. Every guarantee below
names the kind of test that will back it; the README links the actual tests as
they land. If a later milestone changes a decision, this file and
[decisions.md](decisions.md) change with it.

## 1. What the system is

A replicated, append-only log for market events (order-book updates, trades,
status messages) and a replay engine that rebuilds application state from the
log deterministically.

Producers append batches of events. A Raft group of 3 or 5 nodes orders them,
assigns each a dense 64-bit offset, and stores them durably. Consumers read by
offset, commit checkpoints, and replay.

Out of scope: the latency-critical market-data path (Project 1 publishes to the
log asynchronously, see §8), sharding across multiple Raft groups,
geo-replication, authentication and TLS, and runtime membership changes (D10).

## 2. Event lifecycle

These states are distinct. The API never reports one as another.

| State | Meaning |
|---|---|
| Received | The leader parsed and validated the request. Nothing is stored. |
| Proposed | The leader appended the batch to its in-memory Raft log. Lost if the leader crashes or loses leadership before replicating it. |
| Persisted (node N) | The Raft entry is written to N's storage and fsynced (with `--fsync=always`). |
| Replicated | Persisted on at least one node besides the leader. Not yet safe. |
| Committed | Persisted on a majority of voters, in the leader's current term. Raft guarantees it will not be lost or reordered while a majority keeps its storage. |
| Applied (node N) | N's state machine processed the entry: dedup decided, offsets assigned, records written, applied index advanced, all in one atomic batch. Readable on N. |
| Acknowledged | The leader replied success. This happens only after the entry is applied on the leader, so acknowledged implies committed. |

An append that times out, or whose leader steps down while it is pending, has an
**unknown** outcome: it may still commit later. The server reports this as
`outcome_unknown`, never as failure. Clients resolve it by retrying with the same
producer sequence numbers; the retry is deduplicated (G4).

## 3. Event envelope, format v1

| Bytes | Field | Notes |
|---|---|---|
| 1 | format version | envelope layout, currently 1 |
| 1 | schema version | payload schema for this event type; set by the producer |
| 2 | event type | application-defined; 0 invalid; 0xFF00–0xFFFF reserved for system events |
| 4 | payload length | at most 1 MiB |
| 8 | offset | dense, starts at 1, assigned when the entry is applied |
| 8 | producer id | |
| 8 | producer sequence | per producer, starts at 1, contiguous |
| 8 | source timestamp | producer clock, Unix ns; informational |
| 8 | ingest timestamp | leader clock when the batch was proposed, Unix ns; informational |
| N | payload | opaque to the log |
| 4 | CRC32C | Castagnoli, over every preceding byte |

Little-endian, fixed 48-byte header. Project 1's spool file uses the same
encoding with offset 0 (§8).

Only the offset defines order. Timestamps are metadata. The leader writes the
ingest timestamp into the Raft entry, so every replica stores the same value;
reading the clock at apply time would make replicas diverge.

## 4. Failure model

Assumed:

- Processes crash and recover. A node can stop at any instant and restart with
  whatever its disk retained.
- The disk honors fsync. Data fsynced before a crash is readable after restart;
  unsynced data may be lost or torn.
- The network is asynchronous. Messages can be lost, delayed, duplicated and
  reordered; partitions can be arbitrary and asymmetric.
- Clocks drift and jump. Safety never depends on them; timeouts affect liveness
  only.

Tolerated: up to f crashed or partitioned nodes in a cluster of 2f+1 (1 of 3,
2 of 5).

Detected, not tolerated automatically: silent disk corruption such as bit flips
and truncated files. The record CRC and Pebble's block and WAL checksums detect
it. The node stops serving instead of returning bad data. Recovery is an
operator action (D10). A disk that lost acknowledged Raft entries is caught at
whichever point comes first:

- at startup, if the hard state commits past the end of the log;
- or by raft itself, when the leader commits an entry the node no longer has.

Either way the node stops with an explanation. Its peers keep running
(`TestTruncatedRaftLogIsRefusedAtStartup`, `TestTruncatedWALOnFollowerIsDetected`).

Not handled:

- Byzantine nodes, and network corruption that TCP checksums miss.
- A disk that acknowledges fsync and later loses the data. This breaks Raft's
  assumptions, and nothing here can detect it before it causes harm.
- More than f nodes permanently losing storage. Committed data can be lost.

## 5. Guarantees

Each holds under §4 with `--fsync=always`.

- **G1 Durability.** An acknowledged append survives any sequence of crashes,
  restarts and partitions, provided at most f nodes permanently lose storage.
- **G2 Agreement.** Two replicas never hold different records at the same
  offset. Every replica's log is a prefix of the committed log.
- **G3 Density and immutability.** Offsets run 1..N with no gaps. A record never
  changes once applied.
- **G4 Producer idempotence and order.** Each producer's sequence numbers appear
  in the log at most once, in increasing order, without gaps. A retried batch is
  applied once. A batch that would skip a sequence is rejected with
  `sequence_gap`, not applied.
- **G5 Integrity.** A record that fails its CRC is never returned as valid data
  by the API, the replay engine, or snapshot transfer.
- **G6 Linearizability.** Appends are linearizable. Reads are linearizable when
  requested (`linearizable=true`, served via ReadIndex). Default reads come from
  the local replica: a committed prefix that may be stale.
- **G7 Deterministic replay.** Replaying offsets 1..N yields the same state hash
  on every replica and every run. The same holds when replay starts from a
  checkpoint at K and continues with K+1..N.
- **G8 Checkpoints.** Consumer checkpoints go through Raft, so they survive the
  same failures as events. They only move forward.

Liveness needs a majority that is up, connected, and responsive enough to
finish an election. A minority partition makes no progress; its appends time
out unacknowledged.

Not guaranteed:

- Exactly-once delivery to consumers. Delivery is at-least-once, and consumers
  deduplicate by offset. The replay engine does this.
- Any order between producers other than the order the log assigns.
- G1 under `--fsync=never`. That mode exists to measure what fsync costs. Power
  loss can drop acknowledged entries. A plain process crash may too, because
  Pebble buffers unsynced WAL writes in user space; milestone 5 will test which
  is the case.

## 6. Invariants the fault-injection harness checks

After each run, across every replica:

- **I1** Records at common offsets are byte-identical (G2).
- **I2** Every acknowledged append is at its acknowledged offset with its
  original payload (G1).
- **I3** Offsets are dense. Per-producer sequences are unique, increasing and
  contiguous (G3, G4).
- **I4** Every stored record passes its CRC (G5).
- **I5** The applied index never decreases, and it is written in the same batch
  as the records it covers.
- **I6** The history of appends and linearizable reads is linearizable,
  according to Porcupine (G6).
- **I7** Order-book replay gives the same hash on every replica (G7).

## 7. Architecture

```
      producers / publisher                     consumers / replay
               │ HTTP                                  ▲ HTTP (JSON or raw records)
               ▼                                       │
┌───────────────────────────────── node ──────────────────────────────────┐
│ HTTP handlers ── proposals, reads ──► core loop (one goroutine)         │
│      ▲                                  owns raft.RawNode               │
│      └──────────── results ◄──────────  for each Ready:                 │
│                                           1. persist snapshot, entries, │
│ peer transport ◄──── messages ──────────     hard state (fsync)         │
│ (goroutine per peer)                      2. send messages              │
│      │                                    3. install snapshot, apply    │
│      ▼                                       committed entries          │
│ other nodes                               4. Advance                    │
│                                                   │                     │
│                              storage: one Pebble DB                     │
│                              [ raft log | event store | metadata ]      │
└─────────────────────────────────────────────────────────────────────────┘
```

Packages and responsibilities:

| Package | Owns |
|---|---|
| `event` | Envelope encoding, decoding and CRC |
| `storage` | All Pebble access: keyspace, atomic batches, raft-log persistence, snapshot export and import |
| `fsm` | Deterministic apply of committed commands: dedup, offsets, checkpoints |
| `replica` | Drives `raft.RawNode`. No goroutines, injected clock. The server and the test harness share it. |
| `server` | Goroutines, HTTP API, peer transport, metrics |
| `sim` | Deterministic cluster harness for tests |
| `orderbook`, `replay` | Deterministic consumer and replay engine |

**Threading.** One goroutine owns the RawNode, which is not thread-safe. HTTP
handlers send work over channels and wait on a per-request result channel. The
loop drains every pending proposal before handling the next Ready, so concurrent
requests share one fsync (group commit). If the node loses leadership, it fails
its pending requests immediately with `outcome_unknown` instead of letting them
time out.

**Backpressure.** The leader caps uncommitted bytes in Raft. Past the cap,
appends fail at once with `overloaded` instead of queueing without bound.

**Ready ordering** follows etcd-io/raft's contract: persist, then send, then
apply, then Advance. Because a node sends messages only after its log is
durable, a follower never acknowledges an entry it could lose in a crash.

**Storage keyspace** (one Pebble DB):

| Key | Value |
|---|---|
| `e` + be64(offset) | event record |
| `m/applied` | applied Raft index |
| `m/last` | last offset |
| `m/confstate` | Raft ConfState as of the applied index |
| `m/p/` + be64(producer) | last sequence and last offset for the producer |
| `m/c/` + name | consumer checkpoint |
| `r/hs` | Raft HardState |
| `r/snap` | Raft snapshot metadata |
| `r/e/` + be64(index) | Raft entry |

`e` and `m/` make up the state machine; `r/` is the Raft log. Both live in one
DB, so they share one WAL. Pebble replays a WAL prefix on recovery, so once a
later batch is durable every earlier batch is too. State-machine batches
therefore skip fsync: the next synced Raft batch covers them. D4 lists what this
depends on.

Two restart details come from etcd-io/raft's API:

- `raft.Config.Applied` must not exceed the persisted commit index, or raft
  panics. After a snapshot install, the state machine can be ahead of the local
  commit index. So the node passes `min(applied, commit)`, and apply skips any
  entry at or below the state machine's applied index.
- `MemoryStorage` restores membership only from a snapshot. Membership is
  therefore stored in `m/confstate`, in the same batch as the applied index, and
  given to raft on restart.

**Snapshots and compaction.** Every K applied entries (`--snapshot-entries`),
a node records a Raft snapshot at its applied index and drops log entries older
than the index minus a retention margin (`--snapshot-keep`). The snapshot's
data is the state machine itself.

When a follower falls behind the leader's first log index, raft asks to send a
snapshot. The leader then streams its event store from a consistent Pebble
snapshot taken at its current applied index A. That can be newer than raft's
own snapshot S, so the leader rewrites the outgoing message's metadata to A,
A's term and the membership as of A. Every entry up to A is committed, so this
is a valid snapshot. It also keeps the follower's raft log and state machine at
exactly the same index after installation.

The follower writes the stream to a file and fsyncs it before handing the
snapshot to Raft. It deletes the file only once installation commits, and it
detects a partial install because the applied index is written last. A crash
mid-install is recovered at startup by installing again from the file. An
install costs O(log size), the main scaling limit (D6).

**Reads.** Default reads are served by any node from its local committed
prefix. With `linearizable=true`, the node gets a read index from the leader
(ReadIndex), waits until it has applied that index, then reads locally. There
are no leader leases, because leases depend on bounded clock drift.

## 8. Outages and the Project 1 contract

Raft stays off Project 1's hot path. The engine hands each event to a
non-blocking single-producer/single-consumer ring. A background thread writes
the ring to a local spool file in the §3 encoding, using the spool position as
the producer sequence. A separate publisher process tails the spool and appends
to the cluster. After a restart, the publisher asks the cluster for its last
committed sequence and resumes from the next one. G4 makes any resent batches
harmless.

| Situation | Engine | Publisher | What still holds |
|---|---|---|---|
| Log falls behind | Unaffected | Backlog grows; `publisher_backlog_events` rises | Events are durable in the local spool. Replicated durability lags by the backlog. |
| Quorum lost | Unaffected | Appends time out; it retries the same batch with backoff | No new commits. Nothing uncommitted is acknowledged. The spool keeps growing. |
| Log unreachable | Unaffected | Connection errors; same retry loop | Same as above |
| Ring full (spool writer can't keep up) or spool disk full | `try_publish` returns false and a drop counter increments; the hot path never blocks | Once possible, writes an explicit GAP system record with the number of events lost | The log records that events are missing instead of hiding it |

During an outage, the only copy of new events is the spool on the engine host.
That disk is a single point of failure until the backlog drains.
