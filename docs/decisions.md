# Decision log

Each entry records what was chosen, what was rejected, and what it costs. Entries
are added when a decision is made and amended, never silently rewritten, when
measurements or bugs change it.

## D1. Go for the service

Chosen for its networking, goroutines, race detector, and because the two
libraries this design depends on (etcd-io/raft, Pebble) are Go. C++ stays in
Project 1, where its latency control matters.

## D2. Consensus: etcd-io/raft, driven through `RawNode`

We use `go.etcd.io/raft/v3` (v3.7.0). The library provides leader election, log
replication, commit tracking, ReadIndex, PreVote and CheckQuorum. We write
everything around it: storage, transport, the state machine, snapshot transfer,
and the Ready loop.

`RawNode` rather than `Node`: `Node` runs its own goroutine, while `RawNode` is
driven synchronously. That lets the production server and the deterministic
test harness run the same replica code, with the harness controlling every tick
and message delivery.

Rejected:

- hashicorp/raft. It bundles its own log store, snapshot store and transport,
  which hides the persistence ordering this project is supposed to get right
  and test.
- dragonboat. It is built for many Raft groups per process, which is more
  machinery than one group needs.
- Writing Raft ourselves. A subtle consensus bug would undermine every other
  claim here.

Cost: etcd-io/raft draws election-timeout jitter from `crypto/rand`, so the test
harness cannot make Raft's internal timing reproducible from a seed (D13).

## D3. Storage engine: Pebble (`cockroachdb/pebble/v2` v2.1.7)

We need atomic multi-key writes: a batch of records, the applied index, producer
dedup state and membership must land together or not at all. Pebble provides
that with batches. It checksums WAL records and sstable blocks. Its crash
recovery is exercised heavily by CockroachDB. And it ships a crash-simulating
in-memory filesystem (`vfs.NewCrashableMem` + `CrashClone`) that keeps only
fsynced data, which our crash tests use.

Rejected:

- bbolt. Its copy-on-write B+tree costs an fsync and page rewrites per
  transaction, and it has a single writer.
- Badger. Its value-log garbage collection is another failure surface.
- RocksDB via cgo. It complicates builds and CI for no capability we need.
- A custom segment-file log, Kafka-style. **This is probably the better fit for
  append-only data**: an LSM tree pays compaction write amplification on keys
  that are never updated. We still chose Pebble because the atomic metadata
  updates and tested crash recovery would cost far more to build and verify by
  hand. Milestone 10 measures how much the write amplification actually costs.

## D4. One Pebble DB for the Raft log and the state machine

The Raft log batch is fsynced. State-machine batches are not, in replicated
mode. Pebble has one WAL and recovers a prefix of it, so making a later Raft
batch durable makes every earlier state-machine batch durable too. That saves
one fsync per Ready.

This depends on Pebble writing a single WAL and recovering a prefix of it.
WAL failover must stay disabled.

WAL rotation doesn't break the prefix property. I checked Pebble v2.1.7's
source: `DB.rotateWAL` closes the previous log before creating the next one,
and `LogWriter.Close` fsyncs it. Pebble's comment says this ordering is
deliberate, so that Open never sees two logs with unclean tails. So a
batch in a newer WAL can only become durable after everything in older WALs
already is.

Raft-log compaction is safe for the same reason. The compaction batch enters
the WAL after the state-machine batches for the entries it deletes, so it can
only survive a crash if they do. The design first fsynced the compaction batch
"to make earlier batches durable". Milestone 6 showed that fsync was
unnecessary: removing it broke none of the crash tests with compaction, and the
WAL-order argument explains why. It was removed.

Rejected: separate DBs, which need two fsyncs or explicit sync points before
compaction. Also rejected: fsyncing every state-machine batch, which costs an
extra fsync and buys nothing under this ordering.

## D5. Offsets assigned at apply time; ingest timestamp set by the leader

Every replica applies the same committed entries in the same order, so offsets
assigned at apply time are identical everywhere with no coordination. If the
leader assigned offsets at proposal time, two leaders in different terms could
hand the same offset to different uncommitted batches, and that would need
reconciling. For the same reason, anything nondeterministic, such as the clock,
is captured in the entry by the leader and never read during apply.

## D6. A snapshot is the streamed state machine

We compact the Raft log after applying. A follower that needs compacted entries
gets the leader's whole event store, streamed from a consistent Pebble snapshot.
This is simple and reuses the storage format, but each install costs O(total log
size).

Rejected:

- Shipping immutable segment files by reference. It scales better but needs the
  segment store from D3's rejected option.
- Never compacting, with the Raft log serving as the event store. Storage
  doubles and compaction is a stated requirement.

We accept this limit at the scale this project runs at, and the benchmarks
report snapshot install time.

## D7. Idempotent producers with strict sequence contiguity

Each producer numbers its events 1, 2, 3, … The state machine stores each
producer's last applied sequence. A batch entirely at or below it is a duplicate
and is acknowledged without being written again. A batch that overlaps it has
its already-applied prefix skipped. A batch that starts past last+1 is rejected
with `sequence_gap`.

Rejected:

- A dedup window of recent request IDs. It bounds memory but cannot detect
  gaps, and it stops deduplicating retries that arrive after the window.
- No dedup, i.e. at-least-once appends. Every client timeout could then put
  duplicates into the permanent log.

Cost: a producer that loses its own sequence state must take a new producer ID.
The publisher avoids this by deriving sequences from spool positions.

## D8. Writes go to the leader; followers redirect

Followers reject appends with `not_leader` and a leader hint
(`DisableProposalForwarding`). With forwarding, a proposal can be silently
dropped during a leader change, and the client learns of it only by timeout.
Redirecting makes the failure explicit and lets clients cache the leader. Cost:
one extra round trip after a leader change.

## D9. Reads: local prefix by default, ReadIndex when asked, no leases

Committed records never change, so a local read is always a correct prefix, just
possibly not the newest one. Linearizable reads use ReadIndex, which costs a
heartbeat round. Lease reads were rejected because they depend on bounded clock
drift, which §4 does not assume.

## D10. Static membership

The cluster's 3 or 5 members are fixed at bootstrap. Runtime add and remove is
not implemented.

The consequence matters. A node that loses or corrupts its disk must not rejoin
under its old ID with empty storage: it has forgotten its vote and could vote
twice in one term. The safe procedure is to remove it and add a fresh member,
which needs a membership change. Until that exists, such a node stays out and
the cluster runs with one less failure of headroom. **Open question for review.**

## D11. HTTP/1.1 for clients and peers

The client API is JSON for control operations and raw concatenated records for
bulk reads. Peer messages are protobuf `raftpb.Message`s, batched per POST over
keep-alive connections, one sender goroutine per peer. Snapshots stream in a
separate request.

Rejected:

- gRPC. Code generation and dependencies for no correctness benefit.
- Hand-framed TCP. Faster, but more code to get right.

Milestone 10 measures whether the transport is a bottleneck before anything is
changed. There is no TLS or authentication, so run only on a trusted network.

## D12. fsync policy: `always` by default, `never` for measurement only

`always` fsyncs each Raft persistence batch before any message or reply depends
on it. Raft's safety argument requires this. `never` exists to measure the cost
of fsync and is documented as voiding G1. Every benchmark states the platform's
fsync behavior, because macOS and Linux differ a lot.

## D13. Testing: deterministic simulator plus Porcupine

A test-only cluster simulator drives N `replica`s with:

- virtual time;
- a seeded scheduler that drops, delays, duplicates and partitions messages;
- Pebble's crash-simulating filesystem, so a crash loses unsynced data.

Client histories are checked for linearizability with Porcupine
(anishathalye/porcupine). Invariants I1–I7 are checked after every run.

Limitation: a seed reproduces the fault schedule and message scheduling, but not
Raft's internal election jitter (D2). Every failing run therefore saves its full
trace, so it can be diagnosed without being reproduced. Nothing here is formal
verification. Jepsen-style testing against real processes would complement this
and is not done.

## D14. Project 1 integration through a spool file and a publisher process

The engine writes to a local spool through a non-blocking ring. A separate
publisher tails the spool into the cluster (design §8).

Rejected:

- The engine calling HTTP from a background thread with an in-memory buffer.
  Events are lost if the engine crashes during an outage, and memory bounds the
  backlog.
- Embedding a log client that waits on commits. That violates the architectural
  constraint.

Cost: the spool host is a single point of failure until the backlog drains, and
there is one more process to run.

## D15. Reject peer traffic from nodes started with a different peer list

Added in milestone 4. Each node hashes its `--peers` map into a cluster
fingerprint. Every peer request carries it, and a node rejects requests whose
fingerprint differs from its own (HTTP 409, logged as an error).

Bootstrap membership comes from that flag, so a node started with a different
list would form a different initial configuration. The simulator showed what
that looks like: a harness bug bootstrapped nodes with peer sets {1}, {1,2}
and {1,2,3}, and raft panicked when the leader decided a follower needed a
snapshot that didn't exist. etcd guards against the same mistake with a
cluster ID.
