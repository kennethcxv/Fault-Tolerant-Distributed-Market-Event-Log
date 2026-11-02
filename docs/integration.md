# Integrating Project 1 (C++ market-data engine)

Project 1 has no event format of its own yet. This document is the contract it
should implement, and `integration/cpp` is a working reference: a header-only
writer, an example engine, and a self-test. CI builds all three. The fixture in
`fixtures/orderbook-v1` pins the payload format and its replayed state, so a
C++ decoder can check itself against it.

## Pipeline

```
 engine thread ──try_publish──► SPSC ring ──► writer thread ──write()──► spool file
 (hot path: copy + atomic store,  (bounded,      (batches every              │
  never blocks or syscalls)        64K slots)     record in a pass)           │
                                                                              ▼
                         mlog-publisher ──HTTP append, producer seq = spool position──► log cluster
                         (separate process; resumes from the producer's
                          last committed sequence after any restart)
```

Raft never runs on the engine's thread. The engine's own guarantees (latency,
book correctness) don't depend on the log being up.

## Wire format

Every spool record is a log record in the v1 envelope (design §3), with
`offset = 0` and `ingest_ts = 0`. The log fills those in. `producer_seq` is the
record's 1-based position in the spool. `producer_id` is fixed per engine
instance. All integers are little-endian, and the CRC is CRC-32C over every
preceding byte of the record.

Schema v1 order-book payloads (event `schema` byte = 1):

| Type | Event | Payload |
|---|---|---|
| 1 | AddOrder | instrument u32, order u64, side u8 (0 bid, 1 ask), price i64 ticks, qty u64 |
| 2 | CancelOrder | instrument u32, order u64 |
| 3 | ExecuteOrder | instrument u32, order u64, qty u64 |
| 4 | ReplaceOrder | instrument u32, order u64, new order u64, price i64, qty u64 |
| 0xFFFF | GAP | lost count u64 (written by the spool writer, never by the engine) |

Order ids must be unique for the life of the log. A replace loses time
priority. A consumer counts messages that don't fit its book (an unknown
order, an overfill) rather than failing on them. A schema version it doesn't
know stops replay, so a format change can't be misread silently.

## Spool file rules

- Append-only. A torn final record (a crash mid-write) is cut off when the
  writer reopens the file, and the sequence continues.
- A corrupt record before the end stops both the writer (it refuses to append)
  and the publisher (it refuses to skip). That is data loss, and it has to be
  dealt with by hand.
- One spool per producer id. Starting over with a fresh spool requires a new
  producer id, because the log rejects sequence numbers that go backwards
  relative to what it has (`sequence_gap`; decision D7).

## Failure behavior

| Situation | Engine | Spool and publisher | Guarantee |
|---|---|---|---|
| Log healthy | `try_publish` copies and returns | writer drains each pass in one `write()`; publisher appends batches of up to 500 | an event is replicated once its batch is acknowledged |
| Log slow or behind | unaffected | spool grows; `mlog_publisher_unacked_events` and `spool_unread_bytes` rise | the events are on the engine host's disk, not yet replicated |
| Quorum lost or log down | unaffected | publisher retries the same batch with backoff and logs a warning (`mlog_publisher_retrying` = 1); never skips | same; nothing is acknowledged that isn't committed |
| Publisher restarts | unaffected | resumes after the producer's last committed sequence; a resent batch comes back `duplicate` | exactly once from spool to log |
| Ring full (writer can't keep up) | `try_publish` returns false; the drop is counted | writer emits a GAP record with the number lost before the next event | the loss is recorded in the log, not hidden |
| Spool write fails (disk full, I/O error) | unaffected | the batch is lost, the file is cut back to its last good size, and the next GAP reports the loss | same |
| Engine host crash | n/a | records already handed to the OS survive a process crash; unsynced ones are lost on power loss unless `fsync_each_batch` is set | design §8: the spool host is a single point of failure until the backlog drains |

## Measured costs

Measured on an Apple M4 Max (macOS 27.0.1, clang 21). The engine thread is
paced with a busy-wait. The spool is written without fsync, so this is the
page cache, not the disk. Each run is 2,000,000 events
(`example_engine SPOOL 7 2000000 RATE`):

| Offered rate | Dropped | `try_publish` p50 / p99 / p99.9 / max |
|---|---|---|
| 1 M events/s | 0 | 83 ns / 375 ns / 416 ns / 22 µs |
| 2 M events/s | 0 | 42 ns / 333 ns / 375 ns / 195 µs |
| 5 M events/s | 0 | 42 ns / 209 ns / 292 ns / 532 µs |
| 9.8 M events/s (unpaced) | 125,679 (6.3%) | 42 ns / 208 ns / 250 ns / 15 µs |

42 ns is the resolution of `steady_clock` on this machine, so p50 is "at or
below one tick". The maxima are rare stalls I didn't investigate: OS
preemption of the busy-waiting thread is the likely cause, not the ring.
Before the writer batched its writes, one `write()` per record, an unpaced
200,000-event burst dropped 132,396 events. Batching each drain pass into one
`write()` is what made 5 M events/s sustainable.

These figures measure the C++ hand-off only. How fast the log itself absorbs
events is in [benchmarks.md](benchmarks.md).

## Running it

```sh
c++ -std=c++17 -O2 -pthread -o engine integration/cpp/example_engine.cpp
bin/mlog-publisher --spool engine.spool --producer 7 --metrics-listen 127.0.0.1:9101 &
./engine engine.spool 7 1000000 100000      # 1M events at 100k/s
scripts/demo-project1.sh                     # the same, against a 3-node cluster with a leader kill
```

The tests behind this document:

- `internal/spool` (Go reader and publisher; `TestCppSpoolToLog` compiles the
  C++ engine and checks that the log equals the spool record for record);
- `TestCppDropsBecomeGapRecords`;
- `integration/cpp/spool_test.cpp`, which runs under ThreadSanitizer in CI.
