# Benchmarks

Every number here was produced by a script in `bench/` or a Go benchmark, and
the raw output is in `bench/results/`. They were measured on 2026-11-02. The
main thing they show is that **fsync decides almost everything**, and how
expensive an fsync is depends on the platform. A laptop result and a Linux
server result answer different questions.

## Setup

| | macOS (primary) | Linux (GitHub Actions) |
|---|---|---|
| Machine | Apple M4 Max, 16 cores, 128 GB, internal SSD | `ubuntu-latest` shared VM, 4 vCPU (AMD EPYC 7763 / 9V74 / 9V45 or Intel Xeon, varies by run), ext4 |
| OS | macOS 27.0.1 | Linux 7.0 (azure kernel) |
| Go | 1.27.3 | 1.27.3 |
| What Pebble's sync calls | `F_FULLFSYNC` (flushes the drive cache) | `fdatasync` |
| Raw sync cost (`bench/fsyncprobe`, 1 writer) | 4.6 ms | 0.28 ms |

Every node, the load generator and the network (loopback) share one machine.
`mlogctl load` runs closed-loop producers. Each sends one append, waits for
the result, then sends the next. Events are synthetic schema-v1 order-book
messages with 12–36 byte payloads (records of 64–88 bytes). Latency is
measured at the client for the whole HTTP append, through commit and apply on
the leader. Unless stated otherwise: 8 producers, `--fsync always`, 100 ms
Raft tick, snapshot every 10,000 entries.

Reproduce:

```sh
go test -run '^$' -bench . ./internal/fsm ./internal/replay ./internal/event   # microbenchmarks
go run ./bench/fsyncprobe -dir /path/on/the/disk
bench/cluster.sh [OUTDIR]            # configuration × batch matrix (QUICK=1: replicated cases only)
bench/failover.sh; bench/recovery.sh; bench/publisher.sh
gh workflow run bench.yml            # the Linux numbers
```

The "before" Linux runs used the `bench-baseline` branch: the server code at
`dcf5242` (before §7's items 2 and 3) plus the current benchmark scripts.

## 1. The storage write path (no Raft, no network)

`BenchmarkApply`: one command of N 64-byte events per state-machine apply, on
the real filesystem (`bench/results/macos-microbench.txt`,
`linux-gha-run2/bench-linux.txt`).

| Events per synced commit | macOS events/s | Linux events/s |
|---|---|---|
| 1 | 238 | 4,111 |
| 10 | 2,325 | 39,954 |
| 100 | 22,644 | 516,577 |
| 1,000 | 188,235 | 1,109,296 |
| 1,000, **no fsync** | 693,796 | 1,017,874 |

On macOS a synced commit costs about 4.2 ms however small it is, so
throughput is the batch size divided by the cost of one flush until CPU takes
over, at around 1,000 events per commit. Without fsync the path costs about
2.6 µs per command plus about 1.3 µs per event. That covers encoding, CRC,
the Pebble batch and the memtable. On Linux a sync costs about 0.2 ms, so the
synced and unsynced paths converge at around 1,000 events per commit.

The first of two Linux runs measured 6.8 ms per synced commit at batch 1,
slower than at batch 10. The second run, on different hardware, measured
0.24 ms. I treat the first as runner noise; both are kept in
`bench/results/linux-gha-run1` and `linux-gha-run2`.

## 2. Why a laptop cluster can't parallelize fsync

`bench/fsyncprobe` runs N writers, each with its own file, each doing
write-then-sync in a loop.

| Writers | macOS total syncs/s | per-writer sync time | Linux total syncs/s | per-writer sync time |
|---|---|---|---|---|
| 1 | 218 | 4.6 ms | 3,569 | 0.28 ms |
| 2 | 230 | 8.7 ms | 6,430 | 0.31 ms |
| 3 | 248 | 12.1 ms | 8,112 | 0.37 ms |
| 5 | 329 | 15.2 ms | 10,903 | 0.46 ms |

The Mac's SSD serializes cache flushes. Three nodes on one disk get roughly
250 flushes per second between them, and each node's sync gets slower as the
others sync. On the Linux VM, syncs mostly overlap. This one table explains
most of the cluster results below.

## 3. Cost of replication (macOS)

`bench/results/baseline/summary.txt` (the code before the two changes in §7).
All rows use 8 producers.

| Configuration | batch 1 | batch 10 | batch 100 | batch 1000 |
|---|---|---|---|---|
| standalone, fsync always | 735 / 8.7 ms | 7,129 / 9.0 ms | 74,703 / 8.7 ms | 380,138 / 14 ms |
| standalone, fsync never | 44,637 / 0.13 ms | 250,379 / 0.22 ms | 637,901 / 0.56 ms | 737,059 / 2.5 ms |
| 1-node Raft, always | 952 / 8.5 ms | 9,139 / 8.5 ms | 82,671 / 8.6 ms | 321,169 / 11 ms |
| 3-node Raft, always | 255 / 32 ms | 2,418 / 33 ms | 22,028 / 34 ms | 106,877 / 43 ms |
| 3-node Raft, fsync never | 15,051 / 0.42 ms | 128,689 / 0.43 ms | 262,792 / 0.86 ms | 198,396 / 5.4 ms |
| 5-node Raft, always | 200 / 39 ms | 1,877 / 42 ms | 15,713 / 47 ms | 68,461 / 73 ms |

Each cell is events/s and p50 append latency.

What these show:

- **Raft itself is cheap; fsync is not.** A 1-node Raft cluster matches the
  standalone path, even though it writes every event twice (once in the Raft
  log, once in the event store).
- **Replication on one disk costs about 3× throughput** (2.9–3.6×). Each commit needs
  flushes on at least two nodes, and the disk serializes them (§2).
- **Larger batches stop helping somewhere between 100 and 1,000 events.**
  With fsync off, a 3-node cluster peaks at 263k events/s with batches of
  100. At 1,000 it falls to 198k events/s, and p99 reaches 368 ms, because
  very large entries go through the HTTP transport and the follower queues.
- **There is a latency ceiling.** No configuration gets p50 below about
  8.5 ms with fsync on, on this disk: two serialized flushes. On Linux a
  single producer sees p50 ≈ 1.1–1.3 ms on a 3-node cluster
  (`linux-gha-*/cluster-linux/summary.txt`).

## 4. Concurrency (3 nodes, batch 100)

| Producers | macOS events/s, p50, p99 | Linux (EPYC 7763) events/s, p50, p99 |
|---|---|---|
| 1 | 9,580, 9.5 ms, 20 ms | 68,324, 1.3 ms, 4.0 ms |
| 4 | 14,547, 25 ms, 51 ms | 123,455, 2.8 ms, 8.5 ms |
| 16 | 39,519, 37 ms, 116 ms | 177,992, 7.9 ms, 19.6 ms |
| 64 | 100,454, 47 ms, 168 ms | 230,323, 25.7 ms, 56 ms |

macOS figures are from `leader-parallel-drain`; Linux from
`linux-gha-optimized-run1`. Past a few producers, latency grows faster than
throughput: requests queue behind serialized flushes. On this Mac, the
leader averaged 3 commands per fsync with 8 producers, from the
`mlog_commit_group_size` metric.

## 5. Failover, recovery, storage

**Failover** (`bench/failover.sh`). The leader is SIGKILLed under 4
producers, 5 trials per tick setting.

| Tick | Time until another node is leader | Longest client append latency |
|---|---|---|
| 100 ms (default) | 1.07–3.06 s (median 1.57 s) | 2.3–4.3 s |
| 20 ms | 0.24–0.38 s (median 0.34 s) | 0.66–1.32 s |

Clients stall longer than the election itself takes. Their in-flight request
to the dead leader has to fail first, and the client then retries with
backoff of up to 1 s. A 20 ms tick halves the outage but makes elections more
sensitive to pauses (GC, a slow fsync), so the default stays at 100 ms.

**Catch-up** (`bench/recovery.sh`). A follower is stopped while 1,000,000
events are written, then restarted.

| Path | Time to catch up | Follower disk |
|---|---|---|
| Raft log entries (`--snapshot-entries 0`) | 1.88 s | 91 MB |
| Snapshot install (`--snapshot-entries 200`) | 1.08 s | 71 MB |

Without compaction, every event is stored twice: in the Raft log and in the
event store.

**Storage growth and write amplification.** These come from
`/debug/storage` after the benchmark runs. For 1M events from
`mlogctl load`:

- the leader used 63 MB on disk (about 63 bytes per event, with Pebble's
  default compression, Raft log included);
- the WAL took 125 MB of logical writes (each event goes in once as a Raft
  entry and once as a record);
- Pebble's write amplification (WAL + flush + compaction bytes per logical
  byte) was 1.9–2.5× across the benchmark runs.

The keys are strictly increasing offsets, so compaction rewrites
little. D3 had predicted LSM overhead on append-only data. Measured here,
it's moderate, not severe.

## 6. Reading and replay

| Path | Throughput | Source |
|---|---|---|
| Order book apply, in memory | 12.3 M events/s (81 ns/event) | `BenchmarkBookApply` |
| Replay from Pebble (verified read + decode + apply) | 4.8 M events/s | `BenchmarkReplayFromStore` |
| `mlogctl replay` over HTTP from a 3-node cluster, 1M events | 2.8–3.2 M events/s | raw binary reads, CRC checked at the client |
| `mlogctl consume`, 1M events, checkpoint every 100k | 1.9 M events/s | includes local fsync and replicated commits |
| Envelope encode / decode | 9 ns / 8 ns | `BenchmarkEncode`, `BenchmarkDecode` |

Reads cost much less than writes, so replay is not the bottleneck anywhere.

**C++ hand-off and publisher.** The engine-side `try_publish` costs a p50 at
or below 42 ns (the clock's resolution) and a p99 of 200–375 ns. The spool
writer sustained 5 M events/s with no drops. Details are in
[integration.md](integration.md). The publisher drained a spool into a 3-node
cluster at:

| Publisher batch | Drain rate |
|---|---|
| 100 | 9,780 events/s |
| 500 | 41,569 events/s |
| 2,000 | 111,433 events/s |
| 10,000 | 122,458 events/s |

The default batch was changed from 500 to 5,000 because of this.

## 7. What I changed because of measurements, and what it did

1. **Group-commit convoy (standalone).** I instrumented
   `mlog_commit_group_size` because a 4.2 ms fsync was turning into 8.4 ms
   latency. Eight closed-loop clients averaged 3.9 commands per fsync, not
   8. Clients answered together resend at slightly different times, so the
   first one gets a commit to itself. **Not fixed.** A short wait to fill
   groups would help this benchmark, but it would trade latency for
   throughput based on a laptop's 4 ms flush. On Linux the flush is 0.25 ms
   and the trade would look different.
2. **Leader sends before persisting** (Raft thesis §10.2.1, as etcd does).
   This overlaps the leader's fsync with the followers'. **No measurable
   change on macOS**, because the disk serializes the flushes anyway (§2).
   On Linux it was part of a modest gain (next item). Its safety argument is
   in `replica.Ready`. A mutation in which every node sends before persisting
   is caught by `TestCrashAtSendPreservesCommittedWrites`.
3. **Drain all queued input before each Ready.** This lets proposals and
   peer messages that arrived during an fsync share the next one. Together
   with item 2, measured on the same CPU model (EPYC 7763, one run each): +21%
   at batch 1, +9% at batch 100, +14% with 1 producer, +6% with 64. On
   macOS: 255 → 279 events/s at batch 1, 12.8k → 14.5k events/s with 4
   producers, and essentially no change at 64 producers.
4. **Spool writer: one `write()` per drain pass** instead of one per record.
   Before: an unpaced 200k-event burst dropped 132k events. After: 5 M
   events/s sustained with no drops.
5. **Publisher default batch 500 → 5,000.** Drain rate went from 42k to
   over 110k events/s (§6).

The Linux runs carry a warning. Throughput for the *same* code varied by up
to 2.2× between GitHub runners (64k–139k events/s before the changes, 129k–215k
after, for 3 nodes at batch 100), depending on which CPU and disk the job
landed on. Only results from the same
hardware are compared above, and with one run per side they are indicative
only.

## 8. Where the time goes

- **Writes with fsync on:** the disk flush. On this Mac a three-node commit
  needs at least two serialized ~4.5 ms flushes, so a client sees ~9 ms at
  best. On Linux a commit needs about one 0.25 ms flush plus a loopback round
  trip, so a client sees ~1.3 ms.
- **Writes without fsync:** CPU on one machine:
  - JSON and base64 encoding of the append request;
  - Pebble memtable inserts, with every event written twice;
  - three nodes plus the load generator competing for cores.

  It is not profiled in detail yet. `GET /debug/pprof/profile` exists for
  that.
- **Reads and replay:** not a bottleneck at any measured scale.

Not measured: a multi-machine cluster with real network latency, larger
payloads, and runs long enough to see sustained compaction or snapshot cost
under load.
