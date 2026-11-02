# Testing

## Running

```sh
go test ./...                    # everything, about a minute; includes the multi-process test
go test -race ./...              # what CI runs
go test -short ./...             # skips the multi-process test
MLOG_SIM_SEEDS=500 go test ./internal/sim -run TestRandomizedFaults   # more simulator seeds
```

When a simulator or end-to-end check fails, it writes a trace and Porcupine's
HTML visualization of the history to `$MLOG_SIM_ARTIFACTS` (default
`$TMPDIR/mlog-sim-failures`). CI uploads that directory as an artifact.

## Layers

| Layer | Where | What is real | What is simulated |
|---|---|---|---|
| Unit | `internal/{event,storage,fsm,orderbook,replay}` | everything | nothing |
| Simulator | `internal/sim` | replica, raft, storage, state machine | time, network, power loss (Pebble's `CrashableMem`) |
| In-process cluster | `internal/server/*_test.go` | HTTP transport, disks, goroutines | nodes share one process |
| Multi-process | `internal/e2e` | `mlogd` binaries, `SIGKILL`, real sockets | partitions are application-level drops (`/debug/fault`) |

## Required failure scenarios

| Scenario | Tests |
|---|---|
| Leader crash during a write | `sim.TestLeaderCrashDuringWriteTruncatesUncommittedEntries`, `e2e.TestMultiProcessFaults` (SIGKILL under load) |
| Network partition and recovery | `sim.TestIsolatedLeaderIsReplaced`, `sim.TestOneWayPartitionLeaderCannotHearFollowers`, `sim.TestRandomizedFaults`, `e2e.TestMultiProcessFaults` |
| Delayed and duplicated client requests | `sim.TestDelayedAndDuplicatedClientRequests`; message-level duplication and delay in `sim.TestRandomizedFaults` |
| Minority partition attempting writes | `sim.TestMinorityLeaderCannotAcknowledgeWrites`, `server.TestQuorumLossBlocksWritesUntilRecovery`, `scripts/demo-partition.sh` |
| Node restart with persisted state | `sim.TestWholeClusterPowerLoss`, `sim.TestRandomCrashRestartWithTornWrites`, `server.TestLeaderStopElectsNewLeaderAndOldLeaderCatchesUp` |
| Snapshot installation and replay | `sim.TestLaggingFollowerCatchesUpViaSnapshot`, `sim.TestCrashDuringSnapshotInstall`, `server.TestRestartedFollowerCatchesUpViaSnapshot` |
| Corrupted or truncated storage | `storage.TestReadDetectsCorruption`, `sim.TestTruncatedRaftLogIsRefusedAtStartup`, `server.TestCorruptRecordOnFollowerStopsOnlyThatNode`, `server.TestTruncatedWALOnFollowerIsDetected` |
| Consumer restart from checkpoint | `replay.TestConsumerRestartsReachSameState` (100+ random kills, torn temp files), `e2e.TestConsumerProcessSurvivesSIGKILL` (30 SIGKILLs of `mlogctl consume`), `replay.TestConsumerRebuildsWhenLocalStateIsLost`, `replay.TestInterruptedReplayResumesToSameHash` |

## Invariants

The simulator checks all seven after every run, and the multi-process test
checks I1, I2, I3, I6 and I7.

| Invariant | Check |
|---|---|
| I1 replicas byte-identical | `Cluster.CheckAgreement`; `/v1/verify` hashes in e2e |
| I2 acknowledged appends present | `Workload.CheckAcked` |
| I3 dense offsets, each producer sequence once and contiguous | `Workload.CheckAcked` |
| I4 every record passes CRC | every read goes through `storage.ReadRaw` |
| I5 applied index atomic with records | `fsm.TestCrashLeavesAtomicPrefixOfBatches` |
| I6 linearizable history | Porcupine with `sim.LogModel` |
| I7 identical replay hash | `Cluster.CheckAgreement`; replay per node in e2e |

`sim.LogModel` specifies the log: appends carry producer sequence numbers, so a
retried append must come back as a duplicate, never as a second record. An
append that timed out is recorded as possibly taking effect at any later time.
`sim.BoundRetries` then tightens that bound to the return time of the retry
that settled it (the soundness argument is in its comment). Without it,
Porcupine timed out on one of the first 30 randomized seeds.

## How strong are the tests?

To check that the tests can actually fail, I ran each mutation below by hand
against the simulator, one at a time. None of this runs in CI.

| Mutation | Result |
|---|---|
| Raft persistence never fsyncs | caught: `TestWholeClusterPowerLoss`, where raft panics on restart with `tocommit(61) is out of range [lastIndex(3)]` |
| Producer dedup removed | caught: I3, "producer 2 seq 3 appears twice" |
| Term and vote not persisted | caught: raft panics on restart ("term should be set when sending MsgPreVoteResp") |
| ReadIndex reads answered before the read index is applied | **missed at first.** The workload read only from the leader, at uniformly random offsets. After reads went to followers and half of them targeted the newest offsets, Porcupine caught it (I6) |
| fsync removed from snapshot + compaction | not caught, and correctly so. WAL ordering already makes it safe (decision D4), so the fsync was removed |

## Limitations

- A seed reproduces the fault schedule and message scheduling, not raft's
  election jitter. etcd-io/raft draws that from `crypto/rand` (decision D13).
  Traces exist so a failure can be understood without reproducing it.
- The simulator is single-threaded, so it can't find races in the server's
  goroutines. The race detector and the HTTP tests cover those.
- Crash points sit between calls into storage, plus the hook inside snapshot
  install. `CrashClone` captures whatever is unsynced at that moment, but it
  doesn't stop Pebble halfway through its own flush or compaction.
- The multi-process test runs on one machine over loopback. Its partitions are
  dropped at the application layer, and nothing simulates clock skew or
  packet reordering in the kernel.
- No Jepsen run and no formal verification.
