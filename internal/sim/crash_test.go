package sim

import (
	"errors"
	"fmt"
	"testing"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

// The leader appends entries it can never replicate, then crashes. A new
// leader is elected; when the old one restarts, raft overwrites its
// uncommitted suffix, which exercises the conflict truncation in
// storage.SaveRaft. The client retries with the same sequence numbers, so
// each event must appear exactly once.
func TestLeaderCrashDuringWriteTruncatesUncommittedEntries(t *testing.T) {
	c, w := newHarness(t, Options{Nodes: 3, Seed: 11}, 2, 60)
	if err := w.Run(5_000, func() bool { return w.Acked() >= 20 }); err != nil {
		t.Fatal(err)
	}
	old := c.Leader()
	for _, n := range c.Nodes {
		if n != old {
			c.BlockLink(old.ID, n.ID)
		}
	}
	if err := w.Steps(8); err != nil {
		t.Fatal(err)
	}
	_, last := old.rep.LogBounds()
	commit := old.rep.Status().GetCommit()
	if last <= commit {
		t.Fatalf("setup: old leader has no uncommitted entries (last %d, commit %d)", last, commit)
	}
	c.Tracef("old leader %d: last index %d, commit %d", old.ID, last, commit)
	c.Crash(old.ID, 0)
	c.Heal()
	if err := w.Run(5_000, func() bool { l := c.Leader(); return l != nil && w.Acked() >= 40 }); err != nil {
		t.Fatalf("%v\ntrace: %s", err, c.SaveTrace(t.Name()))
	}
	if err := c.Restart(old.ID); err != nil {
		t.Fatal(err)
	}
	finish(t, c, w)
}

// All nodes lose power at once. Every acknowledged write was fsynced on a
// majority before it was acknowledged, so all of them must survive (G1).
func TestWholeClusterPowerLoss(t *testing.T) {
	for seed := uint64(20); seed < 25; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			c, w := newHarness(t, Options{Nodes: 3, Seed: seed}, 3, 80)
			if err := w.Run(5_000, func() bool { return w.Acked() >= 60 }); err != nil {
				t.Fatal(err)
			}
			// Crash mid-flight: some appends are proposed but not acknowledged.
			if err := w.Steps(int(seed % 4)); err != nil {
				t.Fatal(err)
			}
			for _, n := range c.Nodes {
				c.Crash(n.ID, 0)
			}
			for _, n := range c.Nodes {
				if err := c.Restart(n.ID); err != nil {
					t.Fatal(err)
				}
			}
			finish(t, c, w)
		})
	}
}

// Crashes keep a random part of the unsynced data, which is what a real disk
// may do. Raft only relies on synced data; anything extra must not confuse
// recovery.
func TestRandomCrashRestartWithTornWrites(t *testing.T) {
	for seed := uint64(100); seed < 110; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			c, w := newHarness(t, Options{Nodes: 3, Seed: seed}, 3, 100)
			rng := c.Rand()
			for round := 0; round < 12 && !w.Done(); round++ {
				if err := w.Steps(20 + rng.IntN(80)); err != nil {
					t.Fatalf("%v\ntrace: %s", err, c.SaveTrace(t.Name()))
				}
				victim := uint64(1 + rng.IntN(3))
				if c.Node(victim).Up {
					// At most one node down at a time keeps a majority.
					down := 0
					for _, n := range c.Nodes {
						if !n.Up {
							down++
						}
					}
					if down == 0 {
						c.Crash(victim, rng.IntN(101))
					}
				} else if err := c.Restart(victim); err != nil {
					t.Fatalf("restart %d: %v\ntrace: %s", victim, err, c.SaveTrace(t.Name()))
				}
			}
			for _, n := range c.Nodes {
				if err := c.Restart(n.ID); err != nil {
					t.Fatal(err)
				}
			}
			finish(t, c, w)
		})
	}
}

// With --fsync=never nothing is durable across a power loss, so a whole-
// cluster crash can lose acknowledged writes. This test documents that the
// mode voids G1; it is not a guarantee anyone may rely on.
func TestFsyncNeverLosesAcknowledgedWritesOnPowerLoss(t *testing.T) {
	c, w := newHarness(t, Options{Nodes: 3, Seed: 7, NoSync: true}, 2, 1000)
	if err := w.Run(5_000, func() bool { return w.Acked() >= 50 }); err != nil {
		t.Fatal(err)
	}
	acked := w.Acked()
	for _, n := range c.Nodes {
		c.Crash(n.ID, 0)
	}
	for _, n := range c.Nodes {
		if err := c.Restart(n.ID); err != nil {
			t.Fatal(err)
		}
	}
	survived := c.Node(1).rep.FSM().LastOffset()
	t.Logf("acknowledged %d, survived on node 1: %d", acked, survived)
	if survived >= uint64(acked) {
		t.Fatalf("expected acknowledged writes to be lost without fsync; %d of %d survived", survived, acked)
	}
}

// A follower whose disk dropped acknowledged Raft entries must refuse to
// start instead of rejoining with a shorter log; the rest of the cluster
// keeps going.
func TestTruncatedRaftLogIsRefusedAtStartup(t *testing.T) {
	c, w := newHarness(t, Options{Nodes: 3, Seed: 31}, 2, 60)
	if err := w.Run(5_000, func() bool { return w.Acked() >= 30 }); err != nil {
		t.Fatal(err)
	}
	victim := c.Leader().ID%3 + 1
	c.Crash(victim, 0)
	commit := uint64(0)
	if err := c.WithStoppedStore(victim, func(st *storage.Store) error {
		rs, err := st.LoadRaftState()
		commit = rs.HardState.GetCommit()
		if err != nil {
			return err
		}
		return storage.TruncateRaftLogForTest(st, commit-5)
	}); err != nil {
		t.Fatal(err)
	}
	err := c.Restart(victim)
	if !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("restart with truncated log: %v", err)
	}
	t.Logf("refused as expected: %v", err)
	// The remaining majority still makes progress.
	before := w.Acked()
	if err := w.Run(5_000, func() bool { return w.Acked() >= before+20 }); err != nil {
		t.Fatalf("%v\ntrace: %s", err, c.SaveTrace(t.Name()))
	}
}

// Nodes lose power at the moment they send, with leaders isolated at random
// so a new leader must be chosen from nodes that may have just crashed. If
// any node acknowledged entries before they were durable, a committed write
// could disappear here; the leader is the only node allowed to send first,
// because raft counts its own acknowledgment only after it persists.
func TestCrashAtSendPreservesCommittedWrites(t *testing.T) {
	for seed := uint64(300); seed < 340; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			c, w := newHarness(t, Options{Nodes: 5, Seed: seed, CrashAtSendRate: 0.02, MaxDelay: 3}, 3, 60)
			rng := c.Rand()
			for round := 0; round < 40 && !w.Done(); round++ {
				if err := w.Steps(5 + rng.IntN(30)); err != nil {
					t.Fatalf("%v\ntrace: %s", err, c.SaveTrace(t.Name()))
				}
				for _, n := range c.Nodes {
					if !n.Up && rng.IntN(3) == 0 {
						if err := c.Restart(n.ID); err != nil {
							t.Fatalf("restart: %v\ntrace: %s", err, c.SaveTrace(t.Name()))
						}
					}
				}
				if l := c.Leader(); l != nil && rng.IntN(3) == 0 {
					var rest []uint64
					for _, n := range c.Nodes {
						if n != l {
							rest = append(rest, n.ID)
						}
					}
					c.Partition([]uint64{l.ID}, rest)
				} else if rng.IntN(2) == 0 {
					c.Heal()
				}
			}
			c.opts.CrashAtSendRate = 0 // let the cluster recover for the final checks
			t.Logf("power cuts while sending: %d", c.SendCrashes)
			for _, n := range c.Nodes {
				if err := c.Restart(n.ID); err != nil {
					t.Fatal(err)
				}
			}
			finish(t, c, w)
		})
	}
}
