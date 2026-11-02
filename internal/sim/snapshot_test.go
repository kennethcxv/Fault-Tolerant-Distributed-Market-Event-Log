package sim

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

// isolateUntilCompacted cuts follower off until the leader has compacted its
// log past everything the follower has, so only a snapshot can catch it up.
func isolateUntilCompacted(t *testing.T, c *Cluster, w *Workload, follower uint64) {
	t.Helper()
	var others []uint64
	for _, n := range c.Nodes {
		if n.ID != follower {
			others = append(others, n.ID)
		}
	}
	c.Partition([]uint64{follower}, others)
	_, behind := c.Node(follower).rep.LogBounds()
	if err := w.Run(10_000, func() bool {
		l := c.Leader()
		if l == nil || l.ID == follower {
			return false
		}
		first, _ := l.rep.LogBounds()
		return first > behind+1
	}); err != nil {
		t.Fatalf("leader never compacted past the follower: %v\ntrace: %s", err, c.SaveTrace(t.Name()))
	}
}

func TestLaggingFollowerCatchesUpViaSnapshot(t *testing.T) {
	c, w := newHarness(t, Options{Nodes: 3, Seed: 41, SnapshotEntries: 20, CompactKeep: 5}, 3, 120)
	w.ReadPercent = 10
	if err := w.Run(5_000, func() bool { return w.Acked() >= 30 }); err != nil {
		t.Fatal(err)
	}
	follower := c.Leader().ID%3 + 1
	isolateUntilCompacted(t, c, w, follower)
	finish(t, c, w)
	if c.Installs[follower] == 0 {
		t.Fatalf("follower %d caught up without installing a snapshot\ntrace: %s", follower, c.SaveTrace(t.Name()))
	}
}

// The follower crashes in the middle of installing a snapshot: some batches
// are written, the applied index is not. On restart it must finish the
// install from the retained file (replica.Open) and converge.
func TestCrashDuringSnapshotInstall(t *testing.T) {
	c, w := newHarness(t, Options{Nodes: 3, Seed: 43, SnapshotEntries: 20, CompactKeep: 5}, 3, 150)
	if err := w.Run(5_000, func() bool { return w.Acked() >= 30 }); err != nil {
		t.Fatal(err)
	}
	follower := c.Leader().ID%3 + 1
	isolateUntilCompacted(t, c, w, follower)

	var captured *vfs.MemFS
	restore := storage.SetInstallHookForTest(2<<10, func() {
		if captured == nil {
			captured = c.Node(follower).FS().CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 30, RNG: rand.New(rand.NewPCG(1, 2))})
		}
	})
	c.Heal()
	err := w.Run(5_000, func() bool { return captured != nil && c.Installs[follower] > 0 })
	restore()
	if err != nil {
		t.Fatalf("snapshot never installed: %v\ntrace: %s", err, c.SaveTrace(t.Name()))
	}
	c.CrashTo(follower, captured)
	if err := c.WithStoppedStore(follower, func(st *storage.Store) error {
		rs, err := st.LoadRaftState()
		if err != nil {
			return err
		}
		meta, err := st.LoadMeta()
		if err != nil {
			return err
		}
		if snap := rs.Snapshot.GetMetadata().GetIndex(); meta.Applied >= snap {
			return fmt.Errorf("crash point did not interrupt the install: applied %d, snapshot %d", meta.Applied, snap)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Restart(follower); err != nil {
		t.Fatalf("restart after crash mid-install: %v", err)
	}
	finish(t, c, w)
}

// Random crashes with compaction on: the D4 argument says a synced snapshot
// record makes earlier unsynced state-machine batches durable before the log
// entries behind them are deleted. If it were wrong, a crash would leave a
// state machine behind a compacted log and the node could not catch up.
func TestRandomCrashRestartWithCompaction(t *testing.T) {
	for seed := uint64(200); seed < 210; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			c, w := newHarness(t, Options{Nodes: 3, Seed: seed, SnapshotEntries: 15, CompactKeep: 4}, 3, 100)
			rng := c.Rand()
			for round := 0; round < 15 && !w.Done(); round++ {
				if err := w.Steps(10 + rng.IntN(60)); err != nil {
					t.Fatalf("%v\ntrace: %s", err, c.SaveTrace(t.Name()))
				}
				victim := uint64(1 + rng.IntN(3))
				down := 0
				for _, n := range c.Nodes {
					if !n.Up {
						down++
					}
				}
				switch {
				case c.Node(victim).Up && down == 0:
					c.Crash(victim, rng.IntN(101))
				case !c.Node(victim).Up:
					if err := c.Restart(victim); err != nil {
						t.Fatalf("restart %d: %v\ntrace: %s", victim, err, c.SaveTrace(t.Name()))
					}
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
