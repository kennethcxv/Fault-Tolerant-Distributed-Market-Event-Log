package sim

import (
	"testing"
)

func newHarness(t *testing.T, opts Options, clients int, appends uint64) (*Cluster, *Workload) {
	t.Helper()
	c, err := New(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c, NewWorkload(c, clients, appends)
}

// finish heals the network, lets the workload complete and the cluster
// converge, then checks invariants I1-I7. On failure the trace is saved.
func finish(t *testing.T, c *Cluster, w *Workload) {
	t.Helper()
	c.Heal()
	fail := func(err error) {
		t.Helper()
		t.Fatalf("%v\ntrace: %s", err, c.SaveTrace(t.Name()))
	}
	if err := w.Run(20_000, func() bool { return w.Done() && c.Converged() }); err != nil {
		fail(err)
	}
	log, err := c.CheckAgreement()
	if err != nil {
		fail(err)
	}
	if err := w.CheckAcked(log); err != nil {
		fail(err)
	}
	if err := w.CheckLinearizable(); err != nil {
		fail(err)
	}
	t.Logf("t=%dms acked=%d log=%d ops=%d", c.Now, w.Acked(), len(log), len(w.History()))
}

func TestReplicationThreeNodes(t *testing.T) {
	c, w := newHarness(t, Options{Nodes: 3, Seed: 1}, 3, 100)
	w.ReadPercent = 20
	finish(t, c, w)
}

func TestReplicationFiveNodes(t *testing.T) {
	c, w := newHarness(t, Options{Nodes: 5, Seed: 2}, 4, 60)
	w.ReadPercent = 20
	finish(t, c, w)
}

// An isolated leader keeps believing it leads until CheckQuorum makes it
// step down; the majority elects a new leader and keeps accepting writes.
func TestIsolatedLeaderIsReplaced(t *testing.T) {
	c, w := newHarness(t, Options{Nodes: 3, Seed: 3}, 2, 120)
	if err := w.Run(5_000, func() bool { return w.Acked() >= 40 }); err != nil {
		t.Fatal(err)
	}
	old := c.Leader()
	others := []uint64{}
	for _, n := range c.Nodes {
		if n != old {
			others = append(others, n.ID)
		}
	}
	c.Partition([]uint64{old.ID}, others)
	before := w.Acked()
	if err := w.Run(5_000, func() bool {
		l := c.Leader()
		return l != nil && l != old && w.Acked() >= before+30
	}); err != nil {
		t.Fatalf("%v\ntrace: %s", err, c.SaveTrace(t.Name()))
	}
	if old.rep.IsLeader() {
		t.Fatal("isolated leader did not step down")
	}
	finish(t, c, w)
}
