package sim

import (
	"fmt"
	"slices"
	"testing"
)

// A follower's disk is lost for good. The operator removes it and adds a
// fresh node under a new id (reusing the old id with an empty disk would let
// it vote twice in a term). The new node catches up by snapshot, and the
// cluster keeps every guarantee across the replacement.
func TestReplaceFailedNode(t *testing.T) {
	for seed := uint64(81); seed < 101; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			// Half the seeds also lose, duplicate and delay messages.
			opts := Options{Nodes: 3, Seed: seed, SnapshotEntries: 20, CompactKeep: 5}
			if seed%2 == 0 {
				opts.DropRate, opts.DupRate, opts.MaxDelay = 0.05, 0.05, 4
			}
			c, w := newHarness(t, opts, 3, 120)
			w.ReadPercent = 10
			if err := w.Run(5_000, func() bool { return w.Acked() >= 30 }); err != nil {
				t.Fatal(err)
			}
			victim := c.Leader().ID%3 + 1
			c.Crash(victim, 0)
			// Retry until applied: a proposal can be lost with its leader.
			for len(c.Voters()) != 2 {
				if c.ChangeMembership(victim, "") == nil {
					w.Run(300, func() bool { return len(c.Voters()) == 2 })
				} else if err := w.Steps(10); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Run(5_000, func() bool { return len(c.Voters()) == 2 && !slices.Contains(c.Voters(), victim) }); err != nil {
				t.Fatalf("remove: %v\ntrace: %s", err, c.SaveTrace(t.Name()))
			}
			n, err := c.AddNode()
			if err != nil {
				t.Fatal(err)
			}
			for len(c.Voters()) != 3 {
				if c.ChangeMembership(n.ID, fmt.Sprintf("sim://%d", n.ID)) == nil {
					w.Run(300, func() bool { return len(c.Voters()) == 3 })
				} else if err := w.Steps(10); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Run(10_000, func() bool {
				l := c.Leader()
				return len(c.Voters()) == 3 && l != nil && n.Up && n.rep.FSM().Applied() == l.rep.FSM().Applied()
			}); err != nil {
				t.Fatalf("add: %v\ntrace: %s", err, c.SaveTrace(t.Name()))
			}
			finish(t, c, w)
			if got := n.rep.FSM().Members(); got[n.ID] == "" {
				t.Fatalf("new node does not know its own URL: %v", got)
			}
			t.Logf("node %d replaced by node %d (snapshot installs on it: %d)", victim, n.ID, c.Installs[n.ID])
		})
	}
}

// Removing the leader: it steps down (StepDownOnRemoval), stops, and the
// remaining nodes elect a leader and keep going.
func TestRemoveLeader(t *testing.T) {
	c, w := newHarness(t, Options{Nodes: 3, Seed: 91}, 2, 100)
	if err := w.Run(5_000, func() bool { return w.Acked() >= 30 }); err != nil {
		t.Fatal(err)
	}
	old := c.Leader()
	if err := c.ChangeMembership(old.ID, ""); err != nil {
		t.Fatal(err)
	}
	if err := w.Run(10_000, func() bool { l := c.Leader(); return old.Removed && l != nil && l != old && w.Acked() >= 60 }); err != nil {
		t.Fatalf("%v\ntrace: %s", err, c.SaveTrace(t.Name()))
	}
	finish(t, c, w)
}
