package server

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

// A follower that detects a corrupt record stops serving; the leader and the
// other follower keep accepting writes.
func TestCorruptRecordOnFollowerStopsOnlyThatNode(t *testing.T) {
	tc := newTestCluster(t, 3)
	lead := tc.leader()
	appendN(t, tc.client(), 1, 10)
	tc.converged(10)
	victim := lead%3 + 1
	if err := storage.CorruptForTest(tc.nodes[victim].store, 4); err != nil {
		t.Fatal(err)
	}
	_, _, err := client.New(tc.peers[victim]).Read(context.Background(), 1, 100, false)
	var apiErr *api.Error
	if !errors.As(err, &apiErr) || apiErr.Code != api.CodeCorrupt {
		t.Fatalf("read from corrupted follower: %v", err)
	}
	select {
	case <-tc.nodes[victim].Failed():
	case <-time.After(2 * time.Second):
		t.Fatal("corrupted follower kept running")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if res, err := tc.client().Append(ctx, batch(1, 11, "still writable")); err != nil || res.Status != fsm.StatusAppended {
		t.Fatalf("cluster stopped accepting writes: %+v %v", res, err)
	}
}

// Truncating a stopped follower's newest WAL file loses writes it had
// acknowledged, which is outside the fault model (design §4). This test pins
// down what happens instead of claiming a guarantee. Pebble recovers a
// consistent prefix, so the node starts. The leader still remembers the
// follower's old match index, and raft detects that the follower's log is
// behind entries it acknowledged. The node then stops serving, which is
// fail-stop. If it ever rejoins cleanly instead, its log must converge. Either
// way the cluster keeps accepting writes.
func TestTruncatedWALOnFollowerIsDetected(t *testing.T) {
	tc := newTestCluster(t, 3)
	lead := tc.leader()
	appendN(t, tc.client(), 2, 40)
	tc.converged(80)
	victim := lead%3 + 1
	tc.stop(victim)

	wals, _ := filepath.Glob(filepath.Join(tc.dirs[victim], "db", "*.log"))
	if len(wals) == 0 {
		t.Fatal("no WAL files found")
	}
	slices.Sort(wals)
	newest := wals[len(wals)-1]
	info, _ := os.Stat(newest)
	if err := os.Truncate(newest, info.Size()/2); err != nil {
		t.Fatal(err)
	}
	t.Logf("truncated %s from %d to %d bytes", filepath.Base(newest), info.Size(), info.Size()/2)

	n, err := Start(Config{ID: victim, DataDir: tc.dirs[victim], Fsync: true, Peers: tc.peers,
		Listen: tc.peers[victim][len("http://"):], TickInterval: 10 * time.Millisecond, Logger: quiet})
	if err != nil {
		t.Logf("node refused to start: %v", err)
	} else {
		tc.nodes[victim] = n
		t.Logf("restarted with last offset %d (had 80)", n.fsm.LastOffset())
		select {
		case <-n.Failed():
			t.Logf("node stopped itself: %v", n.Err())
			tc.stop(victim)
		case <-time.After(3 * time.Second):
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if res, err := tc.client().Append(ctx, batch(1, 41, "after")); err != nil || res.Status != fsm.StatusAppended {
		t.Fatalf("cluster stopped accepting writes: %+v %v", res, err)
	}
	tc.converged(81)
}

// A follower that was down while the leader compacted its log can only catch
// up through a snapshot streamed over HTTP.
func TestRestartedFollowerCatchesUpViaSnapshot(t *testing.T) {
	tc := newTestClusterWith(t, 3, func(c *Config) { c.SnapshotEntries, c.CompactKeep = 20, 5 })
	lead := tc.leader()
	appendN(t, tc.client(), 2, 10)
	tc.converged(20)
	victim := lead%3 + 1
	tc.stop(victim)
	appendN(t, tc.client(), 4, 40)

	leader := tc.nodes[tc.leader()]
	first, _ := leader.raft.rep.LogBounds()
	if first < 30 {
		t.Fatalf("setup: leader log still starts at %d; it should have compacted", first)
	}
	tc.start(victim, nil)
	// Producers 1 and 2 resent sequences 1-10, which dedup drops: 20 + 140.
	tc.converged(160)
	if got := tc.nodes[victim].raft.rep.SnapshotIndex(); got == 0 {
		t.Fatal("follower caught up without a snapshot")
	}
}

// A node's disk is lost. It is removed, and a new node with a new id and an
// empty directory joins with the cluster id, catches up and serves the same
// log. The cluster accepts writes throughout.
func TestReplaceNodeWithLostDisk(t *testing.T) {
	tc := newTestClusterWith(t, 3, func(c *Config) { c.SnapshotEntries, c.CompactKeep = 20, 5 })
	lead := tc.leader()
	appendN(t, tc.client(), 2, 30)
	tc.converged(60)
	victim := lead%3 + 1
	tc.stop(victim)
	os.RemoveAll(tc.dirs[victim])

	c := tc.client()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c.RemoveMember(ctx, victim); err != nil {
		t.Fatal(err)
	}
	var st api.Status
	for {
		var err error
		st, err = c.Status(ctx, tc.peers[tc.leader()])
		if err != nil {
			t.Fatal(err)
		}
		if !slices.ContainsFunc(st.Members, func(m api.Member) bool { return m.ID == victim }) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	newID, newURL := uint64(4), "http://"+ln.Addr().String()
	if err := c.AddMember(ctx, newID, newURL); err != nil {
		t.Fatal(err)
	}
	peers := map[uint64]string{newID: newURL}
	for id, url := range tc.peers {
		if id != victim {
			peers[id] = url
		}
	}
	n, err := Start(Config{ID: newID, DataDir: t.TempDir(), Fsync: true, Peers: peers, Listener: ln, Join: true,
		ClusterID: st.ClusterID, TickInterval: 10 * time.Millisecond, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	tc.nodes[newID] = n
	tc.peers[newID] = newURL
	delete(tc.peers, victim)

	for p := uint64(1); p <= 2; p++ {
		if res, err := c.Append(ctx, batch(p, 31, "after replacement")); err != nil || res.Status != fsm.StatusAppended {
			t.Fatalf("append after replacement: %+v %v", res, err)
		}
	}
	tc.converged(62)
	got, _ := c.Status(ctx, newURL)
	var ids []uint64
	for _, m := range got.Members {
		ids = append(ids, m.ID)
	}
	if len(ids) != 3 || slices.Contains(ids, victim) || !slices.Contains(ids, newID) {
		t.Fatalf("members after replacement: %v", ids)
	}
}

// Raft would turn a second membership change into a no-op while the first is
// pending; the engine must refuse it explicitly instead of answering
// "proposed".
func TestConcurrentMembershipChangeIsRefused(t *testing.T) {
	tc := newTestCluster(t, 3)
	n := tc.nodes[tc.leader()]
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.raft.changeMembership(ctx, 9, "http://127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	err := n.raft.changeMembership(ctx, 10, "http://127.0.0.1:2")
	var apiErr *api.Error
	if !errors.As(err, &apiErr) || apiErr.Code != api.CodeOverloaded {
		t.Fatalf("second change while the first was pending: %v", err)
	}
}
