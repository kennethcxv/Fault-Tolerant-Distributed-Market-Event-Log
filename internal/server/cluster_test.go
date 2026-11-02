package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
)

type testCluster struct {
	t     *testing.T
	peers map[uint64]string
	dirs  map[uint64]string
	nodes map[uint64]*Node
}

func newTestCluster(t *testing.T, size int) *testCluster {
	t.Helper()
	tc := &testCluster{t: t, peers: map[uint64]string{}, dirs: map[uint64]string{}, nodes: map[uint64]*Node{}}
	lns := map[uint64]net.Listener{}
	for id := uint64(1); id <= uint64(size); id++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[id], tc.peers[id], tc.dirs[id] = ln, "http://"+ln.Addr().String(), t.TempDir()
	}
	for id, ln := range lns {
		tc.start(id, ln)
	}
	t.Cleanup(func() {
		for _, n := range tc.nodes {
			n.Stop()
		}
	})
	return tc
}

func (tc *testCluster) start(id uint64, ln net.Listener) {
	tc.t.Helper()
	if ln == nil {
		var err error
		// The port was released by Stop; retry briefly in case the OS lags.
		for range 50 {
			if ln, err = net.Listen("tcp", strings.TrimPrefix(tc.peers[id], "http://")); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			tc.t.Fatal(err)
		}
	}
	n, err := Start(Config{ID: id, DataDir: tc.dirs[id], Fsync: true, Peers: tc.peers, Listener: ln,
		TickInterval: 10 * time.Millisecond, RequestTimeout: 2 * time.Second, Logger: quiet})
	if err != nil {
		tc.t.Fatal(err)
	}
	tc.nodes[id] = n
}

func (tc *testCluster) stop(id uint64) {
	tc.nodes[id].Stop()
	delete(tc.nodes, id)
}

func (tc *testCluster) client(ids ...uint64) *client.Client {
	var eps []string
	for _, id := range ids {
		eps = append(eps, tc.peers[id])
	}
	if len(ids) == 0 {
		for id := uint64(1); id <= uint64(len(tc.peers)); id++ {
			eps = append(eps, tc.peers[id])
		}
	}
	return client.New(eps...)
}

// leader waits until the running nodes agree on a leader and returns it.
func (tc *testCluster) leader() uint64 {
	tc.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var lead uint64
		agree := true
		for _, n := range tc.nodes {
			st := n.eng.status()
			if lead == 0 {
				lead = st.Leader
			}
			agree = agree && st.Leader == lead && lead != 0
		}
		if agree && tc.nodes[lead] != nil {
			return lead
		}
		time.Sleep(10 * time.Millisecond)
	}
	tc.t.Fatal("no stable leader")
	return 0
}

// converged waits until every running node has applied the same last offset
// and returns it after checking the logs are byte-identical.
func (tc *testCluster) converged(want uint64) {
	tc.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, n := range tc.nodes {
			done = done && n.fsm.LastOffset() == want
		}
		if done {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	var ref []byte
	for id, n := range tc.nodes {
		raw, count, err := n.store.ReadRaw(1, n.fsm.LastOffset(), 1<<30, 1<<30)
		if err != nil || uint64(count) != want {
			tc.t.Fatalf("node %d: %d records (want %d), err %v", id, count, want, err)
		}
		if ref != nil && !bytes.Equal(raw, ref) {
			tc.t.Fatalf("node %d log differs", id)
		}
		ref = raw
	}
}

func appendN(t *testing.T, c *client.Client, producers, batches int) {
	t.Helper()
	var wg sync.WaitGroup
	for p := 1; p <= producers; p++ {
		wg.Go(func() {
			for b := range batches {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				res, err := c.Append(ctx, batch(uint64(p), uint64(b+1), fmt.Sprint(p, "-", b)))
				cancel()
				if err != nil || (res.Status != fsm.StatusAppended && res.Status != fsm.StatusDuplicate) {
					t.Errorf("producer %d batch %d: %+v %v", p, b, res, err)
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestRaftClusterReplicates(t *testing.T) {
	tc := newTestCluster(t, 3)
	tc.leader()
	appendN(t, tc.client(), 4, 50)
	tc.converged(200)
}

func TestRaftFiveNodes(t *testing.T) {
	tc := newTestCluster(t, 5)
	tc.leader()
	appendN(t, tc.client(), 3, 30)
	tc.converged(90)
}

func TestFollowerRedirectsAndServesLinearizableReads(t *testing.T) {
	tc := newTestCluster(t, 3)
	lead := tc.leader()
	follower := lead%3 + 1

	var apiErr *api.Error
	_, err := tc.client(follower).AppendOnce(context.Background(), tc.peers[follower], batch(1, 1, "x"))
	if !errors.As(err, &apiErr) || apiErr.Code != api.CodeNotLeader || apiErr.LeaderURL != tc.peers[lead] {
		t.Fatalf("follower append: %v", err)
	}
	// The client follows the hint.
	c := tc.client(follower)
	if res, err := c.Append(context.Background(), batch(1, 1, "x")); err != nil || res.Status != fsm.StatusAppended {
		t.Fatalf("append via follower: %+v %v", res, err)
	}
	// A linearizable read on the follower must see the acknowledged write.
	evs, _, err := client.New(tc.peers[follower]).Read(context.Background(), 1, 10, true)
	if err != nil || len(evs) != 1 {
		t.Fatalf("linearizable follower read: %d events, %v", len(evs), err)
	}
}

func TestLeaderStopElectsNewLeaderAndOldLeaderCatchesUp(t *testing.T) {
	tc := newTestCluster(t, 3)
	c := tc.client()
	old := tc.leader()
	appendN(t, c, 2, 20)
	tc.stop(old)
	if lead := tc.leader(); lead == old {
		t.Fatal("stopped node still leader")
	}
	for p := uint64(1); p <= 2; p++ {
		if res, err := c.Append(context.Background(), batch(p, 21, "after")); err != nil || res.Status != fsm.StatusAppended {
			t.Fatalf("append after failover: %+v %v", res, err)
		}
	}
	tc.start(old, nil)
	tc.converged(42)
}

// With two of three nodes down there is no quorum: writes must not be
// acknowledged, and must succeed again once a majority is back.
func TestQuorumLossBlocksWritesUntilRecovery(t *testing.T) {
	tc := newTestCluster(t, 3)
	c := tc.client()
	lead := tc.leader()
	appendN(t, c, 1, 5)
	var down []uint64
	for id := range tc.nodes {
		if id != lead && len(down) < 1 {
			down = append(down, id)
		}
	}
	down = append(down, lead)
	for _, id := range down {
		tc.stop(id)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	// Any error is acceptable (no_leader, a refused connection, or
	// outcome_unknown if the old leader took the proposal before stepping
	// down); an acknowledgment is not.
	res, err := c.Append(ctx, batch(1, 6, "no quorum"))
	cancel()
	if err == nil {
		t.Fatalf("append without quorum was acknowledged: %+v", res)
	}
	t.Logf("without quorum: %v", err)
	tc.start(down[0], nil)
	tc.leader()
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err = c.Append(ctx, batch(1, 6, "no quorum"))
	if err != nil || (res.Status != fsm.StatusAppended && res.Status != fsm.StatusDuplicate) {
		t.Fatalf("append after recovery: %+v %v", res, err)
	}
	tc.converged(6)
}

func TestMismatchedPeerListIsRejected(t *testing.T) {
	tc := newTestCluster(t, 3)
	req, _ := http.NewRequest(http.MethodPost, tc.peers[1]+"/raft/message", strings.NewReader(""))
	req.Header.Set(clusterHeader, "not-this-cluster")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("got %d %s", resp.StatusCode, body)
	}
}
