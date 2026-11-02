package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
)

// Real processes: node 3 dies with its disk, is removed, and node 4 joins
// with --join and --cluster-id, catches up and serves the same log.
func TestReplaceDeadNodeProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes")
	}
	c := newCluster(t, 3)
	c.leader(10 * time.Second)
	cl := client.New(c.endpoints()...)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	appendRange := func(producer, from, to uint64) {
		for seq := from; seq <= to; seq += 50 {
			req := api.AppendRequest{ProducerID: producer, FirstSeq: seq}
			for i := uint64(0); i < 50 && seq+i <= to; i++ {
				req.Events = append(req.Events, fsm.EventIn{Type: 1, Schema: 1, Payload: []byte(fmt.Sprint(producer, seq+i))})
			}
			if _, err := cl.Append(ctx, req); err != nil {
				t.Fatal(err)
			}
		}
	}
	appendRange(1, 1, 500)

	victim := c.leader(5*time.Second)%3 + 1
	c.signal(victim, syscall.SIGKILL)
	os.RemoveAll(filepath.Join(c.dir, fmt.Sprintf("n%d", victim)))
	if err := cl.RemoveMember(ctx, victim); err != nil {
		t.Fatal(err)
	}
	var st api.Status
	for {
		var err error
		if st, err = cl.Status(ctx, c.peers[c.leader(5*time.Second)]); err != nil {
			t.Fatal(err)
		}
		if len(st.Members) == 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	appendRange(2, 1, 500)

	newURL := "http://" + freePort(t)
	if err := cl.AddMember(ctx, 4, newURL); err != nil {
		t.Fatal(err)
	}
	peers := []string{"4=" + newURL}
	for _, m := range st.Members {
		peers = append(peers, fmt.Sprintf("%d=%s", m.ID, m.URL))
	}
	logf, _ := os.Create(filepath.Join(c.dir, "n4.log"))
	cmd := exec.Command(c.bin, "--id", "4", "--listen", strings.TrimPrefix(newURL, "http://"),
		"--data", filepath.Join(c.dir, "n4"), "--join", "--cluster-id", st.ClusterID,
		"--peers", strings.Join(peers, ","), "--tick", "20ms", "--snapshot-entries", "5", "--snapshot-keep", "2")
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Signal(syscall.SIGTERM); cmd.Wait() })
	appendRange(3, 1, 200)

	var eps []string
	for _, m := range st.Members {
		eps = append(eps, m.URL)
	}
	eps = append(eps, newURL)
	deadline := time.Now().Add(20 * time.Second)
	for {
		var hashes []string
		for _, ep := range eps {
			if v, err := cl.Verify(ctx, ep, 1200); err == nil && v.To == 1200 {
				hashes = append(hashes, v.SHA256)
			}
		}
		if len(hashes) == 3 && hashes[0] == hashes[1] && hashes[1] == hashes[2] {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("new node did not converge: %v (logs in %s)", hashes, c.dir)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
