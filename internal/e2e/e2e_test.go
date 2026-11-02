// Package e2e runs real mlogd processes: it builds the binary, starts a
// three-node cluster on localhost, and injects faults with SIGKILL and the
// /debug/fault endpoint while clients write and read. Skipped with -short.
package e2e

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/orderbook"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/replay"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/sim"
)

type cluster struct {
	t     *testing.T
	bin   string
	dir   string
	peers map[uint64]string
	procs map[uint64]*exec.Cmd
	mu    sync.Mutex
}

func buildMlogd(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "mlogd")
	out, err := exec.Command("go", "build", "-o", bin, "github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/cmd/mlogd").CombinedOutput()
	if err != nil {
		t.Fatalf("build mlogd: %v\n%s", err, out)
	}
	return bin
}

func freePort(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{t: t, bin: buildMlogd(t), dir: t.TempDir(), peers: map[uint64]string{}, procs: map[uint64]*exec.Cmd{}}
	for id := uint64(1); id <= uint64(n); id++ {
		c.peers[id] = "http://" + freePort(t)
	}
	for id := range c.peers {
		c.start(id)
	}
	t.Cleanup(func() {
		for id := range c.peers {
			c.signal(id, syscall.SIGTERM)
		}
		if t.Failed() {
			t.Logf("node logs are in %s", c.dir)
		}
	})
	return c
}

func (c *cluster) peerFlag() string {
	var parts []string
	for id, url := range c.peers {
		parts = append(parts, fmt.Sprintf("%d=%s", id, url))
	}
	return strings.Join(parts, ",")
}

func (c *cluster) start(id uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	logf, err := os.OpenFile(filepath.Join(c.dir, fmt.Sprintf("n%d.log", id)), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		c.t.Fatal(err)
	}
	cmd := exec.Command(c.bin, "--id", fmt.Sprint(id), "--listen", strings.TrimPrefix(c.peers[id], "http://"),
		"--data", filepath.Join(c.dir, fmt.Sprintf("n%d", id)), "--peers", c.peerFlag(),
		"--tick", "20ms", "--request-timeout", "1s", "--snapshot-entries", "200", "--snapshot-keep", "50")
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		c.t.Fatal(err)
	}
	c.procs[id] = cmd
}

func (c *cluster) signal(id uint64, sig syscall.Signal) {
	c.mu.Lock()
	cmd := c.procs[id]
	delete(c.procs, id)
	c.mu.Unlock()
	if cmd != nil {
		cmd.Process.Signal(sig)
		cmd.Wait()
	}
}

func (c *cluster) endpoints() []string {
	var out []string
	for id := uint64(1); id <= uint64(len(c.peers)); id++ {
		out = append(out, c.peers[id])
	}
	return out
}

func (c *cluster) leader(timeout time.Duration) uint64 {
	cl := client.New(c.endpoints()...)
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, ep := range c.endpoints() {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			st, err := cl.Status(ctx, ep)
			cancel()
			if err == nil && st.Role == "leader" {
				return st.ID
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return 0
}

// history records client operations with wall-clock times relative to start.
type history struct {
	mu    sync.Mutex
	start time.Time
	ops   []porcupine.Operation
	acked map[[2]uint64]uint64
}

func (h *history) now() int64 { return time.Since(h.start).Nanoseconds() }

func (h *history) add(op porcupine.Operation) {
	h.mu.Lock()
	h.ops = append(h.ops, op)
	h.mu.Unlock()
}

// writer appends one event per request for its producer, retrying the same
// sequence until it learns the outcome, exactly as a careful producer must.
func writer(ctx context.Context, h *history, eps []string, clientID int, producer uint64, maxOffset *atomic.Uint64) {
	cl := client.New(eps...)
	gen := orderbook.NewGenerator(producer, 2)
	gen.StartIDsAt(producer << 40)
	target := eps[0]
	seq := uint64(1)
	msg := gen.Next()
	for ctx.Err() == nil {
		req := api.AppendRequest{ProducerID: producer, FirstSeq: seq, Events: []fsm.EventIn{{Type: msg.Type, Schema: 1, Payload: msg.Encode()}}}
		in := sim.OpInput{Producer: producer, Seq: seq, Payload: string(msg.Encode())}
		actx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		call := h.now()
		res, err := cl.AppendOnce(actx, target, req)
		ret := h.now()
		cancel()
		var apiErr *api.Error
		var opErr *net.OpError
		switch {
		case err == nil && (res.Status == fsm.StatusAppended || res.Status == fsm.StatusDuplicate):
			h.add(porcupine.Operation{ClientId: clientID, Input: in, Call: call, Return: ret,
				Output: sim.Output{Status: res.Status, Offset: res.FirstOffset, ProducerLastSeq: res.ProducerLastSeq}})
			if res.Status == fsm.StatusAppended {
				h.mu.Lock()
				h.acked[[2]uint64{producer, seq}] = res.FirstOffset
				h.mu.Unlock()
				for {
					cur := maxOffset.Load()
					if res.FirstOffset <= cur || maxOffset.CompareAndSwap(cur, res.FirstOffset) {
						break
					}
				}
			}
			seq++
			msg = gen.Next()
		case errors.As(err, &apiErr) && apiErr.Code == api.CodeNotLeader && apiErr.LeaderURL != "":
			target = apiErr.LeaderURL // not proposed: not an operation
		case errors.As(err, &apiErr) && apiErr.Code != api.CodeOutcomeUnknown,
			errors.As(err, &opErr) && opErr.Op == "dial":
			target = eps[rand.IntN(len(eps))] // definitely not applied
			time.Sleep(20 * time.Millisecond)
		default:
			// Timed out or connection broke after sending: outcome unknown.
			h.add(porcupine.Operation{ClientId: clientID, Input: in, Call: call, Return: math.MaxInt64, Output: sim.Output{Unknown: true}})
			target = eps[rand.IntN(len(eps))]
		}
	}
}

// reader issues linearizable reads near the tail of the log on random nodes.
func reader(ctx context.Context, h *history, eps []string, clientID int, maxOffset *atomic.Uint64) {
	for ctx.Err() == nil {
		ep := eps[rand.IntN(len(eps))]
		last := maxOffset.Load()
		off := max(1, last+1-uint64(rand.IntN(3)))
		rctx, cancel := context.WithTimeout(context.Background(), time.Second)
		call := h.now()
		evs, _, err := client.New(ep).Read(rctx, off, 1, true)
		ret := h.now()
		cancel()
		if err != nil {
			time.Sleep(20 * time.Millisecond)
			continue // a failed read has no effect: not an operation
		}
		out := sim.Output{NotFound: true}
		if len(evs) == 1 {
			out = sim.Output{Producer: evs[0].ProducerID, Seq: evs[0].ProducerSeq, Payload: string(evs[0].Payload)}
		}
		h.add(porcupine.Operation{ClientId: clientID, Input: sim.OpInput{Read: true, Offset: off}, Call: call, Return: ret, Output: out})
		time.Sleep(5 * time.Millisecond)
	}
}

func TestMultiProcessFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes")
	}
	c := newCluster(t, 3)
	c.leader(10 * time.Second)
	eps := c.endpoints()
	h := &history{start: time.Now(), acked: map[[2]uint64]uint64{}}
	var maxOffset atomic.Uint64
	ctx, stop := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := range 3 {
		wg.Go(func() { writer(ctx, h, eps, i, uint64(i+1), &maxOffset) })
	}
	for i := range 2 {
		wg.Go(func() { reader(ctx, h, eps, 3+i, &maxOffset) })
	}
	fc := client.New(eps...)
	step := func(what string, d time.Duration) {
		t.Logf("%-40s acked so far: %d", what, func() int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.acked) }())
		time.Sleep(d)
	}

	step("warm up", 1500*time.Millisecond)
	lead := c.leader(5 * time.Second)
	c.signal(lead, syscall.SIGKILL)
	step(fmt.Sprintf("SIGKILL leader %d", lead), 1500*time.Millisecond)
	c.start(lead)
	step(fmt.Sprintf("restart node %d", lead), 1500*time.Millisecond)

	lead = c.leader(5 * time.Second)
	var others []uint64
	for id := range c.peers {
		if id != lead {
			others = append(others, id)
		}
	}
	if err := fc.SetFault(context.Background(), c.peers[lead], api.FaultConfig{DropTo: others, DropFrom: others}); err != nil {
		t.Fatal(err)
	}
	step(fmt.Sprintf("isolate leader %d", lead), 2*time.Second)
	if err := fc.SetFault(context.Background(), c.peers[lead], api.FaultConfig{}); err != nil {
		t.Fatal(err)
	}
	step("heal partition", 1500*time.Millisecond)

	lead = c.leader(5 * time.Second)
	follower := lead%3 + 1
	c.signal(follower, syscall.SIGKILL)
	step(fmt.Sprintf("SIGKILL follower %d", follower), time.Second)
	c.start(follower)
	step(fmt.Sprintf("restart node %d", follower), 1500*time.Millisecond)
	stop()
	wg.Wait()

	// Let every node converge, then compare them.
	var last uint64
	deadline := time.Now().Add(20 * time.Second)
	for {
		var offsets []uint64
		for _, ep := range eps {
			if st, err := fc.Status(context.Background(), ep); err == nil {
				offsets = append(offsets, st.LastOffset)
			}
		}
		if len(offsets) == 3 && offsets[0] == offsets[1] && offsets[1] == offsets[2] && offsets[0] > 0 {
			last = offsets[0]
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("nodes did not converge: %v", offsets)
		}
		time.Sleep(100 * time.Millisecond)
	}
	var hashes []string
	var replayHashes [][32]byte
	for _, ep := range eps {
		v, err := fc.Verify(context.Background(), ep, last)
		if err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, v.SHA256)
		eng := replay.New()
		if _, err := eng.Run(context.Background(), replay.ClientSource{Client: client.New(ep)}, last, 1000); err != nil {
			t.Fatal(err)
		}
		replayHashes = append(replayHashes, eng.Book().Hash())
	}
	if hashes[0] != hashes[1] || hashes[1] != hashes[2] {
		t.Fatalf("I1: logs differ: %v", hashes)
	}
	if replayHashes[0] != replayHashes[1] || replayHashes[1] != replayHashes[2] {
		t.Fatal("I7: replay hashes differ")
	}

	// Every acknowledged append is at its offset (I2), each once (I3).
	var evs []event.Event
	for uint64(len(evs)) < last {
		next, _, err := client.New(eps[0]).Read(context.Background(), uint64(len(evs))+1, 10_000, false)
		if err != nil {
			t.Fatal(err)
		}
		evs = append(evs, next...)
	}
	seen := map[[2]uint64]bool{}
	for _, e := range evs {
		k := [2]uint64{e.ProducerID, e.ProducerSeq}
		if seen[k] {
			t.Fatalf("I3: producer %d seq %d appears twice", k[0], k[1])
		}
		seen[k] = true
	}
	for k, off := range h.acked {
		if e := evs[off-1]; e.ProducerID != k[0] || e.ProducerSeq != k[1] {
			t.Fatalf("I2: acked producer %d seq %d at offset %d, found %d/%d", k[0], k[1], off, e.ProducerID, e.ProducerSeq)
		}
	}
	if err := sim.CheckHistory(h.ops, "e2e"); err != nil {
		t.Fatal(err)
	}
	t.Logf("log has %d events; %d acknowledged appends; %d client operations checked for linearizability", last, len(h.acked), len(h.ops))
}
