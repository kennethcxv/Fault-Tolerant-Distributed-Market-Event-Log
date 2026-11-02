package replay

import (
	"context"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/orderbook"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/server"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// serveLog starts a standalone node holding n generated events.
func serveLog(t *testing.T, n int) *client.Client {
	t.Helper()
	node, err := server.Start(server.Config{ID: 1, Listen: "127.0.0.1:0", DataDir: t.TempDir(), Fsync: true, Standalone: true, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { node.Stop() })
	c := client.New(node.Addr())
	g := orderbook.NewGenerator(5, 3)
	for seq := uint64(1); seq <= uint64(n); seq += 500 {
		req := api.AppendRequest{ProducerID: 1, FirstSeq: seq}
		for i := 0; i < 500 && int(seq)+i <= n; i++ {
			m := g.Next()
			req.Events = append(req.Events, fsm.EventIn{Type: m.Type, Schema: 1, Payload: m.Encode()})
		}
		if _, err := c.Append(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func replayHash(t *testing.T, c *client.Client, n uint64) [32]byte {
	t.Helper()
	e := New()
	if _, err := e.Run(context.Background(), ClientSource{Client: c}, n, 1000); err != nil {
		t.Fatal(err)
	}
	return e.Book().Hash()
}

// The consumer is killed at random points, sometimes leaving a torn
// temporary checkpoint behind, and restarted until it reaches the end. Its
// final state must equal an uninterrupted replay, and the cluster checkpoint
// must only move forward.
func TestConsumerRestartsReachSameState(t *testing.T) {
	const n = 6000
	c := serveLog(t, n)
	want := replayHash(t, c, n)
	state := filepath.Join(t.TempDir(), "risk.ckpt")
	rng := rand.New(rand.NewPCG(9, 9))
	var lastCommit uint64
	restarts := 0
	for ; restarts < 500; restarts++ {
		ctx, cancel := context.WithCancel(context.Background())
		timer := time.AfterFunc(time.Duration(rng.IntN(15_000))*time.Microsecond, cancel)
		cons := &Consumer{Name: "risk", Client: c, StateFile: state, Every: 97, Batch: 250, Poll: time.Millisecond, Logger: quiet,
			OnCheckpoint: func(off uint64, _ [32]byte) {
				if off < lastCommit {
					t.Errorf("checkpoint moved backwards: %d after %d", off, lastCommit)
				}
				lastCommit = off
			}}
		err := cons.Run(ctx, n)
		timer.Stop()
		cancel()
		if err == nil {
			if got := cons.Engine().Book().Hash(); got != want {
				t.Fatalf("after %d restarts the consumer's state differs from a clean replay", restarts)
			}
			break
		}
		if ctx.Err() == nil {
			t.Fatalf("consumer failed without being killed: %v", err)
		}
		if rng.IntN(4) == 0 {
			// A crash in the middle of writing the next checkpoint.
			os.WriteFile(state+".tmp", []byte("torn"), 0o644)
		}
	}
	if restarts < 3 {
		t.Fatalf("only %d restarts; the test did not exercise recovery", restarts)
	}
	got, err := c.Consumer(context.Background(), "risk")
	if err != nil || got.Offset != n || got.Backlog != 0 {
		t.Fatalf("cluster checkpoint %+v %v", got, err)
	}
	t.Logf("reached offset %d after %d restarts", n, restarts)
}

// If the local state file is lost, the consumer rebuilds from offset 1.
// The cluster checkpoint is ahead during the rebuild and stays put (commits
// below it are stale), then moves on once the consumer passes it.
func TestConsumerRebuildsWhenLocalStateIsLost(t *testing.T) {
	const n = 3000
	c := serveLog(t, n)
	state := filepath.Join(t.TempDir(), "c.ckpt")
	run := func(until uint64) {
		cons := &Consumer{Name: "audit", Client: c, StateFile: state, Every: 400, Logger: quiet}
		if err := cons.Run(context.Background(), until); err != nil {
			t.Fatal(err)
		}
	}
	run(2000)
	os.Remove(state)
	run(n)
	if got, _ := c.Consumer(context.Background(), "audit"); got.Offset != n {
		t.Fatalf("cluster checkpoint %d", got.Offset)
	}
}

func TestConsumerRefusesDamagedState(t *testing.T) {
	c := serveLog(t, 500)
	state := filepath.Join(t.TempDir(), "c.ckpt")
	os.WriteFile(state, []byte("not a checkpoint"), 0o644)
	cons := &Consumer{Name: "x", Client: c, StateFile: state, Logger: quiet}
	if err := cons.Run(context.Background(), 500); err == nil {
		t.Fatal("consumer started from a damaged state file")
	}
}
