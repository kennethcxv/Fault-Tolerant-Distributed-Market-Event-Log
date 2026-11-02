package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func startStandalone(t *testing.T, dir string) (*Node, *client.Client) {
	t.Helper()
	n, err := Start(Config{ID: 1, Listen: "127.0.0.1:0", DataDir: dir, Fsync: true, Standalone: true, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	return n, client.New(n.Addr())
}

func batch(producer, firstSeq uint64, payloads ...string) api.AppendRequest {
	req := api.AppendRequest{ProducerID: producer, FirstSeq: firstSeq}
	for _, p := range payloads {
		req.Events = append(req.Events, fsm.EventIn{Type: 1, Schema: 1, Payload: []byte(p)})
	}
	return req
}

func TestStandaloneAppendReadCheckpoint(t *testing.T) {
	ctx := context.Background()
	n, c := startStandalone(t, t.TempDir())
	defer n.Stop()

	res, err := c.Append(ctx, batch(1, 1, "a", "b"))
	if err != nil || res.Status != fsm.StatusAppended || res.FirstOffset != 1 || res.LastOffset != 2 {
		t.Fatalf("append: %+v %v", res, err)
	}
	if res, _ := c.Append(ctx, batch(1, 1, "a", "b")); res.Status != fsm.StatusDuplicate {
		t.Fatalf("retry: %+v", res)
	}
	if res, _ := c.Append(ctx, batch(1, 5, "x")); res.Status != fsm.StatusSequenceGap || res.ExpectedSeq != 3 {
		t.Fatalf("gap: %+v", res)
	}
	_, err = c.Append(ctx, api.AppendRequest{ProducerID: 1, FirstSeq: 3})
	var apiErr *api.Error
	if !errors.As(err, &apiErr) || apiErr.Code != api.CodeInvalid {
		t.Fatalf("empty batch: %v", err)
	}

	evs, last, err := c.Read(ctx, 1, 10, true)
	if err != nil || last != 2 || len(evs) != 2 || string(evs[1].Payload) != "b" || evs[1].ProducerSeq != 2 {
		t.Fatalf("read: %+v last=%d err=%v", evs, last, err)
	}
	if res, err := c.Checkpoint(ctx, "replay", 2); err != nil || res.Status != fsm.StatusCheckpointed {
		t.Fatalf("checkpoint: %+v %v", res, err)
	}
	if cr, err := c.Consumer(ctx, "replay"); err != nil || cr.Offset != 2 || cr.Backlog != 0 {
		t.Fatalf("consumer: %+v %v", cr, err)
	}
	if p, err := c.Producer(ctx, 1); err != nil || p.LastSeq != 2 {
		t.Fatalf("producer: %+v %v", p, err)
	}
}

// Concurrent producers must get dense, unique offsets, and their batches must
// be visible in per-producer sequence order.
func TestStandaloneConcurrentProducers(t *testing.T) {
	ctx := context.Background()
	n, c := startStandalone(t, t.TempDir())
	defer n.Stop()

	const producers, batches = 16, 50
	var wg sync.WaitGroup
	for p := uint64(1); p <= producers; p++ {
		wg.Go(func() {
			for b := uint64(0); b < batches; b++ {
				res, err := c.Append(ctx, batch(p, 2*b+1, fmt.Sprint(p, b, 0), fmt.Sprint(p, b, 1)))
				if err != nil || res.Status != fsm.StatusAppended {
					t.Errorf("producer %d batch %d: %+v %v", p, b, res, err)
					return
				}
			}
		})
	}
	wg.Wait()
	total := uint64(producers * batches * 2)
	var all []event.Event
	for from := uint64(1); from <= total; {
		evs, _, err := c.Read(ctx, from, 1000, false)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, evs...)
		from += uint64(len(evs))
	}
	nextSeq := map[uint64]uint64{}
	for i, e := range all {
		if e.Offset != uint64(i+1) {
			t.Fatalf("offset %d at position %d", e.Offset, i)
		}
		if e.ProducerSeq != nextSeq[e.ProducerID]+1 {
			t.Fatalf("producer %d seq %d after %d", e.ProducerID, e.ProducerSeq, nextSeq[e.ProducerID])
		}
		nextSeq[e.ProducerID] = e.ProducerSeq
	}
	if uint64(len(all)) != total {
		t.Fatalf("read %d events, want %d", len(all), total)
	}
}

func TestStandaloneRestartKeepsLogAndDedupState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	n, c := startStandalone(t, dir)
	c.Append(ctx, batch(9, 1, "a", "b", "c"))
	c.Checkpoint(ctx, "risk", 3)
	n.Stop()

	n, c = startStandalone(t, dir)
	defer n.Stop()
	if res, _ := c.Append(ctx, batch(9, 3, "c")); res.Status != fsm.StatusDuplicate {
		t.Fatalf("dedup state lost: %+v", res)
	}
	if res, _ := c.Append(ctx, batch(9, 4, "d")); res.FirstOffset != 4 {
		t.Fatalf("offsets not continued: %+v", res)
	}
	if cr, _ := c.Consumer(ctx, "risk"); cr.Offset != 3 || cr.Backlog != 1 {
		t.Fatalf("checkpoint lost: %+v", cr)
	}
}

// A record failing its checksum must stop the node rather than be served.
func TestCorruptRecordFailsNode(t *testing.T) {
	ctx := context.Background()
	n, c := startStandalone(t, t.TempDir())
	defer n.Stop()
	c.Append(ctx, batch(1, 1, "a", "b"))
	storage.CorruptForTest(n.store, 2)

	_, _, err := c.Read(ctx, 1, 10, false)
	var apiErr *api.Error
	if !errors.As(err, &apiErr) || apiErr.Code != api.CodeCorrupt {
		t.Fatalf("read of corrupt record: %v", err)
	}
	select {
	case <-n.Failed():
	case <-time.After(time.Second):
		t.Fatal("node did not fail")
	}
	short, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if _, err := c.Append(short, batch(1, 3, "c")); !errors.As(err, &apiErr) || apiErr.Code != api.CodeNodeFailed {
		t.Fatalf("append after failure: %v", err)
	}
	st, _ := c.Status(ctx, c.Endpoints()[0])
	if st.Failed == "" {
		t.Fatal("status does not report the failure")
	}
}
