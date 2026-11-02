package sim

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/anishathalye/porcupine"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/orderbook"
)

// Workload drives simulated producers and readers the way real clients
// behave: one outstanding request each, retried with the same producer
// sequence after a timeout or a leader change. It records every acknowledged
// append and a client history for the linearizability checker.
type Workload struct {
	c       *Cluster
	clients []*simClient
	nextReq uint64
	// Timeout is how long a client waits before retrying, in virtual ms.
	Timeout int64
	// ReadPercent of client operations are linearizable reads.
	ReadPercent int

	acked   map[[2]uint64]uint64 // (producer, seq) -> offset
	history []porcupine.Operation
	errs    []string
}

type simClient struct {
	id       int
	producer uint64
	gen      *orderbook.Generator
	nextSeq  uint64
	events   map[uint64]fsm.EventIn // seq -> event, so retries resend identical bytes
	limit    uint64                 // stop after this many acknowledged appends

	op       *pendingOp
	attempts int
}

type pendingOp struct {
	node   uint64
	reqID  uint64
	input  OpInput
	callAt int64
}

func NewWorkload(c *Cluster, clients int, appendsPerClient uint64) *Workload {
	w := &Workload{c: c, Timeout: 300, acked: map[[2]uint64]uint64{}}
	for i := range clients {
		w.clients = append(w.clients, &simClient{
			id: i, producer: uint64(i + 1), nextSeq: 1, limit: appendsPerClient,
			gen: orderbook.NewGenerator(c.opts.Seed*1000+uint64(i), 2), events: map[uint64]fsm.EventIn{},
		})
	}
	prevResult, prevRead := c.opts.OnResult, c.opts.OnRead
	c.opts.OnResult = func(node uint64, r fsm.Result) {
		w.onResult(node, r)
		if prevResult != nil {
			prevResult(node, r)
		}
	}
	c.opts.OnRead = func(node uint64, rctx []byte) {
		w.onRead(node, rctx)
		if prevRead != nil {
			prevRead(node, rctx)
		}
	}
	return w
}

// Done reports whether every client has all its appends acknowledged.
func (w *Workload) Done() bool {
	for _, cl := range w.clients {
		if cl.nextSeq <= cl.limit || cl.op != nil {
			return false
		}
	}
	return true
}

// Acked returns the number of acknowledged appends.
func (w *Workload) Acked() int { return len(w.acked) }

// Tick lets every idle client issue its next operation and times out stale
// ones. Call it once per cluster step.
func (w *Workload) Tick() {
	for _, cl := range w.clients {
		if cl.op != nil && w.c.Now-cl.op.callAt > w.Timeout {
			// Outcome unknown: record the attempt as possibly applied at any
			// later point, then retry the same sequence.
			w.finish(cl, Output{Unknown: true}, math.MaxInt64)
		}
		if cl.op == nil {
			w.issue(cl)
		}
	}
}

func (w *Workload) issue(cl *simClient) {
	leader := w.c.Leader()
	if leader == nil {
		return
	}
	w.nextReq++
	reqID := w.nextReq
	if w.ReadPercent > 0 && w.c.rng.IntN(100) < w.ReadPercent {
		in := OpInput{Read: true, Offset: 1 + uint64(w.c.rng.IntN(int(leader.rep.FSM().LastOffset())+2))}
		rctx := binary.LittleEndian.AppendUint64(nil, reqID)
		leader.rep.ReadIndex(rctx)
		cl.op = &pendingOp{node: leader.ID, reqID: reqID, input: in, callAt: w.c.Now}
		return
	}
	if cl.nextSeq > cl.limit {
		return
	}
	ev, ok := cl.events[cl.nextSeq]
	if !ok {
		m := cl.gen.Next()
		ev = fsm.EventIn{Type: m.Type, Schema: orderbook.SchemaV1, SourceTS: w.c.Now * 1e6, Payload: m.Encode()}
		cl.events[cl.nextSeq] = ev
	}
	cmd := fsm.Command{ReqID: reqID, Append: &fsm.Append{
		ProducerID: cl.producer, FirstSeq: cl.nextSeq, IngestTS: w.c.Now * 1e6, Events: []fsm.EventIn{ev},
	}}
	if err := w.c.Propose(leader.ID, &cmd); err != nil {
		return // proposal dropped before it entered the log: not an operation
	}
	cl.attempts++
	cl.op = &pendingOp{node: leader.ID, reqID: reqID, callAt: w.c.Now,
		input: OpInput{Producer: cl.producer, Seq: cl.nextSeq, Payload: string(ev.Payload)}}
}

func (w *Workload) onResult(node uint64, r fsm.Result) {
	for _, cl := range w.clients {
		op := cl.op
		if op == nil || op.input.Read || op.node != node || op.reqID != r.ReqID {
			continue
		}
		out := Output{Status: r.Status, Offset: r.FirstOffset, ProducerLastSeq: r.ProducerLastSeq}
		switch r.Status {
		case fsm.StatusAppended:
			w.acked[[2]uint64{cl.producer, op.input.Seq}] = r.FirstOffset
		case fsm.StatusDuplicate:
			// An earlier attempt committed. Its offset is known only when it
			// was the producer's latest append.
			if r.ProducerLastSeq == op.input.Seq {
				w.acked[[2]uint64{cl.producer, op.input.Seq}] = r.ProducerLastOffset
			}
		default:
			w.errs = append(w.errs, fmt.Sprintf("producer %d seq %d: unexpected %s", cl.producer, op.input.Seq, r.Status))
		}
		cl.nextSeq++
		w.finish(cl, out, w.c.Now)
		return
	}
}

func (w *Workload) onRead(node uint64, rctx []byte) {
	reqID := binary.LittleEndian.Uint64(rctx)
	for _, cl := range w.clients {
		op := cl.op
		if op == nil || !op.input.Read || op.node != node || op.reqID != reqID {
			continue
		}
		n := w.c.Node(node)
		out := Output{NotFound: true}
		if last := n.rep.FSM().LastOffset(); op.input.Offset <= last {
			evs, err := n.store.Read(op.input.Offset, last, 1)
			if err != nil || len(evs) != 1 {
				w.errs = append(w.errs, fmt.Sprintf("node %d read %d: %v", node, op.input.Offset, err))
				return
			}
			out = Output{Producer: evs[0].ProducerID, Seq: evs[0].ProducerSeq, Payload: string(evs[0].Payload)}
		}
		w.finish(cl, out, w.c.Now)
		return
	}
}

func (w *Workload) finish(cl *simClient, out Output, returnAt int64) {
	op := cl.op
	cl.op = nil
	if op.input.Read && out.Unknown {
		return // reads have no effect, so an unanswered read is not an operation
	}
	w.history = append(w.history, porcupine.Operation{ClientId: cl.id, Input: op.input, Call: op.callAt, Output: out, Return: returnAt})
}

// CheckAcked verifies invariants I2 and I3 against the given log: every
// acknowledged append is at its offset with its payload, each (producer, seq)
// appears once, and each producer's sequences are contiguous.
func (w *Workload) CheckAcked(log []event.Event) error {
	if len(w.errs) > 0 {
		return fmt.Errorf("workload errors: %v", w.errs)
	}
	seen := map[[2]uint64]bool{}
	last := map[uint64]uint64{}
	for i, e := range log {
		if e.Offset != uint64(i+1) {
			return fmt.Errorf("I3: offset %d at position %d", e.Offset, i)
		}
		key := [2]uint64{e.ProducerID, e.ProducerSeq}
		if seen[key] {
			return fmt.Errorf("I3: producer %d seq %d appears twice", e.ProducerID, e.ProducerSeq)
		}
		seen[key] = true
		if e.ProducerSeq != last[e.ProducerID]+1 {
			return fmt.Errorf("I3: producer %d seq %d follows %d", e.ProducerID, e.ProducerSeq, last[e.ProducerID])
		}
		last[e.ProducerID] = e.ProducerSeq
	}
	for key, off := range w.acked {
		if off == 0 || off > uint64(len(log)) {
			return fmt.Errorf("I2: acked producer %d seq %d at offset %d, log has %d events", key[0], key[1], off, len(log))
		}
		e := log[off-1]
		cl := w.clients[key[0]-1]
		want := cl.events[key[1]]
		if e.ProducerID != key[0] || e.ProducerSeq != key[1] || e.Type != want.Type || !bytes.Equal(e.Payload, want.Payload) {
			return fmt.Errorf("I2: acked producer %d seq %d at offset %d, found producer %d seq %d", key[0], key[1], off, e.ProducerID, e.ProducerSeq)
		}
	}
	return nil
}

func (w *Workload) History() []porcupine.Operation { return w.history }

// Run advances the cluster with the workload active until done returns true.
func (w *Workload) Run(maxSteps int, done func() bool) error {
	return w.c.Run(maxSteps, func() bool {
		if done() {
			return true
		}
		w.Tick()
		return false
	})
}

// Steps advances n ticks with the workload active.
func (w *Workload) Steps(n int) error {
	for range n {
		w.Tick()
		if err := w.c.Step(); err != nil {
			return err
		}
	}
	return nil
}
