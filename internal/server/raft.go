package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"log/slog"
	"slices"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/replica"
)

// raftEngine owns the replica. Everything that touches it runs on the run
// goroutine; HTTP handlers and transport goroutines talk to it over channels.
type raftEngine struct {
	id    uint64
	rep   *replica.Replica
	tr    *transport
	peers map[uint64]string
	tick  time.Duration
	fsync bool
	log   *slog.Logger
	m     *metrics

	proposals chan *raftProposal
	reads     chan chan error
	inbound   chan []*raftpb.Message
	reports   chan uint64
	statusReq chan chan api.Status
	stopped   chan struct{}

	// Owned by run. Request ids start from a random epoch so that a result
	// for an entry proposed by an earlier incarnation of this node can never
	// be delivered to a new request.
	nextID      uint64
	waiters     map[uint64]waiter
	readWaiters map[uint64]chan error
}

type raftProposal struct {
	cmd  *fsm.Command
	done chan proposeResult
}

type waiter struct {
	done  chan proposeResult
	start time.Time
}

type proposeResult struct {
	res fsm.Result
	err error
}

func newRaftEngine(id uint64, rep *replica.Replica, peers map[uint64]string, tick time.Duration, fsync bool, log *slog.Logger, f *faults, m *metrics) *raftEngine {
	var seed [8]byte
	rand.Read(seed[:])
	e := &raftEngine{
		id: id, rep: rep, peers: peers, tick: tick, fsync: fsync, log: log, m: m,
		proposals:   make(chan *raftProposal, 4096),
		reads:       make(chan chan error, 1024),
		inbound:     make(chan []*raftpb.Message, 1024),
		reports:     make(chan uint64, 1024),
		statusReq:   make(chan chan api.Status),
		stopped:     make(chan struct{}),
		nextID:      binary.LittleEndian.Uint64(seed[:]),
		waiters:     map[uint64]waiter{},
		readWaiters: map[uint64]chan error{},
	}
	e.tr = newTransport(id, peers, log, f, func(peer uint64) {
		select {
		case e.reports <- peer:
		default:
		}
	})
	return e
}

func (e *raftEngine) run(ctx context.Context) error {
	defer close(e.stopped)
	defer e.failWaiters(&api.Error{Code: api.CodeOutcomeUnknown, Message: "node stopped while the command was pending"})
	e.tr.start(ctx)
	ticker := time.NewTicker(e.tick)
	defer ticker.Stop()
	ticks := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			e.rep.Tick()
			if ticks++; ticks%10 == 0 && e.rep.IsLeader() {
				e.m.observeProgress(e.rep)
			}
		case msgs := <-e.inbound:
			for _, m := range msgs {
				// Errors here are stale or misdirected messages, which Raft
				// expects under partitions and restarts.
				e.rep.Step(m)
			}
		case p := <-e.proposals:
			// Take everything already queued so it shares one Ready (and
			// one fsync). Only this goroutine receives, so len is safe.
			e.handleProposal(p)
			for n := len(e.proposals); n > 0; n-- {
				e.handleProposal(<-e.proposals)
			}
		case done := <-e.reads:
			e.readIndex(done)
		case peer := <-e.reports:
			e.rep.ReportUnreachable(peer)
		case ch := <-e.statusReq:
			ch <- e.buildStatus()
		}
		if err := e.processReady(); err != nil {
			return err
		}
	}
}

func (e *raftEngine) processReady() error {
	for e.rep.HasReady() {
		out, err := e.rep.Ready()
		if err != nil {
			return err
		}
		e.tr.send(out.Messages)
		e.m.observeReady(&out)
		for _, r := range out.Results {
			e.m.observeResult(r)
			if w, ok := e.waiters[r.ReqID]; ok {
				e.m.commitLatency.Observe(time.Since(w.start).Seconds())
				w.done <- proposeResult{res: r}
				delete(e.waiters, r.ReqID)
			}
		}
		for _, rctx := range out.Reads {
			id := binary.LittleEndian.Uint64(rctx)
			if ch, ok := e.readWaiters[id]; ok {
				ch <- nil
				delete(e.readWaiters, id)
			}
		}
		if out.SoftState != nil {
			e.onRoleChange(*out.SoftState)
		}
	}
	return nil
}

// onRoleChange resolves requests that can no longer complete normally. A
// write proposed by a deposed leader may still commit under the new leader,
// so its outcome is unknown, not failed. Reads are safe to retry.
func (e *raftEngine) onRoleChange(s raft.SoftState) {
	e.log.Info("raft role changed", "role", s.RaftState.String(), "leader", s.Lead, "term", e.rep.Term())
	e.m.onRoleChange(s)
	if s.RaftState != raft.StateLeader {
		e.failWaiters(&api.Error{Code: api.CodeOutcomeUnknown, Message: "leadership changed while the command was pending; retry with the same sequence numbers"})
	}
	for id, ch := range e.readWaiters {
		ch <- &api.Error{Code: api.CodeNoLeader, Message: "leadership changed during a linearizable read; retry"}
		delete(e.readWaiters, id)
	}
	e.rep.DropPendingReads()
}

func (e *raftEngine) failWaiters(err error) {
	for id, w := range e.waiters {
		w.done <- proposeResult{err: err}
		delete(e.waiters, id)
	}
}

func (e *raftEngine) notLeader() error {
	lead := e.rep.SoftState().Lead
	if lead == 0 {
		return &api.Error{Code: api.CodeNoLeader, Message: "no leader is known; an election may be in progress or quorum is lost"}
	}
	return &api.Error{Code: api.CodeNotLeader, Message: "this node is not the leader", LeaderID: lead, LeaderURL: e.peers[lead]}
}

func (e *raftEngine) handleProposal(p *raftProposal) {
	if !e.rep.IsLeader() {
		p.done <- proposeResult{err: e.notLeader()}
		return
	}
	e.nextID++
	p.cmd.ReqID = e.nextID
	if err := e.rep.Propose(p.cmd.Marshal()); err != nil {
		// Dropped before entering the log (uncommitted-size limit or a
		// leadership transfer): the command was definitely not applied.
		p.done <- proposeResult{err: &api.Error{Code: api.CodeOverloaded, Message: "proposal dropped: " + err.Error()}}
		return
	}
	e.waiters[p.cmd.ReqID] = waiter{done: p.done, start: time.Now()}
	e.m.proposals.Inc()
}

func (e *raftEngine) readIndex(done chan error) {
	if e.rep.SoftState().Lead == 0 {
		done <- e.notLeader()
		return
	}
	e.nextID++
	e.readWaiters[e.nextID] = done
	e.rep.ReadIndex(binary.LittleEndian.AppendUint64(nil, e.nextID))
}

// propose is called by HTTP handlers.
func (e *raftEngine) propose(ctx context.Context, cmd *fsm.Command) (fsm.Result, error) {
	p := &raftProposal{cmd: cmd, done: make(chan proposeResult, 1)}
	select {
	case e.proposals <- p:
	case <-e.stopped:
		return fsm.Result{}, &api.Error{Code: api.CodeNodeFailed, Message: "node is not running"}
	case <-ctx.Done():
		return fsm.Result{}, &api.Error{Code: api.CodeOverloaded, Message: "proposal queue full; command was not proposed"}
	}
	select {
	case r := <-p.done:
		return r.res, r.err
	case <-e.stopped:
	case <-ctx.Done():
	}
	return fsm.Result{}, &api.Error{Code: api.CodeOutcomeUnknown, Message: "timed out waiting for the command to commit; retry with the same sequence numbers"}
}

func (e *raftEngine) readBarrier(ctx context.Context) error {
	done := make(chan error, 1)
	select {
	case e.reads <- done:
	case <-e.stopped:
		return &api.Error{Code: api.CodeNodeFailed, Message: "node is not running"}
	case <-ctx.Done():
		return &api.Error{Code: api.CodeTimeout, Message: "read queue full"}
	}
	select {
	case err := <-done:
		return err
	case <-e.stopped:
		return &api.Error{Code: api.CodeNodeFailed, Message: "node is not running"}
	case <-ctx.Done():
		return &api.Error{Code: api.CodeTimeout, Message: "could not confirm leadership in time; quorum may be lost"}
	}
}

func (e *raftEngine) status() api.Status {
	ch := make(chan api.Status, 1)
	select {
	case e.statusReq <- ch:
		return <-ch
	case <-e.stopped:
	case <-time.After(2 * time.Second):
		return api.Status{ID: e.id, Mode: "raft", Role: "unresponsive"}
	}
	return api.Status{ID: e.id, Mode: "raft", Role: "stopped"}
}

func (e *raftEngine) buildStatus() api.Status {
	st := e.rep.Status()
	first, last := e.rep.LogBounds()
	f := e.rep.FSM()
	s := api.Status{
		ID: e.id, Mode: "raft", Role: roleName(st.RaftState), Term: st.GetTerm(), Leader: st.Lead,
		LeaderURL: e.peers[st.Lead], Commit: st.GetCommit(), Applied: f.Applied(), LastOffset: f.LastOffset(),
		FirstIndex: first, LastIndex: last, Snapshot: e.rep.SnapshotIndex(), Fsync: fsyncName(e.fsync),
	}
	ids := make([]uint64, 0, len(st.Progress))
	for id := range st.Progress {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		pr := st.Progress[id]
		s.Peers = append(s.Peers, api.PeerStatus{ID: id, URL: e.peers[id], Match: pr.Match, Next: pr.Next,
			State: pr.State.String(), Lag: last - min(pr.Match, last)})
	}
	return s
}

func roleName(s raft.StateType) string {
	switch s {
	case raft.StateLeader:
		return "leader"
	case raft.StateCandidate, raft.StatePreCandidate:
		return "candidate"
	}
	return "follower"
}
