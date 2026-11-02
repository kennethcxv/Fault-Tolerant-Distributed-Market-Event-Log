package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
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
	id      uint64
	cluster string
	rep     *replica.Replica
	tr      *transport
	peers   map[uint64]string
	tick    time.Duration
	fsync   bool
	log     *slog.Logger
	m       *metrics

	proposals chan *raftProposal
	reads     chan chan error
	inbound   chan []*raftpb.Message
	reports   chan peerReport
	statusReq chan chan api.Status
	memberReq chan memberChange
	stopped   chan struct{}

	// Owned by run. Request ids start from a random epoch so that a result
	// for an entry proposed by an earlier incarnation of this node can never
	// be delivered to a new request.
	nextID      uint64
	confPending bool // a membership change is proposed but not yet applied
	waiters     map[uint64]waiter
	readWaiters map[uint64]readWaiter
}

type readWaiter struct {
	done     chan error
	deadline time.Time
}

type memberChange struct {
	id   uint64
	url  string // "" removes
	done chan error
}

// peerReport tells the run loop that a peer was unreachable or that a
// snapshot send finished.
type peerReport struct {
	peer     uint64
	snapshot bool
	ok       bool
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

func newRaftEngine(id uint64, cluster string, rep *replica.Replica, flagPeers map[uint64]string, tick time.Duration, fsync bool, log *slog.Logger, f *faults, m *metrics) *raftEngine {
	// Address book: peers from flags, overridden by the replicated membership.
	peers := map[uint64]string{}
	for id, url := range flagPeers {
		peers[id] = url
	}
	for id, url := range rep.FSM().Members() {
		peers[id] = url
	}
	var seed [8]byte
	rand.Read(seed[:])
	e := &raftEngine{
		id: id, cluster: cluster, rep: rep, peers: peers, tick: tick, fsync: fsync, log: log, m: m,
		proposals:   make(chan *raftProposal, 4096),
		reads:       make(chan chan error, 1024),
		inbound:     make(chan []*raftpb.Message, 1024),
		reports:     make(chan peerReport, 1024),
		statusReq:   make(chan chan api.Status),
		memberReq:   make(chan memberChange),
		stopped:     make(chan struct{}),
		nextID:      binary.LittleEndian.Uint64(seed[:]),
		waiters:     map[uint64]waiter{},
		readWaiters: map[uint64]readWaiter{},
	}
	e.tr = newTransport(id, cluster, peers, log, f, func(peer uint64) {
		select {
		case e.reports <- peerReport{peer: peer}:
		default:
		}
	})
	return e
}

func (e *raftEngine) run(ctx context.Context) (err error) {
	defer close(e.stopped)
	// etcd-io/raft panics when it detects state it cannot continue from, most
	// importantly a leader committing entries this node's log no longer has
	// (its disk lost acknowledged writes). Stop the node with that reason
	// instead of crashing the process mid-request.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("raft stopped this node: %v (if the log was lost or truncated, the node must be replaced)", r)
		}
	}()
	defer e.failWaiters(&api.Error{Code: api.CodeOutcomeUnknown, Message: "node stopped while the command was pending"})
	e.tr.start(ctx)
	ticker := time.NewTicker(e.tick)
	defer ticker.Stop()
	ticks, proposed := 0, 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			e.rep.Tick()
			if ticks++; ticks%10 == 0 {
				if e.rep.IsLeader() {
					e.m.observeProgress(e.rep)
				}
				now := time.Now()
				for id, w := range e.readWaiters {
					if now.After(w.deadline) {
						delete(e.readWaiters, id)
					}
				}
			}
		case msgs := <-e.inbound:
			for _, m := range msgs {
				// Errors here are stale or misdirected messages, which Raft
				// expects under partitions and restarts.
				e.rep.Step(m)
			}
		case p := <-e.proposals:
			e.handleProposal(p)
			proposed++
		case done := <-e.reads:
			e.readIndex(done)
		case r := <-e.reports:
			if r.snapshot {
				e.rep.ReportSnapshot(r.peer, r.ok)
				e.m.snapshotsSent.WithLabelValues(fmt.Sprint(r.ok)).Inc()
			} else {
				e.rep.ReportUnreachable(r.peer)
			}
		case ch := <-e.statusReq:
			ch <- e.buildStatus()
		case mc := <-e.memberReq:
			mc.done <- e.proposeMembership(mc)
		}
		// Take everything else already queued before the next Ready, so one
		// persistence round (one fsync) covers all of it: on the leader,
		// proposals that arrived microseconds apart; on a follower, several
		// append batches from the leader. Bounded so a flood of input cannot
		// starve the Ready loop.
		for drained := 0; drained < 4096; drained++ {
			select {
			case msgs := <-e.inbound:
				for _, m := range msgs {
					e.rep.Step(m)
				}
				continue
			case p := <-e.proposals:
				e.handleProposal(p)
				proposed++
				continue
			case done := <-e.reads:
				e.readIndex(done)
				continue
			default:
			}
			break
		}
		if proposed > 0 {
			e.m.groupSize.Observe(float64(proposed))
			proposed = 0
		}
		if err := e.processReady(ctx); err != nil {
			return err
		}
	}
}

func (e *raftEngine) processReady(ctx context.Context) error {
	for e.rep.HasReady() {
		out, err := e.rep.Ready(func(msgs []*raftpb.Message) { e.sendMessages(ctx, msgs) })
		if err != nil {
			return err
		}
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
			if w, ok := e.readWaiters[id]; ok {
				w.done <- nil
				delete(e.readWaiters, id)
			}
		}
		if out.SoftState != nil {
			e.onRoleChange(*out.SoftState)
		}
		if out.Members != nil {
			e.confPending = false
		}
		for id, url := range out.Members {
			if id != e.id && url != e.peers[id] {
				e.log.Info("membership changed", "peer", id, "url", url)
				e.tr.setPeer(id, url)
				if url == "" {
					delete(e.peers, id)
				} else {
					e.peers[id] = url
				}
			}
		}
		if out.Removed {
			return errors.New("this node was removed from the cluster by a membership change")
		}
	}
	return nil
}

// proposeMembership validates and proposes one voter change. It returns once
// the change is proposed; callers watch /v1/status for it to take effect.
func (e *raftEngine) proposeMembership(mc memberChange) error {
	if !e.rep.IsLeader() {
		return e.notLeader()
	}
	// Raft allows one pending change and silently turns a second into a
	// no-op; reject it explicitly instead.
	if e.confPending {
		return &api.Error{Code: api.CodeOverloaded, Message: "another membership change is still being applied; retry"}
	}
	voters := e.rep.FSM().ConfState().GetVoters()
	isVoter := slices.Contains(voters, mc.id)
	cc := &raftpb.ConfChange{NodeId: new(mc.id)}
	switch {
	case mc.id == 0:
		return &api.Error{Code: api.CodeInvalid, Message: "member id must be >= 1"}
	case mc.url != "" && isVoter:
		return &api.Error{Code: api.CodeInvalid, Message: fmt.Sprintf("node %d is already a member", mc.id)}
	case mc.url == "" && !isVoter:
		return &api.Error{Code: api.CodeInvalid, Message: fmt.Sprintf("node %d is not a member", mc.id)}
	case mc.url == "" && len(voters) == 1:
		return &api.Error{Code: api.CodeInvalid, Message: "cannot remove the last member"}
	case mc.url != "":
		cc.Type, cc.Context = raftpb.ConfChangeAddNode.Enum(), []byte(mc.url)
	default:
		cc.Type = raftpb.ConfChangeRemoveNode.Enum()
	}
	if err := e.rep.ProposeConfChange(cc); err != nil {
		return &api.Error{Code: api.CodeOverloaded, Message: "membership change dropped: " + err.Error()}
	}
	e.confPending = true
	e.log.Info("membership change proposed", "type", cc.GetType().String(), "node", mc.id, "url", mc.url)
	return nil
}

func (e *raftEngine) changeMembership(ctx context.Context, id uint64, url string) error {
	mc := memberChange{id: id, url: url, done: make(chan error, 1)}
	select {
	case e.memberReq <- mc:
	case <-e.stopped:
		return &api.Error{Code: api.CodeNodeFailed, Message: "node is not running"}
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-mc.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sendMessages hands ordinary messages to the per-peer queues and starts a
// stream for each snapshot. The snapshot is prepared here, on the goroutine
// that applies entries, so its metadata matches the state being streamed.
func (e *raftEngine) sendMessages(ctx context.Context, msgs []*raftpb.Message) {
	plain := msgs[:0:0]
	for _, m := range msgs {
		if m.GetType() != raftpb.MsgSnap {
			plain = append(plain, m)
			continue
		}
		to := m.GetTo()
		msg, ss, err := e.rep.PrepareSnapshot(m)
		if err != nil {
			e.log.Error("cannot prepare snapshot", "peer", to, "err", err)
			e.rep.ReportSnapshot(to, false)
			continue
		}
		e.log.Info("sending snapshot", "peer", to, "index", ss.Applied)
		go e.tr.sendSnapshot(ctx, msg, ss, func(ok bool) {
			select {
			case e.reports <- peerReport{peer: to, snapshot: true, ok: ok}:
			case <-ctx.Done():
			}
		})
	}
	e.tr.send(plain)
}

// onRoleChange resolves requests that can no longer complete normally. A
// write proposed by a deposed leader may still commit under the new leader,
// so its outcome is unknown, not failed. Reads are safe to retry.
func (e *raftEngine) onRoleChange(s raft.SoftState) {
	e.log.Info("raft role changed", "role", s.RaftState.String(), "leader", s.Lead, "term", e.rep.Term())
	e.m.onRoleChange(s)
	e.confPending = false // a new leader's log decides; a stale flag must not block changes
	if s.RaftState != raft.StateLeader {
		e.failWaiters(&api.Error{Code: api.CodeOutcomeUnknown, Message: "leadership changed while the command was pending; retry with the same sequence numbers"})
	}
	for id, w := range e.readWaiters {
		w.done <- &api.Error{Code: api.CodeNoLeader, Message: "leadership changed during a linearizable read; retry"}
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
	// The caller gives up after its request timeout; the deadline lets the
	// loop forget reads raft never answers (dropped while no leader).
	e.readWaiters[e.nextID] = readWaiter{done: done, deadline: time.Now().Add(time.Minute)}
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
		ID: e.id, Mode: "raft", ClusterID: e.cluster, Role: roleName(st.RaftState), Term: st.GetTerm(), Leader: st.Lead,
		LeaderURL: e.peers[st.Lead], Commit: st.GetCommit(), Applied: f.Applied(), LastOffset: f.LastOffset(),
		FirstIndex: first, LastIndex: last, Snapshot: e.rep.SnapshotIndex(), Fsync: fsyncName(e.fsync),
	}
	for _, id := range f.ConfState().GetVoters() {
		s.Members = append(s.Members, api.Member{ID: id, URL: e.peers[id]})
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
