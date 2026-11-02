// Package replica drives one etcd-io/raft RawNode over our storage and state
// machine. It has no goroutines and reads no clocks, so the server and the
// deterministic test harness run the same code; the caller owns ticking,
// message delivery and threading (one goroutine at a time).
//
// For each Ready it follows the library's contract (design §7):
//
//  1. persist the snapshot, entries and hard state, fsynced when MustSync;
//  2. hand the messages to the caller, which sends them only after (1);
//  3. install the snapshot and apply committed entries;
//  4. Advance.
package replica

import (
	"errors"
	"fmt"
	"log/slog"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

type Config struct {
	ID uint64
	// Peers lists every voter. It is used only to bootstrap a new cluster;
	// after that, membership comes from the replicated log.
	Peers         []uint64
	ElectionTick  int
	HeartbeatTick int
	// Sync fsyncs Raft state before messages are sent. false voids durability
	// and exists only to measure what fsync costs.
	Sync   bool
	Logger *slog.Logger
}

// Output is what one Ready produced for the caller.
type Output struct {
	// Messages must be sent after Ready returns (state is persisted by then).
	Messages []*raftpb.Message
	// Results of commands applied on this node, in log order. A result's
	// ReqID matches the proposer's request only on the proposing node.
	Results []fsm.Result
	// Reads are request contexts of ReadIndex calls that are now safe to
	// serve: this node has applied everything committed before the request.
	Reads [][]byte
	// SoftState is set when the leader or this node's role changed.
	SoftState *raft.SoftState
	// Commit is the new commit index, or 0 if it did not change.
	Commit uint64

	PersistTime time.Duration
	Synced      bool
	ApplyTime   time.Duration
}

type Replica struct {
	cfg   Config
	log   *slog.Logger
	rn    *raft.RawNode
	ms    *raft.MemoryStorage
	store *storage.Store
	fsm   *fsm.FSM
	soft  raft.SoftState
	reads []raft.ReadState
}

// memStorage serves the log from memory but reports the ConfState saved with
// the state machine. MemoryStorage alone knows only the ConfState of its last
// snapshot, so after a restart before the first snapshot raft would come up
// with no voters.
type memStorage struct {
	*raft.MemoryStorage
	confState *raftpb.ConfState
}

func (m memStorage) InitialState() (*raftpb.HardState, *raftpb.ConfState, error) {
	hs, _, err := m.MemoryStorage.InitialState()
	return hs, m.confState, err
}

// Open loads persisted Raft state and the state machine, bootstrapping a new
// cluster if the store is empty.
func Open(cfg Config, st *storage.Store, f *fsm.FSM) (*Replica, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	rs, err := st.LoadRaftState()
	if err != nil {
		return nil, err
	}
	ms := raft.NewMemoryStorage()
	snapIndex := rs.Snapshot.GetMetadata().GetIndex()
	if snapIndex > 0 {
		if err := ms.ApplySnapshot(rs.Snapshot); err != nil {
			return nil, err
		}
	}
	if err := ms.SetHardState(rs.HardState); err != nil {
		return nil, err
	}
	if err := ms.Append(rs.Entries); err != nil {
		return nil, err
	}
	fresh := raft.IsEmptyHardState(rs.HardState) && len(rs.Entries) == 0 && snapIndex == 0
	if fresh && f.Applied() > 0 {
		return nil, errors.New("replica: state machine has data but the raft log is empty; was this directory used in standalone mode?")
	}
	// raft panics if Applied exceeds the persisted commit index. After a
	// snapshot install the state machine can be ahead of it; entries in
	// between are re-delivered and skipped by the state machine.
	applied := max(snapIndex, min(f.Applied(), rs.HardState.GetCommit()))
	log := cfg.Logger
	rn, err := raft.NewRawNode(&raft.Config{
		ID:                        cfg.ID,
		ElectionTick:              cfg.ElectionTick,
		HeartbeatTick:             cfg.HeartbeatTick,
		Storage:                   memStorage{ms, f.ConfState()},
		Applied:                   applied,
		MaxSizePerMsg:             1 << 20,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 64 << 20,
		// CheckQuorum makes a partitioned leader step down; PreVote keeps a
		// rejoining node from forcing an election with a higher term.
		CheckQuorum:               true,
		PreVote:                   true,
		DisableProposalForwarding: true,
		Logger:                    raftLogger{log.With("component", "raft")},
	})
	if err != nil {
		return nil, err
	}
	if fresh {
		peers := make([]raft.Peer, len(cfg.Peers))
		for i, id := range cfg.Peers {
			peers[i] = raft.Peer{ID: id}
		}
		if err := rn.Bootstrap(peers); err != nil {
			return nil, err
		}
	}
	r := &Replica{cfg: cfg, log: log, rn: rn, ms: ms, store: st, fsm: f}
	r.soft = rn.BasicStatus().SoftState
	log.Info("replica opened", "fresh", fresh, "applied", f.Applied(), "commit", rs.HardState.GetCommit(),
		"term", rs.HardState.GetTerm(), "snapshot", snapIndex, "log_entries", len(rs.Entries))
	return r, nil
}

func (r *Replica) Tick()                        { r.rn.Tick() }
func (r *Replica) Step(m *raftpb.Message) error { return r.rn.Step(m) }
func (r *Replica) Propose(data []byte) error    { return r.rn.Propose(data) }
func (r *Replica) ReadIndex(rctx []byte)        { r.rn.ReadIndex(rctx) }
func (r *Replica) ReportUnreachable(id uint64)  { r.rn.ReportUnreachable(id) }
func (r *Replica) Campaign() error              { return r.rn.Campaign() }
func (r *Replica) HasReady() bool               { return r.rn.HasReady() }
func (r *Replica) Status() raft.Status          { return r.rn.Status() }
func (r *Replica) SoftState() raft.SoftState    { return r.soft }
func (r *Replica) Term() uint64                 { return r.rn.BasicStatus().GetTerm() }
func (r *Replica) IsLeader() bool               { return r.soft.RaftState == raft.StateLeader }
func (r *Replica) FSM() *fsm.FSM                { return r.fsm }
func (r *Replica) LogBounds() (first, last uint64) {
	first, _ = r.ms.FirstIndex()
	last, _ = r.ms.LastIndex()
	return
}
func (r *Replica) TransferLeadership(to uint64) { r.rn.TransferLeader(to) }
func (r *Replica) SnapshotIndex() uint64        { s, _ := r.ms.Snapshot(); return s.GetMetadata().GetIndex() }

// ReportSnapshot tells raft whether a snapshot sent to id was installed.
func (r *Replica) ReportSnapshot(id uint64, ok bool) {
	status := raft.SnapshotFailure
	if ok {
		status = raft.SnapshotFinish
	}
	r.rn.ReportSnapshot(id, status)
}

// Ready processes one raft.Ready. Call it while HasReady is true. An error
// means storage failed and the replica must not be used again.
func (r *Replica) Ready() (Output, error) {
	rd := r.rn.Ready()
	var out Output
	if rd.SoftState != nil && *rd.SoftState != r.soft {
		r.soft = *rd.SoftState
		s := r.soft
		out.SoftState = &s
	}
	start := time.Now()
	synced, err := r.persist(&rd)
	if err != nil {
		return out, fmt.Errorf("replica: persist raft state: %w", err)
	}
	out.PersistTime, out.Synced = time.Since(start), synced
	out.Commit = rd.HardState.GetCommit()
	out.Messages = rd.Messages

	start = time.Now()
	if !raft.IsEmptySnap(rd.Snapshot) {
		return out, errors.New("replica: snapshot install is not implemented")
	}
	if out.Results, err = r.apply(rd.CommittedEntries); err != nil {
		return out, fmt.Errorf("replica: apply: %w", err)
	}
	out.ApplyTime = time.Since(start)

	r.reads = append(r.reads, rd.ReadStates...)
	applied := r.fsm.Applied()
	kept := r.reads[:0]
	for _, rs := range r.reads {
		if rs.Index <= applied {
			out.Reads = append(out.Reads, rs.RequestCtx)
		} else {
			kept = append(kept, rs)
		}
	}
	r.reads = kept
	r.rn.Advance(rd)
	return out, nil
}

// DropPendingReads forgets ReadIndex requests that have a read index but are
// waiting for apply; the caller fails them on leadership change.
func (r *Replica) DropPendingReads() { r.reads = nil }

func (r *Replica) persist(rd *raft.Ready) (bool, error) {
	hasSnap := !raft.IsEmptySnap(rd.Snapshot)
	if raft.IsEmptyHardState(rd.HardState) && len(rd.Entries) == 0 && !hasSnap {
		return false, nil
	}
	prevLast, _ := r.ms.LastIndex()
	sync := r.cfg.Sync && (rd.MustSync || hasSnap)
	if err := r.store.SaveRaft(rd.HardState, rd.Entries, rd.Snapshot, prevLast, sync); err != nil {
		return false, err
	}
	if hasSnap {
		if err := r.ms.ApplySnapshot(rd.Snapshot); err != nil {
			return false, err
		}
	}
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := r.ms.SetHardState(rd.HardState); err != nil {
			return false, err
		}
	}
	return sync, r.ms.Append(rd.Entries)
}

// apply hands membership changes to raft and everything to the state
// machine, which writes without fsync: the next synced Raft write makes the
// batch durable (single WAL, decision D4), and until then the entries are
// still in the Raft log for re-application.
func (r *Replica) apply(ents []*raftpb.Entry) ([]fsm.Result, error) {
	if len(ents) == 0 {
		return nil, nil
	}
	var cs *raftpb.ConfState
	for _, e := range ents {
		if e.GetIndex() <= r.fsm.Applied() {
			continue
		}
		var cc raftpb.ConfChangeI
		switch e.GetType() {
		case raftpb.EntryConfChange:
			c := &raftpb.ConfChange{}
			if err := proto.Unmarshal(e.GetData(), c); err != nil {
				return nil, fmt.Errorf("conf change at %d: %w", e.GetIndex(), err)
			}
			cc = c
		case raftpb.EntryConfChangeV2:
			c := &raftpb.ConfChangeV2{}
			if err := proto.Unmarshal(e.GetData(), c); err != nil {
				return nil, fmt.Errorf("conf change at %d: %w", e.GetIndex(), err)
			}
			cc = c
		default:
			continue
		}
		cs = r.rn.ApplyConfChange(cc)
	}
	return r.fsm.Apply(ents, cs, false)
}

type raftLogger struct{ l *slog.Logger }

func (r raftLogger) Debug(v ...any)              { r.l.Debug(fmt.Sprint(v...)) }
func (r raftLogger) Debugf(f string, v ...any)   { r.l.Debug(fmt.Sprintf(f, v...)) }
func (r raftLogger) Info(v ...any)               { r.l.Info(fmt.Sprint(v...)) }
func (r raftLogger) Infof(f string, v ...any)    { r.l.Info(fmt.Sprintf(f, v...)) }
func (r raftLogger) Warning(v ...any)            { r.l.Warn(fmt.Sprint(v...)) }
func (r raftLogger) Warningf(f string, v ...any) { r.l.Warn(fmt.Sprintf(f, v...)) }
func (r raftLogger) Error(v ...any)              { r.l.Error(fmt.Sprint(v...)) }
func (r raftLogger) Errorf(f string, v ...any)   { r.l.Error(fmt.Sprintf(f, v...)) }
func (r raftLogger) Fatal(v ...any)              { r.Panic(v...) }
func (r raftLogger) Fatalf(f string, v ...any)   { r.Panicf(f, v...) }
func (r raftLogger) Panic(v ...any)              { s := fmt.Sprint(v...); r.l.Error(s); panic(s) }
func (r raftLogger) Panicf(f string, v ...any)   { s := fmt.Sprintf(f, v...); r.l.Error(s); panic(s) }
