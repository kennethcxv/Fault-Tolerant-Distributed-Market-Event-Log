// Package server runs a node: it owns the goroutines, the HTTP API and the
// peer transport around the single-threaded state machine.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/replica"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

type Config struct {
	ID      uint64
	Listen  string // host:port
	DataDir string
	FS      vfs.FS // nil means the OS filesystem
	// Fsync=false skips fsync on every write. It exists to measure fsync cost
	// and voids the durability guarantee (design §5).
	Fsync bool
	// Standalone runs without Raft: proposals are applied directly with one
	// fsync per group. It is the single-node baseline for benchmarks.
	Standalone bool
	// Peers maps every voter's id to its base URL (raft mode). All nodes must
	// be started with the same map.
	Peers map[uint64]string
	// SnapshotEntries and CompactKeep control log compaction (replica.Config).
	SnapshotEntries uint64
	CompactKeep     uint64
	// TickInterval is one Raft tick; elections time out after 10-20 ticks
	// and leaders heartbeat every tick.
	TickInterval time.Duration
	// RequestTimeout bounds how long a client waits for its command to apply.
	RequestTimeout time.Duration
	// Listener, if set, is used instead of listening on Listen (tests).
	Listener net.Listener
	Logger   *slog.Logger
}

// engine orders and applies commands. The standalone engine applies them
// directly; the Raft engine replicates them first.
type engine interface {
	run(ctx context.Context) error
	propose(ctx context.Context, cmd *fsm.Command) (fsm.Result, error)
	// readBarrier returns once this node has applied everything committed
	// before the call (ReadIndex in Raft mode).
	readBarrier(ctx context.Context) error
	status() api.Status
}

type Node struct {
	cfg   Config
	log   *slog.Logger
	store *storage.Store
	fsm   *fsm.FSM
	eng   engine
	raft  *raftEngine // nil in standalone mode
	m     *metrics
	flts  *faults
	ln    net.Listener
	http  *http.Server

	ctx       context.Context
	cancel    context.CancelFunc
	engDone   chan struct{}
	failOnce  sync.Once
	failErr   error
	failed    chan struct{}
	closeOnce sync.Once
}

func Start(cfg Config) (*Node, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 5 * time.Second
	}
	if cfg.TickInterval == 0 {
		cfg.TickInterval = 100 * time.Millisecond
	}
	if !cfg.Standalone && cfg.Peers[cfg.ID] == "" {
		return nil, fmt.Errorf("server: --peers must include this node's id %d", cfg.ID)
	}
	log := cfg.Logger.With("node", cfg.ID)
	st, err := storage.Open(storage.Options{Dir: cfg.DataDir, FS: cfg.FS, Logger: log})
	if err != nil {
		return nil, err
	}
	f, err := fsm.Open(st)
	if err != nil {
		st.Close()
		return nil, fmt.Errorf("server: load state: %w", err)
	}
	n := &Node{cfg: cfg, log: log, store: st, fsm: f, engDone: make(chan struct{}), failed: make(chan struct{}),
		m: newMetrics(f, st), flts: newFaults()}
	if cfg.Standalone {
		n.eng = newStandalone(cfg.ID, f, cfg.Fsync, n.m)
	} else {
		peers := make([]uint64, 0, len(cfg.Peers))
		for id := range cfg.Peers {
			peers = append(peers, id)
		}
		rep, err := replica.Open(replica.Config{ID: cfg.ID, Peers: peers, ElectionTick: 10, HeartbeatTick: 1,
			Sync: cfg.Fsync, SnapshotEntries: cfg.SnapshotEntries, CompactKeep: cfg.CompactKeep, Logger: log}, st, f)
		if err != nil {
			st.Close()
			return nil, err
		}
		n.raft = newRaftEngine(cfg.ID, rep, cfg.Peers, cfg.TickInterval, cfg.Fsync, log, n.flts, n.m)
		n.eng = n.raft
	}
	n.ln = cfg.Listener
	if n.ln == nil {
		if n.ln, err = net.Listen("tcp", cfg.Listen); err != nil {
			st.Close()
			return nil, err
		}
	}
	n.ctx, n.cancel = context.WithCancel(context.Background())
	n.http = &http.Server{Handler: n.routes(), ReadHeaderTimeout: 5 * time.Second}

	go func() {
		defer close(n.engDone)
		if err := n.eng.run(n.ctx); err != nil {
			n.fail(err)
		}
	}()
	go func() {
		if err := n.http.Serve(n.ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			n.fail(fmt.Errorf("http: %w", err))
		}
	}()
	log.Info("node started", "listen", n.ln.Addr().String(), "applied", f.Applied(), "last_offset", f.LastOffset(),
		"fsync", cfg.Fsync, "standalone", cfg.Standalone)
	return n, nil
}

func (n *Node) Addr() string { return n.ln.Addr().String() }

// Failed is closed when the node hits an unrecoverable error (storage failure
// or detected corruption). The node keeps answering HTTP with node_failed
// until Stop, so operators can still read its status.
func (n *Node) Failed() <-chan struct{} { return n.failed }

func (n *Node) Err() error {
	select {
	case <-n.failed:
		return n.failErr
	default:
		return nil
	}
}

// fail stops the engine: after a storage error the in-memory state may be
// ahead of disk, and after corruption the data cannot be trusted.
func (n *Node) fail(err error) {
	n.failOnce.Do(func() {
		n.failErr = err
		n.log.Error("node failed; no longer serving", "err", err)
		n.m.failed.Set(1)
		close(n.failed)
		n.cancel()
	})
}

func (n *Node) Stop() error {
	var err error
	n.closeOnce.Do(func() {
		n.cancel()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err = n.http.Shutdown(ctx)
		<-n.engDone
		err = errors.Join(err, n.store.Close())
	})
	return err
}

// standalone applies commands without replication. Commands waiting in the
// queue are applied together, so concurrent clients share one fsync.
type standalone struct {
	id        uint64
	fsm       *fsm.FSM
	sync      bool
	m         *metrics
	proposals chan proposal
	stopped   chan struct{}
}

type proposal struct {
	data  []byte
	done  chan fsm.Result
	start time.Time
}

const maxGroupCommit = 1024

func newStandalone(id uint64, f *fsm.FSM, sync bool, m *metrics) *standalone {
	return &standalone{id: id, fsm: f, sync: sync, m: m, proposals: make(chan proposal, 4096), stopped: make(chan struct{})}
}

func (s *standalone) run(ctx context.Context) error {
	defer close(s.stopped)
	var group []proposal
	for {
		group = group[:0]
		select {
		case <-ctx.Done():
			return nil
		case p := <-s.proposals:
			group = append(group, p)
		}
	drain:
		for len(group) < maxGroupCommit {
			select {
			case p := <-s.proposals:
				group = append(group, p)
			default:
				break drain
			}
		}
		next := s.fsm.Applied()
		ents := make([]*raftpb.Entry, len(group))
		for i := range group {
			next++
			ents[i] = &raftpb.Entry{Index: new(next), Data: group[i].data}
		}
		start := time.Now()
		results, err := s.fsm.Apply(ents, nil, s.sync)
		if err != nil {
			return err
		}
		if s.sync {
			s.m.walSync.Observe(time.Since(start).Seconds())
		}
		for i := range group {
			s.m.observeResult(results[i])
			s.m.commitLatency.Observe(time.Since(group[i].start).Seconds())
			group[i].done <- results[i]
		}
	}
}

func (s *standalone) propose(ctx context.Context, cmd *fsm.Command) (fsm.Result, error) {
	p := proposal{data: cmd.Marshal(), done: make(chan fsm.Result, 1), start: time.Now()}
	select {
	case s.proposals <- p:
	case <-s.stopped:
		return fsm.Result{}, &api.Error{Code: api.CodeNodeFailed, Message: "node is not running"}
	case <-ctx.Done():
		return fsm.Result{}, &api.Error{Code: api.CodeOverloaded, Message: "proposal queue full; command was not proposed"}
	}
	select {
	case r := <-p.done:
		return r, nil
	case <-s.stopped:
	case <-ctx.Done():
	}
	return fsm.Result{}, &api.Error{Code: api.CodeOutcomeUnknown, Message: "command was proposed but its result is unknown; retry with the same sequence numbers"}
}

func (s *standalone) readBarrier(context.Context) error { return nil }

func (s *standalone) status() api.Status {
	applied := s.fsm.Applied()
	return api.Status{ID: s.id, Mode: "standalone", Role: "standalone", Leader: s.id,
		Commit: applied, Applied: applied, LastOffset: s.fsm.LastOffset(), Fsync: fsyncName(s.sync)}
}

func fsyncName(sync bool) string {
	if sync {
		return "always"
	}
	return "never"
}
