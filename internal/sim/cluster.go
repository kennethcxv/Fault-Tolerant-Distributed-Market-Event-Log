// Package sim is a deterministic cluster harness for tests. It runs N
// replicas in one goroutine on virtual time, with a seeded network that can
// drop, delay, duplicate and partition messages, and with Pebble's
// crash-simulating filesystem so a crash loses exactly the unsynced data.
//
// Determinism: the fault schedule, message delays and delivery order come
// from the seed. etcd-io/raft draws election jitter from crypto/rand, which
// the harness cannot control, so a seed reproduces the scenario but not
// necessarily the exact interleaving (decision D13). Every run keeps a trace
// so failures can be diagnosed without being reproduced.
package sim

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/replica"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

// TickMillis is the virtual time between raft ticks.
const TickMillis = 10

type Options struct {
	Nodes int
	Seed  uint64
	// Network faults, applied to every message independently.
	DropRate float64
	DupRate  float64
	MaxDelay int // ticks; delays are uniform in [0, MaxDelay]
	NoSync   bool
	// SnapshotEntries and CompactKeep configure log compaction (0 = off).
	SnapshotEntries uint64
	CompactKeep     uint64
	Logger          *slog.Logger
	OnResult        func(node uint64, r fsm.Result)
	OnRead          func(node uint64, rctx []byte)
	OnRoleSet       func(node uint64, s raft.SoftState)
}

type Node struct {
	ID    uint64
	Up    bool
	fs    *vfs.MemFS
	store *storage.Store
	rep   *replica.Replica
}

func (n *Node) Replica() *replica.Replica { return n.rep }
func (n *Node) Store() *storage.Store     { return n.store }

type envelope struct {
	at   int64
	seq  int // tie-breaker so delivery order is total
	msg  *raftpb.Message
	snap []byte // state machine stream for MsgSnap
}

type Cluster struct {
	opts    Options
	rng     *rand.Rand
	Now     int64 // virtual milliseconds
	Nodes   []*Node
	net     []envelope
	sent    int
	blocked map[[2]uint64]bool // [from, to]
	trace   []string
	// Installs counts snapshots installed per node.
	Installs map[uint64]int
}

func New(opts Options) (*Cluster, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	c := &Cluster{opts: opts, rng: rand.New(rand.NewPCG(opts.Seed, 0x51)), blocked: map[[2]uint64]bool{}, Installs: map[uint64]int{}}
	for id := uint64(1); id <= uint64(opts.Nodes); id++ {
		c.Nodes = append(c.Nodes, &Node{ID: id, fs: vfs.NewCrashableMem()})
	}
	// Open only after every node exists: each bootstraps with the full peer list.
	for _, n := range c.Nodes {
		if err := c.open(n); err != nil {
			return nil, err
		}
	}
	return c, nil
}

func (c *Cluster) peers() []uint64 {
	ids := make([]uint64, len(c.Nodes))
	for i, n := range c.Nodes {
		ids[i] = n.ID
	}
	return ids
}

func (c *Cluster) open(n *Node) error {
	st, err := storage.Open(storage.Options{Dir: "data", FS: n.fs, Logger: c.opts.Logger})
	if err != nil {
		return fmt.Errorf("node %d: %w", n.ID, err)
	}
	f, err := fsm.Open(st)
	if err != nil {
		st.Close()
		return fmt.Errorf("node %d: %w", n.ID, err)
	}
	rep, err := replica.Open(replica.Config{
		ID: n.ID, Peers: c.peers(), ElectionTick: 10, HeartbeatTick: 1,
		Sync: !c.opts.NoSync, SnapshotEntries: c.opts.SnapshotEntries, CompactKeep: c.opts.CompactKeep,
		Logger: c.opts.Logger.With("node", n.ID),
	}, st, f)
	if err != nil {
		st.Close()
		return fmt.Errorf("node %d: %w", n.ID, err)
	}
	n.store, n.rep, n.Up = st, rep, true
	return nil
}

// Tracef appends a line to the run's trace.
func (c *Cluster) Tracef(format string, args ...any) {
	c.trace = append(c.trace, fmt.Sprintf("t=%6dms ", c.Now)+fmt.Sprintf(format, args...))
}

func (c *Cluster) Trace() string { return strings.Join(c.trace, "\n") }

func (c *Cluster) Rand() *rand.Rand { return c.rng }

func (c *Cluster) Node(id uint64) *Node { return c.Nodes[id-1] }

// Step advances virtual time by one tick: every live node ticks, due
// messages are delivered, and every node's Ready is processed.
func (c *Cluster) Step() error {
	c.Now += TickMillis
	for _, n := range c.Nodes {
		if n.Up {
			n.rep.Tick()
		}
	}
	due := c.net[:0:0]
	rest := c.net[:0]
	for _, e := range c.net {
		if e.at <= c.Now {
			due = append(due, e)
		} else {
			rest = append(rest, e)
		}
	}
	c.net = rest
	slices.SortFunc(due, func(a, b envelope) int { return int(a.at-b.at)*1_000_000 + a.seq - b.seq })
	for _, e := range due {
		to := c.Node(e.msg.GetTo())
		if !to.Up || c.blocked[[2]uint64{e.msg.GetFrom(), e.msg.GetTo()}] {
			c.snapshotDone(e, false)
			continue
		}
		if e.snap != nil {
			index := e.msg.GetSnapshot().GetMetadata().GetIndex()
			if err := to.store.ReceiveSnapshot(bytes.NewReader(e.snap), index); err != nil {
				c.Tracef("node %d: rejected snapshot %d: %v", to.ID, index, err)
				c.snapshotDone(e, false)
				continue
			}
			c.Tracef("node %d: received snapshot %d (%d bytes) from %d", to.ID, index, len(e.snap), e.msg.GetFrom())
		}
		// Step errors (stale or unknown peers) are expected under faults.
		to.rep.Step(e.msg)
		c.snapshotDone(e, true)
	}
	return c.processReady()
}

func (c *Cluster) processReady() error {
	for _, n := range c.Nodes {
		for n.Up && n.rep.HasReady() {
			out, err := n.rep.Ready()
			if err != nil {
				return fmt.Errorf("node %d: %w", n.ID, err)
			}
			if out.SoftState != nil {
				c.Tracef("node %d: %s, leader %d, term %d", n.ID, out.SoftState.RaftState, out.SoftState.Lead, n.rep.Term())
				if c.opts.OnRoleSet != nil {
					c.opts.OnRoleSet(n.ID, *out.SoftState)
				}
			}
			if out.SnapshotInstalled > 0 {
				c.Installs[n.ID]++
				c.Tracef("node %d: installed snapshot %d", n.ID, out.SnapshotInstalled)
			}
			for _, m := range out.Messages {
				if m.GetType() != raftpb.MsgSnap {
					c.send(m, nil)
					continue
				}
				msg, ss, err := n.rep.PrepareSnapshot(m)
				if err != nil {
					return fmt.Errorf("node %d: prepare snapshot: %w", n.ID, err)
				}
				var buf bytes.Buffer
				_, err = ss.WriteTo(&buf)
				ss.Close()
				if err != nil {
					return fmt.Errorf("node %d: stream snapshot: %w", n.ID, err)
				}
				c.send(msg, buf.Bytes())
			}
			for _, r := range out.Results {
				if c.opts.OnResult != nil {
					c.opts.OnResult(n.ID, r)
				}
			}
			for _, rctx := range out.Reads {
				if c.opts.OnRead != nil {
					c.opts.OnRead(n.ID, rctx)
				}
			}
		}
	}
	return nil
}

func (c *Cluster) send(m *raftpb.Message, snap []byte) {
	copies := 1
	if c.rng.Float64() < c.opts.DupRate {
		copies = 2
	}
	for range copies {
		if c.rng.Float64() < c.opts.DropRate {
			c.snapshotDone(envelope{msg: m, snap: snap}, false)
			continue
		}
		delay := int64(0)
		if c.opts.MaxDelay > 0 {
			delay = int64(c.rng.IntN(c.opts.MaxDelay+1)) * TickMillis
		}
		c.sent++
		// Delivery is no earlier than the next tick, as with a real network.
		c.net = append(c.net, envelope{at: c.Now + TickMillis + delay, seq: c.sent, msg: m, snap: snap})
	}
}

// snapshotDone reports a snapshot's fate to its sender, as the real
// transport does once the stream finishes or fails.
func (c *Cluster) snapshotDone(e envelope, ok bool) {
	if e.snap == nil {
		return
	}
	if from := c.Node(e.msg.GetFrom()); from.Up {
		from.rep.ReportSnapshot(e.msg.GetTo(), ok)
	}
}

// Run steps until done returns true or maxSteps elapse.
func (c *Cluster) Run(maxSteps int, done func() bool) error {
	for i := 0; i < maxSteps; i++ {
		if done() {
			return nil
		}
		if err := c.Step(); err != nil {
			return err
		}
	}
	if done() {
		return nil
	}
	return fmt.Errorf("sim: condition not reached after %d steps (t=%dms)", maxSteps, c.Now)
}

// Leader returns the live node that is leader in the highest term, or nil.
func (c *Cluster) Leader() *Node {
	var best *Node
	var bestTerm uint64
	for _, n := range c.Nodes {
		if n.Up && n.rep.IsLeader() {
			if t := n.rep.Term(); best == nil || t > bestTerm {
				best, bestTerm = n, t
			}
		}
	}
	return best
}

// Propose submits a command to node id. It fails if the node is down or not
// the leader; the result arrives later through OnResult.
func (c *Cluster) Propose(id uint64, cmd *fsm.Command) error {
	n := c.Node(id)
	if !n.Up {
		return fmt.Errorf("node %d is down", id)
	}
	if !n.rep.IsLeader() {
		return fmt.Errorf("node %d is not the leader", id)
	}
	return n.rep.Propose(cmd.Marshal())
}

// Partition blocks traffic between the given groups; nodes not listed form
// their own group. Heal removes every block.
func (c *Cluster) Partition(groups ...[]uint64) {
	group := map[uint64]int{}
	for gi, g := range groups {
		for _, id := range g {
			group[id] = gi + 1
		}
	}
	c.blocked = map[[2]uint64]bool{}
	for _, a := range c.Nodes {
		for _, b := range c.Nodes {
			if a != b && group[a.ID] != group[b.ID] {
				c.blocked[[2]uint64{a.ID, b.ID}] = true
			}
		}
	}
	c.Tracef("partition %v", groups)
}

// BlockLink drops messages from one node to another (one direction).
func (c *Cluster) BlockLink(from, to uint64) {
	c.blocked[[2]uint64{from, to}] = true
	c.Tracef("block %d -> %d", from, to)
}

func (c *Cluster) Heal() {
	c.blocked = map[[2]uint64]bool{}
	c.Tracef("heal")
}

// Crash stops a node as a power loss would: only data that was fsynced
// survives (plus a random part of the rest when unsyncedPercent > 0).
func (c *Cluster) Crash(id uint64, unsyncedPercent int) {
	n := c.Node(id)
	if !n.Up {
		return
	}
	crashed := n.fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: unsyncedPercent, RNG: rand.New(rand.NewPCG(c.rng.Uint64(), 1))})
	n.store.Close() // writes after the clone point are discarded with the old fs
	n.fs, n.Up, n.rep, n.store = crashed, false, nil, nil
	c.Tracef("crash node %d (unsynced kept %d%%)", id, unsyncedPercent)
}

// CrashTo stops a node with its disk replaced by an earlier crash clone of
// it, for crashes that must happen at a specific point inside a call.
func (c *Cluster) CrashTo(id uint64, disk *vfs.MemFS) {
	n := c.Node(id)
	if n.Up {
		n.store.Close()
	}
	n.fs, n.Up, n.rep, n.store = disk, false, nil, nil
	c.Tracef("crash node %d at a captured point", id)
}

// FS exposes a node's filesystem so tests can capture crash clones.
func (n *Node) FS() *vfs.MemFS { return n.fs }

// Restart reopens a crashed node from what its disk retained.
func (c *Cluster) Restart(id uint64) error {
	n := c.Node(id)
	if n.Up {
		return nil
	}
	c.Tracef("restart node %d", id)
	return c.open(n)
}

// Close releases every live node's store.
func (c *Cluster) Close() {
	for _, n := range c.Nodes {
		if n.Up {
			n.store.Close()
			n.Up = false
		}
	}
}

// WithStoppedStore opens a crashed node's storage so a test can damage it
// before Restart.
func (c *Cluster) WithStoppedStore(id uint64, fn func(*storage.Store) error) error {
	n := c.Node(id)
	if n.Up {
		return fmt.Errorf("node %d is running", id)
	}
	st, err := storage.Open(storage.Options{Dir: "data", FS: n.fs, Logger: c.opts.Logger})
	if err != nil {
		return err
	}
	return errors.Join(fn(st), st.Close())
}
