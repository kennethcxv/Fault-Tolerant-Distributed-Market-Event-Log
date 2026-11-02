package sim

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/orderbook"
)

// Converged reports whether every live node has applied the same index and
// a leader exists, i.e. the cluster has caught up.
func (c *Cluster) Converged() bool {
	if c.Leader() == nil {
		return false
	}
	var applied uint64
	first := true
	for _, n := range c.Nodes {
		if !n.Up {
			continue
		}
		a := n.rep.FSM().Applied()
		if !first && a != applied {
			return false
		}
		applied, first = a, false
	}
	return true
}

// Log reads node id's whole log, verifying every record (invariant I4).
func (c *Cluster) Log(id uint64) ([]event.Event, []byte, error) {
	n := c.Node(id)
	last := n.rep.FSM().LastOffset()
	raw, count, err := n.store.ReadRaw(1, last, math.MaxInt, math.MaxInt)
	if err != nil {
		return nil, nil, fmt.Errorf("node %d: %w", id, err)
	}
	if uint64(count) != last {
		return nil, nil, fmt.Errorf("node %d: read %d records, last offset %d", id, count, last)
	}
	evs, err := event.DecodeAll(raw)
	return evs, raw, err
}

// CheckAgreement verifies I1 and I7 across live nodes: identical record bytes
// and identical order-book replay hashes. It returns node 1's (or the first
// live node's) log for further checks.
func (c *Cluster) CheckAgreement() ([]event.Event, error) {
	var ref []event.Event
	var refRaw []byte
	var refHash [32]byte
	var refID uint64
	for _, n := range c.Nodes {
		if !n.Up {
			continue
		}
		evs, raw, err := c.Log(n.ID)
		if err != nil {
			return nil, err
		}
		book := orderbook.New()
		for i := range evs {
			if err := book.Apply(&evs[i]); err != nil {
				return nil, fmt.Errorf("node %d replay: %w", n.ID, err)
			}
		}
		if ref == nil {
			ref, refRaw, refHash, refID = evs, raw, book.Hash(), n.ID
			continue
		}
		if !bytes.Equal(raw, refRaw) {
			return nil, fmt.Errorf("I1: node %d log (%d events) differs from node %d log (%d events)", n.ID, len(evs), refID, len(ref))
		}
		if book.Hash() != refHash {
			return nil, fmt.Errorf("I7: node %d replay hash differs from node %d", n.ID, refID)
		}
	}
	return ref, nil
}

// CheckLinearizable runs Porcupine over the workload history (I6).
func (w *Workload) CheckLinearizable() error {
	if w.external {
		return nil
	}
	return CheckHistory(w.history, fmt.Sprintf("seed-%d", w.c.opts.Seed))
}

// CheckHistory checks a client history against LogModel after BoundRetries.
// On failure it writes Porcupine's visualization next to the traces.
func CheckHistory(history []porcupine.Operation, name string) error {
	BoundRetries(history)
	res, info := porcupine.CheckOperationsVerbose(LogModel, history, 60*time.Second)
	switch res {
	case porcupine.Ok:
		return nil
	case porcupine.Unknown:
		return fmt.Errorf("I6: linearizability check timed out on %d operations", len(history))
	}
	path := filepath.Join(artifactDir(), name+"-history.html")
	if err := porcupine.VisualizePath(LogModel, info, path); err == nil {
		return fmt.Errorf("I6: history of %d operations is not linearizable; visualization at %s", len(history), path)
	}
	return fmt.Errorf("I6: history of %d operations is not linearizable", len(history))
}

// SaveTrace writes the run's trace for a failed test and returns its path.
func (c *Cluster) SaveTrace(name string) string {
	path := filepath.Join(artifactDir(), fmt.Sprintf("%s-seed-%d.trace", name, c.opts.Seed))
	os.WriteFile(path, []byte(c.Trace()+"\n"), 0o644)
	return path
}

// artifactDir is where failure traces go; CI uploads it when tests fail.
func artifactDir() string {
	dir := os.Getenv("MLOG_SIM_ARTIFACTS")
	if dir == "" {
		dir = filepath.Join(os.TempDir(), "mlog-sim-failures")
	}
	os.MkdirAll(dir, 0o755)
	return dir
}
