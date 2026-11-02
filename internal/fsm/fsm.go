// Package fsm is the replicated state machine: it turns committed commands
// into stored events. Apply must be a pure function of the committed entries
// and the current state, because every replica runs it independently and the
// replicas must end up byte-identical (design G2, D5).
package fsm

import (
	"fmt"
	"sync/atomic"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

type Status string

const (
	StatusAppended            Status = "appended"
	StatusDuplicate           Status = "duplicate"
	StatusSequenceGap         Status = "sequence_gap"
	StatusInvalid             Status = "invalid"
	StatusCheckpointed        Status = "checkpointed"
	StatusCheckpointStale     Status = "checkpoint_stale"
	StatusCheckpointBeyondEnd Status = "checkpoint_beyond_end"
)

// Result is the outcome of one applied command.
type Result struct {
	ReqID  uint64 `json:"-"`
	Index  uint64 `json:"raft_index"`
	Status Status `json:"status"`
	// Appended: the offsets written. Skipped counts events at the front of the
	// batch that were already applied by an earlier attempt.
	FirstOffset uint64 `json:"first_offset,omitempty"`
	LastOffset  uint64 `json:"last_offset,omitempty"`
	Skipped     int    `json:"duplicates_skipped,omitempty"`
	// Duplicate and sequence_gap: the producer's position.
	ProducerLastSeq    uint64 `json:"producer_last_seq,omitempty"`
	ProducerLastOffset uint64 `json:"producer_last_offset,omitempty"`
	ExpectedSeq        uint64 `json:"expected_seq,omitempty"`
	// Checkpoint results: the consumer's stored offset after the command.
	Checkpoint uint64 `json:"checkpoint,omitempty"`
	Error      string `json:"error,omitempty"`
}

// FSM caches the state machine metadata in memory. It is owned by one
// goroutine; only LastOffset and Applied may be read concurrently.
type FSM struct {
	store      *storage.Store
	applied    uint64
	lastOffset uint64
	producers  map[uint64]storage.Producer
	consumers  map[string]uint64
	confState  *raftpb.ConfState
	members    map[uint64]string

	publishedLast    atomic.Uint64
	publishedApplied atomic.Uint64
	buf              []byte
}

func Open(st *storage.Store) (*FSM, error) {
	f := &FSM{store: st}
	return f, f.Reload()
}

// Reload rereads all state from storage, e.g. after a snapshot install.
func (f *FSM) Reload() error {
	meta, err := f.store.LoadMeta()
	if err != nil {
		return err
	}
	producers, err := f.store.LoadProducers()
	if err != nil {
		return err
	}
	consumers, err := f.store.LoadConsumers()
	if err != nil {
		return err
	}
	members, err := f.store.LoadMembers()
	if err != nil {
		return err
	}
	f.members = members
	f.applied, f.lastOffset, f.confState = meta.Applied, meta.LastOffset, meta.ConfState
	f.producers, f.consumers = producers, consumers
	f.publish()
	return nil
}

func (f *FSM) Applied() uint64              { return f.publishedApplied.Load() }
func (f *FSM) LastOffset() uint64           { return f.publishedLast.Load() }
func (f *FSM) ConfState() *raftpb.ConfState { return f.confState }

// Members returns a copy of the replicated peer URLs (owner goroutine only).
func (f *FSM) Members() map[uint64]string {
	out := make(map[uint64]string, len(f.members))
	for id, url := range f.members {
		out[id] = url
	}
	return out
}

// Membership is the effect of the conf-change entries in one Apply call:
// raft's resulting ConfState and the voters whose URLs changed ("" when a
// voter was removed).
type Membership struct {
	ConfState *raftpb.ConfState
	URLs      map[uint64]string
}

func (f *FSM) publish() {
	f.publishedLast.Store(f.lastOffset)
	f.publishedApplied.Store(f.applied)
}

// Apply applies committed entries in one atomic batch: records, producer and
// consumer state, membership (if m is non-nil) and the applied index land
// together or not at all. Entries at or below the applied index are
// skipped, which makes re-delivery after a restart or snapshot install safe.
//
// An error means storage failed; in-memory state may be ahead of disk and the
// caller must stop the node.
func (f *FSM) Apply(ents []*raftpb.Entry, m *Membership, sync bool) ([]Result, error) {
	b := f.store.NewBatch()
	defer b.Close()
	var results []Result
	start := f.applied
	for _, e := range ents {
		if e.GetIndex() <= f.applied {
			continue
		}
		if e.GetType() == raftpb.EntryNormal && len(e.GetData()) > 0 {
			r, err := f.applyCommand(b, e.GetIndex(), e.GetData())
			if err != nil {
				return nil, err
			}
			results = append(results, r)
		}
		f.applied = e.GetIndex()
	}
	if f.applied == start && m == nil {
		return nil, nil
	}
	if m != nil {
		if err := b.SetConfState(m.ConfState); err != nil {
			return nil, err
		}
		f.confState = m.ConfState
		for id, url := range m.URLs {
			if err := b.SetMember(id, url); err != nil {
				return nil, err
			}
			if url == "" {
				delete(f.members, id)
			} else {
				f.members[id] = url
			}
		}
	}
	if err := b.SetApplied(f.applied); err != nil {
		return nil, err
	}
	if err := b.SetLastOffset(f.lastOffset); err != nil {
		return nil, err
	}
	if err := b.Commit(sync); err != nil {
		return nil, fmt.Errorf("fsm: commit through index %d: %w", f.applied, err)
	}
	f.publish()
	return results, nil
}

func (f *FSM) applyCommand(b *storage.Batch, index uint64, data []byte) (Result, error) {
	cmd, err := Unmarshal(data)
	if err == nil {
		err = cmd.Validate()
	}
	if err != nil {
		return Result{ReqID: cmd.ReqID, Index: index, Status: StatusInvalid, Error: err.Error()}, nil
	}
	if cmd.Checkpoint != nil {
		return f.applyCheckpoint(b, index, cmd)
	}
	return f.applyAppend(b, index, cmd)
}

// applyAppend implements producer idempotence (design G4, D7): a batch is
// applied only from the producer's next expected sequence onward.
func (f *FSM) applyAppend(b *storage.Batch, index uint64, cmd Command) (Result, error) {
	a := cmd.Append
	p := f.producers[a.ProducerID]
	res := Result{ReqID: cmd.ReqID, Index: index, ProducerLastSeq: p.LastSeq, ProducerLastOffset: p.LastOffset}
	last := a.FirstSeq + uint64(len(a.Events)) - 1
	switch {
	case last <= p.LastSeq:
		res.Status = StatusDuplicate
		return res, nil
	case a.FirstSeq > p.LastSeq+1:
		res.Status = StatusSequenceGap
		res.ExpectedSeq = p.LastSeq + 1
		return res, nil
	}
	skip := int(p.LastSeq + 1 - a.FirstSeq)
	res.Status, res.Skipped, res.FirstOffset = StatusAppended, skip, f.lastOffset+1
	for i := skip; i < len(a.Events); i++ {
		in := &a.Events[i]
		f.lastOffset++
		e := event.Event{
			Schema: in.Schema, Type: in.Type, Offset: f.lastOffset,
			ProducerID: a.ProducerID, ProducerSeq: a.FirstSeq + uint64(i),
			SourceTS: in.SourceTS, IngestTS: a.IngestTS, Payload: in.Payload,
		}
		f.buf = e.AppendTo(f.buf[:0])
		if err := b.PutEvent(f.lastOffset, f.buf); err != nil {
			return res, err
		}
	}
	p = storage.Producer{LastSeq: last, LastOffset: f.lastOffset}
	f.producers[a.ProducerID] = p
	res.LastOffset, res.ProducerLastSeq, res.ProducerLastOffset = f.lastOffset, p.LastSeq, p.LastOffset
	return res, b.SetProducer(a.ProducerID, p)
}

// applyCheckpoint only moves a consumer forward, so a delayed or retried
// checkpoint can never rewind it (G8).
func (f *FSM) applyCheckpoint(b *storage.Batch, index uint64, cmd Command) (Result, error) {
	c := cmd.Checkpoint
	cur := f.consumers[c.Consumer]
	res := Result{ReqID: cmd.ReqID, Index: index, Checkpoint: cur}
	switch {
	case c.Offset > f.lastOffset:
		res.Status = StatusCheckpointBeyondEnd
		res.Error = fmt.Sprintf("offset %d is past the last offset %d", c.Offset, f.lastOffset)
		return res, nil
	case c.Offset < cur:
		res.Status = StatusCheckpointStale
		return res, nil
	}
	f.consumers[c.Consumer] = c.Offset
	res.Status, res.Checkpoint = StatusCheckpointed, c.Offset
	return res, b.SetConsumer(c.Consumer, c.Offset)
}
