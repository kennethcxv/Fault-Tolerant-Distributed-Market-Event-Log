package storage

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// Raft log keyspace:
//
//	r/hs              raftpb.HardState
//	r/snap            raftpb.Snapshot (metadata; the data is the state machine)
//	r/e/<be64 index>  raftpb.Entry
var (
	keyHardState    = []byte("r/hs")
	keySnapshot     = []byte("r/snap")
	prefixRaftEntry = []byte("r/e/")
)

func raftEntryKey(index uint64) []byte {
	return binary.BigEndian.AppendUint64(append([]byte(nil), prefixRaftEntry...), index)
}

// RaftState is everything etcd-io/raft needs persisted, as loaded at startup.
type RaftState struct {
	HardState *raftpb.HardState
	Snapshot  *raftpb.Snapshot
	Entries   []*raftpb.Entry
}

func (s *Store) LoadRaftState() (RaftState, error) {
	rs := RaftState{HardState: &raftpb.HardState{}, Snapshot: &raftpb.Snapshot{}}
	if err := s.getProto(keyHardState, rs.HardState); err != nil {
		return rs, err
	}
	if err := s.getProto(keySnapshot, rs.Snapshot); err != nil {
		return rs, err
	}
	first := rs.Snapshot.GetMetadata().GetIndex() + 1
	err := s.scanPrefix(prefixRaftEntry, func(k, v []byte) error {
		e := &raftpb.Entry{}
		if err := proto.Unmarshal(v, e); err != nil {
			return fmt.Errorf("%w: raft entry %x: %w", ErrCorrupt, k, err)
		}
		if e.GetIndex() < first {
			// Kept for slow followers before the restart. MemoryStorage
			// restarts at the snapshot, so these are not reloaded.
			return nil
		}
		if want := first + uint64(len(rs.Entries)); e.GetIndex() != want || binary.BigEndian.Uint64(k[len(prefixRaftEntry):]) != want {
			return fmt.Errorf("%w: raft log expected index %d, found %d", ErrCorrupt, want, e.GetIndex())
		}
		rs.Entries = append(rs.Entries, e)
		return nil
	})
	return rs, err
}

// SaveRaft persists one Ready's worth of Raft state atomically: a received
// snapshot (dropping every older entry), new entries (dropping any persisted
// entries after them, which a leader has overwritten), and the hard state.
// prevLast is the last persisted index before this call.
//
// The batch must be durable before any message that depends on it is sent;
// sync=false is only for the measurement-only fsync=never mode.
func (s *Store) SaveRaft(hs *raftpb.HardState, ents []*raftpb.Entry, snap *raftpb.Snapshot, prevLast uint64, sync bool) error {
	b := s.db.NewBatch()
	defer b.Close()
	if snapIndex := snap.GetMetadata().GetIndex(); snapIndex > 0 {
		if err := s.setProto(b, keySnapshot, snap); err != nil {
			return err
		}
		// MemoryStorage.ApplySnapshot discards the whole log; mirror that.
		if err := b.DeleteRange(raftEntryKey(0), raftEntryKey(max(snapIndex, prevLast)+1), nil); err != nil {
			return err
		}
	}
	for _, e := range ents {
		v, err := proto.Marshal(e)
		if err != nil {
			return err
		}
		if err := b.Set(raftEntryKey(e.GetIndex()), v, nil); err != nil {
			return err
		}
	}
	if n := len(ents); n > 0 {
		if last := ents[n-1].GetIndex(); last < prevLast {
			if err := b.DeleteRange(raftEntryKey(last+1), raftEntryKey(prevLast+1), nil); err != nil {
				return err
			}
		}
	}
	if hs != nil && (hs.GetTerm() != 0 || hs.GetVote() != 0 || hs.GetCommit() != 0) {
		if err := s.setProto(b, keyHardState, hs); err != nil {
			return err
		}
	}
	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	return b.Commit(opts)
}

// SaveSnapshotAndCompact records a locally created snapshot and drops log
// entries through compactThrough. It does not need its own fsync: the batch
// enters the single WAL after the state-machine batches for those entries,
// and Pebble recovers a WAL prefix, so the deletion can only survive a crash
// if the state those entries produced survives too (decision D4). Removing a
// Sync here did not break any crash test, which is consistent with this.
func (s *Store) SaveSnapshotAndCompact(snap *raftpb.Snapshot, compactThrough uint64) error {
	b := s.db.NewBatch()
	defer b.Close()
	if err := s.setProto(b, keySnapshot, snap); err != nil {
		return err
	}
	if compactThrough > 0 {
		if err := b.DeleteRange(raftEntryKey(0), raftEntryKey(compactThrough+1), nil); err != nil {
			return err
		}
	}
	return b.Commit(pebble.NoSync)
}

func (s *Store) getProto(key []byte, m proto.Message) error {
	v, err := s.get(key)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := proto.Unmarshal(v, m); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrCorrupt, key, err)
	}
	return nil
}

func (s *Store) setProto(b *pebble.Batch, key []byte, m proto.Message) error {
	v, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	return b.Set(key, v, nil)
}
