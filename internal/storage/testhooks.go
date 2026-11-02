package storage

import "github.com/cockroachdb/pebble/v2"

// CorruptForTest flips one payload bit of the record at offset, bypassing
// every check. It exists for fault-injection tests; nothing else calls it.
func CorruptForTest(s *Store, offset uint64) error {
	v, err := s.get(eventKey(offset))
	if err != nil {
		return err
	}
	v[len(v)/2] ^= 1
	return s.db.Set(eventKey(offset), v, pebble.Sync)
}

// TruncateRaftLogForTest deletes Raft entries from index onward, simulating
// a disk that lost acknowledged writes.
func TruncateRaftLogForTest(s *Store, from uint64) error {
	return s.db.DeleteRange(raftEntryKey(from), raftEntryKey(1<<63), pebble.Sync)
}
