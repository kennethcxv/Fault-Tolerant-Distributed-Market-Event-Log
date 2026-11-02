package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// A state-machine snapshot is the whole state machine keyspace streamed from
// a consistent Pebble snapshot (decision D6):
//
//	"MLOGSNP1" | be64 applied index
//	repeated: be32 key length | key | be32 value length | value
//	be32 0xFFFFFFFF | be64 pair count | be32 CRC32C of everything before it
//
// The receiver writes the stream to snap/<applied>.snap and fsyncs it before
// handing the snapshot to raft. Installing replaces the state machine from
// that file; the applied index is written last, so an interrupted install is
// visible as applied < snapshot index and is simply repeated from the file.

const (
	snapMagic = "MLOGSNP1"
	snapEnd   = 0xFFFFFFFF
	snapDir   = "snap"
)

// installChunk bounds the size of each Pebble batch during install (a var so
// tests can force several batches).
var installChunk = 16 << 20

var (
	smStart    = []byte("e")
	smEnd      = []byte("n")
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
)

// StateSnapshot is a point-in-time view of the state machine. It must be
// taken on the goroutine that applies entries, so Applied matches the data.
type StateSnapshot struct {
	snap    *pebble.Snapshot
	Applied uint64
}

func (s *Store) SnapshotStateMachine() (*StateSnapshot, error) {
	snap := s.db.NewSnapshot()
	v, closer, err := snap.Get(keyApplied)
	if err != nil {
		snap.Close()
		return nil, fmt.Errorf("storage: snapshot has no applied index: %w", err)
	}
	applied := binary.BigEndian.Uint64(v)
	closer.Close()
	return &StateSnapshot{snap: snap, Applied: applied}, nil
}

func (ss *StateSnapshot) Close() error { return ss.snap.Close() }

// WriteTo streams the snapshot. It is safe to call from any goroutine.
func (ss *StateSnapshot) WriteTo(w io.Writer) (int64, error) {
	it, err := ss.snap.NewIter(&pebble.IterOptions{LowerBound: smStart, UpperBound: smEnd})
	if err != nil {
		return 0, err
	}
	defer it.Close()
	crc := crc32.New(castagnoli)
	bw := bufio.NewWriterSize(io.MultiWriter(w, crc), 1<<20)
	var written int64
	put := func(b []byte) error {
		n, err := bw.Write(b)
		written += int64(n)
		return err
	}
	if err := put(binary.BigEndian.AppendUint64([]byte(snapMagic), ss.Applied)); err != nil {
		return written, err
	}
	var count uint64
	var hdr []byte
	for valid := it.First(); valid; valid = it.Next() {
		v, err := it.ValueAndErr()
		if err != nil {
			return written, err
		}
		hdr = binary.BigEndian.AppendUint32(hdr[:0], uint32(len(it.Key())))
		if err := errors.Join(put(hdr), put(it.Key())); err != nil {
			return written, err
		}
		hdr = binary.BigEndian.AppendUint32(hdr[:0], uint32(len(v)))
		if err := errors.Join(put(hdr), put(v)); err != nil {
			return written, err
		}
		count++
	}
	if err := it.Error(); err != nil {
		return written, err
	}
	tail := binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint32(nil, snapEnd), count)
	if err := errors.Join(put(tail), bw.Flush()); err != nil {
		return written, err
	}
	n, err := w.Write(binary.BigEndian.AppendUint32(nil, crc.Sum32()))
	return written + int64(n), err
}

func (s *Store) snapPath(index uint64) string {
	return s.fs.PathJoin(s.dir, snapDir, fmt.Sprintf("%016d.snap", index))
}

// ReceiveSnapshot verifies a snapshot stream for the given applied index and
// stores it durably. Only after it returns may raft be told about the
// snapshot, because raft will expect it to be installable after a crash.
func (s *Store) ReceiveSnapshot(r io.Reader, index uint64) error {
	dir := s.fs.PathJoin(s.dir, snapDir)
	if err := s.fs.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := syncDir(s.fs, s.dir); err != nil { // make the snap directory's entry durable
		return err
	}
	final := s.snapPath(index)
	tmp := final + ".tmp"
	f, err := s.fs.Create(tmp, vfs.WriteCategoryUnspecified)
	if err != nil {
		return err
	}
	// Copy through our own buffer: io.Copy would hand a bytes.Reader's
	// backing array straight to Write, and the filesystem may modify the
	// buffers it is given (Pebble's MemFS does in invariant builds).
	_, err = io.CopyBuffer(f, struct{ io.Reader }{r}, make([]byte, 1<<20))
	if err == nil {
		err = f.Sync()
	}
	err = errors.Join(err, f.Close())
	if err == nil {
		err = s.verifySnapshotFile(tmp, index)
	}
	if err != nil {
		s.fs.Remove(tmp)
		return fmt.Errorf("storage: receive snapshot %d: %w", index, err)
	}
	if err := s.fs.Rename(tmp, final); err != nil {
		return err
	}
	return syncDir(s.fs, dir)
}

// HasSnapshotFile reports whether a received snapshot for index is on disk.
func (s *Store) HasSnapshotFile(index uint64) bool {
	_, err := s.fs.Stat(s.snapPath(index))
	return err == nil
}

// InstallSnapshot replaces the state machine with the received snapshot for
// index, then deletes every snapshot file. Safe to repeat after a crash.
func (s *Store) InstallSnapshot(index uint64) error {
	path := s.snapPath(index)
	if err := s.verifySnapshotFile(path, index); err != nil {
		return fmt.Errorf("storage: install snapshot %d: %w", index, err)
	}
	f, err := s.fs.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	br := bufio.NewReaderSize(f, 1<<20)
	if _, err := io.ReadFull(br, make([]byte, len(snapMagic)+8)); err != nil {
		return err
	}
	// Clearing the keyspace also removes m/applied, so until the final batch
	// commits a restart sees applied < index and installs again.
	b := s.db.NewBatch()
	if err := b.DeleteRange(smStart, smEnd, nil); err != nil {
		return err
	}
	var applied []byte
	for {
		key, val, err := readPair(br)
		if err != nil {
			b.Close()
			return err
		}
		if key == nil {
			break
		}
		if bytes.Equal(key, keyApplied) {
			applied = val
			continue
		}
		if err := b.Set(key, val, nil); err != nil {
			b.Close()
			return err
		}
		if b.Len() >= installChunk {
			if err := b.Commit(pebble.NoSync); err != nil {
				return err
			}
			b.Close()
			b = s.db.NewBatch()
			if installHook != nil {
				installHook()
			}
		}
	}
	if applied == nil {
		b.Close()
		return fmt.Errorf("%w: snapshot %d has no applied index", ErrCorrupt, index)
	}
	if err := b.Set(keyApplied, applied, nil); err != nil {
		b.Close()
		return err
	}
	if err := b.Commit(pebble.Sync); err != nil {
		return err
	}
	b.Close()
	s.RemoveSnapshotFiles(index)
	return nil
}

// RemoveSnapshotFiles deletes received snapshot files at or below upTo. A
// newer file may belong to a snapshot that raft has not been handed yet.
func (s *Store) RemoveSnapshotFiles(upTo uint64) {
	dir := s.fs.PathJoin(s.dir, snapDir)
	names, _ := s.fs.List(dir)
	for _, name := range names {
		var index uint64
		if _, err := fmt.Sscanf(name, "%016d.snap", &index); err != nil || index <= upTo {
			s.fs.Remove(s.fs.PathJoin(dir, name))
		}
	}
}

// installHook runs after each intermediate install batch; crash tests use it.
var installHook func()

// verifySnapshotFile checks the trailer checksum over the whole file, then
// the structure: header for the expected index and a matching pair count.
func (s *Store) verifySnapshotFile(path string, index uint64) error {
	f, err := s.fs.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	body := st.Size() - 4
	if body < int64(len(snapMagic))+8+4+8 {
		return fmt.Errorf("%w: snapshot file too short", ErrCorrupt)
	}
	sum := make([]byte, 4)
	if _, err := f.ReadAt(sum, body); err != nil {
		return err
	}
	crc := crc32.New(castagnoli)
	if _, err := io.Copy(crc, io.NewSectionReader(f, 0, body)); err != nil {
		return err
	}
	if crc.Sum32() != binary.BigEndian.Uint32(sum) {
		return fmt.Errorf("%w: snapshot checksum mismatch", ErrCorrupt)
	}
	br := bufio.NewReaderSize(io.NewSectionReader(f, 0, body), 1<<20)
	head := make([]byte, len(snapMagic)+8)
	if _, err := io.ReadFull(br, head); err != nil || string(head[:len(snapMagic)]) != snapMagic {
		return fmt.Errorf("%w: bad snapshot header", ErrCorrupt)
	}
	if got := binary.BigEndian.Uint64(head[len(snapMagic):]); got != index {
		return fmt.Errorf("%w: snapshot is for index %d, expected %d", ErrCorrupt, got, index)
	}
	var count uint64
	for {
		key, _, err := readPair(br)
		if err != nil {
			return err
		}
		if key == nil {
			break
		}
		count++
	}
	tail := make([]byte, 8)
	if _, err := io.ReadFull(br, tail); err != nil || binary.BigEndian.Uint64(tail) != count {
		return fmt.Errorf("%w: snapshot pair count mismatch", ErrCorrupt)
	}
	if _, err := br.ReadByte(); err != io.EOF {
		return fmt.Errorf("%w: trailing bytes in snapshot", ErrCorrupt)
	}
	return nil
}

// readPair reads one key/value; key is nil at the end marker.
func readPair(r *bufio.Reader) (key, val []byte, err error) {
	var n [4]byte
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, nil, fmt.Errorf("%w: truncated snapshot", ErrCorrupt)
	}
	klen := binary.BigEndian.Uint32(n[:])
	if klen == snapEnd {
		return nil, nil, nil
	}
	if klen > 1<<16 {
		return nil, nil, fmt.Errorf("%w: snapshot key length %d", ErrCorrupt, klen)
	}
	key = make([]byte, klen)
	if _, err := io.ReadFull(r, key); err != nil {
		return nil, nil, fmt.Errorf("%w: truncated snapshot", ErrCorrupt)
	}
	if _, err := io.ReadFull(r, n[:]); err != nil {
		return nil, nil, fmt.Errorf("%w: truncated snapshot", ErrCorrupt)
	}
	vlen := binary.BigEndian.Uint32(n[:])
	if vlen > 64<<20 {
		return nil, nil, fmt.Errorf("%w: snapshot value length %d", ErrCorrupt, vlen)
	}
	val = make([]byte, vlen)
	if _, err := io.ReadFull(r, val); err != nil {
		return nil, nil, fmt.Errorf("%w: truncated snapshot", ErrCorrupt)
	}
	return key, val, nil
}

func syncDir(fs vfs.FS, dir string) error {
	f, err := fs.OpenDir(dir)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close())
}
