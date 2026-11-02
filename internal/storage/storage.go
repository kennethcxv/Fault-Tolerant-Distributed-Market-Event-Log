// Package storage is the only code that talks to Pebble. It owns the key
// layout, atomic write batches, verified reads, Raft-log persistence and
// snapshot transfer.
//
// Keyspace (one Pebble DB, see docs/design.md §7):
//
//	e<be64 offset>     event record (event package encoding)
//	m/applied          be64 applied Raft index
//	m/last             be64 last assigned offset
//	m/confstate        raftpb.ConfState as of m/applied
//	m/p/<be64 id>      producer: be64 last seq, be64 last offset
//	m/c/<name>         consumer checkpoint, be64 offset
//	m/member/<be64 id> peer URL of a voter (replicated with membership)
//	r/...              Raft log and node identity (raftlog.go)
//
// Keys from "e" up to "n" are the state machine; "r" keys are the Raft log.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
)

var (
	ErrNotFound = errors.New("storage: not found")
	// ErrCorrupt means stored data failed verification. The node must stop
	// serving rather than return it.
	ErrCorrupt = errors.New("storage: corrupt data")
	// ErrUnavailable means a snapshot install overlapped the read; the state
	// machine was being replaced, so the result was discarded. Retry.
	ErrUnavailable = errors.New("storage: snapshot install in progress")
)

var (
	keyApplied     = []byte("m/applied")
	keyLastOffset  = []byte("m/last")
	keyConfState   = []byte("m/confstate")
	prefixProducer = []byte("m/p/")
	prefixConsumer = []byte("m/c/")
	prefixMember   = []byte("m/member/")
)

func eventKey(offset uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte{'e'}, offset)
}

type Options struct {
	Dir string
	// FS defaults to the OS filesystem. Tests pass vfs.NewCrashableMem().
	FS     vfs.FS
	Logger *slog.Logger
}

type Store struct {
	db  *pebble.DB
	fs  vfs.FS
	dir string
	log *slog.Logger
	// installGen is odd while InstallSnapshot is replacing the state machine
	// in several batches. Readers on other goroutines check it before and
	// after reading (a seqlock), so a half-installed state is never mistaken
	// for corruption or served.
	installGen atomic.Uint64
	tmpSeq     atomic.Uint64
}

// stable runs read and returns ErrUnavailable if a snapshot install was in
// progress at any point during it.
func (s *Store) stable(read func() error) error {
	gen := s.installGen.Load()
	if gen%2 == 1 {
		return ErrUnavailable
	}
	err := read()
	if s.installGen.Load() != gen {
		return ErrUnavailable
	}
	return err
}

func Open(o Options) (*Store, error) {
	if o.FS == nil {
		o.FS = vfs.Default
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	// Pebble creates dbDir and fsyncs each directory it creates. Creating
	// o.Dir ourselves first would hide it from that and leave its entry
	// unsynced (found by TestCrashLeavesAtomicPrefixOfBatches).
	dbDir := o.FS.PathJoin(o.Dir, "db")
	// WALFailover stays nil: D4 relies on Pebble writing a single WAL, so a
	// synced batch makes every earlier batch durable too.
	db, err := pebble.Open(dbDir, &pebble.Options{
		FS:     o.FS,
		Logger: pebbleLogger{o.Logger},
	})
	if err != nil {
		if errors.Is(err, pebble.ErrCorruption) {
			err = fmt.Errorf("%w: %w", ErrCorrupt, err)
		}
		return nil, fmt.Errorf("storage: open %s: %w", o.Dir, err)
	}
	return &Store{db: db, fs: o.FS, dir: o.Dir, log: o.Logger}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Meta is the state machine's position, read once at startup.
type Meta struct {
	Applied    uint64
	LastOffset uint64
	ConfState  *raftpb.ConfState
}

func (s *Store) LoadMeta() (Meta, error) {
	var m Meta
	var err error
	if m.Applied, err = s.getUint64(keyApplied); err != nil {
		return m, err
	}
	if m.LastOffset, err = s.getUint64(keyLastOffset); err != nil {
		return m, err
	}
	m.ConfState = &raftpb.ConfState{}
	v, err := s.get(keyConfState)
	if errors.Is(err, ErrNotFound) {
		return m, nil
	}
	if err != nil {
		return m, err
	}
	if err := proto.Unmarshal(v, m.ConfState); err != nil {
		return m, fmt.Errorf("%w: confstate: %w", ErrCorrupt, err)
	}
	return m, nil
}

type Producer struct {
	LastSeq    uint64 `json:"last_seq"`
	LastOffset uint64 `json:"last_offset"`
}

func (s *Store) LoadProducers() (map[uint64]Producer, error) {
	out := map[uint64]Producer{}
	err := s.scanPrefix(prefixProducer, func(k, v []byte) error {
		if len(k) != len(prefixProducer)+8 || len(v) != 16 {
			return fmt.Errorf("%w: producer key %q", ErrCorrupt, k)
		}
		out[binary.BigEndian.Uint64(k[len(prefixProducer):])] = Producer{
			LastSeq:    binary.BigEndian.Uint64(v),
			LastOffset: binary.BigEndian.Uint64(v[8:]),
		}
		return nil
	})
	return out, err
}

// LoadMembers returns the replicated peer URLs by node id.
func (s *Store) LoadMembers() (map[uint64]string, error) {
	out := map[uint64]string{}
	err := s.scanPrefix(prefixMember, func(k, v []byte) error {
		if len(k) != len(prefixMember)+8 {
			return fmt.Errorf("%w: member key %q", ErrCorrupt, k)
		}
		out[binary.BigEndian.Uint64(k[len(prefixMember):])] = string(v)
		return nil
	})
	return out, err
}

// ListConsumers is LoadConsumers for other goroutines (HTTP, metrics).
func (s *Store) ListConsumers() (out map[string]uint64, err error) {
	err = s.stable(func() error {
		out, err = s.LoadConsumers()
		return err
	})
	return out, err
}

func (s *Store) LoadConsumers() (map[string]uint64, error) {
	out := map[string]uint64{}
	err := s.scanPrefix(prefixConsumer, func(k, v []byte) error {
		if len(v) != 8 {
			return fmt.Errorf("%w: consumer key %q", ErrCorrupt, k)
		}
		out[string(k[len(prefixConsumer):])] = binary.BigEndian.Uint64(v)
		return nil
	})
	return out, err
}

// GetProducer reads one producer's committed position. Safe for concurrent use.
func (s *Store) GetProducer(id uint64) (p Producer, err error) {
	err = s.stable(func() error {
		v, err := s.get(binary.BigEndian.AppendUint64(append([]byte(nil), prefixProducer...), id))
		if err != nil {
			return err
		}
		if len(v) != 16 {
			return fmt.Errorf("%w: producer %d", ErrCorrupt, id)
		}
		p = Producer{LastSeq: binary.BigEndian.Uint64(v), LastOffset: binary.BigEndian.Uint64(v[8:])}
		return nil
	})
	return p, err
}

// GetConsumer reads one consumer's committed checkpoint. Safe for concurrent use.
func (s *Store) GetConsumer(name string) (off uint64, err error) {
	key := append(append([]byte(nil), prefixConsumer...), name...)
	err = s.stable(func() error {
		if _, err := s.get(key); err != nil {
			return err
		}
		off, err = s.getUint64(key)
		return err
	})
	return off, err
}

// ReadRaw returns verified records from..upTo (inclusive), back to back,
// stopping after maxEvents records or once maxBytes is reached (always at
// least one record if any are in range). upTo must be an offset the caller
// knows was applied; a record missing below it, a misplaced record or a
// checksum failure is ErrCorrupt and nothing past it is returned.
func (s *Store) ReadRaw(from, upTo uint64, maxEvents, maxBytes int) (raw []byte, n int, err error) {
	err = s.stable(func() error {
		raw, n, err = s.readRaw(from, upTo, maxEvents, maxBytes)
		return err
	})
	if err != nil {
		return nil, 0, err
	}
	return raw, n, nil
}

func (s *Store) readRaw(from, upTo uint64, maxEvents, maxBytes int) ([]byte, int, error) {
	if from == 0 {
		from = 1
	}
	if from > upTo || maxEvents <= 0 {
		return nil, 0, nil
	}
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: eventKey(from), UpperBound: eventKey(upTo + 1)})
	if err != nil {
		return nil, 0, err
	}
	defer it.Close()
	var out []byte
	n := 0
	want := from
	for valid := it.First(); valid && n < maxEvents && (n == 0 || len(out) < maxBytes); valid = it.Next() {
		off := binary.BigEndian.Uint64(it.Key()[1:])
		if off != want {
			return nil, 0, fmt.Errorf("%w: expected offset %d, found %d", ErrCorrupt, want, off)
		}
		v, err := it.ValueAndErr()
		if err != nil {
			return nil, 0, err
		}
		e, size, err := event.Decode(v)
		if err == nil && (size != len(v) || e.Offset != off) {
			err = fmt.Errorf("record under key %d claims offset %d", off, e.Offset)
		}
		if err != nil {
			return nil, 0, fmt.Errorf("%w: offset %d: %w", ErrCorrupt, off, err)
		}
		out = append(out, v...)
		n++
		want++
	}
	if err := it.Error(); err != nil {
		return nil, 0, err
	}
	if n < maxEvents && len(out) < maxBytes && want <= upTo {
		return nil, 0, fmt.Errorf("%w: offset %d missing (applied through %d)", ErrCorrupt, want, upTo)
	}
	return out, n, nil
}

// Read is ReadRaw decoded, without a byte limit.
func (s *Store) Read(from, upTo uint64, maxEvents int) ([]event.Event, error) {
	raw, _, err := s.ReadRaw(from, upTo, maxEvents, math.MaxInt)
	if err != nil {
		return nil, err
	}
	return event.DecodeAll(raw)
}

// Batch is an atomic group of state-machine writes.
type Batch struct{ b *pebble.Batch }

func (s *Store) NewBatch() *Batch { return &Batch{b: s.db.NewBatch()} }

// PutEvent stores an encoded record. Pebble copies rec, so callers may reuse it.
func (b *Batch) PutEvent(offset uint64, rec []byte) error {
	return b.b.Set(eventKey(offset), rec, nil)
}

func (b *Batch) SetApplied(index uint64) error { return b.setUint64(keyApplied, index) }

func (b *Batch) SetLastOffset(offset uint64) error { return b.setUint64(keyLastOffset, offset) }

func (b *Batch) SetProducer(id uint64, p Producer) error {
	key := binary.BigEndian.AppendUint64(append([]byte(nil), prefixProducer...), id)
	val := binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, p.LastSeq), p.LastOffset)
	return b.b.Set(key, val, nil)
}

func (b *Batch) SetConsumer(name string, offset uint64) error {
	return b.setUint64(append(append([]byte(nil), prefixConsumer...), name...), offset)
}

// SetMember records a voter's URL; an empty URL removes the member.
func (b *Batch) SetMember(id uint64, url string) error {
	key := binary.BigEndian.AppendUint64(append([]byte(nil), prefixMember...), id)
	if url == "" {
		return b.b.Delete(key, nil)
	}
	return b.b.Set(key, []byte(url), nil)
}

func (b *Batch) SetConfState(cs *raftpb.ConfState) error {
	v, err := proto.Marshal(cs)
	if err != nil {
		return err
	}
	return b.b.Set(keyConfState, v, nil)
}

// Commit applies the batch atomically. With sync=false the batch is durable
// once any later synced write to the same Store completes (single WAL).
func (b *Batch) Commit(sync bool) error {
	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	return b.b.Commit(opts)
}

func (b *Batch) Close() error { return b.b.Close() }

func (b *Batch) setUint64(key []byte, v uint64) error {
	return b.b.Set(key, binary.BigEndian.AppendUint64(nil, v), nil)
}

func (s *Store) get(key []byte) ([]byte, error) {
	v, closer, err := s.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer closer.Close()
	return append([]byte(nil), v...), nil
}

func (s *Store) getUint64(key []byte) (uint64, error) {
	v, err := s.get(key)
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(v) != 8 {
		return 0, fmt.Errorf("%w: key %q has %d bytes", ErrCorrupt, key, len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

func (s *Store) scanPrefix(prefix []byte, fn func(k, v []byte) error) error {
	upper := append(append([]byte(nil), prefix[:len(prefix)-1]...), prefix[len(prefix)-1]+1)
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	if err != nil {
		return err
	}
	defer it.Close()
	for valid := it.First(); valid; valid = it.Next() {
		v, err := it.ValueAndErr()
		if err != nil {
			return err
		}
		if err := fn(it.Key(), v); err != nil {
			return err
		}
	}
	return it.Error()
}

// pebbleLogger keeps Pebble's per-flush and per-compaction chatter at debug.
type pebbleLogger struct{ l *slog.Logger }

func (p pebbleLogger) Infof(format string, args ...any) { p.l.Debug(fmt.Sprintf(format, args...)) }
func (p pebbleLogger) Errorf(format string, args ...any) {
	p.l.Error(fmt.Sprintf(format, args...), "component", "pebble")
}
func (p pebbleLogger) Fatalf(format string, args ...any) {
	p.l.Error(fmt.Sprintf(format, args...), "component", "pebble")
	panic(fmt.Sprintf(format, args...))
}

// Stats summarizes Pebble's view of the store, for /debug/storage and the
// storage benchmarks. WriteAmp is bytes physically written (WAL, flushes,
// compactions) per logical byte written.
type Stats struct {
	DiskUsageBytes  uint64  `json:"disk_usage_bytes"`
	WALLogicalBytes uint64  `json:"wal_logical_bytes"`
	WALBytesWritten uint64  `json:"wal_bytes_written"`
	FlushedBytes    uint64  `json:"flushed_bytes"`
	CompactedBytes  uint64  `json:"compacted_bytes"`
	WriteAmp        float64 `json:"write_amp"`
	Tables          int64   `json:"tables"`
	Compactions     int64   `json:"compactions"`
}

func (s *Store) Stats() Stats {
	m := s.db.Metrics()
	total := m.Total()
	return Stats{
		DiskUsageBytes:  m.DiskSpaceUsage(),
		WALLogicalBytes: m.WAL.BytesIn,
		WALBytesWritten: m.WAL.BytesWritten,
		FlushedBytes:    total.TableBytesFlushed,
		CompactedBytes:  total.TableBytesCompacted,
		WriteAmp:        total.WriteAmp(),
		Tables:          total.TablesCount,
		Compactions:     m.Compact.Count,
	}
}
