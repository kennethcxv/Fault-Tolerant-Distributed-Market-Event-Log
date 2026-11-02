package storage

import (
	"errors"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
)

func openMem(t *testing.T) *Store {
	t.Helper()
	s, err := Open(Options{Dir: "n", FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func put(t *testing.T, s *Store, offsets ...uint64) {
	t.Helper()
	b := s.NewBatch()
	defer b.Close()
	for _, off := range offsets {
		e := event.Event{Schema: 1, Type: 1, Offset: off, ProducerID: 1, ProducerSeq: off, Payload: []byte("0123456789")}
		if err := b.PutEvent(off, e.AppendTo(nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
}

func TestReadLimits(t *testing.T) {
	s := openMem(t)
	put(t, s, 1, 2, 3, 4, 5)
	raw, n, err := s.ReadRaw(2, 5, 2, 1<<20)
	if err != nil || n != 2 {
		t.Fatalf("maxEvents: n=%d err=%v", n, err)
	}
	evs, _ := event.DecodeAll(raw)
	if evs[0].Offset != 2 || evs[1].Offset != 3 {
		t.Fatalf("got offsets %d,%d", evs[0].Offset, evs[1].Offset)
	}
	// maxBytes smaller than one record still returns one record.
	if _, n, err := s.ReadRaw(1, 5, 100, 1); err != nil || n != 1 {
		t.Fatalf("maxBytes: n=%d err=%v", n, err)
	}
	// upTo bounds the read even if more records exist.
	if _, n, err := s.ReadRaw(4, 4, 100, 1<<20); err != nil || n != 1 {
		t.Fatalf("upTo: n=%d err=%v", n, err)
	}
	if _, n, err := s.ReadRaw(6, 5, 100, 1<<20); err != nil || n != 0 {
		t.Fatalf("past end: n=%d err=%v", n, err)
	}
}

func TestReadDetectsCorruption(t *testing.T) {
	cases := map[string]func(s *Store){
		"missing middle record": func(s *Store) { s.db.Delete(eventKey(3), pebble.Sync) },
		"missing tail record":   func(s *Store) { s.db.Delete(eventKey(5), pebble.Sync) },
		"flipped payload bit": func(s *Store) {
			v, _ := s.get(eventKey(3))
			v[event.HeaderSize] ^= 1
			s.db.Set(eventKey(3), v, pebble.Sync)
		},
		"record under wrong key": func(s *Store) {
			v, _ := s.get(eventKey(4))
			s.db.Set(eventKey(3), v, pebble.Sync)
		},
		"truncated value": func(s *Store) {
			v, _ := s.get(eventKey(3))
			s.db.Set(eventKey(3), v[:len(v)-1], pebble.Sync)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			s := openMem(t)
			put(t, s, 1, 2, 3, 4, 5)
			corrupt(s)
			if _, _, err := s.ReadRaw(1, 5, 100, 1<<20); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("got %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestProducerAndConsumerRoundTrip(t *testing.T) {
	s := openMem(t)
	b := s.NewBatch()
	b.SetProducer(42, Producer{LastSeq: 7, LastOffset: 100})
	b.SetConsumer("risk", 55)
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
	if p, err := s.GetProducer(42); err != nil || p != (Producer{7, 100}) {
		t.Fatalf("%+v %v", p, err)
	}
	if _, err := s.GetProducer(43); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown producer: %v", err)
	}
	if c, err := s.GetConsumer("risk"); err != nil || c != 55 {
		t.Fatalf("%d %v", c, err)
	}
	ps, _ := s.LoadProducers()
	cs, _ := s.LoadConsumers()
	if len(ps) != 1 || len(cs) != 1 {
		t.Fatalf("%v %v", ps, cs)
	}
}

// Directory fsync behaves differently across platforms (macOS uses
// F_FULLFSYNC); make sure opening on the real filesystem works.
func TestOpenOnRealFilesystem(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(Options{Dir: dir + "/node"})
	if err != nil {
		t.Fatal(err)
	}
	put(t, s, 1)
	s.Close()
	s, err = Open(Options{Dir: dir + "/node"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, n, err := s.ReadRaw(1, 1, 1, 1<<20); err != nil || n != 1 {
		t.Fatalf("n=%d err=%v", n, err)
	}
}
