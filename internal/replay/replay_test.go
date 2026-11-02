package replay

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/orderbook"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

var update = flag.Bool("update", false, "rewrite the golden fixture")

const (
	fixtureDir    = "../../fixtures/orderbook-v1"
	fixtureSeed   = 20261102
	fixtureInsts  = 4
	fixtureEvents = 5000
)

// fixtureRecords builds the fixture exactly as the log would store it:
// producer 1, sequence = offset, deterministic timestamps.
func fixtureRecords() []byte {
	g := orderbook.NewGenerator(fixtureSeed, fixtureInsts)
	var out []byte
	for i := uint64(1); i <= fixtureEvents; i++ {
		m := g.Next()
		e := event.Event{Schema: orderbook.SchemaV1, Type: m.Type, Offset: i, ProducerID: 1, ProducerSeq: i,
			SourceTS: 1_700_000_000_000_000_000 + int64(i)*1000, IngestTS: 1_700_000_000_000_050_000 + int64(i)*1000,
			Payload: m.Encode()}
		out = e.AppendTo(out)
	}
	return out
}

func describe(b *orderbook.Book) string {
	h := b.Hash()
	var sb strings.Builder
	fmt.Fprintf(&sb, "# Order book schema v1 fixture. Regenerate: go test ./internal/replay -run Golden -update\n")
	fmt.Fprintf(&sb, "generator seed=%d instruments=%d events=%d\n", fixtureSeed, fixtureInsts, fixtureEvents)
	fmt.Fprintf(&sb, "sha256 %s\n", hex.EncodeToString(h[:]))
	fmt.Fprintf(&sb, "stats %+v\n", b.Stats)
	fmt.Fprintf(&sb, "resting_orders %d\n", b.Orders())
	for inst := uint32(0); inst < fixtureInsts; inst++ {
		bp, bq, ap, aq := b.Top(inst)
		fmt.Fprintf(&sb, "top instrument=%d bid=%d x %d ask=%d x %d\n", inst, bp, bq, ap, aq)
	}
	return sb.String()
}

// TestGoldenFixture pins both the encoded feed and the replayed state. A
// failure means the generator, the schema or the book semantics changed.
func TestGoldenFixture(t *testing.T) {
	records := fixtureRecords()
	evs, err := event.DecodeAll(records)
	if err != nil {
		t.Fatal(err)
	}
	b := orderbook.New()
	for i := range evs {
		if err := b.Apply(&evs[i]); err != nil {
			t.Fatal(err)
		}
	}
	got := describe(b)
	if *update {
		os.MkdirAll(fixtureDir, 0o755)
		os.WriteFile(filepath.Join(fixtureDir, "events.bin"), records, 0o644)
		os.WriteFile(filepath.Join(fixtureDir, "expected.txt"), []byte(got), 0o644)
	}
	wantRecords, err := os.ReadFile(filepath.Join(fixtureDir, "events.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(records, wantRecords) {
		t.Fatal("generator no longer reproduces fixtures/orderbook-v1/events.bin")
	}
	want, _ := os.ReadFile(filepath.Join(fixtureDir, "expected.txt"))
	if got != string(want) {
		t.Fatalf("replay of fixture changed:\n got: %s\nwant: %s", got, want)
	}
}

// logStore writes n generated events through the state machine, as the log
// would, and returns a source over them.
func logStore(t *testing.T, n int) (*storage.Store, StoreSource) {
	t.Helper()
	st, err := storage.Open(storage.Options{Dir: "n", FS: vfs.NewMem()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	f, err := fsm.Open(st)
	if err != nil {
		t.Fatal(err)
	}
	g := orderbook.NewGenerator(99, 3)
	for seq := uint64(1); seq <= uint64(n); seq += 100 {
		a := &fsm.Append{ProducerID: 1, FirstSeq: seq}
		for i := uint64(0); i < 100 && seq+i <= uint64(n); i++ {
			m := g.Next()
			a.Events = append(a.Events, fsm.EventIn{Type: m.Type, Schema: 1, Payload: m.Encode()})
		}
		c := fsm.Command{Append: a}
		if _, err := f.Apply([]*raftpb.Entry{{Index: new(f.Applied() + 1), Data: c.Marshal()}}, nil, false); err != nil {
			t.Fatal(err)
		}
	}
	return st, StoreSource{Store: st, UpTo: f.LastOffset()}
}

func fullHash(t *testing.T, src Source, until uint64) [32]byte {
	t.Helper()
	e := New()
	if _, err := e.Run(context.Background(), src, until, 512); err != nil {
		t.Fatal(err)
	}
	return e.Book().Hash()
}

// cancelAfter cancels the replay context once n events have been read.
type cancelAfter struct {
	Source
	n      int
	cancel context.CancelFunc
}

func (c *cancelAfter) Read(ctx context.Context, from uint64, max int) ([]event.Event, error) {
	evs, err := c.Source.Read(ctx, from, max)
	c.n -= len(evs)
	if c.n <= 0 {
		c.cancel()
	}
	return evs, err
}

func TestInterruptedReplayResumesToSameHash(t *testing.T) {
	_, src := logStore(t, 3000)
	want := fullHash(t, src, 0)
	e := New()
	for _, stopAfter := range []int{700, 1, 1500} {
		ctx, cancel := context.WithCancel(context.Background())
		_, err := e.Run(ctx, &cancelAfter{Source: src, n: stopAfter, cancel: cancel}, 0, 256)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected interruption, got %v", err)
		}
	}
	if _, err := e.Run(context.Background(), src, 0, 256); err != nil {
		t.Fatal(err)
	}
	if e.Book().Hash() != want || e.Next() != 3001 {
		t.Fatalf("resumed replay diverged (next=%d)", e.Next())
	}
}

// rewinding re-delivers the last few events of every batch.
type rewinding struct{ Source }

func (r rewinding) Read(ctx context.Context, from uint64, max int) ([]event.Event, error) {
	if from > 5 {
		from -= 5
	}
	return r.Source.Read(ctx, from, max)
}

func TestRepeatedDeliveryIsIgnored(t *testing.T) {
	_, src := logStore(t, 2000)
	want := fullHash(t, src, 0)
	e := New()
	n, err := e.Run(context.Background(), rewinding{src}, 0, 100)
	if err != nil || n != 2000 || e.Book().Hash() != want {
		t.Fatalf("applied %d, err %v, hash match %v", n, err, e.Book().Hash() == want)
	}
}

// skipping drops one offset from its results.
type skipping struct {
	Source
	skip uint64
}

func (s skipping) Read(ctx context.Context, from uint64, max int) ([]event.Event, error) {
	evs, err := s.Source.Read(ctx, from, max)
	out := evs[:0]
	for _, e := range evs {
		if e.Offset != s.skip {
			out = append(out, e)
		}
	}
	return out, err
}

func TestMissingRecordStopsReplay(t *testing.T) {
	_, src := logStore(t, 1000)
	e := New()
	_, err := e.Run(context.Background(), skipping{src, 500}, 0, 128)
	if !errors.Is(err, ErrGap) || e.Next() != 500 {
		t.Fatalf("err %v next %d", err, e.Next())
	}
}

func TestCorruptRecordStopsReplayAtLastGoodOffset(t *testing.T) {
	st, src := logStore(t, 1000)
	if err := storage.CorruptForTest(st, 640); err != nil {
		t.Fatal(err)
	}
	e := New()
	_, err := e.Run(context.Background(), src, 0, 100)
	if !errors.Is(err, storage.ErrCorrupt) || e.Next() != 601 {
		// The batch containing 640 is rejected whole, so replay stops at the
		// end of the previous batch, never past the corrupt record.
		t.Fatalf("err %v next %d", err, e.Next())
	}
	if e.Book().Hash() != fullHash(t, src, 600) {
		t.Fatal("state after stopping does not equal replay of the good prefix")
	}
}

// Replaying 1..K, checkpointing to disk, then restoring and replaying K+1..M
// must equal replaying 1..M directly, for an explicit end offset M.
func TestCheckpointRestoreAndExplicitEndOffset(t *testing.T) {
	_, src := logStore(t, 4000)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "replay.ckpt")

	e := New()
	if _, err := e.Run(ctx, src, 1234, 300); err != nil {
		t.Fatal(err)
	}
	if err := WriteCheckpointFile(path, e.Checkpoint()); err != nil {
		t.Fatal(err)
	}
	cp, err := ReadCheckpointFile(path)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := Restore(cp)
	if err != nil || restored.Next() != 1235 {
		t.Fatalf("restore: %v next=%d", err, restored.Next())
	}
	if _, err := restored.Run(ctx, src, 3333, 300); err != nil {
		t.Fatal(err)
	}
	if restored.Next() != 3334 || restored.Book().Hash() != fullHash(t, src, 3333) {
		t.Fatal("checkpoint + suffix differs from direct replay to 3333")
	}
}

func TestDamagedCheckpointFileIsRejected(t *testing.T) {
	_, src := logStore(t, 500)
	e := New()
	e.Run(context.Background(), src, 0, 100)
	path := filepath.Join(t.TempDir(), "c")
	WriteCheckpointFile(path, e.Checkpoint())
	data, _ := os.ReadFile(path)
	data[len(data)/2] ^= 0x40
	os.WriteFile(path, data, 0o644)
	if _, err := ReadCheckpointFile(path); !errors.Is(err, ErrBadCheckpoint) {
		t.Fatalf("got %v", err)
	}
	cp := e.Checkpoint()
	cp.Offset++
	if _, err := Restore(cp); !errors.Is(err, ErrBadCheckpoint) {
		t.Fatalf("offset mismatch accepted: %v", err)
	}
}
