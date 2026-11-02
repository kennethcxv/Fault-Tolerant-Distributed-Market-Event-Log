package fsm

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

func openFSM(t *testing.T, fs vfs.FS) (*FSM, *storage.Store) {
	t.Helper()
	st, err := storage.Open(storage.Options{Dir: "node", FS: fs})
	if err != nil {
		t.Fatal(err)
	}
	f, err := Open(st)
	if err != nil {
		t.Fatal(err)
	}
	return f, st
}

func appendCmd(producer, firstSeq uint64, payloads ...string) []byte {
	a := &Append{ProducerID: producer, FirstSeq: firstSeq, IngestTS: 1000}
	for _, p := range payloads {
		a.Events = append(a.Events, EventIn{Type: 1, Schema: 1, SourceTS: 5, Payload: []byte(p)})
	}
	c := Command{ReqID: 99, Append: a}
	return c.Marshal()
}

func checkpointCmd(name string, offset uint64) []byte {
	c := Command{Checkpoint: &Checkpoint{Consumer: name, Offset: offset}}
	return c.Marshal()
}

// applyOne applies data as the entry at the next index.
func applyOne(t *testing.T, f *FSM, data []byte) Result {
	t.Helper()
	idx := f.applied + 1
	res, err := f.Apply([]*raftpb.Entry{{Index: &idx, Data: data}}, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 1 {
		t.Fatalf("got %d results", len(res))
	}
	return res[0]
}

func TestAppendAssignsDenseOffsetsAcrossProducers(t *testing.T) {
	f, st := openFSM(t, vfs.NewMem())
	r1 := applyOne(t, f, appendCmd(1, 1, "a", "b"))
	r2 := applyOne(t, f, appendCmd(2, 1, "c"))
	r3 := applyOne(t, f, appendCmd(1, 3, "d"))
	if r1.FirstOffset != 1 || r1.LastOffset != 2 || r2.FirstOffset != 3 || r3.FirstOffset != 4 {
		t.Fatalf("offsets: %+v %+v %+v", r1, r2, r3)
	}
	evs, err := st.Read(1, f.LastOffset(), 100)
	if err != nil {
		t.Fatal(err)
	}
	want := []struct {
		pid, seq uint64
		p        string
	}{{1, 1, "a"}, {1, 2, "b"}, {2, 1, "c"}, {1, 3, "d"}}
	for i, w := range want {
		e := evs[i]
		if e.Offset != uint64(i+1) || e.ProducerID != w.pid || e.ProducerSeq != w.seq || string(e.Payload) != w.p || e.IngestTS != 1000 {
			t.Fatalf("event %d = %+v", i, e)
		}
	}
}

func TestProducerDedupAndGaps(t *testing.T) {
	f, _ := openFSM(t, vfs.NewMem())
	applyOne(t, f, appendCmd(7, 1, "a", "b", "c"))

	dup := applyOne(t, f, appendCmd(7, 2, "b", "c"))
	if dup.Status != StatusDuplicate || dup.ProducerLastSeq != 3 || dup.ProducerLastOffset != 3 {
		t.Fatalf("duplicate: %+v", dup)
	}
	overlap := applyOne(t, f, appendCmd(7, 3, "c", "d", "e"))
	if overlap.Status != StatusAppended || overlap.Skipped != 1 || overlap.FirstOffset != 4 || overlap.LastOffset != 5 {
		t.Fatalf("overlap: %+v", overlap)
	}
	gap := applyOne(t, f, appendCmd(7, 9, "x"))
	if gap.Status != StatusSequenceGap || gap.ExpectedSeq != 6 {
		t.Fatalf("gap: %+v", gap)
	}
	newProducerGap := applyOne(t, f, appendCmd(8, 2, "y"))
	if newProducerGap.Status != StatusSequenceGap || newProducerGap.ExpectedSeq != 1 {
		t.Fatalf("new producer must start at 1: %+v", newProducerGap)
	}
	if f.LastOffset() != 5 {
		t.Fatalf("last offset %d", f.LastOffset())
	}
}

func TestInvalidCommandIsRejectedButAdvancesIndex(t *testing.T) {
	f, _ := openFSM(t, vfs.NewMem())
	for _, data := range [][]byte{{0xEE}, appendCmd(0, 1, "x"), {kindAppend, 1, 2}} {
		r := applyOne(t, f, data)
		if r.Status != StatusInvalid {
			t.Fatalf("%x: %+v", data, r)
		}
	}
	if f.Applied() != 3 || f.LastOffset() != 0 {
		t.Fatalf("applied %d last %d", f.Applied(), f.LastOffset())
	}
}

func TestRedeliveredEntriesAreSkipped(t *testing.T) {
	f, _ := openFSM(t, vfs.NewMem())
	one, two := uint64(1), uint64(2)
	ents := []*raftpb.Entry{{Index: &one, Data: appendCmd(1, 1, "a")}, {Index: &two, Data: appendCmd(2, 1, "b")}}
	if _, err := f.Apply(ents, nil, true); err != nil {
		t.Fatal(err)
	}
	res, err := f.Apply(ents, nil, true)
	if err != nil || len(res) != 0 || f.LastOffset() != 2 {
		t.Fatalf("re-apply: %v %+v last=%d", err, res, f.LastOffset())
	}
}

func TestCheckpointsOnlyMoveForward(t *testing.T) {
	f, st := openFSM(t, vfs.NewMem())
	applyOne(t, f, appendCmd(1, 1, "a", "b", "c"))
	if r := applyOne(t, f, checkpointCmd("replay", 2)); r.Status != StatusCheckpointed || r.Checkpoint != 2 {
		t.Fatalf("%+v", r)
	}
	if r := applyOne(t, f, checkpointCmd("replay", 1)); r.Status != StatusCheckpointStale || r.Checkpoint != 2 {
		t.Fatalf("%+v", r)
	}
	if r := applyOne(t, f, checkpointCmd("replay", 4)); r.Status != StatusCheckpointBeyondEnd {
		t.Fatalf("%+v", r)
	}
	if got, err := st.GetConsumer("replay"); err != nil || got != 2 {
		t.Fatalf("stored checkpoint %d %v", got, err)
	}
}

func TestStateSurvivesReopen(t *testing.T) {
	fs := vfs.NewMem()
	f, st := openFSM(t, fs)
	applyOne(t, f, appendCmd(1, 1, "a", "b"))
	applyOne(t, f, checkpointCmd("c", 1))
	cs := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	idx := f.applied + 1
	if _, err := f.Apply([]*raftpb.Entry{{Index: &idx}}, cs, true); err != nil {
		t.Fatal(err)
	}
	st.Close()

	g, st2 := openFSM(t, fs)
	defer st2.Close()
	if g.Applied() != 3 || g.LastOffset() != 2 || g.consumers["c"] != 1 || g.producers[1].LastSeq != 2 {
		t.Fatalf("reloaded: applied=%d last=%d consumers=%v producers=%v", g.Applied(), g.LastOffset(), g.consumers, g.producers)
	}
	if len(g.ConfState().GetVoters()) != 3 {
		t.Fatalf("confstate %v", g.ConfState())
	}
	if r := applyOne(t, g, appendCmd(1, 2, "b")); r.Status != StatusDuplicate {
		t.Fatalf("dedup state lost across restart: %+v", r)
	}
}

// Unsynced batches can be lost in a crash, but each batch must be all or
// nothing: the applied index, last offset, producer state and records must
// agree after recovery (invariant I5).
func TestCrashLeavesAtomicPrefixOfBatches(t *testing.T) {
	for seed := range uint64(20) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 1))
			fs := vfs.NewCrashableMem()
			f, st := openFSM(t, fs)
			synced := uint64(0)
			for i := uint64(1); i <= 200; i++ {
				// Entry i carries i%3+1 events, so applied index and offsets differ.
				payloads := make([]string, i%3+1)
				for j := range payloads {
					payloads[j] = fmt.Sprintf("e%d.%d", i, j)
				}
				sync := rng.IntN(10) == 0
				idx := f.applied + 1
				if _, err := f.Apply([]*raftpb.Entry{{Index: &idx, Data: appendCmd(1, f.producers[1].LastSeq+1, payloads...)}}, nil, sync); err != nil {
					t.Fatal(err)
				}
				if sync {
					synced = i
				}
			}
			crashed := fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: rng.IntN(101), RNG: rand.New(rand.NewPCG(seed, 2))})
			st.Close()

			g, st2 := openFSM(t, crashed)
			defer st2.Close()
			if g.Applied() < synced {
				t.Fatalf("lost synced entry: applied %d < synced %d", g.Applied(), synced)
			}
			wantLast := uint64(0)
			for i := uint64(1); i <= g.Applied(); i++ {
				wantLast += i%3 + 1
			}
			if g.LastOffset() != wantLast || g.producers[1].LastSeq != wantLast {
				t.Fatalf("applied %d implies last offset %d, got last=%d producerSeq=%d", g.Applied(), wantLast, g.LastOffset(), g.producers[1].LastSeq)
			}
			evs, err := st2.Read(1, g.LastOffset(), math.MaxInt)
			if err != nil || uint64(len(evs)) != wantLast {
				t.Fatalf("read %d events, err %v", len(evs), err)
			}
			// A record past the last offset would mean a torn batch.
			if extra, err := st2.Read(wantLast+1, math.MaxUint64, 1); err == nil && len(extra) > 0 {
				t.Fatalf("record %d exists beyond last offset", wantLast+1)
			}
		})
	}
}

func TestCommandRoundTrip(t *testing.T) {
	data := appendCmd(3, 10, "x", "", "yz")
	c, err := Unmarshal(data)
	if err != nil || c.ReqID != 99 || len(c.Append.Events) != 3 || !bytes.Equal(c.Append.Events[2].Payload, []byte("yz")) {
		t.Fatalf("%+v %v", c, err)
	}
	if !bytes.Equal(c.Marshal(), data) {
		t.Fatal("re-marshal differs")
	}
	for n := range len(data) {
		if _, err := Unmarshal(data[:n]); err == nil {
			t.Fatalf("truncated command (%d bytes) accepted", n)
		}
	}
}
