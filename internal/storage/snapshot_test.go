package storage

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/cockroachdb/pebble/v2/vfs"
)

// fill writes n events plus metadata the way the state machine would.
func fill(t *testing.T, s *Store, n uint64, applied uint64) {
	t.Helper()
	put(t, s, seq(1, n)...)
	b := s.NewBatch()
	b.SetApplied(applied)
	b.SetLastOffset(n)
	b.SetProducer(1, Producer{LastSeq: n, LastOffset: n})
	b.SetConsumer("risk", n/2)
	if err := b.Commit(true); err != nil {
		t.Fatal(err)
	}
}

func seq(from, to uint64) []uint64 {
	var out []uint64
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

func stateBytes(t *testing.T, s *Store) []byte {
	t.Helper()
	ss, err := s.SnapshotStateMachine()
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	var buf bytes.Buffer
	if _, err := ss.WriteTo(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestSnapshotTransferReplacesStateMachine(t *testing.T) {
	src, dst := openMem(t), openMem(t)
	fill(t, src, 500, 77)
	fill(t, dst, 20, 5) // stale state that must disappear entirely
	stream := stateBytes(t, src)

	if err := dst.ReceiveSnapshot(bytes.NewReader(stream), 77); err != nil {
		t.Fatal(err)
	}
	if err := dst.InstallSnapshot(77); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stateBytes(t, dst), stream) {
		t.Fatal("installed state differs from the source")
	}
	if dst.HasSnapshotFile(77) {
		t.Fatal("snapshot file not cleaned up")
	}
	if _, _, err := dst.ReadRaw(1, 500, 1000, 1<<30); err != nil {
		t.Fatal(err)
	}
}

func TestDamagedSnapshotStreamIsRejected(t *testing.T) {
	src := openMem(t)
	fill(t, src, 100, 10)
	stream := stateBytes(t, src)
	dst := openMem(t)
	if err := dst.ReceiveSnapshot(bytes.NewReader(stream), 11); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("wrong index accepted: %v", err)
	}
	for _, cut := range []int{0, 10, len(stream) / 2, len(stream) - 1} {
		if err := dst.ReceiveSnapshot(bytes.NewReader(stream[:cut]), 10); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("stream cut at %d accepted: %v", cut, err)
		}
	}
	flipped := bytes.Clone(stream)
	flipped[len(flipped)/2] ^= 1
	if err := dst.ReceiveSnapshot(bytes.NewReader(flipped), 10); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("flipped bit accepted: %v", err)
	}
	if dst.HasSnapshotFile(10) {
		t.Fatal("rejected snapshot left a file behind")
	}
}

// A crash between install batches leaves applied missing; installing again
// from the retained file must produce exactly the source state.
func TestCrashDuringInstallIsRecoveredByReinstalling(t *testing.T) {
	src := openMem(t)
	fill(t, src, 2000, 300)
	stream := stateBytes(t, src)

	defer func(n int) { installChunk = n }(installChunk)
	installChunk = 4 << 10
	for crashAfter := 1; crashAfter <= 5; crashAfter++ {
		fs := vfs.NewCrashableMem()
		dst, err := Open(Options{Dir: "n", FS: fs})
		if err != nil {
			t.Fatal(err)
		}
		fill(t, dst, 50, 40)
		if err := dst.ReceiveSnapshot(bytes.NewReader(stream), 300); err != nil {
			t.Fatal(err)
		}
		var crashed *vfs.MemFS
		batches := 0
		installHook = func() {
			if batches++; batches == crashAfter {
				crashed = fs.CrashClone(vfs.CrashCloneCfg{UnsyncedDataPercent: 50, RNG: rand.New(rand.NewPCG(uint64(crashAfter), 0))})
			}
		}
		err = dst.InstallSnapshot(300)
		installHook = nil
		dst.Close()
		if err != nil || crashed == nil {
			t.Fatalf("install: %v (crashed=%v)", err, crashed != nil)
		}

		after, err := Open(Options{Dir: "n", FS: crashed})
		if err != nil {
			t.Fatal(err)
		}
		meta, _ := after.LoadMeta()
		if meta.Applied >= 300 {
			t.Fatalf("crash after batch %d: applied %d before the install finished", crashAfter, meta.Applied)
		}
		if !after.HasSnapshotFile(300) {
			t.Fatal("snapshot file lost before install finished")
		}
		if err := after.InstallSnapshot(300); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(stateBytes(t, after), stream) {
			t.Fatalf("crash after batch %d: reinstalled state differs", crashAfter)
		}
		after.Close()
	}
}

// Reads on other goroutines while a snapshot is installed in several batches
// must see either a consistent state or ErrUnavailable, never ErrCorrupt:
// a half-installed keyspace looks exactly like missing records. Found in
// review; before the fix a follower serving reads failed itself as corrupt.
func TestReadsDuringInstallAreNeverCorrupt(t *testing.T) {
	src := openMem(t)
	fill(t, src, 3000, 77)
	stream := stateBytes(t, src)
	dst := openMem(t)
	fill(t, dst, 1000, 5)
	defer SetInstallHookForTest(2<<10, nil)()

	stop := make(chan struct{})
	errs := make(chan error, 1)
	unavailable := 0
	go func() {
		defer close(errs)
		for {
			select {
			case <-stop:
				return
			default:
			}
			// A reader's upTo comes from the state machine's published last
			// offset, which is stale during the install.
			_, _, err := dst.ReadRaw(1, 1000, 1000, 1<<30)
			if errors.Is(err, ErrUnavailable) {
				unavailable++
				continue
			}
			if err != nil {
				errs <- err
				return
			}
		}
	}()
	for i := 0; i < 20; i++ {
		if err := dst.ReceiveSnapshot(bytes.NewReader(stream), 77); err != nil {
			t.Fatal(err)
		}
		if err := dst.InstallSnapshot(77); err != nil {
			t.Fatal(err)
		}
	}
	close(stop)
	if err := <-errs; err != nil {
		t.Fatalf("reader saw %v during an install", err)
	}
	t.Logf("%d reads overlapped an install and were told to retry", unavailable)
}

// A record that rotted on the leader's disk must not be shipped (G5).
func TestSnapshotRefusesToStreamCorruptRecords(t *testing.T) {
	src := openMem(t)
	fill(t, src, 100, 10)
	if err := CorruptForTest(src, 50); err != nil {
		t.Fatal(err)
	}
	ss, err := src.SnapshotStateMachine()
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	if _, err := ss.WriteTo(io.Discard); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("streamed a corrupt record: %v", err)
	}
}
