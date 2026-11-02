package spool

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
)

func record(seq uint64, payload string) []byte {
	e := event.Event{Schema: 1, Type: 1, ProducerID: 7, ProducerSeq: seq, Payload: []byte(payload)}
	return e.AppendTo(nil)
}

func TestReaderTailsAGrowingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "s.spool")
	f, _ := os.Create(path)
	defer f.Close()
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("empty spool: %v", err)
	}
	rec := record(1, "a")
	f.Write(rec[:10]) // the writer is mid-record
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("partial record: %v", err)
	}
	f.Write(rec[10:])
	f.Write(record(2, "b"))
	for seq := uint64(1); seq <= 2; seq++ {
		e, err := r.Next()
		if err != nil || e.ProducerSeq != seq {
			t.Fatalf("seq %d: %+v %v", seq, e, err)
		}
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("at end: %v", err)
	}
}

func TestReaderRejectsCorruptionAndSequenceBreaks(t *testing.T) {
	for name, data := range map[string][]byte{
		"flipped bit":   func() []byte { b := append(record(1, "a"), record(2, "b")...); b[50] ^= 1; return b }(),
		"skipped seq":   append(record(1, "a"), record(3, "c")...),
		"starts at two": record(2, "b"),
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "s.spool")
			os.WriteFile(path, data, 0o644)
			r, _ := Open(path)
			defer r.Close()
			var err error
			for err == nil {
				_, err = r.Next()
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("got %v, want ErrCorrupt", err)
			}
		})
	}
}
