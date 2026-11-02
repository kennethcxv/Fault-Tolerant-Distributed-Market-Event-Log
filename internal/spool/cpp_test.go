package spool

import (
	"context"
	"io"
	"log/slog"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/orderbook"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/server"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// buildEngine compiles the C++ example engine; the test is skipped when no
// C++ compiler is installed.
func buildEngine(t *testing.T) string {
	t.Helper()
	cxx, err := exec.LookPath("c++")
	if err != nil {
		t.Skip("no C++ compiler")
	}
	bin := filepath.Join(t.TempDir(), "example_engine")
	out, err := exec.Command(cxx, "-std=c++17", "-O2", "-Wall", "-Wextra", "-Werror", "-pthread", "-o", bin,
		"../../integration/cpp/example_engine.cpp").CombinedOutput()
	if err != nil {
		t.Fatalf("compile: %v\n%s", err, out)
	}
	return bin
}

func runEngine(t *testing.T, bin, spool string, args ...string) map[string]uint64 {
	t.Helper()
	out, err := exec.Command(bin, append([]string{spool, "7"}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("engine: %v\n%s", err, out)
	}
	t.Logf("engine: %s", out)
	stats := map[string]uint64{}
	for _, m := range regexp.MustCompile(`(\w+)=(\d+)`).FindAllStringSubmatch(string(out), -1) {
		stats[m[1]], _ = strconv.ParseUint(m[2], 10, 64)
	}
	return stats
}

func readSpool(t *testing.T, path string) []event.Event {
	t.Helper()
	r, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var out []event.Event
	for {
		e, err := r.Next()
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, e)
	}
}

// The C++ engine writes a spool; the publisher, restarted midway, carries it
// into the log. The log must hold exactly the spool's records, once each and
// in order, and replaying either must give the same order book.
func TestCppSpoolToLog(t *testing.T) {
	bin := buildEngine(t)
	dir := t.TempDir()
	spoolPath := filepath.Join(dir, "engine.spool")
	stats := runEngine(t, bin, spoolPath, "30000")
	spooled := readSpool(t, spoolPath)
	if uint64(len(spooled)) != stats["written"] {
		t.Fatalf("spool has %d records, engine reports %d written", len(spooled), stats["written"])
	}

	node, err := server.Start(server.Config{ID: 1, Listen: "127.0.0.1:0", DataDir: t.TempDir(), Fsync: true, Standalone: true, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()
	c := client.New(node.Addr())

	// First run is cut short; the second resumes from the log's position.
	for i, d := range []time.Duration{30 * time.Millisecond, 20 * time.Second} {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		p := &Publisher{Client: c, Path: spoolPath, ProducerID: 7, Logger: quiet}
		go func() {
			for p.AckedSeq.Load() < uint64(len(spooled)) && ctx.Err() == nil {
				time.Sleep(5 * time.Millisecond)
			}
			cancel()
		}()
		p.Run(ctx)
		cancel()
		t.Logf("publisher run %d stopped at acked seq %d", i+1, p.AckedSeq.Load())
	}

	var logged []event.Event
	for from := uint64(1); from <= uint64(len(spooled)); {
		evs, _, err := c.Read(context.Background(), from, 10_000, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) == 0 {
			t.Fatalf("log has only %d of %d records", from-1, len(spooled))
		}
		logged = append(logged, evs...)
		from += uint64(len(evs))
	}
	for i, s := range spooled {
		l := logged[i]
		if l.ProducerID != 7 || l.ProducerSeq != s.ProducerSeq || l.Type != s.Type || l.SourceTS != s.SourceTS || string(l.Payload) != string(s.Payload) {
			t.Fatalf("record %d differs: spool %+v, log %+v", i+1, s, l)
		}
	}
	a, b := orderbook.New(), orderbook.New()
	for i := range spooled {
		spooled[i].Offset = uint64(i + 1)
		a.Apply(&spooled[i])
		b.Apply(&logged[i])
	}
	if a.Hash() != b.Hash() {
		t.Fatal("replaying the spool and the log gives different books")
	}
}

// With a tiny ring the engine drops events; the drops must reach the log as
// GAP records whose counts add up to the engine's own drop count.
func TestCppDropsBecomeGapRecords(t *testing.T) {
	bin := buildEngine(t)
	spoolPath := filepath.Join(t.TempDir(), "tiny.spool")
	stats := runEngine(t, bin, spoolPath, "20000", "tiny")
	if stats["dropped"] == 0 {
		t.Skip("the engine happened not to drop anything")
	}
	book := orderbook.New()
	for i, e := range readSpool(t, spoolPath) {
		e.Offset = uint64(i + 1)
		book.Apply(&e)
	}
	if book.Stats.Gaps != stats["gaps"] || book.Stats.GapEvents != stats["dropped"] {
		t.Fatalf("GAP records report %d gaps / %d events; engine reports %d / %d",
			book.Stats.Gaps, book.Stats.GapEvents, stats["gaps"], stats["dropped"])
	}
}
