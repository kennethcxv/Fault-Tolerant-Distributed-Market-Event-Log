package sim

import (
	"fmt"
	"testing"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
)

// probeReqBase keeps test-issued request ids clear of the workload's.
const probeReqBase = 1 << 40

func probe(producer, seq uint64, reqID uint64, payload string) *fsm.Command {
	return &fsm.Command{ReqID: reqID, Append: &fsm.Append{ProducerID: producer, FirstSeq: seq,
		Events: []fsm.EventIn{{Type: 999, Schema: 1, Payload: []byte(payload)}}}}
}

// A leader cut off in a minority (2 of 5) keeps believing it leads for a
// while and accepts proposals into its log. None may be acknowledged, and
// once the majority has moved on they are overwritten.
func TestMinorityLeaderCannotAcknowledgeWrites(t *testing.T) {
	var stale uint64
	var minorityResults []fsm.Result
	c, w := newHarness(t, Options{Nodes: 5, Seed: 51, OnResult: func(node uint64, r fsm.Result) {
		if node == stale && r.ReqID >= probeReqBase {
			minorityResults = append(minorityResults, r)
		}
	}}, 2, 80)
	if err := w.Run(5_000, func() bool { return w.Acked() >= 20 }); err != nil {
		t.Fatal(err)
	}
	old := c.Leader()
	stale = old.ID
	peer := old.ID%5 + 1
	var rest []uint64
	for _, n := range c.Nodes {
		if n.ID != old.ID && n.ID != peer {
			rest = append(rest, n.ID)
		}
	}
	c.Partition([]uint64{old.ID, peer}, rest)
	proposed := 0
	for i := uint64(1); i <= 5 && old.rep.IsLeader(); i++ {
		if c.Propose(old.ID, probe(99, i, probeReqBase+i, fmt.Sprint("minority-", i))) == nil {
			proposed++
		}
		if err := w.Steps(1); err != nil {
			t.Fatal(err)
		}
	}
	if proposed == 0 {
		t.Fatal("setup: the isolated leader accepted no proposals")
	}
	before := w.Acked()
	if err := w.Run(10_000, func() bool { l := c.Leader(); return l != nil && l != old && w.Acked() >= before+20 }); err != nil {
		t.Fatalf("majority made no progress: %v\ntrace: %s", err, c.SaveTrace(t.Name()))
	}
	finish(t, c, w)
	if len(minorityResults) > 0 {
		t.Fatalf("minority leader acknowledged writes: %+v", minorityResults)
	}
	log, _ := c.CheckAgreement()
	for _, e := range log {
		if e.ProducerID == 99 {
			t.Fatalf("write proposed only in the minority is in the log at offset %d", e.Offset)
		}
	}
	t.Logf("minority leader accepted %d proposals; none acknowledged or committed", proposed)
}

// Clients resend: the same request may arrive several times, late, or after
// newer ones. Network messages are also duplicated and delayed. Each
// sequence must be applied exactly once, gaps rejected, overlaps trimmed.
func TestDelayedAndDuplicatedClientRequests(t *testing.T) {
	results := map[uint64][]fsm.Status{}
	var leader uint64
	c, w := newHarness(t, Options{Nodes: 3, Seed: 61, DupRate: 0.2, MaxDelay: 6, OnResult: func(node uint64, r fsm.Result) {
		if node == leader && r.ReqID >= probeReqBase {
			results[r.ReqID] = append(results[r.ReqID], r.Status)
		}
	}}, 1, 30)
	if err := w.Run(5_000, func() bool { return c.Leader() != nil && w.Acked() >= 5 }); err != nil {
		t.Fatal(err)
	}
	leader = c.Leader().ID
	// This test checks the exact result of every request it sends instead.
	w.ExternalWrites()
	req := uint64(probeReqBase)
	submit := func(cmd *fsm.Command) uint64 {
		t.Helper()
		if err := c.Propose(leader, cmd); err != nil {
			t.Fatalf("propose: %v", err)
		}
		return cmd.ReqID
	}
	settle := func() {
		t.Helper()
		if err := w.Steps(40); err != nil {
			t.Fatal(err)
		}
	}
	next := func() uint64 { req++; return req }

	for seq := uint64(1); seq <= 3; seq++ {
		submit(probe(77, seq, next(), fmt.Sprint("e", seq)))
	}
	settle()
	// The same request three times in one tick.
	dupIDs := []uint64{submit(probe(77, 4, next(), "e4")), submit(probe(77, 4, next(), "e4")), submit(probe(77, 4, next(), "e4"))}
	settle()
	late := submit(probe(77, 2, next(), "e2")) // a delayed retry of an old request
	gap := submit(probe(77, 6, next(), "e6"))  // skips sequence 5
	overlapCmd := probe(77, 4, next(), "e4")   // a batch re-covering 4 and adding 5
	overlapCmd.Append.Events = append(overlapCmd.Append.Events, fsm.EventIn{Type: 999, Schema: 1, Payload: []byte("e5")})
	overlap := submit(overlapCmd)
	settle()

	count := map[fsm.Status]int{}
	for _, id := range dupIDs {
		if len(results[id]) != 1 {
			t.Fatalf("request %d got %d results", id, len(results[id]))
		}
		count[results[id][0]]++
	}
	if count[fsm.StatusAppended] != 1 || count[fsm.StatusDuplicate] != 2 {
		t.Fatalf("three copies of one request: %v", count)
	}
	if got := results[late]; len(got) != 1 || got[0] != fsm.StatusDuplicate {
		t.Fatalf("late retry: %v", got)
	}
	if got := results[gap]; len(got) != 1 || got[0] != fsm.StatusSequenceGap {
		t.Fatalf("gap: %v", got)
	}
	if got := results[overlap]; len(got) != 1 || got[0] != fsm.StatusAppended {
		t.Fatalf("overlap: %v", got)
	}
	finish(t, c, w)
	log, _ := c.CheckAgreement()
	var seqs []uint64
	for _, e := range log {
		if e.ProducerID == 77 {
			seqs = append(seqs, e.ProducerSeq)
		}
	}
	if fmt.Sprint(seqs) != "[1 2 3 4 5]" {
		t.Fatalf("producer 77 sequences in the log: %v", seqs)
	}
}

// One-way failure: the leader's messages reach the followers, but nothing
// gets back. It cannot commit; CheckQuorum makes it step down and a follower
// takes over, even though the old leader's heartbeats were still arriving.
func TestOneWayPartitionLeaderCannotHearFollowers(t *testing.T) {
	c, w := newHarness(t, Options{Nodes: 3, Seed: 71}, 2, 80)
	if err := w.Run(5_000, func() bool { return w.Acked() >= 20 }); err != nil {
		t.Fatal(err)
	}
	old := c.Leader()
	for _, n := range c.Nodes {
		if n != old {
			c.BlockLink(n.ID, old.ID)
		}
	}
	before := w.Acked()
	if err := w.Run(10_000, func() bool { l := c.Leader(); return l != nil && l != old && w.Acked() >= before+20 }); err != nil {
		t.Fatalf("no progress under one-way partition: %v\ntrace: %s", err, c.SaveTrace(t.Name()))
	}
	finish(t, c, w)
}
