package sim

import (
	"fmt"
	"hash/fnv"
	"slices"

	"github.com/anishathalye/porcupine"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
)

// OpInput is one client operation: an append of a single event from a
// producer, or a linearizable read of one offset.
type OpInput struct {
	Read     bool
	Offset   uint64
	Producer uint64
	Seq      uint64
	Payload  string
}

// Output is what the client observed. Unknown marks an append that timed
// out; it may or may not have taken effect.
type Output struct {
	Unknown         bool
	Status          fsm.Status
	Offset          uint64
	ProducerLastSeq uint64
	NotFound        bool
	Producer, Seq   uint64
	Payload         string
}

type logEntry struct {
	producer, seq uint64
	payload       string
}

// logState is the sequential specification: the log, plus each producer's
// last applied sequence. States are never mutated, only replaced.
type logState struct {
	entries []logEntry
	lastSeq []uint64 // index producer-1
}

func (s logState) last(producer uint64) uint64 {
	if int(producer) > len(s.lastSeq) {
		return 0
	}
	return s.lastSeq[producer-1]
}

func (s logState) append(in OpInput) logState {
	entries := append(slices.Clip(s.entries), logEntry{in.Producer, in.Seq, in.Payload})
	lastSeq := slices.Clone(s.lastSeq)
	for len(lastSeq) < int(in.Producer) {
		lastSeq = append(lastSeq, 0)
	}
	lastSeq[in.Producer-1] = in.Seq
	return logState{entries: entries, lastSeq: lastSeq}
}

// LogModel specifies the log for Porcupine. Appends carry producer sequence
// numbers, so the model also checks deduplication: a retry of an applied
// sequence must come back as a duplicate, never as a second append.
var LogModel = porcupine.Model{
	Init: func() any { return logState{} },
	Step: func(state, input, output any) (bool, any) {
		s, in, out := state.(logState), input.(OpInput), output.(Output)
		n := uint64(len(s.entries))
		if in.Read {
			if out.NotFound {
				return in.Offset > n, s
			}
			if in.Offset == 0 || in.Offset > n {
				return false, s
			}
			return s.entries[in.Offset-1] == logEntry{out.Producer, out.Seq, out.Payload}, s
		}
		last := s.last(in.Producer)
		switch {
		case out.Unknown:
			// Linearized late enough, an attempt that never committed is
			// indistinguishable from one that committed after its retry.
			if in.Seq == last+1 {
				return true, s.append(in)
			}
			return true, s
		case out.Status == fsm.StatusAppended:
			return in.Seq == last+1 && out.Offset == n+1, s.append(in)
		case out.Status == fsm.StatusDuplicate:
			return in.Seq <= last && out.ProducerLastSeq == last, s
		}
		return false, s
	},
	Equal: func(a, b any) bool {
		x, y := a.(logState), b.(logState)
		return slices.Equal(x.entries, y.entries) && slices.Equal(x.lastSeq, y.lastSeq)
	},
	Hash: func(state any) uint64 {
		s := state.(logState)
		h := fnv.New64a()
		fmt.Fprint(h, len(s.entries), s.lastSeq)
		return h.Sum64()
	},
	DescribeOperation: func(input, output any) string {
		in, out := input.(OpInput), output.(Output)
		if in.Read {
			if out.NotFound {
				return fmt.Sprintf("read(%d) -> none", in.Offset)
			}
			return fmt.Sprintf("read(%d) -> p%d/s%d", in.Offset, out.Producer, out.Seq)
		}
		if out.Unknown {
			return fmt.Sprintf("append(p%d/s%d) -> ?", in.Producer, in.Seq)
		}
		return fmt.Sprintf("append(p%d/s%d) -> %s @%d", in.Producer, in.Seq, out.Status, out.Offset)
	},
	DescribeState: func(state any) string {
		s := state.(logState)
		return fmt.Sprintf("%d entries, last seqs %v", len(s.entries), s.lastSeq)
	},
}
