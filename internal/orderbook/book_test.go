package orderbook

import (
	"bytes"
	"errors"
	"testing"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
)

// feed wraps messages as consecutive schema-v1 events starting at offset 1.
func feed(t *testing.T, b *Book, msgs ...Msg) {
	t.Helper()
	for _, m := range msgs {
		e := event.Event{Offset: b.Stats.LastOffset + 1, Type: m.Type, Schema: SchemaV1, Payload: m.Encode()}
		if err := b.Apply(&e); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPriceTimePriority(t *testing.T) {
	b := New()
	feed(t, b,
		Msg{Type: TypeAddOrder, Instrument: 1, Order: 1, Side: Bid, Price: 100, Qty: 10},
		Msg{Type: TypeAddOrder, Instrument: 1, Order: 2, Side: Bid, Price: 101, Qty: 20},
		Msg{Type: TypeAddOrder, Instrument: 1, Order: 3, Side: Bid, Price: 101, Qty: 30},
		Msg{Type: TypeAddOrder, Instrument: 1, Order: 4, Side: Ask, Price: 103, Qty: 5},
	)
	if bp, bq, ap, aq := b.Top(1); bp != 101 || bq != 50 || ap != 103 || aq != 5 {
		t.Fatalf("top = %d x %d / %d x %d", bp, bq, ap, aq)
	}
	feed(t, b,
		Msg{Type: TypeExecuteOrder, Instrument: 1, Order: 2, Qty: 20}, // fills order 2 completely
		Msg{Type: TypeExecuteOrder, Instrument: 1, Order: 3, Qty: 10},
		Msg{Type: TypeCancelOrder, Instrument: 1, Order: 4},
	)
	if bp, bq, ap, _ := b.Top(1); bp != 101 || bq != 20 || ap != 0 {
		t.Fatalf("after fills: bid %d x %d ask %d", bp, bq, ap)
	}
	// Replace moves order 1 to a better price, behind nobody at the new level.
	feed(t, b, Msg{Type: TypeReplaceOrder, Instrument: 1, Order: 1, NewOrder: 9, Price: 102, Qty: 7})
	if bp, bq, _, _ := b.Top(1); bp != 102 || bq != 7 {
		t.Fatalf("after replace: %d x %d", bp, bq)
	}
	if b.Stats.ExecutedQty != 30 || b.Stats.Rejected != 0 || b.Orders() != 2 {
		t.Fatalf("stats %+v orders %d", b.Stats, b.Orders())
	}
}

func TestInvalidMessagesAreCountedNotFatal(t *testing.T) {
	b := New()
	feed(t, b,
		Msg{Type: TypeAddOrder, Instrument: 1, Order: 1, Side: Bid, Price: 100, Qty: 10},
		Msg{Type: TypeAddOrder, Instrument: 1, Order: 1, Side: Bid, Price: 100, Qty: 10}, // duplicate id
		Msg{Type: TypeCancelOrder, Instrument: 1, Order: 77},                             // unknown order
		Msg{Type: TypeExecuteOrder, Instrument: 1, Order: 1, Qty: 11},                    // overfill
		Msg{Type: TypeCancelOrder, Instrument: 2, Order: 1},                              // wrong instrument
		Msg{Type: TypeAddOrder, Instrument: 1, Order: 2, Side: Ask, Price: 0, Qty: 1},    // bad price
	)
	if b.Stats.Rejected != 5 || b.Orders() != 1 {
		t.Fatalf("stats %+v", b.Stats)
	}
	malformed := event.Event{Offset: 7, Type: TypeAddOrder, Schema: SchemaV1, Payload: []byte{1, 2}}
	unknown := event.Event{Offset: 8, Type: 500, Schema: 3}
	gap := event.Event{Offset: 9, Type: event.TypeGap, Schema: 1, Payload: []byte{12, 0, 0, 0, 0, 0, 0, 0}}
	for _, e := range []event.Event{malformed, unknown, gap} {
		if err := b.Apply(&e); err != nil {
			t.Fatal(err)
		}
	}
	if b.Stats.Malformed != 1 || b.Stats.Ignored != 1 || b.Stats.Gaps != 1 || b.Stats.GapEvents != 12 || b.Stats.LastOffset != 9 {
		t.Fatalf("stats %+v", b.Stats)
	}
	future := event.Event{Offset: 10, Type: TypeAddOrder, Schema: 2, Payload: []byte{}}
	if err := b.Apply(&future); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("schema 2: %v", err)
	}
}

func generate(seed uint64, n int) []event.Event {
	g := NewGenerator(seed, 4)
	out := make([]event.Event, n)
	for i := range out {
		m := g.Next()
		out[i] = event.Event{Offset: uint64(i + 1), Type: m.Type, Schema: SchemaV1, Payload: m.Encode()}
	}
	return out
}

func replay(t *testing.T, b *Book, evs []event.Event) {
	t.Helper()
	for i := range evs {
		if err := b.Apply(&evs[i]); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGeneratorAndReplayAreDeterministic(t *testing.T) {
	a, b := New(), New()
	replay(t, a, generate(42, 20_000))
	replay(t, b, generate(42, 20_000))
	if a.Hash() != b.Hash() {
		t.Fatal("same feed, different hash")
	}
	c := New()
	replay(t, c, generate(43, 20_000))
	if a.Hash() == c.Hash() {
		t.Fatal("different feeds, same hash")
	}
	// The generator's invalid-message rate shows up as rejects, not malformed,
	// and its books never cross.
	if a.Stats.Rejected == 0 || a.Stats.Malformed != 0 || a.Orders() == 0 {
		t.Fatalf("unexpected feed shape: %+v orders=%d", a.Stats, a.Orders())
	}
	for inst := uint32(0); inst < 4; inst++ {
		if bp, _, ap, _ := a.Top(inst); bp == 0 || ap == 0 || bp >= ap {
			t.Fatalf("instrument %d: bid %d ask %d", inst, bp, ap)
		}
	}
}

// Restoring a snapshot taken at any point and continuing must reach the same
// state as an uninterrupted replay (design G7).
func TestSnapshotRestoreMatchesFullReplay(t *testing.T) {
	evs := generate(7, 10_000)
	full := New()
	replay(t, full, evs)
	for _, k := range []int{0, 1, 999, 5_000, 9_999, 10_000} {
		partial := New()
		replay(t, partial, evs[:k])
		snap, _ := partial.MarshalBinary()
		restored, err := UnmarshalBinary(snap)
		if err != nil {
			t.Fatalf("k=%d: %v", k, err)
		}
		if again, _ := restored.MarshalBinary(); !bytes.Equal(again, snap) {
			t.Fatalf("k=%d: restored snapshot re-encodes differently", k)
		}
		replay(t, restored, evs[k:])
		if restored.Hash() != full.Hash() {
			t.Fatalf("k=%d: restored+suffix hash differs from full replay", k)
		}
	}
}

func TestUnmarshalRejectsDamage(t *testing.T) {
	b := New()
	replay(t, b, generate(3, 2_000))
	snap, _ := b.MarshalBinary()
	for n := 0; n < len(snap); n += 7 {
		if _, err := UnmarshalBinary(snap[:n]); err == nil {
			t.Fatalf("truncated snapshot (%d of %d bytes) accepted", n, len(snap))
		}
	}
}

func FuzzUnmarshal(f *testing.F) {
	b := New()
	evs := generate(1, 300)
	for i := range evs {
		b.Apply(&evs[i])
	}
	snap, _ := b.MarshalBinary()
	f.Add(snap)
	f.Fuzz(func(t *testing.T, data []byte) {
		restored, err := UnmarshalBinary(data)
		if err != nil {
			return
		}
		if again, _ := restored.MarshalBinary(); !bytes.Equal(again, data) {
			t.Fatal("accepted snapshot is not canonical")
		}
	})
}
