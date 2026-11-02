package orderbook

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
)

// ErrUnsupportedSchema stops replay: the book cannot interpret the payload,
// and skipping it would silently produce a different state.
var ErrUnsupportedSchema = errors.New("orderbook: unsupported schema version")

// Stats are part of the state and the hash. Rejected counts well-formed
// messages that don't fit the current book (unknown order, duplicate id,
// overfill); Malformed counts payloads that failed to decode. Neither stops
// replay, and both are deterministic.
type Stats struct {
	LastOffset  uint64
	Events      uint64
	Adds        uint64
	Cancels     uint64
	Executes    uint64
	Replaces    uint64
	Rejected    uint64
	Malformed   uint64
	Ignored     uint64
	Gaps        uint64
	GapEvents   uint64
	ExecutedQty uint64
}

type order struct {
	id         uint64
	inst       uint32
	side       Side
	qty        uint64
	lvl        *level
	prev, next *order
}

type level struct {
	owner      *bookSide
	price      int64
	head, tail *order
	count      uint32
	qty        uint64
}

type bookSide struct {
	prices []int64 // ascending
	levels map[int64]*level
}

type instrument struct {
	sides [2]bookSide
}

// Book is a price-time priority order book. It is not safe for concurrent use.
type Book struct {
	Stats       Stats
	orders      map[uint64]*order
	instruments map[uint32]*instrument
}

func New() *Book {
	return &Book{orders: map[uint64]*order{}, instruments: map[uint32]*instrument{}}
}

// Apply applies one committed event. Its only error is ErrUnsupportedSchema.
func (b *Book) Apply(e *event.Event) error {
	if e.Type >= TypeAddOrder && e.Type <= TypeReplaceOrder && e.Schema != SchemaV1 {
		return fmt.Errorf("%w: offset %d has type %d schema %d", ErrUnsupportedSchema, e.Offset, e.Type, e.Schema)
	}
	b.Stats.LastOffset = e.Offset
	b.Stats.Events++
	if e.Type == event.TypeGap {
		b.Stats.Gaps++
		if len(e.Payload) >= 8 {
			b.Stats.GapEvents += binary.LittleEndian.Uint64(e.Payload)
		}
		return nil
	}
	if _, ok := payloadSize[e.Type]; !ok {
		b.Stats.Ignored++
		return nil
	}
	m, err := Decode(e.Type, e.Payload)
	if err != nil {
		b.Stats.Malformed++
		return nil
	}
	if !b.apply(m) {
		b.Stats.Rejected++
	}
	return nil
}

func (b *Book) apply(m Msg) bool {
	switch m.Type {
	case TypeAddOrder:
		if !b.add(m.Instrument, m.Order, m.Side, m.Price, m.Qty) {
			return false
		}
		b.Stats.Adds++
	case TypeCancelOrder:
		o := b.orders[m.Order]
		if o == nil || o.inst != m.Instrument {
			return false
		}
		b.remove(o)
		b.Stats.Cancels++
	case TypeExecuteOrder:
		o := b.orders[m.Order]
		if o == nil || o.inst != m.Instrument || m.Qty == 0 || m.Qty > o.qty {
			return false
		}
		o.qty -= m.Qty
		o.lvl.qty -= m.Qty
		if o.qty == 0 {
			b.remove(o)
		}
		b.Stats.Executes++
		b.Stats.ExecutedQty += m.Qty
	case TypeReplaceOrder:
		// A replace loses time priority, as on most venues.
		o := b.orders[m.Order]
		if o == nil || o.inst != m.Instrument || b.orders[m.NewOrder] != nil || m.Qty == 0 || m.Price <= 0 {
			return false
		}
		side := o.side
		b.remove(o)
		b.add(m.Instrument, m.NewOrder, side, m.Price, m.Qty)
		b.Stats.Replaces++
	}
	return true
}

func (b *Book) add(inst uint32, id uint64, side Side, price int64, qty uint64) bool {
	if b.orders[id] != nil || qty == 0 || price <= 0 || side > Ask {
		return false
	}
	in := b.instruments[inst]
	if in == nil {
		in = &instrument{}
		in.sides[Bid].levels = map[int64]*level{}
		in.sides[Ask].levels = map[int64]*level{}
		b.instruments[inst] = in
	}
	s := &in.sides[side]
	lvl := s.levels[price]
	if lvl == nil {
		lvl = &level{owner: s, price: price}
		s.levels[price] = lvl
		i, _ := slices.BinarySearch(s.prices, price)
		s.prices = slices.Insert(s.prices, i, price)
	}
	o := &order{id: id, inst: inst, side: side, qty: qty, lvl: lvl, prev: lvl.tail}
	if lvl.tail != nil {
		lvl.tail.next = o
	} else {
		lvl.head = o
	}
	lvl.tail = o
	lvl.count++
	lvl.qty += qty
	b.orders[id] = o
	return true
}

func (b *Book) remove(o *order) {
	lvl := o.lvl
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		lvl.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		lvl.tail = o.prev
	}
	lvl.count--
	lvl.qty -= o.qty
	delete(b.orders, o.id)
	if lvl.count > 0 {
		return
	}
	s := lvl.owner
	delete(s.levels, lvl.price)
	i, _ := slices.BinarySearch(s.prices, lvl.price)
	s.prices = slices.Delete(s.prices, i, i+1)
}

// Top returns the best bid and ask for an instrument (zero if empty).
func (b *Book) Top(inst uint32) (bidPx int64, bidQty uint64, askPx int64, askQty uint64) {
	in := b.instruments[inst]
	if in == nil {
		return
	}
	if p := in.sides[Bid].prices; len(p) > 0 {
		l := in.sides[Bid].levels[p[len(p)-1]]
		bidPx, bidQty = l.price, l.qty
	}
	if p := in.sides[Ask].prices; len(p) > 0 {
		l := in.sides[Ask].levels[p[0]]
		askPx, askQty = l.price, l.qty
	}
	return
}

func (b *Book) Orders() int { return len(b.orders) }

const snapshotMagic = "OBK1"

// MarshalBinary is the canonical encoding of the whole state: stats, then
// instruments by id, bids best-first, asks best-first, orders in time
// priority. Two books are equal iff their encodings are equal, so this is
// both the snapshot format and the input to Hash.
func (b *Book) MarshalBinary() ([]byte, error) {
	out := []byte(snapshotMagic)
	s := b.Stats
	for _, v := range []uint64{s.LastOffset, s.Events, s.Adds, s.Cancels, s.Executes, s.Replaces,
		s.Rejected, s.Malformed, s.Ignored, s.Gaps, s.GapEvents, s.ExecutedQty} {
		out = binary.LittleEndian.AppendUint64(out, v)
	}
	ids := make([]uint32, 0, len(b.instruments))
	for id := range b.instruments {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(ids)))
	for _, id := range ids {
		in := b.instruments[id]
		out = binary.LittleEndian.AppendUint32(out, id)
		for side := Bid; side <= Ask; side++ {
			prices := in.sides[side].prices
			out = binary.LittleEndian.AppendUint32(out, uint32(len(prices)))
			for i := range prices {
				p := prices[i]
				if side == Bid {
					p = prices[len(prices)-1-i]
				}
				lvl := in.sides[side].levels[p]
				out = binary.LittleEndian.AppendUint64(out, uint64(p))
				out = binary.LittleEndian.AppendUint32(out, lvl.count)
				for o := lvl.head; o != nil; o = o.next {
					out = binary.LittleEndian.AppendUint64(out, o.id)
					out = binary.LittleEndian.AppendUint64(out, o.qty)
				}
			}
		}
	}
	return out, nil
}

// Hash is SHA-256 of the canonical encoding.
func (b *Book) Hash() [32]byte {
	data, _ := b.MarshalBinary()
	return sha256.Sum256(data)
}

var errSnapshot = errors.New("orderbook: invalid snapshot")

// UnmarshalBinary restores a book from MarshalBinary output, rejecting
// anything that MarshalBinary could not have produced.
func UnmarshalBinary(data []byte) (*Book, error) {
	r := reader{buf: data}
	if string(r.next(4)) != snapshotMagic {
		return nil, errSnapshot
	}
	b := New()
	s := &b.Stats
	for _, p := range []*uint64{&s.LastOffset, &s.Events, &s.Adds, &s.Cancels, &s.Executes, &s.Replaces,
		&s.Rejected, &s.Malformed, &s.Ignored, &s.Gaps, &s.GapEvents, &s.ExecutedQty} {
		*p = r.u64()
	}
	prevInst := int64(-1)
	for range r.u32() {
		id := r.u32()
		if r.err != nil || int64(id) <= prevInst {
			return nil, errSnapshot
		}
		prevInst = int64(id)
		for side := Bid; side <= Ask; side++ {
			levels := r.u32()
			var prices []int64
			for range levels {
				price, count := int64(r.u64()), r.u32()
				if r.err != nil || count == 0 {
					return nil, errSnapshot
				}
				prices = append(prices, price)
				for range count {
					if !b.add(id, r.u64(), side, price, r.u64()) || r.err != nil {
						return nil, errSnapshot
					}
				}
			}
			// Levels must be strictly ordered best-first.
			if side == Bid {
				slices.Reverse(prices)
			}
			if !sort.SliceIsSorted(prices, func(i, j int) bool { return prices[i] < prices[j] }) || len(slices.Compact(prices)) != len(prices) {
				return nil, errSnapshot
			}
		}
	}
	if r.err != nil || len(r.buf) != 0 {
		return nil, errSnapshot
	}
	return b, nil
}

type reader struct {
	buf []byte
	err error
}

func (r *reader) next(n int) []byte {
	if r.err != nil || len(r.buf) < n {
		r.err = errSnapshot
		return make([]byte, n)
	}
	v := r.buf[:n]
	r.buf = r.buf[n:]
	return v
}

func (r *reader) u32() uint32 { return binary.LittleEndian.Uint32(r.next(4)) }
func (r *reader) u64() uint64 { return binary.LittleEndian.Uint64(r.next(8)) }
