package orderbook

import "math/rand/v2"

// Generator produces a deterministic synthetic schema-v1 feed for tests,
// fixtures and benchmarks. It tracks live orders so that most messages are
// valid, and emits about 2% that the book must reject (cancels of unknown
// orders), so rejection handling is part of every replay.
type Generator struct {
	rng    *rand.Rand
	// Mid price per instrument, in ticks. Bids rest strictly below it and asks
	// strictly above, so the book never crosses, as on a real venue's feed.
	insts []int64
	nextID uint64
	live   []genOrder
}

type genOrder struct {
	id    uint64
	inst  uint32
	side  Side
	price int64
	qty   uint64
}

func NewGenerator(seed uint64, instruments int) *Generator {
	g := &Generator{rng: rand.New(rand.NewPCG(seed, 0x6d6c6f67))}
	for i := 0; i < instruments; i++ {
		g.insts = append(g.insts, 10_000+int64(i)*500)
	}
	return g
}

func (g *Generator) Next() Msg {
	r := g.rng.IntN(100)
	switch {
	case len(g.live) < 64 || r < 45:
		return g.add()
	case r < 75:
		o := g.take(g.rng.IntN(len(g.live)))
		return Msg{Type: TypeCancelOrder, Instrument: o.inst, Order: o.id}
	case r < 90:
		i := g.rng.IntN(len(g.live))
		qty := min(g.live[i].qty, 100*uint64(1+g.rng.IntN(5)))
		o := g.live[i]
		if qty == o.qty {
			g.take(i)
		} else {
			g.live[i].qty -= qty
		}
		return Msg{Type: TypeExecuteOrder, Instrument: o.inst, Order: o.id, Qty: qty}
	case r < 98:
		o := g.take(g.rng.IntN(len(g.live)))
		n := genOrder{id: g.id(), inst: o.inst, side: o.side, price: g.price(o.inst, o.side), qty: 100 * uint64(1+g.rng.IntN(10))}
		g.put(n)
		return Msg{Type: TypeReplaceOrder, Instrument: o.inst, Order: o.id, NewOrder: n.id, Price: n.price, Qty: n.qty}
	default:
		return Msg{Type: TypeCancelOrder, Instrument: 0, Order: g.id()} // never added
	}
}

func (g *Generator) add() Msg {
	inst := uint32(g.rng.IntN(len(g.insts)))
	side := Side(g.rng.IntN(2))
	o := genOrder{id: g.id(), inst: inst, side: side, price: g.price(inst, side), qty: 100 * uint64(1+g.rng.IntN(10))}
	g.put(o)
	return Msg{Type: TypeAddOrder, Instrument: o.inst, Order: o.id, Side: o.side, Price: o.price, Qty: o.qty}
}

// price picks a level 1-20 ticks away from the mid on the order's side.
func (g *Generator) price(inst uint32, side Side) int64 {
	away := int64(1 + g.rng.IntN(20))
	if side == Ask {
		return g.insts[inst] + away
	}
	return g.insts[inst] - away
}

func (g *Generator) id() uint64 {
	g.nextID++
	return g.nextID
}

func (g *Generator) put(o genOrder) { g.live = append(g.live, o) }

// take removes live[i] by swapping in the last element.
func (g *Generator) take(i int) genOrder {
	o := g.live[i]
	last := len(g.live) - 1
	g.live[i] = g.live[last]
	g.live = g.live[:last]
	return o
}
