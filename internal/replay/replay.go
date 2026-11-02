// Package replay rebuilds order-book state from committed events and
// checkpoints it. Replay is deterministic: the same committed prefix always
// yields the same state hash (design G7).
package replay

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/orderbook"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

// ErrGap means the source skipped an offset. Applying past a gap would
// produce a state that no committed prefix corresponds to.
var ErrGap = errors.New("replay: gap in event offsets")

// Source supplies verified events in offset order. An empty result means
// nothing is available at from yet.
type Source interface {
	Read(ctx context.Context, from uint64, max int) ([]event.Event, error)
}

// StoreSource reads a local store directly, up to an offset known to be applied.
type StoreSource struct {
	Store *storage.Store
	UpTo  uint64
}

func (s StoreSource) Read(_ context.Context, from uint64, max int) ([]event.Event, error) {
	return s.Store.Read(from, s.UpTo, max)
}

// ClientSource reads from a running cluster.
type ClientSource struct {
	Client       *client.Client
	Linearizable bool
}

func (s ClientSource) Read(ctx context.Context, from uint64, max int) ([]event.Event, error) {
	evs, _, err := s.Client.Read(ctx, from, max, s.Linearizable)
	return evs, err
}

// Engine applies events to an order book in strict offset order.
type Engine struct {
	book *orderbook.Book
}

func New() *Engine { return &Engine{book: orderbook.New()} }

func (e *Engine) Book() *orderbook.Book { return e.book }

// Next is the offset the engine expects to apply next.
func (e *Engine) Next() uint64 { return e.book.Stats.LastOffset + 1 }

// Apply applies ev if it is the next offset. Offsets already applied are
// skipped and reported as not applied, so overlapping or repeated deliveries
// (at-least-once consumers, resumed reads) are harmless.
func (e *Engine) Apply(ev *event.Event) (bool, error) {
	switch next := e.Next(); {
	case ev.Offset < next:
		return false, nil
	case ev.Offset > next:
		return false, fmt.Errorf("%w: expected offset %d, got %d", ErrGap, next, ev.Offset)
	}
	return true, e.book.Apply(ev)
}

// Run replays from Next() through until (inclusive), or until the source runs
// dry when until is 0. It stops at the first error; the engine then reflects
// exactly the events before the failing one, so a resumed Run continues
// cleanly. It returns the number of events applied.
func (e *Engine) Run(ctx context.Context, src Source, until uint64, batch int) (int, error) {
	if until == 0 {
		until = math.MaxUint64
	}
	applied := 0
	for e.Next() <= until {
		if err := ctx.Err(); err != nil {
			return applied, err
		}
		evs, err := src.Read(ctx, e.Next(), int(min(uint64(batch), until-e.Next()+1)))
		if err != nil {
			return applied, fmt.Errorf("replay: read at offset %d: %w", e.Next(), err)
		}
		// A batch with nothing at or past Next means the source has no new
		// events; at-least-once sources may still re-send old ones.
		if len(evs) == 0 || evs[len(evs)-1].Offset < e.Next() {
			break
		}
		for i := range evs {
			if evs[i].Offset > until {
				return applied, nil
			}
			ok, err := e.Apply(&evs[i])
			if err != nil {
				return applied, err
			}
			if ok {
				applied++
			}
		}
	}
	return applied, nil
}
