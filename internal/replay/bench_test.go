package replay

import (
	"context"
	"testing"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/orderbook"
)

// BenchmarkBookApply is the order book alone: decode a schema-v1 payload and
// apply it, per event.
func BenchmarkBookApply(b *testing.B) {
	g := orderbook.NewGenerator(1, 8)
	evs := make([]event.Event, 1<<16)
	for i := range evs {
		m := g.Next()
		evs[i] = event.Event{Type: m.Type, Schema: 1, Payload: m.Encode()}
	}
	b.ResetTimer()
	book := orderbook.New()
	for i := 0; i < b.N; i++ {
		e := &evs[i&(len(evs)-1)]
		e.Offset = uint64(i + 1)
		book.Apply(e)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/s")
}

// BenchmarkReplayFromStore reads verified records from Pebble (CRC checked),
// decodes them and applies them to a book: offline replay.
func BenchmarkReplayFromStore(b *testing.B) {
	_, src := logStore(b, 100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := New()
		if _, err := e.Run(context.Background(), src, 0, 5000); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)*100_000/b.Elapsed().Seconds(), "events/s")
}
