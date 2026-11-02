package main

import (
	"context"
	"flag"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/orderbook"
)

// cmdLoad runs closed-loop producers: each sends one batch, waits for the
// result, and sends the next. Events are schema-v1 order-book messages from
// the deterministic generator, seeded by producer id, so a rerun resumes the
// same feed from the producer's last committed sequence.
func cmdLoad(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("load", flag.ExitOnError)
	producers := fs.Int("producers", 4, "concurrent producers")
	base := fs.Uint64("producer-base", 1, "id of the first producer")
	events := fs.Int("events", 10_000, "events per producer")
	batch := fs.Int("batch", 100, "events per append request")
	fs.Parse(args)

	var (
		mu         sync.Mutex
		latencies  []time.Duration
		appended   int
		duplicates int
		firstErr   error
	)
	start := time.Now()
	var wg sync.WaitGroup
	for p := 0; p < *producers; p++ {
		id := *base + uint64(p)
		wg.Go(func() {
			prod, err := e.c.Producer(ctx, id)
			if err != nil {
				mu.Lock()
				firstErr = cmpErr(firstErr, err)
				mu.Unlock()
				return
			}
			gen := orderbook.NewGenerator(id, 4)
			gen.StartIDsAt(id << 40)
			for range prod.LastSeq {
				gen.Next() // regenerate the part of the feed already committed
			}
			seq := prod.LastSeq + 1
			for sent := 0; sent < *events; {
				req := api.AppendRequest{ProducerID: id, FirstSeq: seq}
				for len(req.Events) < *batch && sent+len(req.Events) < *events {
					m := gen.Next()
					req.Events = append(req.Events, fsm.EventIn{Type: m.Type, Schema: orderbook.SchemaV1, SourceTS: time.Now().UnixNano(), Payload: m.Encode()})
				}
				t0 := time.Now()
				res, err := e.c.Append(ctx, req)
				lat := time.Since(t0)
				mu.Lock()
				if err != nil {
					firstErr = cmpErr(firstErr, fmt.Errorf("producer %d seq %d: %w", id, seq, err))
					mu.Unlock()
					return
				}
				latencies = append(latencies, lat)
				switch res.Status {
				case fsm.StatusAppended:
					appended += int(res.LastOffset - res.FirstOffset + 1)
				case fsm.StatusDuplicate:
					duplicates += len(req.Events)
				}
				mu.Unlock()
				seq += uint64(len(req.Events))
				sent += len(req.Events)
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)
	slices.Sort(latencies)
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[min(len(latencies)-1, int(p*float64(len(latencies))))]
	}
	fmt.Printf("producers=%d batch=%d events/producer=%d elapsed=%s\n", *producers, *batch, *events, elapsed.Round(time.Millisecond))
	fmt.Printf("appended=%d duplicates=%d requests=%d throughput=%.0f events/s\n", appended, duplicates, len(latencies), float64(appended)/elapsed.Seconds())
	fmt.Printf("append latency p50=%s p90=%s p99=%s p99.9=%s max=%s\n", pct(0.50), pct(0.90), pct(0.99), pct(0.999), pct(1))
	return firstErr
}

func cmpErr(old, err error) error {
	if old != nil {
		return old
	}
	return err
}
