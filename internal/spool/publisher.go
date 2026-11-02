package spool

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
)

// Publisher tails a spool into the log as one producer. The spool position
// is the producer sequence, so after any restart it resumes from the
// producer's last committed sequence and a resent batch is deduplicated by
// the log (G4): spool to log is exactly once.
//
// During an outage (no quorum, log unreachable) it retries the same batch
// until it succeeds; the engine keeps writing to the spool, which absorbs
// the backlog. It never skips records.
type Publisher struct {
	Client     *client.Client
	Path       string
	ProducerID uint64
	Batch      int           // records per append (default 5000)
	Linger     time.Duration // wait this long to fill a batch (default 2ms)
	Poll       time.Duration // wait at the end of the spool (default 10ms)
	Logger     *slog.Logger

	ReadSeq  atomic.Uint64 // last sequence read from the spool
	AckedSeq atomic.Uint64 // last sequence the log confirmed
	Retrying atomic.Bool   // true while appends are failing
	Unread   atomic.Int64  // spool bytes not read yet
}

func (p *Publisher) Run(ctx context.Context) error {
	p.Batch = cmp.Or(p.Batch, 5000)
	p.Linger = cmp.Or(p.Linger, 2*time.Millisecond)
	p.Poll = cmp.Or(p.Poll, 10*time.Millisecond)
	if p.Logger == nil {
		p.Logger = slog.Default()
	}
	prod, err := p.Client.Producer(ctx, p.ProducerID)
	if err != nil {
		return fmt.Errorf("publisher: read producer %d position: %w", p.ProducerID, err)
	}
	p.AckedSeq.Store(prod.LastSeq)
	r, err := Open(p.Path)
	if err != nil {
		return err
	}
	defer r.Close()
	p.Logger.Info("publisher starting", "spool", p.Path, "producer", p.ProducerID, "resume_after", prod.LastSeq)

	var batch []event.Event
	lingerUntil := time.Time{}
	for {
		e, err := r.Next()
		switch {
		case err == nil:
			if e.ProducerID != p.ProducerID {
				return fmt.Errorf("%w: record %d is from producer %d, publisher is %d", ErrCorrupt, e.ProducerSeq, e.ProducerID, p.ProducerID)
			}
			p.ReadSeq.Store(e.ProducerSeq)
			if e.ProducerSeq <= prod.LastSeq {
				continue // already in the log
			}
			if len(batch) == 0 {
				lingerUntil = time.Now().Add(p.Linger)
			}
			batch = append(batch, e)
			if len(batch) < p.Batch {
				continue
			}
		case errors.Is(err, io.EOF):
			p.updateUnread(r)
			if len(batch) == 0 || time.Now().Before(lingerUntil) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(min(p.Poll, time.Until(lingerUntil)+time.Millisecond)):
				}
				continue
			}
		default:
			return err
		}
		if err := p.send(ctx, batch); err != nil {
			return err
		}
		batch = batch[:0]
		p.updateUnread(r)
	}
}

// updateUnread records how much of the spool has not been read yet: the
// backlog on disk. It is refreshed after every batch, not only at the end of
// the file, because the end is never reached while the publisher is behind.
func (p *Publisher) updateUnread(r *Reader) {
	if st, err := os.Stat(p.Path); err == nil {
		p.Unread.Store(st.Size() - r.Offset())
	}
}

// send appends one batch, retrying until it is acknowledged or ctx ends.
func (p *Publisher) send(ctx context.Context, batch []event.Event) error {
	req := api.AppendRequest{ProducerID: p.ProducerID, FirstSeq: batch[0].ProducerSeq}
	for _, e := range batch {
		req.Events = append(req.Events, fsm.EventIn{Type: e.Type, Schema: e.Schema, SourceTS: e.SourceTS, Payload: e.Payload})
	}
	backoff := 50 * time.Millisecond
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, 10*time.Second)
		res, err := p.Client.Append(actx, req)
		cancel()
		if err == nil {
			switch res.Status {
			case fsm.StatusAppended, fsm.StatusDuplicate:
				p.AckedSeq.Store(batch[len(batch)-1].ProducerSeq)
				if p.Retrying.Swap(false) {
					p.Logger.Info("publisher recovered", "attempts", attempt, "acked_seq", p.AckedSeq.Load())
				}
				return nil
			case fsm.StatusSequenceGap:
				// The log expects an earlier sequence than the spool holds:
				// records the log never got are gone from the spool.
				return fmt.Errorf("publisher: log expects sequence %d but spool continues at %d; spool was replaced or truncated", res.ExpectedSeq, req.FirstSeq)
			}
			return fmt.Errorf("publisher: append rejected: %+v", res)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var apiErr *api.Error
		if errors.As(err, &apiErr) && apiErr.Code == api.CodeInvalid {
			return fmt.Errorf("publisher: spool record rejected by the log: %w", err)
		}
		if !p.Retrying.Swap(true) || attempt%20 == 0 {
			p.Logger.Warn("append failing; holding position and retrying (the spool keeps the backlog)",
				"first_seq", req.FirstSeq, "events", len(batch), "attempt", attempt, "err", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, 2*time.Second)
	}
}
