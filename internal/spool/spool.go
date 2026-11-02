// Package spool reads the append-only spool file that Project 1's engine
// writes (integration/cpp/mlog_spool.h) and publishes it into the log.
package spool

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
)

// ErrCorrupt means the spool holds a record that fails its checksum or
// breaks the sequence. Publishing stops: skipping it would hide data loss.
var ErrCorrupt = errors.New("spool: corrupt")

// Reader tails a spool file. Next returns io.EOF when no complete record is
// available yet; the writer may still be appending, so a partial record at
// the end is not an error.
type Reader struct {
	f       *os.File
	buf     []byte
	pos     int64 // file offset of buf[0]
	nextSeq uint64
}

func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return &Reader{f: f, nextSeq: 1}, nil
}

func (r *Reader) Close() error { return r.f.Close() }

// Offset is the file offset just past the last record returned.
func (r *Reader) Offset() int64 { return r.pos }

func (r *Reader) Next() (event.Event, error) {
	for {
		e, n, err := event.Decode(r.buf)
		switch {
		case err == nil:
			if e.ProducerSeq != r.nextSeq {
				return e, fmt.Errorf("%w: record at byte %d has sequence %d, expected %d", ErrCorrupt, r.pos, e.ProducerSeq, r.nextSeq)
			}
			r.buf, r.pos = r.buf[n:], r.pos+int64(n)
			r.nextSeq++
			e.Payload = append([]byte(nil), e.Payload...) // buf is reused
			return e, nil
		case !errors.Is(err, event.ErrTruncated):
			return e, fmt.Errorf("%w: record at byte %d: %v", ErrCorrupt, r.pos, err)
		}
		chunk := make([]byte, 1<<20)
		m, err := r.f.ReadAt(chunk, r.pos+int64(len(r.buf)))
		if m == 0 {
			if err == nil || errors.Is(err, io.EOF) {
				return event.Event{}, io.EOF
			}
			return event.Event{}, err
		}
		r.buf = append(r.buf, chunk[:m]...)
	}
}
