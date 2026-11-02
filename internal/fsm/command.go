package fsm

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
)

const (
	MaxBatchEvents   = 10_000
	MaxBatchBytes    = 4 << 20
	MaxConsumerName  = 128
	kindAppend       = 1
	kindCheckpoint   = 2
	appendHeaderSize = 1 + 8 + 8 + 8 + 8 + 4
	eventHeaderSize  = 2 + 1 + 8 + 4
)

var errMalformed = errors.New("fsm: malformed command")

// Command is the payload of one Raft entry. ReqID only routes the result back
// to the waiting request on the node that proposed it; it never affects state.
type Command struct {
	ReqID      uint64
	Append     *Append
	Checkpoint *Checkpoint
}

// Append is a batch from one producer covering sequences FirstSeq..FirstSeq+len(Events)-1.
type Append struct {
	ProducerID uint64
	FirstSeq   uint64
	// IngestTS is stamped by the leader at proposal time so that every replica
	// stores the same value (reading the clock during apply would diverge).
	IngestTS int64
	Events   []EventIn
}

type EventIn struct {
	Type     uint16 `json:"type"`
	Schema   uint8  `json:"schema"`
	SourceTS int64  `json:"source_ts"`
	Payload  []byte `json:"payload"`
}

type Checkpoint struct {
	Consumer string
	Offset   uint64
}

// Validate is run by the leader before proposing and again during apply, so a
// command that slipped through is rejected identically on every replica.
func (c *Command) Validate() error {
	switch {
	case c.Append != nil && c.Checkpoint == nil:
		a := c.Append
		if a.ProducerID == 0 || a.FirstSeq == 0 {
			return errors.New("producer_id and first_seq must be >= 1")
		}
		if len(a.Events) == 0 || len(a.Events) > MaxBatchEvents {
			return fmt.Errorf("batch must have 1..%d events", MaxBatchEvents)
		}
		size := 0
		for i := range a.Events {
			e := event.Event{Type: a.Events[i].Type, Schema: a.Events[i].Schema, Payload: a.Events[i].Payload}
			if err := e.Validate(); err != nil {
				return fmt.Errorf("event %d: %w", i, err)
			}
			if e.Type >= event.TypeSystemMin && e.Type != event.TypeGap {
				return fmt.Errorf("event %d: type %#x is reserved", i, e.Type)
			}
			size += e.EncodedLen()
		}
		if size > MaxBatchBytes {
			return fmt.Errorf("batch is %d bytes, limit %d", size, MaxBatchBytes)
		}
	case c.Checkpoint != nil && c.Append == nil:
		if n := len(c.Checkpoint.Consumer); n == 0 || n > MaxConsumerName {
			return fmt.Errorf("consumer name must be 1..%d bytes", MaxConsumerName)
		}
	default:
		return errors.New("command must be exactly one of append or checkpoint")
	}
	return nil
}

func (c *Command) Marshal() []byte {
	if c.Checkpoint != nil {
		b := []byte{kindCheckpoint}
		b = binary.LittleEndian.AppendUint64(b, c.ReqID)
		b = binary.LittleEndian.AppendUint64(b, c.Checkpoint.Offset)
		return append(b, c.Checkpoint.Consumer...)
	}
	a := c.Append
	size := appendHeaderSize
	for i := range a.Events {
		size += eventHeaderSize + len(a.Events[i].Payload)
	}
	b := make([]byte, 0, size)
	b = append(b, kindAppend)
	b = binary.LittleEndian.AppendUint64(b, c.ReqID)
	b = binary.LittleEndian.AppendUint64(b, a.ProducerID)
	b = binary.LittleEndian.AppendUint64(b, a.FirstSeq)
	b = binary.LittleEndian.AppendUint64(b, uint64(a.IngestTS))
	b = binary.LittleEndian.AppendUint32(b, uint32(len(a.Events)))
	for _, e := range a.Events {
		b = binary.LittleEndian.AppendUint16(b, e.Type)
		b = append(b, e.Schema)
		b = binary.LittleEndian.AppendUint64(b, uint64(e.SourceTS))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(e.Payload)))
		b = append(b, e.Payload...)
	}
	return b
}

// Unmarshal decodes a command. Payloads alias data.
func Unmarshal(data []byte) (Command, error) {
	if len(data) == 0 {
		return Command{}, errMalformed
	}
	switch data[0] {
	case kindCheckpoint:
		if len(data) < 17 {
			return Command{}, errMalformed
		}
		return Command{
			ReqID: binary.LittleEndian.Uint64(data[1:]),
			Checkpoint: &Checkpoint{
				Offset:   binary.LittleEndian.Uint64(data[9:]),
				Consumer: string(data[17:]),
			},
		}, nil
	case kindAppend:
		if len(data) < appendHeaderSize {
			return Command{}, errMalformed
		}
		c := Command{ReqID: binary.LittleEndian.Uint64(data[1:]), Append: &Append{
			ProducerID: binary.LittleEndian.Uint64(data[9:]),
			FirstSeq:   binary.LittleEndian.Uint64(data[17:]),
			IngestTS:   int64(binary.LittleEndian.Uint64(data[25:])),
		}}
		n := binary.LittleEndian.Uint32(data[33:])
		if n > MaxBatchEvents {
			return Command{}, errMalformed
		}
		rest := data[appendHeaderSize:]
		c.Append.Events = make([]EventIn, 0, n)
		for range n {
			if len(rest) < eventHeaderSize {
				return Command{}, errMalformed
			}
			plen := binary.LittleEndian.Uint32(rest[11:])
			if uint64(len(rest)-eventHeaderSize) < uint64(plen) {
				return Command{}, errMalformed
			}
			end := eventHeaderSize + int(plen)
			c.Append.Events = append(c.Append.Events, EventIn{
				Type:     binary.LittleEndian.Uint16(rest),
				Schema:   rest[2],
				SourceTS: int64(binary.LittleEndian.Uint64(rest[3:])),
				Payload:  rest[eventHeaderSize:end:end],
			})
			rest = rest[end:]
		}
		if len(rest) != 0 {
			return Command{}, errMalformed
		}
		return c, nil
	}
	return Command{}, errMalformed
}
