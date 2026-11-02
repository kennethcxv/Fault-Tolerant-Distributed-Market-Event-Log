// Package event defines the versioned envelope every stored market event uses.
//
// Layout (format v1, little-endian, 48-byte header):
//
//	0  u8   format version
//	1  u8   schema version (payload schema, producer-defined)
//	2  u16  event type
//	4  u32  payload length
//	8  u64  offset (0 until assigned by the log)
//	16 u64  producer id
//	24 u64  producer sequence
//	32 i64  source timestamp, unix ns
//	40 i64  ingest timestamp, unix ns
//	48 ...  payload
//	   u32  CRC32C over every preceding byte
//
// The same bytes are used on disk, in raw API reads, in snapshots and in the
// Project 1 spool file, so a record is checksummed once and verified everywhere.
package event

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	FormatV1    = 1
	HeaderSize  = 48
	TrailerSize = 4
	MaxPayload  = 1 << 20

	// Types at or above TypeSystemMin are reserved for the log itself.
	TypeSystemMin uint16 = 0xFF00
	// TypeGap marks events a producer knows it lost before they reached the log.
	TypeGap uint16 = 0xFFFF
)

var (
	ErrTruncated = errors.New("event: truncated record")
	ErrCorrupt   = errors.New("event: corrupt record")
	ErrFormat    = errors.New("event: unsupported format version")
	ErrInvalid   = errors.New("event: invalid field")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type Event struct {
	Schema      uint8  `json:"schema"`
	Type        uint16 `json:"type"`
	Offset      uint64 `json:"offset"`
	ProducerID  uint64 `json:"producer_id"`
	ProducerSeq uint64 `json:"producer_seq"`
	SourceTS    int64  `json:"source_ts"`
	IngestTS    int64  `json:"ingest_ts"`
	Payload     []byte `json:"payload"`
}

// Validate checks the fields a producer controls.
func (e *Event) Validate() error {
	switch {
	case e.Type == 0:
		return fmt.Errorf("%w: event type 0", ErrInvalid)
	case e.Schema == 0:
		return fmt.Errorf("%w: schema version 0", ErrInvalid)
	case len(e.Payload) > MaxPayload:
		return fmt.Errorf("%w: payload %d bytes exceeds %d", ErrInvalid, len(e.Payload), MaxPayload)
	}
	return nil
}

func (e *Event) EncodedLen() int { return HeaderSize + len(e.Payload) + TrailerSize }

// AppendTo appends the encoded record, checksum included, to dst.
func (e *Event) AppendTo(dst []byte) []byte {
	start := len(dst)
	dst = append(dst, FormatV1, e.Schema)
	dst = binary.LittleEndian.AppendUint16(dst, e.Type)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(e.Payload)))
	dst = binary.LittleEndian.AppendUint64(dst, e.Offset)
	dst = binary.LittleEndian.AppendUint64(dst, e.ProducerID)
	dst = binary.LittleEndian.AppendUint64(dst, e.ProducerSeq)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.SourceTS))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.IngestTS))
	dst = append(dst, e.Payload...)
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], castagnoli))
}

// Decode parses one record from the start of buf and returns its encoded
// length. Payload aliases buf. ErrTruncated means buf ends mid-record, which a
// reader tailing a file should treat as "not written yet", not corruption.
func Decode(buf []byte) (Event, int, error) {
	if len(buf) < HeaderSize {
		return Event{}, 0, ErrTruncated
	}
	if buf[0] != FormatV1 {
		return Event{}, 0, fmt.Errorf("%w: %d", ErrFormat, buf[0])
	}
	n := binary.LittleEndian.Uint32(buf[4:])
	if n > MaxPayload {
		// An impossible length is corruption, not a short read.
		return Event{}, 0, fmt.Errorf("%w: payload length %d", ErrCorrupt, n)
	}
	total := HeaderSize + int(n) + TrailerSize
	if len(buf) < total {
		return Event{}, 0, ErrTruncated
	}
	body := buf[:total-TrailerSize]
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(buf[total-TrailerSize:]) {
		return Event{}, 0, fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	return Event{
		Schema:      buf[1],
		Type:        binary.LittleEndian.Uint16(buf[2:]),
		Offset:      binary.LittleEndian.Uint64(buf[8:]),
		ProducerID:  binary.LittleEndian.Uint64(buf[16:]),
		ProducerSeq: binary.LittleEndian.Uint64(buf[24:]),
		SourceTS:    int64(binary.LittleEndian.Uint64(buf[32:])),
		IngestTS:    int64(binary.LittleEndian.Uint64(buf[40:])),
		Payload:     buf[HeaderSize : HeaderSize+int(n) : HeaderSize+int(n)],
	}, total, nil
}

// DecodeAll decodes back-to-back records, as returned by raw reads.
func DecodeAll(buf []byte) ([]Event, error) {
	var out []Event
	for len(buf) > 0 {
		e, n, err := Decode(buf)
		if err != nil {
			return out, err
		}
		out = append(out, e)
		buf = buf[n:]
	}
	return out, nil
}
