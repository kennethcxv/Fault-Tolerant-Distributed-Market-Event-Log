// Package orderbook is the deterministic consumer used for replay: a
// price-time priority limit order book fed by schema-v1 market events.
//
// Schema v1 is an ITCH-style order-level feed defined here because Project 1
// (the C++ market-data engine) has no wire format yet. docs/integration.md
// describes it for Project 1 to implement. All fields are little-endian.
//
//	AddOrder      (type 1): instrument u32, order u64, side u8 (0 bid, 1 ask), price i64 ticks, qty u64
//	CancelOrder   (type 2): instrument u32, order u64
//	ExecuteOrder  (type 3): instrument u32, order u64, qty u64
//	ReplaceOrder  (type 4): instrument u32, order u64, new order u64, price i64, qty u64
//
// Order ids are unique across instruments for the life of the log.
package orderbook

import (
	"encoding/binary"
	"fmt"
)

const SchemaV1 = 1

const (
	TypeAddOrder     uint16 = 1
	TypeCancelOrder  uint16 = 2
	TypeExecuteOrder uint16 = 3
	TypeReplaceOrder uint16 = 4
)

type Side uint8

const (
	Bid Side = 0
	Ask Side = 1
)

// Msg is one decoded schema-v1 message. Fields unused by a type are zero.
type Msg struct {
	Type       uint16
	Instrument uint32
	Order      uint64
	NewOrder   uint64
	Side       Side
	Price      int64
	Qty        uint64
}

var payloadSize = map[uint16]int{
	TypeAddOrder:     4 + 8 + 1 + 8 + 8,
	TypeCancelOrder:  4 + 8,
	TypeExecuteOrder: 4 + 8 + 8,
	TypeReplaceOrder: 4 + 8 + 8 + 8 + 8,
}

// Encode returns the payload for m.
func (m Msg) Encode() []byte {
	b := make([]byte, 0, payloadSize[m.Type])
	b = binary.LittleEndian.AppendUint32(b, m.Instrument)
	b = binary.LittleEndian.AppendUint64(b, m.Order)
	switch m.Type {
	case TypeAddOrder:
		b = append(b, byte(m.Side))
		b = binary.LittleEndian.AppendUint64(b, uint64(m.Price))
		b = binary.LittleEndian.AppendUint64(b, m.Qty)
	case TypeExecuteOrder:
		b = binary.LittleEndian.AppendUint64(b, m.Qty)
	case TypeReplaceOrder:
		b = binary.LittleEndian.AppendUint64(b, m.NewOrder)
		b = binary.LittleEndian.AppendUint64(b, uint64(m.Price))
		b = binary.LittleEndian.AppendUint64(b, m.Qty)
	}
	return b
}

// Decode parses a schema-v1 payload of the given event type.
func Decode(typ uint16, p []byte) (Msg, error) {
	want, ok := payloadSize[typ]
	if !ok {
		return Msg{}, fmt.Errorf("orderbook: unknown event type %d", typ)
	}
	if len(p) != want {
		return Msg{}, fmt.Errorf("orderbook: type %d payload is %d bytes, want %d", typ, len(p), want)
	}
	m := Msg{Type: typ, Instrument: binary.LittleEndian.Uint32(p), Order: binary.LittleEndian.Uint64(p[4:])}
	switch typ {
	case TypeAddOrder:
		m.Side = Side(p[12])
		m.Price = int64(binary.LittleEndian.Uint64(p[13:]))
		m.Qty = binary.LittleEndian.Uint64(p[21:])
	case TypeExecuteOrder:
		m.Qty = binary.LittleEndian.Uint64(p[12:])
	case TypeReplaceOrder:
		m.NewOrder = binary.LittleEndian.Uint64(p[12:])
		m.Price = int64(binary.LittleEndian.Uint64(p[20:]))
		m.Qty = binary.LittleEndian.Uint64(p[28:])
	}
	return m, nil
}
