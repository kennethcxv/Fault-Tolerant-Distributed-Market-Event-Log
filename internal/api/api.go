// Package api holds the JSON types shared by the HTTP server and client.
package api

import (
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
)

type AppendRequest struct {
	ProducerID uint64        `json:"producer_id"`
	FirstSeq   uint64        `json:"first_seq"`
	Events     []fsm.EventIn `json:"events"`
}

// AppendResponse and CheckpointResponse are the state machine's result.
type AppendResponse = fsm.Result

type CheckpointRequest struct {
	Offset uint64 `json:"offset"`
}

type ReadResponse struct {
	Events     []event.Event `json:"events"`
	LastOffset uint64        `json:"last_offset"`
}

type ConsumerResponse struct {
	Consumer   string `json:"consumer"`
	Offset     uint64 `json:"offset"`
	LastOffset uint64 `json:"last_offset"`
	Backlog    uint64 `json:"backlog"`
}

// Error codes. outcome_unknown is the only one where the request may still
// take effect; every other code means it was not applied.
const (
	CodeNotLeader      = "not_leader"
	CodeNoLeader       = "no_leader"
	CodeOverloaded     = "overloaded"
	CodeOutcomeUnknown = "outcome_unknown"
	CodeInvalid        = "invalid_request"
	CodeNotFound       = "not_found"
	CodeCorrupt        = "corrupt_data"
	CodeNodeFailed     = "node_failed"
	CodeTimeout        = "timeout"
)

type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	LeaderID  uint64 `json:"leader_id,omitempty"`
	LeaderURL string `json:"leader_url,omitempty"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

type PeerStatus struct {
	ID    uint64 `json:"id"`
	URL   string `json:"url"`
	Match uint64 `json:"match"`
	Next  uint64 `json:"next"`
	State string `json:"state"`
	// Lag is the leader's last index minus the peer's match index.
	Lag uint64 `json:"lag"`
}

type Status struct {
	ID         uint64       `json:"id"`
	Mode       string       `json:"mode"`
	Role       string       `json:"role"`
	Term       uint64       `json:"term"`
	Leader     uint64       `json:"leader"`
	LeaderURL  string       `json:"leader_url,omitempty"`
	Commit     uint64       `json:"commit"`
	Applied    uint64       `json:"applied"`
	LastOffset uint64       `json:"last_offset"`
	FirstIndex uint64       `json:"raft_first_index"`
	LastIndex  uint64       `json:"raft_last_index"`
	Snapshot   uint64       `json:"snapshot_index"`
	Fsync      string       `json:"fsync"`
	Peers      []PeerStatus `json:"peers,omitempty"`
	Failed     string       `json:"failed,omitempty"`
}

type VerifyResponse struct {
	To         uint64 `json:"to"`
	LastOffset uint64 `json:"last_offset"`
	SHA256     string `json:"sha256"`
}

// FaultConfig is a node's injected network faults (PUT /debug/fault). It is
// how the multi-process demos and tests create partitions without root access.
type FaultConfig struct {
	DropTo   []uint64 `json:"drop_to,omitempty"`   // drop everything sent to these peers
	DropFrom []uint64 `json:"drop_from,omitempty"` // drop everything received from these peers
	DropRate float64  `json:"drop_rate,omitempty"` // drop this fraction of outbound batches
	DelayMS  int      `json:"delay_ms,omitempty"`  // delay every outbound batch
}
