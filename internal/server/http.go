package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/event"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

const (
	maxReadEvents = 10_000
	maxReadBytes  = 4 << 20
	// JSON base64 inflates payloads by 4/3; leave room for field names too.
	maxRequestBody = 2 * fsm.MaxBatchBytes
)

func (n *Node) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/append", n.guard(n.handleAppend))
	mux.HandleFunc("GET /v1/events", n.guard(n.handleRead))
	mux.HandleFunc("GET /v1/producers/{id}", n.guard(n.handleProducer))
	mux.HandleFunc("GET /v1/consumers/{name}", n.guard(n.handleConsumer))
	mux.HandleFunc("POST /v1/consumers/{name}/checkpoint", n.guard(n.handleCheckpoint))
	mux.HandleFunc("GET /v1/status", n.handleStatus)
	mux.Handle("GET /metrics", promhttp.HandlerFor(n.m.reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /debug/fault", n.handleGetFault)
	mux.HandleFunc("PUT /debug/fault", n.handleSetFault)
	if n.raft != nil {
		mux.HandleFunc("POST /raft/message", n.handleRaftMessage)
	}
	return mux
}

// handleRaftMessage accepts a batch of Raft messages from a peer.
func (n *Node) handleRaftMessage(w http.ResponseWriter, r *http.Request) {
	if got := r.Header.Get(clusterHeader); got != n.raft.tr.cluster {
		n.log.Error("rejecting peer message from a node with a different --peers list",
			"remote", r.RemoteAddr, "their_cluster", got, "our_cluster", n.raft.tr.cluster)
		http.Error(w, "cluster id mismatch: nodes were started with different --peers", http.StatusConflict)
		return
	}
	from, _ := strconv.ParseUint(r.Header.Get("X-Mlog-From"), 10, 64)
	msgs, err := decodeMessages(http.MaxBytesReader(w, r.Body, 64<<20))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if n.flts.dropInbound(from) {
		w.WriteHeader(http.StatusNoContent) // look delivered, as a lossy network would
		return
	}
	select {
	case n.raft.inbound <- msgs:
		w.WriteHeader(http.StatusNoContent)
	case <-n.raft.stopped:
		http.Error(w, "node stopped", http.StatusServiceUnavailable)
	case <-r.Context().Done():
	}
}

func (n *Node) handleGetFault(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, n.flts.get())
}

// handleSetFault replaces this node's injected faults; an empty body clears them.
func (n *Node) handleSetFault(w http.ResponseWriter, r *http.Request) {
	var cfg FaultConfig
	if r.ContentLength != 0 && !decodeBody(w, r, &cfg) {
		return
	}
	n.flts.set(cfg)
	n.log.Warn("fault injection updated", "faults", cfg)
	writeJSON(w, http.StatusOK, cfg)
}

// guard rejects requests once the node has failed.
func (n *Node) guard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := n.Err(); err != nil {
			writeError(w, &api.Error{Code: api.CodeNodeFailed, Message: err.Error()})
			return
		}
		h(w, r)
	}
}

func (n *Node) handleAppend(w http.ResponseWriter, r *http.Request) {
	var req api.AppendRequest
	if !decodeBody(w, r, &req) {
		return
	}
	cmd := fsm.Command{Append: &fsm.Append{
		ProducerID: req.ProducerID, FirstSeq: req.FirstSeq, Events: req.Events,
		IngestTS: time.Now().UnixNano(),
	}}
	n.proposeAndRespond(w, r, &cmd)
}

func (n *Node) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	var req api.CheckpointRequest
	if !decodeBody(w, r, &req) {
		return
	}
	cmd := fsm.Command{Checkpoint: &fsm.Checkpoint{Consumer: r.PathValue("name"), Offset: req.Offset}}
	n.proposeAndRespond(w, r, &cmd)
}

func (n *Node) proposeAndRespond(w http.ResponseWriter, r *http.Request, cmd *fsm.Command) {
	if err := cmd.Validate(); err != nil {
		writeError(w, &api.Error{Code: api.CodeInvalid, Message: err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), n.cfg.RequestTimeout)
	defer cancel()
	res, err := n.eng.propose(ctx, cmd)
	if err != nil {
		writeError(w, err)
		return
	}
	code := http.StatusOK
	switch res.Status {
	case fsm.StatusSequenceGap, fsm.StatusCheckpointBeyondEnd:
		code = http.StatusConflict
	case fsm.StatusInvalid:
		code = http.StatusBadRequest
	}
	writeJSON(w, code, res)
}

// handleRead serves committed records from the local replica. format=raw
// returns the stored records back to back, checksums included, so clients can
// verify integrity end to end.
func (n *Node) handleRead(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	from, err1 := parseUint(q.Get("from"), 1)
	limit, err2 := parseUint(q.Get("limit"), 1000)
	if err := errors.Join(err1, err2); err != nil {
		writeError(w, &api.Error{Code: api.CodeInvalid, Message: err.Error()})
		return
	}
	if q.Get("linearizable") == "true" && !n.barrier(w, r) {
		return
	}
	upTo := n.fsm.LastOffset()
	raw, count, err := n.store.ReadRaw(from, upTo, int(min(limit, maxReadEvents)), maxReadBytes)
	if err != nil {
		n.storageError(w, err)
		return
	}
	if q.Get("format") == "raw" {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Last-Offset", strconv.FormatUint(upTo, 10))
		w.Header().Set("X-Count", strconv.Itoa(count))
		w.Write(raw)
		return
	}
	events, err := event.DecodeAll(raw)
	if err != nil {
		n.storageError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, api.ReadResponse{Events: events, LastOffset: upTo})
}

func (n *Node) handleProducer(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseUint(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, &api.Error{Code: api.CodeInvalid, Message: "producer id must be an unsigned integer"})
		return
	}
	if r.URL.Query().Get("linearizable") == "true" && !n.barrier(w, r) {
		return
	}
	p, err := n.store.GetProducer(id)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		n.storageError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"producer_id": id, "last_seq": p.LastSeq, "last_offset": p.LastOffset})
}

func (n *Node) handleConsumer(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if r.URL.Query().Get("linearizable") == "true" && !n.barrier(w, r) {
		return
	}
	last := n.fsm.LastOffset()
	off, err := n.store.GetConsumer(name)
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		n.storageError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, api.ConsumerResponse{Consumer: name, Offset: off, LastOffset: last, Backlog: last - min(off, last)})
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	st := n.eng.status()
	if err := n.Err(); err != nil {
		st.Failed = err.Error()
	}
	writeJSON(w, http.StatusOK, st)
}

func (n *Node) barrier(w http.ResponseWriter, r *http.Request) bool {
	ctx, cancel := context.WithTimeout(r.Context(), n.cfg.RequestTimeout)
	defer cancel()
	if err := n.eng.readBarrier(ctx); err != nil {
		writeError(w, err)
		return false
	}
	return true
}

// storageError fails the node on corruption: serving anything else from a
// store that returned bad data would risk handing out wrong records.
func (n *Node) storageError(w http.ResponseWriter, err error) {
	if errors.Is(err, storage.ErrCorrupt) || errors.Is(err, event.ErrCorrupt) {
		n.fail(err)
		writeError(w, &api.Error{Code: api.CodeCorrupt, Message: err.Error()})
		return
	}
	n.log.Error("storage read failed", "err", err)
	writeError(w, err)
}

func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody)).Decode(v); err != nil {
		writeError(w, &api.Error{Code: api.CodeInvalid, Message: "bad request body: " + err.Error()})
		return false
	}
	return true
}

func parseUint(s string, def uint64) (uint64, error) {
	if s == "" {
		return def, nil
	}
	return strconv.ParseUint(s, 10, 64)
}

var errorStatus = map[string]int{
	api.CodeNotLeader:      http.StatusMisdirectedRequest,
	api.CodeNoLeader:       http.StatusServiceUnavailable,
	api.CodeOverloaded:     http.StatusServiceUnavailable,
	api.CodeNodeFailed:     http.StatusServiceUnavailable,
	api.CodeOutcomeUnknown: http.StatusGatewayTimeout,
	api.CodeTimeout:        http.StatusGatewayTimeout,
	api.CodeInvalid:        http.StatusBadRequest,
	api.CodeNotFound:       http.StatusNotFound,
	api.CodeCorrupt:        http.StatusInternalServerError,
}

func writeError(w http.ResponseWriter, err error) {
	var apiErr *api.Error
	if !errors.As(err, &apiErr) {
		apiErr = &api.Error{Code: "internal", Message: err.Error()}
	}
	code, ok := errorStatus[apiErr.Code]
	if !ok {
		code = http.StatusInternalServerError
	}
	writeJSON(w, code, apiErr)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
