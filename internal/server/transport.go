package server

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protodelim"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
)

const (
	peerQueue     = 4096
	maxPeerBatch  = 256
	clusterHeader = "X-Mlog-Cluster"
)

// clusterID fingerprints the peer list. Nodes started with different peer
// lists would bootstrap different memberships; rejecting their messages
// turns that misconfiguration into a loud error instead of divergence.
func clusterID(peers map[uint64]string) string {
	ids := make([]uint64, 0, len(peers))
	for id := range peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	h := sha256.New()
	for _, id := range ids {
		fmt.Fprintf(h, "%d=%s;", id, peers[id])
	}
	return hex.EncodeToString(h.Sum(nil)[:8])
}

// transport sends Raft messages to peers over HTTP. Each peer has one
// goroutine and a bounded queue: messages to a peer stay in order, and a slow
// or dead peer never blocks the Raft loop. Raft tolerates loss, so a full
// queue or failed request drops messages and reports the peer unreachable.
//
// Messages are marshalled on the peer goroutines while the Raft loop may be
// persisting the same entries. That is safe because nothing mutates an entry
// after raft creates it, and protobuf-go allows concurrent marshalling of an
// unmodified message (the race detector runs over the cluster tests).
type transport struct {
	id          uint64
	cluster     string
	hc          *http.Client
	snapClient  *http.Client // no overall timeout: snapshots can be large
	log         *slog.Logger
	peers       map[uint64]*peerSender
	unreachable func(id uint64)
	faults      *faults
}

type peerSender struct {
	id  uint64
	url string
	ch  chan *raftpb.Message
}

func newTransport(id uint64, peers map[uint64]string, log *slog.Logger, f *faults, unreachable func(uint64)) *transport {
	t := &transport{
		id: id, cluster: clusterID(peers), log: log, faults: f, unreachable: unreachable,
		hc:         &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 4}},
		snapClient: &http.Client{},
		peers:      map[uint64]*peerSender{},
	}
	for pid, url := range peers {
		if pid != id {
			t.peers[pid] = &peerSender{id: pid, url: url, ch: make(chan *raftpb.Message, peerQueue)}
		}
	}
	return t
}

func (t *transport) start(ctx context.Context) {
	for _, p := range t.peers {
		go t.run(ctx, p)
	}
}

// send queues messages without blocking.
func (t *transport) send(msgs []*raftpb.Message) {
	for _, m := range msgs {
		p := t.peers[m.GetTo()]
		if p == nil {
			continue
		}
		select {
		case p.ch <- m:
		default:
			t.unreachable(p.id)
		}
	}
}

func (t *transport) run(ctx context.Context, p *peerSender) {
	var buf bytes.Buffer
	for {
		var first *raftpb.Message
		select {
		case <-ctx.Done():
			return
		case first = <-p.ch:
		}
		batch := []*raftpb.Message{first}
	drain:
		for len(batch) < maxPeerBatch {
			select {
			case m := <-p.ch:
				batch = append(batch, m)
			default:
				break drain
			}
		}
		if delay := t.faults.outbound(p.id); delay < 0 {
			continue // dropped by fault injection
		} else if delay > 0 {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
		}
		buf.Reset()
		for _, m := range batch {
			if _, err := protodelim.MarshalTo(&buf, m); err != nil {
				t.log.Error("encode raft message", "err", err)
			}
		}
		if err := t.post(ctx, p, buf.Bytes()); err != nil {
			t.log.Debug("send to peer failed", "peer", p.id, "messages", len(batch), "err", err)
			t.unreachable(p.id)
		}
	}
}

func (t *transport) post(ctx context.Context, p *peerSender, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url+"/raft/message", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set(clusterHeader, t.cluster)
	req.Header.Set("X-Mlog-From", fmt.Sprint(t.id))
	resp, err := t.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("peer %d answered HTTP %d", p.id, resp.StatusCode)
	}
	return nil
}

// sendSnapshot streams a prepared MsgSnap and the state machine to its peer
// and reports the outcome; raft keeps the follower in snapshot state until
// it hears back. The stream is one request: the delimited message, then the
// state machine stream (storage.StateSnapshot.WriteTo).
func (t *transport) sendSnapshot(ctx context.Context, m *raftpb.Message, ss interface {
	io.WriterTo
	Close() error
}, done func(ok bool)) {
	defer ss.Close()
	p := t.peers[m.GetTo()]
	if p == nil || t.faults.outbound(p.id) < 0 {
		done(false)
		return
	}
	pr, pw := io.Pipe()
	go func() {
		_, err := protodelim.MarshalTo(pw, m)
		if err == nil {
			_, err = ss.WriteTo(pw)
		}
		pw.CloseWithError(err)
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url+"/raft/snapshot", pr)
	if err != nil {
		pr.CloseWithError(err)
		done(false)
		return
	}
	req.Header.Set(clusterHeader, t.cluster)
	req.Header.Set("X-Mlog-From", fmt.Sprint(t.id))
	start := time.Now()
	resp, err := t.snapClient.Do(req)
	if err == nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			err = fmt.Errorf("peer answered HTTP %d", resp.StatusCode)
		}
	}
	pr.CloseWithError(err)
	if err != nil {
		t.log.Warn("snapshot send failed", "peer", p.id, "index", m.GetSnapshot().GetMetadata().GetIndex(), "err", err)
		done(false)
		return
	}
	t.log.Info("snapshot sent", "peer", p.id, "index", m.GetSnapshot().GetMetadata().GetIndex(), "duration", time.Since(start))
	done(true)
}

// decodeMessages parses a peer request body.
func decodeMessages(r io.Reader) ([]*raftpb.Message, error) {
	br := bufio.NewReader(r)
	var out []*raftpb.Message
	for {
		m := &raftpb.Message{}
		if err := protodelim.UnmarshalFrom(br, m); err == io.EOF {
			return out, nil
		} else if err != nil {
			return out, err
		}
		out = append(out, m)
	}
}

// faults is the fault-injection state behind /debug/fault: per-peer drops in
// either direction, random loss and added delay. It is how the multiprocess
// demos create partitions without root access to the network.
type faults struct {
	mu   sync.Mutex
	cfg  api.FaultConfig
	rand *rand.Rand
}

func newFaults() *faults { return &faults{rand: rand.New(rand.NewPCG(rand.Uint64(), 0))} }

func (f *faults) set(cfg api.FaultConfig) {
	f.mu.Lock()
	f.cfg = cfg
	f.mu.Unlock()
}

func (f *faults) get() api.FaultConfig {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cfg
}

// outbound returns -1 to drop a batch to peer, otherwise the delay to add.
func (f *faults) outbound(peer uint64) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	if slices.Contains(f.cfg.DropTo, peer) || (f.cfg.DropRate > 0 && f.rand.Float64() < f.cfg.DropRate) {
		return -1
	}
	return time.Duration(f.cfg.DelayMS) * time.Millisecond
}

func (f *faults) dropInbound(peer uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Contains(f.cfg.DropFrom, peer)
}
