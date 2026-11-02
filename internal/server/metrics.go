package server

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.etcd.io/raft/v3"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/replica"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

// metrics are per node, in their own registry, so several nodes can run in
// one process (tests) without colliding.
type metrics struct {
	reg *prometheus.Registry

	term          prometheus.Gauge
	isLeader      prometheus.Gauge
	commitIndex   prometheus.Gauge
	leaderChanges prometheus.Counter
	elections     prometheus.Counter
	proposals     prometheus.Counter
	results       *prometheus.CounterVec
	events        prometheus.Counter
	commitLatency prometheus.Histogram
	walSync       prometheus.Histogram
	persist       prometheus.Histogram
	apply         prometheus.Histogram
	peerLag       *prometheus.GaugeVec
	failed        prometheus.Gauge
	snapshotsSent *prometheus.CounterVec
	snapshots     *prometheus.CounterVec
	groupSize     prometheus.Histogram

	lastLeader uint64
}

var latencyBuckets = prometheus.ExponentialBuckets(50e-6, 2, 18) // 50µs .. ~6.5s

func newMetrics(f *fsm.FSM, st *storage.Store) *metrics {
	m := &metrics{reg: prometheus.NewRegistry()}
	g := func(name, help string) prometheus.Gauge {
		return prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
	}
	c := func(name, help string) prometheus.Counter {
		return prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
	}
	h := func(name, help string) prometheus.Histogram {
		return prometheus.NewHistogram(prometheus.HistogramOpts{Name: name, Help: help, Buckets: latencyBuckets})
	}
	m.term = g("mlog_raft_term", "Current Raft term.")
	m.isLeader = g("mlog_raft_is_leader", "1 if this node is the leader.")
	m.commitIndex = g("mlog_raft_commit_index", "Raft commit index known to this node.")
	m.leaderChanges = c("mlog_raft_leader_changes_total", "Times this node observed a different leader.")
	m.elections = c("mlog_raft_elections_total", "Elections (or pre-votes) started by this node.")
	m.proposals = c("mlog_proposals_total", "Commands this node proposed as leader.")
	m.results = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "mlog_apply_results_total",
		Help: "Applied commands by outcome (appended, duplicate, sequence_gap, ...)."}, []string{"status"})
	m.events = c("mlog_events_appended_total", "Events written to the log by this node's state machine.")
	m.commitLatency = h("mlog_commit_latency_seconds", "Leader: proposal to applied result, per command.")
	m.walSync = h("mlog_wal_sync_seconds", "Synced Raft persistence batch (write + fsync).")
	m.persist = h("mlog_raft_persist_seconds", "Every Raft persistence batch, synced or not.")
	m.apply = h("mlog_apply_seconds", "State machine apply per Ready.")
	m.peerLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "mlog_replication_lag_entries",
		Help: "Leader only: leader last index minus the peer's match index."}, []string{"peer"})
	m.failed = g("mlog_node_failed", "1 once the node stopped serving after a storage error or corruption.")
	m.groupSize = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "mlog_commit_group_size",
		Help: "Client commands sharing one persistence round (group commit).", Buckets: prometheus.ExponentialBuckets(1, 2, 11)})
	m.snapshotsSent = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "mlog_snapshots_sent_total",
		Help: "Snapshot streams sent to followers, by success."}, []string{"ok"})
	m.snapshots = prometheus.NewCounterVec(prometheus.CounterOpts{Name: "mlog_snapshots_total",
		Help: "Snapshots taken locally (created) or received from the leader (installed)."}, []string{"kind"})
	m.reg.MustRegister(m.term, m.isLeader, m.commitIndex, m.leaderChanges, m.elections, m.proposals, m.results,
		m.events, m.commitLatency, m.walSync, m.persist, m.apply, m.peerLag, m.failed, m.snapshotsSent, m.snapshots, m.groupSize,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "mlog_applied_index", Help: "State machine applied index."},
			func() float64 { return float64(f.Applied()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "mlog_last_offset", Help: "Last event offset in the local log."},
			func() float64 { return float64(f.LastOffset()) }),
		consumerBacklog{f: f, st: st},
		collectors.NewGoCollector(),
	)
	return m
}

func (m *metrics) observeReady(out *replica.Output) {
	m.persist.Observe(out.PersistTime.Seconds())
	if out.Synced {
		m.walSync.Observe(out.PersistTime.Seconds())
	}
	if len(out.Results) > 0 {
		m.apply.Observe(out.ApplyTime.Seconds())
	}
	if out.Commit > 0 {
		m.commitIndex.Set(float64(out.Commit))
	}
	if out.SnapshotCreated > 0 {
		m.snapshots.WithLabelValues("created").Inc()
	}
	if out.SnapshotInstalled > 0 {
		m.snapshots.WithLabelValues("installed").Inc()
	}
}

func (m *metrics) observeResult(r fsm.Result) {
	m.results.WithLabelValues(string(r.Status)).Inc()
	if r.Status == fsm.StatusAppended {
		m.events.Add(float64(r.LastOffset - r.FirstOffset + 1))
	}
}

func (m *metrics) onRoleChange(s raft.SoftState) {
	if s.Lead != m.lastLeader && s.Lead != 0 {
		m.leaderChanges.Inc()
		m.lastLeader = s.Lead
	}
	if s.RaftState == raft.StateCandidate || s.RaftState == raft.StatePreCandidate {
		m.elections.Inc()
	}
	leader := 0.0
	if s.RaftState == raft.StateLeader {
		leader = 1
	} else {
		m.peerLag.Reset()
	}
	m.isLeader.Set(leader)
}

func (m *metrics) observeProgress(rep *replica.Replica) {
	st := rep.Status()
	_, last := rep.LogBounds()
	m.term.Set(float64(st.GetTerm()))
	for id, pr := range st.Progress {
		m.peerLag.WithLabelValues(strconv.FormatUint(id, 10)).Set(float64(last - min(pr.Match, last)))
	}
}

// consumerBacklog reports each consumer's distance from the end of the log,
// read from storage at scrape time.
type consumerBacklog struct {
	f  *fsm.FSM
	st *storage.Store
}

var backlogDesc = prometheus.NewDesc("mlog_consumer_backlog_events",
	"Last offset minus the consumer's committed checkpoint.", []string{"consumer"}, nil)

func (c consumerBacklog) Describe(ch chan<- *prometheus.Desc) { ch <- backlogDesc }

func (c consumerBacklog) Collect(ch chan<- prometheus.Metric) {
	consumers, err := c.st.LoadConsumers()
	if err != nil {
		return
	}
	last := c.f.LastOffset()
	for name, off := range consumers {
		ch <- prometheus.MustNewConstMetric(backlogDesc, prometheus.GaugeValue, float64(last-min(off, last)), name)
	}
}
