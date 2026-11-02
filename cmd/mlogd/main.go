// Command mlogd runs one node of the market event log.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/server"
)

func main() {
	var (
		id         = flag.Uint64("id", 1, "node id (must match its entry in --peers)")
		listen     = flag.String("listen", "127.0.0.1:7001", "address for the client API and peer traffic")
		data       = flag.String("data", "", "data directory (required)")
		fsync      = flag.String("fsync", "always", "always | never (never voids durability; for measurement only)")
		standalone = flag.Bool("standalone", false, "run a single node without Raft")
		peers      = flag.String("peers", "", "every voter as id=url, comma-separated, identical on all nodes "+
			"(e.g. 1=http://127.0.0.1:7001,2=http://127.0.0.1:7002,3=http://127.0.0.1:7003)")
		tick     = flag.Duration("tick", 100*time.Millisecond, "raft tick; election timeout is 10-20 ticks")
		join     = flag.Bool("join", false, "join an existing cluster with an empty data directory (after mlogctl member add)")
		cluster  = flag.String("cluster-id", "", "cluster id to join (printed by mlogctl member add/list)")
		snapN    = flag.Uint64("snapshot-entries", 10_000, "snapshot and compact the raft log every N applied entries (0 = never)")
		keep     = flag.Uint64("snapshot-keep", 5_000, "raft entries kept behind each snapshot for lagging followers")
		timeout  = flag.Duration("request-timeout", 5*time.Second, "how long a write waits to be applied")
		logLevel = flag.String("log-level", "info", "debug | info | warn | error")
	)
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		fatal(err)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if *data == "" {
		fatal(fmt.Errorf("--data is required"))
	}
	if *fsync != "always" && *fsync != "never" {
		fatal(fmt.Errorf("--fsync must be always or never"))
	}

	peerMap, err := parsePeers(*peers)
	if err != nil {
		fatal(err)
	}
	if !*standalone && len(peerMap) == 0 {
		fatal(fmt.Errorf("--peers is required unless --standalone is set"))
	}
	n, err := server.Start(server.Config{
		ID: *id, Listen: *listen, DataDir: *data, Fsync: *fsync == "always", Standalone: *standalone,
		Peers: peerMap, TickInterval: *tick, RequestTimeout: *timeout, Logger: log,
		SnapshotEntries: *snapN, CompactKeep: *keep, Join: *join, ClusterID: *cluster,
	})
	if err != nil {
		log.Error("start failed", "err", err)
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	code := 0
	select {
	case s := <-sig:
		log.Info("shutting down", "signal", s.String())
	case <-n.Failed():
		// The node has stopped serving; exit so a supervisor notices.
		code = 1
	}
	if err := n.Stop(); err != nil {
		log.Error("stop", "err", err)
	}
	os.Exit(code)
}

func parsePeers(s string) (map[uint64]string, error) {
	out := map[uint64]string{}
	if s == "" {
		return out, nil
	}
	for _, part := range strings.Split(s, ",") {
		id, url, ok := strings.Cut(strings.TrimSpace(part), "=")
		n, err := strconv.ParseUint(id, 10, 64)
		if !ok || err != nil || n == 0 || !strings.HasPrefix(url, "http") {
			return nil, fmt.Errorf("bad --peers entry %q, want id=http://host:port", part)
		}
		if _, dup := out[n]; dup {
			return nil, fmt.Errorf("--peers lists id %d twice", n)
		}
		out[n] = strings.TrimRight(url, "/")
	}
	return out, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "mlogd:", err)
	os.Exit(2)
}
