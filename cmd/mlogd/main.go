// Command mlogd runs one node of the market event log.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
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
		timeout    = flag.Duration("request-timeout", 5*time.Second, "how long a write waits to be applied")
		logLevel   = flag.String("log-level", "info", "debug | info | warn | error")
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

	n, err := server.Start(server.Config{
		ID: *id, Listen: *listen, DataDir: *data, Fsync: *fsync == "always",
		Standalone: *standalone, RequestTimeout: *timeout, Logger: log,
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

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "mlogd:", err)
	os.Exit(2)
}
