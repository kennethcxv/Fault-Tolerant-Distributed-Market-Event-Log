// Command mlog-publisher tails a spool file written by the market-data engine
// (integration/cpp/mlog_spool.h) into the event log as one producer.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/spool"
)

func main() {
	path := flag.String("spool", "", "spool file to publish (required)")
	producer := flag.Uint64("producer", 0, "producer id written by the engine (required)")
	endpoints := flag.String("endpoints", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "log nodes")
	batch := flag.Int("batch", 500, "records per append")
	metrics := flag.String("metrics-listen", "", "serve Prometheus metrics on this address (e.g. 127.0.0.1:9101)")
	flag.Parse()
	if *path == "" || *producer == 0 {
		fmt.Fprintln(os.Stderr, "usage: mlog-publisher --spool FILE --producer ID [--endpoints a,b,c] [--metrics-listen addr]")
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, nil))
	p := &spool.Publisher{Client: client.New(strings.Split(*endpoints, ",")...), Path: *path,
		ProducerID: *producer, Batch: *batch, Logger: log}

	if *metrics != "" {
		reg := prometheus.NewRegistry()
		gauge := func(name, help string, f func() float64) prometheus.Collector {
			return prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help}, f)
		}
		reg.MustRegister(
			gauge("mlog_publisher_read_seq", "Last spool sequence read.", func() float64 { return float64(p.ReadSeq.Load()) }),
			gauge("mlog_publisher_acked_seq", "Last sequence the log acknowledged.", func() float64 { return float64(p.AckedSeq.Load()) }),
			gauge("mlog_publisher_unacked_events", "Read from the spool but not yet acknowledged.", func() float64 {
				return float64(p.ReadSeq.Load() - min(p.AckedSeq.Load(), p.ReadSeq.Load()))
			}),
			gauge("mlog_publisher_spool_unread_bytes", "Spool bytes not read yet (the backlog on disk).", func() float64 { return float64(p.Unread.Load()) }),
			gauge("mlog_publisher_retrying", "1 while appends to the log are failing.", func() float64 {
				if p.Retrying.Load() {
					return 1
				}
				return 0
			}),
		)
		go func() {
			if err := http.ListenAndServe(*metrics, promhttp.HandlerFor(reg, promhttp.HandlerOpts{})); err != nil {
				log.Error("metrics server", "err", err)
			}
		}()
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	err := p.Run(ctx)
	if errors.Is(err, context.Canceled) {
		log.Info("stopped", "acked_seq", p.AckedSeq.Load())
		return
	}
	log.Error("publisher stopped", "err", err, "acked_seq", p.AckedSeq.Load())
	os.Exit(1)
}
