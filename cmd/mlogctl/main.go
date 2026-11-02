// Command mlogctl inspects and drives a market event log cluster.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"
)

type command struct {
	summary string
	run     func(ctx context.Context, env *env, args []string) error
	// long commands run until done or interrupted unless --timeout is given.
	long bool
}

var commands = map[string]command{
	"status":     {summary: "show every node's role, term, commit/applied index and lag", run: cmdStatus},
	"leader":     {summary: "print the current leader's id (for scripts)", run: cmdLeader},
	"append":     {summary: "append events: append --producer ID --seq N [--type T] PAYLOAD...", run: cmdAppend},
	"read":       {summary: "read events: read [--from N] [--limit N] [--linearizable]", run: cmdRead},
	"producer":   {summary: "show a producer's last committed sequence: producer ID", run: cmdProducer},
	"checkpoint": {summary: "get or set a consumer checkpoint: checkpoint NAME [OFFSET]", run: cmdCheckpoint},
	"replay":     {summary: "rebuild the order book from the log and print its state hash", run: cmdReplay, long: true},
	"consume":    {summary: "follow the log into an order book, checkpointing locally and in the cluster", run: cmdConsume, long: true},
	"verify":     {summary: "checksum every node's log over a common prefix and compare", run: cmdVerify},
	"fault":      {summary: "inject network faults: fault show | isolate ID | set ID [flags] | heal", run: cmdFault},
	"load":       {summary: "append synthetic order-book events and report throughput and latency", run: cmdLoad, long: true},
}

func main() {
	global := flag.NewFlagSet("mlogctl", flag.ExitOnError)
	endpoints := global.String("endpoints", envOr("MLOG_ENDPOINTS", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"),
		"comma-separated node addresses (env MLOG_ENDPOINTS)")
	timeout := global.Duration("timeout", 0, "overall timeout (default 10s; none for replay, consume and load)")
	global.Usage = usage(global)
	global.Parse(os.Args[1:])
	if global.NArg() == 0 {
		global.Usage()
		os.Exit(2)
	}
	cmd, ok := commands[global.Arg(0)]
	if !ok {
		fmt.Fprintf(os.Stderr, "mlogctl: unknown command %q\n", global.Arg(0))
		global.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *timeout == 0 && !cmd.long {
		*timeout = 10 * time.Second
	}
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}
	if err := cmd.run(ctx, newEnv(strings.Split(*endpoints, ",")), global.Args()[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "mlogctl:", err)
		os.Exit(1)
	}
}

func usage(fs *flag.FlagSet) func() {
	return func() {
		fmt.Fprintln(os.Stderr, "usage: mlogctl [--endpoints a,b,c] [--timeout d] COMMAND [args]\n\ncommands:")
		names := make([]string, 0, len(commands))
		for name := range commands {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(os.Stderr, "  %-11s %s\n", name, commands[name].summary)
		}
		fmt.Fprintln(os.Stderr, "\nflags:")
		fs.PrintDefaults()
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
