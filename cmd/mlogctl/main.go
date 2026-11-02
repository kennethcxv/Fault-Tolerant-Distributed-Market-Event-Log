// Command mlogctl inspects and drives a market event log cluster.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

type command struct {
	summary string
	run     func(ctx context.Context, env *env, args []string) error
}

var commands = map[string]command{
	"status":     {"show every node's role, term, commit/applied index and lag", cmdStatus},
	"append":     {"append events: append --producer ID --seq N [--type T] PAYLOAD...", cmdAppend},
	"read":       {"read events: read [--from N] [--limit N] [--linearizable]", cmdRead},
	"producer":   {"show a producer's last committed sequence: producer ID", cmdProducer},
	"checkpoint": {"get or set a consumer checkpoint: checkpoint NAME [OFFSET]", cmdCheckpoint},
	"replay":     {"rebuild the order book from the log and print its state hash", cmdReplay},
}

func main() {
	global := flag.NewFlagSet("mlogctl", flag.ExitOnError)
	endpoints := global.String("endpoints", envOr("MLOG_ENDPOINTS", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003"),
		"comma-separated node addresses (env MLOG_ENDPOINTS)")
	timeout := global.Duration("timeout", 10*time.Second, "overall timeout for the command")
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
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
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
