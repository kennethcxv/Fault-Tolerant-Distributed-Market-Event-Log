package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
)

// nodeEndpoints maps node ids to endpoints by asking each endpoint its id.
func (e *env) nodeEndpoints(ctx context.Context) map[uint64]string {
	out := map[uint64]string{}
	for _, ep := range e.c.Endpoints() {
		if st, err := e.c.Status(ctx, ep); err == nil {
			out[st.ID] = ep
		}
	}
	return out
}

func cmdFault(ctx context.Context, e *env, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: fault show | isolate ID | set ID [--drop-to ids] [--drop-from ids] [--drop-rate f] [--delay-ms n] | heal")
	}
	nodes := e.nodeEndpoints(ctx)
	switch args[0] {
	case "show":
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "NODE\tENDPOINT\tDROP_TO\tDROP_FROM\tDROP_RATE\tDELAY_MS")
		for id, ep := range nodes {
			f, err := e.c.Fault(ctx, ep)
			if err != nil {
				fmt.Fprintf(w, "%d\t%s\t%v\n", id, ep, err)
				continue
			}
			fmt.Fprintf(w, "%d\t%s\t%v\t%v\t%g\t%d\n", id, ep, f.DropTo, f.DropFrom, f.DropRate, f.DelayMS)
		}
		return w.Flush()
	case "heal":
		for id, ep := range nodes {
			if err := e.c.SetFault(ctx, ep, api.FaultConfig{}); err != nil {
				return fmt.Errorf("node %d: %w", id, err)
			}
		}
		fmt.Printf("cleared faults on %d nodes\n", len(nodes))
		return nil
	case "isolate":
		if len(args) != 2 {
			return fmt.Errorf("usage: fault isolate ID")
		}
		id, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil || nodes[id] == "" {
			return fmt.Errorf("unknown or unreachable node %q", args[1])
		}
		var others []uint64
		for other := range nodes {
			if other != id {
				others = append(others, other)
			}
		}
		// Dropping in both directions on the isolated node is enough; the
		// others are left untouched, as with a real cable pull.
		if err := e.c.SetFault(ctx, nodes[id], api.FaultConfig{DropTo: others, DropFrom: others}); err != nil {
			return err
		}
		fmt.Printf("node %d isolated from %v (heal with: mlogctl fault heal)\n", id, others)
		return nil
	case "set":
		fs := flag.NewFlagSet("fault set", flag.ExitOnError)
		dropTo := fs.String("drop-to", "", "comma-separated peer ids")
		dropFrom := fs.String("drop-from", "", "comma-separated peer ids")
		rate := fs.Float64("drop-rate", 0, "fraction of outbound batches to drop")
		delay := fs.Int("delay-ms", 0, "delay added to every outbound batch")
		if len(args) < 2 {
			return fmt.Errorf("usage: fault set ID [flags]")
		}
		fs.Parse(args[2:])
		id, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil || nodes[id] == "" {
			return fmt.Errorf("unknown or unreachable node %q", args[1])
		}
		cfg := api.FaultConfig{DropTo: ids(*dropTo), DropFrom: ids(*dropFrom), DropRate: *rate, DelayMS: *delay}
		if err := e.c.SetFault(ctx, nodes[id], cfg); err != nil {
			return err
		}
		return printJSON(cfg)
	}
	return fmt.Errorf("unknown fault command %q", args[0])
}

func ids(s string) []uint64 {
	var out []uint64
	for _, f := range strings.Split(s, ",") {
		if n, err := strconv.ParseUint(strings.TrimSpace(f), 10, 64); err == nil {
			out = append(out, n)
		}
	}
	return out
}

// cmdLeader prints the leader id agreed by the reachable nodes, or fails.
func cmdLeader(ctx context.Context, e *env, args []string) error {
	for _, ep := range e.c.Endpoints() {
		if st, err := e.c.Status(ctx, ep); err == nil && st.Role == "leader" {
			fmt.Println(st.ID)
			return nil
		}
	}
	return fmt.Errorf("no node reports itself as leader")
}
