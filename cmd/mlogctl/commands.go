package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
)

type env struct {
	c *client.Client
}

func newEnv(endpoints []string) *env { return &env{c: client.New(endpoints...)} }

func cmdStatus(ctx context.Context, e *env, args []string) error {
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NODE\tID\tROLE\tTERM\tLEADER\tCOMMIT\tAPPLIED\tLAST_OFFSET\tRAFT_LOG\tSNAPSHOT\tFSYNC\tNOTE")
	var leader *api.Status
	for _, ep := range e.c.Endpoints() {
		st, err := e.c.Status(ctx, ep)
		if err != nil {
			fmt.Fprintf(w, "%s\t-\tunreachable\t\t\t\t\t\t\t\t\t%v\n", ep, err)
			continue
		}
		note := st.Failed
		if note == "" && st.Applied < st.Commit {
			note = fmt.Sprintf("applying, %d behind commit", st.Commit-st.Applied)
		}
		raftLog := fmt.Sprintf("%d..%d", st.FirstIndex, st.LastIndex)
		if st.FirstIndex > st.LastIndex {
			raftLog = "empty"
		}
		fmt.Fprintf(w, "%s\t%d\t%s\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%s\t%s\n", ep, st.ID, st.Role, st.Term, st.Leader,
			st.Commit, st.Applied, st.LastOffset, raftLog, st.Snapshot, st.Fsync, note)
		if st.Role == "leader" {
			leader = &st
		}
	}
	w.Flush()
	if consumers, err := e.c.Consumers(ctx); err == nil && len(consumers) > 0 {
		fmt.Println("\nconsumers:")
		w = tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "NAME\tCHECKPOINT\tBACKLOG")
		for _, c := range consumers {
			fmt.Fprintf(w, "%s\t%d\t%d\n", c.Consumer, c.Offset, c.Backlog)
		}
		w.Flush()
	}
	if leader != nil && len(leader.Peers) > 0 {
		fmt.Printf("\nreplication (from leader %d):\n", leader.ID)
		w = tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "PEER\tURL\tSTATE\tMATCH\tNEXT\tLAG")
		for _, p := range leader.Peers {
			fmt.Fprintf(w, "%d\t%s\t%s\t%d\t%d\t%d\n", p.ID, p.URL, p.State, p.Match, p.Next, p.Lag)
		}
		w.Flush()
	}
	return nil
}

func cmdAppend(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("append", flag.ExitOnError)
	producer := fs.Uint64("producer", 0, "producer id (required)")
	seq := fs.Uint64("seq", 0, "sequence of the first payload; default: producer's last committed + 1")
	typ := fs.Uint("type", 1, "event type")
	schema := fs.Uint("schema", 1, "payload schema version")
	fs.Parse(args)
	if *producer == 0 || fs.NArg() == 0 {
		return fmt.Errorf("usage: append --producer ID [--seq N] PAYLOAD [PAYLOAD]")
	}
	if *seq == 0 {
		p, err := e.c.Producer(ctx, *producer)
		if err != nil {
			return err
		}
		*seq = p.LastSeq + 1
	}
	req := api.AppendRequest{ProducerID: *producer, FirstSeq: *seq}
	now := time.Now().UnixNano()
	for _, p := range fs.Args() {
		req.Events = append(req.Events, fsm.EventIn{Type: uint16(*typ), Schema: uint8(*schema), SourceTS: now, Payload: []byte(p)})
	}
	res, err := e.c.Append(ctx, req)
	if err != nil {
		return err
	}
	return printJSON(res)
}

func cmdRead(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("read", flag.ExitOnError)
	from := fs.Uint64("from", 1, "first offset")
	limit := fs.Int("limit", 20, "maximum events")
	lin := fs.Bool("linearizable", false, "confirm with the leader that the read is up to date")
	fs.Parse(args)
	events, last, err := e.c.Read(ctx, *from, *limit, *lin)
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "OFFSET\tPRODUCER\tSEQ\tTYPE\tSCHEMA\tINGEST\tPAYLOAD")
	for _, ev := range events {
		fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%d\t%s\t%s\n", ev.Offset, ev.ProducerID, ev.ProducerSeq, ev.Type, ev.Schema,
			time.Unix(0, ev.IngestTS).UTC().Format(time.RFC3339Nano), showPayload(ev.Payload))
	}
	w.Flush()
	fmt.Printf("(%d events, node's last offset %d)\n", len(events), last)
	return nil
}

func showPayload(p []byte) string {
	if utf8.Valid(p) && len(p) <= 80 {
		return strconv.Quote(string(p))
	}
	if len(p) > 32 {
		return fmt.Sprintf("%x… (%d bytes)", p[:32], len(p))
	}
	return fmt.Sprintf("%x", p)
}

func cmdProducer(ctx context.Context, e *env, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: producer ID")
	}
	id, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return err
	}
	p, err := e.c.Producer(ctx, id)
	if err != nil {
		return err
	}
	return printJSON(p)
}

func cmdCheckpoint(ctx context.Context, e *env, args []string) error {
	switch len(args) {
	case 1:
		r, err := e.c.Consumer(ctx, args[0])
		if err != nil {
			return err
		}
		return printJSON(r)
	case 2:
		off, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return err
		}
		r, err := e.c.Checkpoint(ctx, args[0], off)
		if err != nil {
			return err
		}
		return printJSON(r)
	}
	return fmt.Errorf("usage: checkpoint NAME [OFFSET]")
}

// cmdVerify compares replicas: each node re-reads and checksums records
// 1..N, where N is the smallest last offset among reachable nodes.
func cmdVerify(ctx context.Context, e *env, args []string) error {
	to := uint64(1<<63 - 1)
	reachable := 0
	for _, ep := range e.c.Endpoints() {
		if st, err := e.c.Status(ctx, ep); err == nil && st.Failed == "" {
			to = min(to, st.LastOffset)
			reachable++
		}
	}
	if reachable == 0 {
		return fmt.Errorf("no reachable nodes")
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintf(w, "NODE\tCHECKED\tLAST_OFFSET\tSHA256(1..%d)\n", to)
	hashes := map[string]bool{}
	verified := 0
	for _, ep := range e.c.Endpoints() {
		v, err := e.c.Verify(ctx, ep, to)
		if err != nil {
			fmt.Fprintf(w, "%s\t-\t-\t%v\n", ep, err)
			continue
		}
		hashes[v.SHA256] = true
		verified++
		fmt.Fprintf(w, "%s\t%d\t%d\t%s\n", ep, v.To, v.LastOffset, v.SHA256)
	}
	w.Flush()
	if len(hashes) > 1 {
		return fmt.Errorf("replicas disagree on offsets 1..%d", to)
	}
	fmt.Printf("%d nodes agree on offsets 1..%d\n", verified, to)
	return nil
}
