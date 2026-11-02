package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
)

// leaderStatus returns the status reported by the current leader.
func (e *env) leaderStatus(ctx context.Context) (api.Status, error) {
	for {
		for _, ep := range e.c.Endpoints() {
			if st, err := e.c.Status(ctx, ep); err == nil && st.Role == "leader" {
				return st, nil
			}
		}
		select {
		case <-ctx.Done():
			return api.Status{}, fmt.Errorf("no leader reachable")
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// cmdMember changes membership one voter at a time. To replace a node whose
// disk is gone: remove it first (the remaining majority keeps quorum), then
// add a new node under a new id. Never re-add the old id with an empty disk:
// it has forgotten its vote and could vote twice in one term.
func cmdMember(ctx context.Context, e *env, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: member list | add ID URL | remove ID")
	}
	switch args[0] {
	case "list":
		st, err := e.leaderStatus(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("cluster id: %s (leader %d, term %d)\n", st.ClusterID, st.Leader, st.Term)
		w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tURL")
		for _, m := range st.Members {
			fmt.Fprintf(w, "%d\t%s\n", m.ID, m.URL)
		}
		return w.Flush()
	case "add", "remove":
		if (args[0] == "add" && len(args) != 3) || (args[0] == "remove" && len(args) != 2) {
			return fmt.Errorf("usage: member add ID URL | member remove ID")
		}
		id, err := strconv.ParseUint(args[1], 10, 64)
		if err != nil {
			return err
		}
		var url string
		if args[0] == "add" {
			url = strings.TrimRight(args[2], "/")
			if !strings.Contains(url, "://") {
				url = "http://" + url
			}
			err = e.c.AddMember(ctx, id, url)
		} else {
			err = e.c.RemoveMember(ctx, id)
		}
		if err != nil {
			return err
		}
		// Wait until the leader has applied the change.
		for {
			st, err := e.leaderStatus(ctx)
			if err != nil {
				return err
			}
			has := slices.ContainsFunc(st.Members, func(m api.Member) bool { return m.ID == id })
			if has == (args[0] == "add") {
				verb := "removed"
				if args[0] == "add" {
					verb = "added"
				}
				fmt.Printf("node %d %s; members now:", id, verb)
				var peers []string
				for _, m := range st.Members {
					fmt.Printf(" %d", m.ID)
					peers = append(peers, fmt.Sprintf("%d=%s", m.ID, m.URL))
				}
				fmt.Println()
				if args[0] == "add" {
					fmt.Printf("start it with an empty data directory:\n  mlogd --id %d --listen %s --data DIR --join --cluster-id %s --peers %s\n",
						id, strings.TrimPrefix(strings.TrimPrefix(url, "http://"), "https://"), st.ClusterID, strings.Join(peers, ","))
				}
				return nil
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("the change was proposed but not applied in time (another change may have been pending); check with: mlogctl member list")
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
	return fmt.Errorf("unknown member command %q", args[0])
}
