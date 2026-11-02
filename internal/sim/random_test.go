package sim

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

// TestRandomizedFaults combines every fault the harness supports: message
// loss, duplication and delay; symmetric and one-way partitions; crashes that
// keep a random part of unsynced data; restarts; and log compaction with
// snapshot transfer. Every run checks I1-I7, including linearizability.
// MLOG_SIM_SEEDS raises the number of seeds (default 30).
func TestRandomizedFaults(t *testing.T) {
	seeds := 30
	if v, err := strconv.Atoi(os.Getenv("MLOG_SIM_SEEDS")); err == nil {
		seeds = v
	}
	for seed := uint64(1000); seed < 1000+uint64(seeds); seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			nodes := 3
			if seed%4 == 0 {
				nodes = 5
			}
			opts := Options{Nodes: nodes, Seed: seed, DropRate: float64(seed%5) * 0.02, DupRate: 0.05,
				MaxDelay: int(seed % 9), SnapshotEntries: 30, CompactKeep: 10}
			c, w := newHarness(t, opts, 3, 60)
			w.ReadPercent = 20
			rng := c.Rand()
			tolerate := (nodes - 1) / 2
			var crashes, partitions, links int
			for round := 0; round < 40 && !w.Done(); round++ {
				if err := w.Steps(10 + rng.IntN(50)); err != nil {
					t.Fatalf("%v\ntrace: %s", err, c.SaveTrace(t.Name()))
				}
				down := 0
				for _, n := range c.Nodes {
					if !n.Up {
						down++
					}
				}
				id := uint64(1 + rng.IntN(nodes))
				switch rng.IntN(6) {
				case 0:
					if c.Node(id).Up && down < tolerate {
						c.Crash(id, rng.IntN(101))
						crashes++
					}
				case 1:
					if err := c.Restart(id); err != nil {
						t.Fatalf("restart %d: %v\ntrace: %s", id, err, c.SaveTrace(t.Name()))
					}
				case 2:
					// Isolate a random minority.
					var minority, majority []uint64
					for _, n := range c.Nodes {
						if len(minority) < tolerate && rng.IntN(2) == 0 {
							minority = append(minority, n.ID)
						} else {
							majority = append(majority, n.ID)
						}
					}
					c.Partition(minority, majority)
					partitions++
				case 3:
					to := uint64(1 + rng.IntN(nodes))
					if to != id {
						c.BlockLink(id, to)
						links++
					}
				case 4:
					c.Heal()
				}
			}
			for _, n := range c.Nodes {
				if err := c.Restart(n.ID); err != nil {
					t.Fatal(err)
				}
			}
			finish(t, c, w)
			installs := 0
			for _, n := range c.Installs {
				installs += n
			}
			unknown := 0
			for _, op := range w.History() {
				if op.Output.(Output).Unknown {
					unknown++
				}
			}
			t.Logf("nodes=%d crashes=%d partitions=%d one-way=%d snapshot-installs=%d timed-out-ops=%d",
				nodes, crashes, partitions, links, installs, unknown)
		})
	}
}
