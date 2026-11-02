package e2e

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/api"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/fsm"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/orderbook"
	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/replay"
)

// The consumer process is SIGKILLed at random points and restarted until it
// finishes; its final state hash must equal a clean replay and its cluster
// checkpoint must reach the end of the log.
func TestConsumerProcessSurvivesSIGKILL(t *testing.T) {
	if testing.Short() {
		t.Skip("starts real processes")
	}
	c := newCluster(t, 3)
	c.leader(10 * time.Second)
	cl := client.New(c.endpoints()...)
	const n = 20_000
	g := orderbook.NewGenerator(3, 4)
	for seq := uint64(1); seq <= n; seq += 1000 {
		req := api.AppendRequest{ProducerID: 1, FirstSeq: seq}
		for range 1000 {
			m := g.Next()
			req.Events = append(req.Events, fsm.EventIn{Type: m.Type, Schema: 1, Payload: m.Encode()})
		}
		if _, err := cl.Append(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
	eng := replay.New()
	if _, err := eng.Run(context.Background(), replay.ClientSource{Client: cl}, n, 5000); err != nil {
		t.Fatal(err)
	}
	want := eng.Book().Hash()

	ctl := filepath.Join(t.TempDir(), "mlogctl")
	if out, err := exec.Command("go", "build", "-o", ctl, "github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/cmd/mlogctl").CombinedOutput(); err != nil {
		t.Fatalf("build mlogctl: %v\n%s", err, out)
	}
	state := filepath.Join(t.TempDir(), "e2e.ckpt")
	kills := 0
	for attempt := 0; ; attempt++ {
		var out bytes.Buffer
		cmd := exec.Command(ctl, "--endpoints", strings.Join(c.endpoints(), ","), "consume",
			"--name", "e2e", "--state", state, "--every", "500", "--to", fmt.Sprint(n))
		cmd.Stdout, cmd.Stderr = &out, &out
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		var err error
		if attempt < 40 {
			select {
			case err = <-done:
			case <-time.After(time.Duration(10+rand.IntN(60)) * time.Millisecond):
				cmd.Process.Signal(syscall.SIGKILL)
				<-done
				kills++
				continue
			}
		} else {
			err = <-done
		}
		if err != nil {
			t.Fatalf("consumer failed: %v\n%s", err, out.String())
		}
		var got string
		for _, line := range strings.Split(out.String(), "\n") {
			if h, ok := strings.CutPrefix(line, "sha256 "); ok {
				got = strings.TrimSpace(h)
			}
		}
		if got != hex.EncodeToString(want[:]) {
			t.Fatalf("after %d kills the consumer reports %s, clean replay is %x", kills, got, want)
		}
		break
	}
	cp, err := cl.Consumer(context.Background(), "e2e")
	if err != nil || cp.Offset != n {
		t.Fatalf("cluster checkpoint %+v %v", cp, err)
	}
	t.Logf("consumer killed %d times; final state matches a clean replay of %d events", kills, n)
}
