package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/replay"
)

func cmdReplay(ctx context.Context, e *env, args []string) error {
	fs := flag.NewFlagSet("replay", flag.ExitOnError)
	from := fs.String("from-checkpoint", "", "resume from this checkpoint file instead of offset 1")
	to := fs.Uint64("to", 0, "last offset to apply (default: everything committed)")
	save := fs.String("save-checkpoint", "", "write a checkpoint file when done")
	batch := fs.Int("batch", 5000, "events per read")
	lin := fs.Bool("linearizable", false, "read through the leader's read index")
	fs.Parse(args)

	eng := replay.New()
	if *from != "" {
		cp, err := replay.ReadCheckpointFile(*from)
		if err != nil {
			return err
		}
		if eng, err = replay.Restore(cp); err != nil {
			return err
		}
	}
	start := eng.Next()
	t0 := time.Now()
	n, err := eng.Run(ctx, replay.ClientSource{Client: e.c, Linearizable: *lin}, *to, *batch)
	elapsed := time.Since(t0)
	if err != nil {
		fmt.Printf("replay stopped after %d events: %v\n", n, err)
	}
	b := eng.Book()
	h := b.Hash()
	fmt.Printf("offsets   %d..%d (%d events applied in %s, %.0f events/s)\n", start, eng.Next()-1, n,
		elapsed.Round(time.Millisecond), float64(n)/max(elapsed.Seconds(), 1e-9))
	fmt.Printf("sha256    %s\n", hex.EncodeToString(h[:]))
	fmt.Printf("stats     %+v\n", b.Stats)
	fmt.Printf("orders    %d resting\n", b.Orders())
	if *save != "" {
		if err := replay.WriteCheckpointFile(*save, eng.Checkpoint()); err != nil {
			return err
		}
		fmt.Printf("checkpoint written to %s at offset %d\n", *save, eng.Next()-1)
	}
	return err
}
