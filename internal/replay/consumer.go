package replay

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"time"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/client"
)

// Consumer follows the log into an order book and checkpoints its progress.
//
// Every Every events it first writes its state to StateFile (atomically, with
// fsync), then commits the offset to the cluster's replicated consumer
// checkpoint. The local file is what the consumer resumes from; the cluster
// checkpoint is for operators (backlog) and for consumers without local
// state. A crash between the two steps leaves the cluster checkpoint behind,
// which only means some events are delivered again after restart, and the
// replay engine skips offsets it has already applied. Delivery is therefore
// at least once, and the resulting state is the same as exactly-once
// application, because replay is deterministic.
type Consumer struct {
	Name      string
	Client    *client.Client
	StateFile string
	Every     uint64        // events between checkpoints
	Batch     int           // events per read
	Poll      time.Duration // wait between reads at the tail when following
	Logger    *slog.Logger
	// OnCheckpoint, if set, is called after each checkpoint is committed.
	OnCheckpoint func(offset uint64, hash [32]byte)

	eng *Engine
}

// Engine returns the consumer's current engine (nil before Run).
func (c *Consumer) Engine() *Engine { return c.eng }

// Run replays until offset `until` (0: until ctx ends, following the tail).
func (c *Consumer) Run(ctx context.Context, until uint64) error {
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	c.Every, c.Batch, c.Poll = cmp.Or(c.Every, 1000), cmp.Or(c.Batch, 1000), cmp.Or(c.Poll, 100*time.Millisecond)
	if err := c.load(); err != nil {
		return err
	}
	committed, err := c.Client.Consumer(ctx, c.Name)
	if err != nil {
		return fmt.Errorf("consumer %s: read cluster checkpoint: %w", c.Name, err)
	}
	c.Logger.Info("consumer starting", "name", c.Name, "local_offset", c.eng.Next()-1,
		"cluster_checkpoint", committed.Offset, "last_offset", committed.LastOffset)
	// A crash after writing the local state but before committing it leaves
	// the cluster checkpoint behind; catch it up before doing anything else.
	if local := c.eng.Next() - 1; local > committed.Offset {
		if _, err := c.Client.Checkpoint(ctx, c.Name, local); err != nil {
			return fmt.Errorf("consumer %s: commit offset %d: %w", c.Name, local, err)
		}
	}
	src := ClientSource{Client: c.Client}
	for until == 0 || c.eng.Next() <= until {
		target := c.eng.Next() - 1 + c.Every
		if until != 0 {
			target = min(target, until)
		}
		n, err := c.eng.Run(ctx, src, target, c.Batch)
		if err != nil {
			return err
		}
		if c.eng.Next()-1 == target || (n > 0 && until == 0) {
			if err := c.checkpoint(ctx); err != nil {
				return err
			}
		}
		if n == 0 { // at the tail
			if until != 0 && c.eng.Next() > until {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(c.Poll):
			}
		}
	}
	return nil
}

func (c *Consumer) load() error {
	cp, err := ReadCheckpointFile(c.StateFile)
	if errors.Is(err, fs.ErrNotExist) {
		c.eng = New()
		return nil
	}
	if err != nil {
		return fmt.Errorf("consumer %s: %w (delete the file to rebuild from offset 1)", c.Name, err)
	}
	c.eng, err = Restore(cp)
	return err
}

func (c *Consumer) checkpoint(ctx context.Context) error {
	cp := c.eng.Checkpoint()
	if err := WriteCheckpointFile(c.StateFile, cp); err != nil {
		return fmt.Errorf("consumer %s: write state: %w", c.Name, err)
	}
	res, err := c.Client.Checkpoint(ctx, c.Name, cp.Offset)
	if err != nil {
		return fmt.Errorf("consumer %s: commit offset %d: %w", c.Name, cp.Offset, err)
	}
	// A stale result only means an earlier commit was already further along
	// (for example after the local file was rebuilt from scratch).
	c.Logger.Debug("checkpoint", "name", c.Name, "offset", cp.Offset, "result", res.Status)
	if c.OnCheckpoint != nil {
		c.OnCheckpoint(cp.Offset, cp.Hash)
	}
	return nil
}
