package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/orderbook"
)

// Checkpoint is a replay position plus the full book state at that position.
type Checkpoint struct {
	Offset uint64
	Hash   [32]byte
	State  []byte
}

var ErrBadCheckpoint = errors.New("replay: invalid checkpoint")

const checkpointMagic = "MLOGRCP1"

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func (e *Engine) Checkpoint() *Checkpoint {
	state, _ := e.book.MarshalBinary()
	return &Checkpoint{Offset: e.book.Stats.LastOffset, Hash: sha256.Sum256(state), State: state}
}

// Restore builds an engine from a checkpoint after checking that the state
// matches its hash and offset.
func Restore(cp *Checkpoint) (*Engine, error) {
	if sha256.Sum256(cp.State) != cp.Hash {
		return nil, fmt.Errorf("%w: state does not match hash", ErrBadCheckpoint)
	}
	book, err := orderbook.UnmarshalBinary(cp.State)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadCheckpoint, err)
	}
	if book.Stats.LastOffset != cp.Offset {
		return nil, fmt.Errorf("%w: state is at offset %d, header says %d", ErrBadCheckpoint, book.Stats.LastOffset, cp.Offset)
	}
	return &Engine{book: book}, nil
}

// WriteCheckpointFile writes cp atomically: a crash leaves either the old
// file or the new one, never a torn mix (temp file, fsync, rename, fsync dir).
func WriteCheckpointFile(path string, cp *Checkpoint) error {
	buf := []byte(checkpointMagic)
	buf = binary.LittleEndian.AppendUint64(buf, cp.Offset)
	buf = append(buf, cp.Hash[:]...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(cp.State)))
	buf = append(buf, cp.State...)
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, castagnoli))

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

func ReadCheckpointFile(path string) (*Checkpoint, error) {
	buf, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	const fixed = len(checkpointMagic) + 8 + 32 + 4
	if len(buf) < fixed+4 || !bytes.HasPrefix(buf, []byte(checkpointMagic)) {
		return nil, fmt.Errorf("%w: %s: bad header", ErrBadCheckpoint, path)
	}
	body, sum := buf[:len(buf)-4], binary.LittleEndian.Uint32(buf[len(buf)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return nil, fmt.Errorf("%w: %s: checksum mismatch", ErrBadCheckpoint, path)
	}
	cp := &Checkpoint{Offset: binary.LittleEndian.Uint64(buf[8:])}
	copy(cp.Hash[:], buf[16:48])
	if n := binary.LittleEndian.Uint32(buf[48:]); int(n) != len(body)-fixed {
		return nil, fmt.Errorf("%w: %s: length mismatch", ErrBadCheckpoint, path)
	}
	cp.State = body[fixed:]
	return cp, nil
}
