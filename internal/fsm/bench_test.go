package fsm

import (
	"fmt"
	"testing"

	"go.etcd.io/raft/v3/raftpb"

	"github.com/kennethcxv/Fault-Tolerant-Distributed-Market-Event-Log/internal/storage"
)

// BenchmarkApply measures the storage layer alone, on the real filesystem:
// one command of `batch` 64-byte events per Apply, committed with or without
// fsync. This is the single-node write path with no Raft and no network.
func BenchmarkApply(b *testing.B) {
	for _, sync := range []bool{true, false} {
		for _, batch := range []int{1, 10, 100, 1000} {
			b.Run(fmt.Sprintf("sync=%v/batch=%d", sync, batch), func(b *testing.B) {
				st, err := storage.Open(storage.Options{Dir: b.TempDir()})
				if err != nil {
					b.Fatal(err)
				}
				defer st.Close()
				f, err := Open(st)
				if err != nil {
					b.Fatal(err)
				}
				payload := make([]byte, 64)
				a := &Append{ProducerID: 1}
				for range batch {
					a.Events = append(a.Events, EventIn{Type: 1, Schema: 1, Payload: payload})
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					a.FirstSeq = uint64(i*batch + 1)
					cmd := Command{Append: a}
					if _, err := f.Apply([]*raftpb.Entry{{Index: new(uint64(i + 1)), Data: cmd.Marshal()}}, nil, sync); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(b.N*batch)/b.Elapsed().Seconds(), "events/s")
			})
		}
	}
}
