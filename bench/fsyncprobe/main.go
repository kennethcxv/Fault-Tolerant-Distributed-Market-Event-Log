// Command fsyncprobe measures whether concurrent fsyncs on one disk overlap.
// It runs N writers, each appending 4 KiB and calling File.Sync in a loop on
// its own file, and reports total and per-writer syncs per second. If the
// total stays flat as N grows, the device serializes cache flushes, and
// nodes sharing that disk cannot persist in parallel.
//
//	go run ./bench/fsyncprobe [-dir DIR] [-seconds S]
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	dir := flag.String("dir", os.TempDir(), "directory on the disk to test")
	secs := flag.Float64("seconds", 3, "duration per writer count")
	flag.Parse()
	buf := make([]byte, 4096)
	for _, writers := range []int{1, 2, 3, 5} {
		var total atomic.Int64
		var wg sync.WaitGroup
		stop := time.Now().Add(time.Duration(*secs * float64(time.Second)))
		for w := range writers {
			wg.Go(func() {
				f, err := os.Create(filepath.Join(*dir, fmt.Sprintf("fsyncprobe-%d", w)))
				if err != nil {
					panic(err)
				}
				defer os.Remove(f.Name())
				defer f.Close()
				for time.Now().Before(stop) {
					if _, err := f.Write(buf); err != nil {
						panic(err)
					}
					if err := f.Sync(); err != nil {
						panic(err)
					}
					total.Add(1)
				}
			})
		}
		wg.Wait()
		rate := float64(total.Load()) / *secs
		fmt.Printf("writers=%d total_syncs_per_s=%.0f per_writer=%.0f mean_sync=%.2fms\n",
			writers, rate, rate/float64(writers), 1000*float64(writers)/rate)
	}
}
