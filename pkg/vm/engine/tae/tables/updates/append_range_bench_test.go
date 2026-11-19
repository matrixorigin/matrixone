// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package updates

import (
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func benchmarkAppendHistory(count, pending int) *AppendMVCCHandle {
	h := NewAppendMVCCHandle(&catalog.ObjectEntry{})
	for i := range count {
		node := NewEmptyAppendNode()
		node.startRow, node.maxRow = uint32(i), uint32(i+1)
		node.Start = types.BuildTS(int64(i+1), 0)
		node.Prepare, node.End = node.Start, node.Start
		h.OnReplayAppendNode(node)
	}
	for i := range pending {
		h.AddAppendNodeLocked(mockTxn(), uint32(count+i), uint32(count+i+1))
	}
	return h
}

func BenchmarkCollectAppendWindow(b *testing.B) {
	for _, n := range []int{64, 1024, 8192} {
		counts := []int{0, 1, 32}
		if n/2 != 32 {
			counts = append(counts, n/2)
		}
		counts = append(counts, n)
		for _, k := range counts {
			for _, pending := range []int{0, 8} {
				b.Run(fmt.Sprintf("N%d/K%d/P%d", n, k, pending), func(b *testing.B) {
					h := benchmarkAppendHistory(n, pending)
					mp := mpool.MustNewZero()
					b.Cleanup(func() { mpool.DeleteMPool(mp) })
					from, to := types.BuildTS(int64(n-k+1), 0), types.BuildTS(int64(n), 0)
					if k == 0 {
						to = from
					}
					// Exclude initial collection/index construction from warm latency.
					for range 2 {
						h.RLock()
						_, coldCommits, coldAborts := h.CollectAppendLocked(from, to, mp)
						h.RUnlock()
						if coldCommits != nil {
							coldCommits.Close()
							coldAborts.Close()
						}
					}
					b.ReportAllocs()
					for b.Loop() {
						h.RLock()
						_, commits, aborts := h.CollectAppendLocked(from, to, mp)
						h.RUnlock()
						if commits != nil {
							commits.Close()
							aborts.Close()
						}
					}
				})
			}
		}
	}
}

func BenchmarkAppendRangeWriter(b *testing.B) {
	for _, n := range []int{64, 1024, 8192} {
		b.Run(fmt.Sprintf("N%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				h := NewAppendMVCCHandle(&catalog.ObjectEntry{})
				for i := range n {
					txn := mockTxn()
					txn.PrepareTS = types.BuildTS(int64(i+1), 0)
					txn.CommitTS = txn.PrepareTS
					node, _ := h.AddAppendNodeLocked(txn, uint32(i), uint32(i+1))
					if err := node.PrepareCommit(); err != nil {
						b.Fatal(err)
					}
					if err := node.ApplyCommit(txn.ID); err != nil {
						b.Fatal(err)
					}
				}
				runtime.KeepAlive(h)
			}
		})
	}
}

func BenchmarkAppendRangeWriteAndCollect(b *testing.B) {
	for _, every := range []int{1, 64} {
		b.Run(fmt.Sprintf("N1024/every%d", every), func(b *testing.B) {
			mp := mpool.MustNewZero()
			b.Cleanup(func() { mpool.DeleteMPool(mp) })
			b.ReportAllocs()
			for b.Loop() {
				h := NewAppendMVCCHandle(&catalog.ObjectEntry{})
				for i := range 1024 {
					txn := mockTxn()
					txn.PrepareTS = types.BuildTS(int64(i+1), 0)
					txn.CommitTS = txn.PrepareTS
					node, _ := h.AddAppendNodeLocked(txn, uint32(i), uint32(i+1))
					if err := node.PrepareCommit(); err != nil {
						b.Fatal(err)
					}
					if err := node.ApplyCommit(txn.ID); err != nil {
						b.Fatal(err)
					}
					if (i+1)%every == 0 {
						h.RLock()
						_, commits, aborts := h.CollectAppendLocked(types.BuildTS(int64(i-every+2), 0), txn.PrepareTS, mp)
						h.RUnlock()
						if commits != nil {
							commits.Close()
							aborts.Close()
						}
					}
				}
			}
		})
	}
}

// Measure live Go state separately from latency. Forced GC and fixture
// construction belong only to this storage control, never query timing.
func BenchmarkAppendRangeStorage(b *testing.B) {
	for _, n := range []int{64, 8192} {
		b.Run(fmt.Sprintf("N%d", n), func(b *testing.B) {
			var bytesPerNode float64
			for b.Loop() {
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				var handles [16]*AppendMVCCHandle
				mp := mpool.MustNewZero()
				for i := range handles {
					handles[i] = benchmarkAppendHistory(n, 0)
					for range 2 {
						handles[i].RLock()
						_, commits, aborts := handles[i].CollectAppendLocked(types.BuildTS(int64(n+1), 0), types.BuildTS(int64(n+1), 0), mp)
						handles[i].RUnlock()
						if commits != nil {
							commits.Close()
							aborts.Close()
						}
					}
				}
				mpool.DeleteMPool(mp)
				runtime.GC()
				runtime.ReadMemStats(&after)
				bytesPerNode = float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)) / float64(len(handles)*n)
				runtime.KeepAlive(handles)
			}
			b.ReportMetric(bytesPerNode, "heap-B/node")
		})
	}
}

// Two CPUs let the writer overlap real MVCC collection. Each measured unit
// appends 128 nodes to the same warm 8192-node history and drains its logtail.
// Rebuild the bounded fixture outside timing instead of growing it with b.N.
func BenchmarkAppendRangeConcurrent(b *testing.B) {
	b.StopTimer()
	mp := mpool.MustNewZero()
	b.Cleanup(func() { mpool.DeleteMPool(mp) })
	var writes, reads appendLatencySamples
	b.ReportAllocs()
	for range b.N {
		benchmarkConcurrentAppendUnit(b, mp, &writes, &reads)
	}
	b.StopTimer()
	writes.report(b, "write")
	reads.report(b, "collect")
}

type appendLatencySamples struct {
	values [8192]int64
	count  int
}

func (s *appendLatencySamples) record(elapsed time.Duration) {
	s.values[s.count%len(s.values)] = int64(elapsed)
	s.count++
}

func (s *appendLatencySamples) report(b *testing.B, name string) {
	values := s.values[:min(s.count, len(s.values))]
	if len(values) == 0 {
		return
	}
	slices.Sort(values)
	for _, percentile := range []int{95, 99} {
		b.ReportMetric(float64(values[(len(values)*percentile+99)/100-1]), fmt.Sprintf("%s-p%d-ns", name, percentile))
	}
}

func benchmarkConcurrentAppendUnit(b *testing.B, mp *mpool.MPool, writes, reads *appendLatencySamples) {
	b.StopTimer()
	h := benchmarkAppendHistory(8192, 0)
	for range 2 {
		h.RLock()
		_, commits, aborts := h.CollectAppendLocked(types.BuildTS(8193, 0), types.BuildTS(8193, 0), mp)
		h.RUnlock()
		if commits != nil {
			commits.Close()
			aborts.Close()
		}
	}
	var cutoff atomic.Int64
	cutoff.Store(8192)
	var writerDone atomic.Bool
	start, drained := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(drained)
		<-start
		from := int64(8193)
		for {
			to := cutoff.Load()
			if to >= from {
				started := time.Now()
				h.RLock()
				_, commits, aborts := h.CollectAppendLocked(types.BuildTS(from, 0), types.BuildTS(to, 0), mp)
				h.RUnlock()
				if commits != nil {
					commits.Close()
					aborts.Close()
				}
				reads.record(time.Since(started))
				from = to + 1
			}
			if writerDone.Load() && from > cutoff.Load() {
				return
			}
			runtime.Gosched()
		}
	}()
	b.StartTimer()
	close(start)
	defer func() { writerDone.Store(true); <-drained }()
	for row := 8192; row < 8320; row++ {
		started := time.Now()
		txn := MockTxnWithStartTS(types.BuildTS(1, 0))
		txn.PrepareTS = types.BuildTS(int64(row+1), 0)
		txn.CommitTS = txn.PrepareTS
		h.Lock()
		node, _ := h.AddAppendNodeLocked(txn, uint32(row), uint32(row+1))
		h.Unlock()
		if err := node.PrepareCommit(); err != nil {
			b.Fatal(err)
		}
		if err := node.ApplyCommit(txn.ID); err != nil {
			b.Fatal(err)
		}
		cutoff.Store(int64(row + 1))
		writes.record(time.Since(started))
	}
}
