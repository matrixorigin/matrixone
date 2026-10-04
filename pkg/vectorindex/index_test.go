// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package vectorindex

import (
	"fmt"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/system"
	"github.com/stretchr/testify/require"
	usearch "github.com/unum-cloud/usearch/golang"
)

func TestUSearch(t *testing.T) {

	// Create Index
	vectorSize := 3
	vectorsCount := 100
	conf := usearch.DefaultConfig(uint(vectorSize))
	conf.Metric = usearch.L2sq
	index, err := usearch.NewIndex(conf)
	if err != nil {
		panic("Failed to create Index")
	}
	defer index.Destroy()

	// Add to Index
	err = index.Reserve(uint(vectorsCount))
	if err != nil {
		panic("Failed to reserve")
	}

	for i := 0; i < vectorsCount; i++ {
		err = index.Add(usearch.Key(i), []float32{float32(i), float32(i + 1), float32(i + 2)})
		if err != nil {
			panic("Failed to add")
		}
	}

	index2, err := usearch.NewIndex(conf)
	if err != nil {
		panic("Failed to create Index")
	}
	defer index2.Destroy()

	// Add to Index
	err = index2.Reserve(uint(vectorsCount))
	if err != nil {
		panic("Failed to reserve")
	}
	for i := 0; i < vectorsCount; i++ {
		err = index2.Add(usearch.Key(i+vectorsCount), []float32{float32(i + vectorsCount), float32(i + 1 + vectorsCount), float32(i + 2 + vectorsCount)})
		if err != nil {
			panic("Failed to add")
		}
	}

	index.Save("hnsw0.bin")
	index2.Save("hnsw1.bin")

	// Search
	keys, distances, err := index.Search([]float32{0.0, 1.0, 2.0}, 3)
	if err != nil {
		panic("Failed to search")
	}
	fmt.Println(keys, distances)

	keys, distances, err = index2.Search([]float32{0.0, 1.0, 2.0}, 3)
	if err != nil {
		panic("Failed to search")
	}
	fmt.Println(keys, distances)
}

func TestSafeHeap(t *testing.T) {

	var wg sync.WaitGroup

	h := NewSearchResultSafeHeap(40)
	for j := 0; j < 4; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				h.Push(&SearchResult{int64(usearch.Key(j*10 + i)), float64(j*10 + i)})
			}
		}()
	}

	wg.Wait()

	n := h.Len()
	fmt.Printf("Len = %d\n", n)
	for i := 0; i < n; i++ {
		srif := h.Pop()
		sr := srif.(*SearchResult)
		fmt.Printf("id = %d, score = %f\n", sr.Id, sr.Distance)
	}
}

func TestSafeHeapAny(t *testing.T) {

	var wg sync.WaitGroup

	h := NewSearchResultSafeHeap(40)
	for j := 0; j < 4; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				h.Push(&SearchResultAnyKey{any(int64(usearch.Key(j*10 + i))), float64(j*10 + i)})
			}
		}()
	}

	wg.Wait()

	n := h.Len()
	fmt.Printf("Len = %d\n", n)
	for i := 0; i < n; i++ {
		srif := h.Pop()
		sr := srif.(*SearchResultAnyKey)
		fmt.Printf("id = %d, score = %f\n", sr.Id, sr.Distance)
	}
}

// TestSafeHeapBounded covers #25637: many index shards each push up to `limit` candidates,
// but the merge heap must retain only the global best `limit` (smallest distance) — not
// shard_count * limit. Here 4 shards push 40 candidates concurrently into a heap bounded to
// 10; it must keep exactly the 10 smallest distances (0..9).
func TestSafeHeapBounded(t *testing.T) {
	const limit = 10
	h := NewSearchResultSafeHeap(limit)

	var wg sync.WaitGroup
	for j := 0; j < 4; j++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				d := float64(shard*10 + i) // distances 0..39 across the 4 shards
				h.Push(&SearchResult{int64(shard*10 + i), d})
			}
		}(j)
	}
	wg.Wait()

	// bounded: never retains more than `limit`, regardless of how many were pushed.
	require.Equal(t, limit, h.Len())

	// It is a max-heap, so Pop yields worst-first; draining must produce exactly the 10
	// smallest distances 9,8,...,0. Getting these back proves the retained set is {0..9}.
	got := make([]float64, 0, limit)
	for h.Len() > 0 {
		got = append(got, h.Pop().(*SearchResult).Distance)
	}
	require.Equal(t, limit, len(got))
	for i, d := range got {
		require.Equal(t, float64(limit-1-i), d, "retained set must be the %d smallest distances", limit)
	}
}

func TestConcurrent(t *testing.T) {
	index := newConcurrentSearchIndex(t)
	// Visit every vector from each worker. The old 64 * 20,000 repetitions of
	// query zero were a throughput/soak workload, not additional semantic cells.
	// Keep that workload available in BenchmarkConcurrentSearch instead.
	t.Run("shared_query", func(t *testing.T) {
		// Preserve the original same-query contention and reuse each worker's
		// search context, without turning the functional check into a soak.
		runConcurrentSearch(t, index, 2, false)
	})
	t.Run("all_vectors", func(t *testing.T) {
		runConcurrentSearch(t, index, concurrentSearchVectors, true)
	})
}

// BenchmarkConcurrentSearch retains the original 1,280,000-search workload via
// -run '^$' -bench '^BenchmarkConcurrentSearch$' -benchtime=20000x.
// One benchmark operation is a batch of 64 concurrent searches.
func BenchmarkConcurrentSearch(b *testing.B) {
	index := newConcurrentSearchIndex(b)
	b.ResetTimer()
	runConcurrentSearch(b, index, b.N, false)
	b.StopTimer()
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/concurrentSearchWorkers, "ns/search")
}

const (
	concurrentSearchVectors = 100
	concurrentSearchWorkers = 64
)

func newConcurrentSearchIndex(tb testing.TB) *usearch.Index {
	tb.Helper()
	conf := usearch.DefaultConfig(3)
	conf.Metric = usearch.L2sq
	index, err := usearch.NewIndex(conf)
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, index.Destroy()) })
	require.NoError(tb, index.Reserve(concurrentSearchVectors))
	require.NoError(tb, index.ChangeThreadsSearch(concurrentSearchWorkers))
	for i := 0; i < concurrentSearchVectors; i++ {
		require.NoError(tb, index.Add(usearch.Key(i), []float32{float32(i), float32(i + 1), float32(i + 2)}))
	}
	return index
}

func runConcurrentSearch(tb testing.TB, index *usearch.Index, iterations int, variedQueries bool) {
	tb.Helper()
	var ready, wg sync.WaitGroup
	ready.Add(concurrentSearchWorkers)
	wg.Add(concurrentSearchWorkers)
	start := make(chan struct{})
	for worker := 0; worker < concurrentSearchWorkers; worker++ {
		go func() {
			defer wg.Done()
			ready.Done()
			<-start
			for j := 0; j < iterations; j++ {
				key := 0
				if variedQueries {
					key = (worker + j) % concurrentSearchVectors
				}
				keys, distances, err := index.Search([]float32{float32(key), float32(key + 1), float32(key + 2)}, 3)
				// Check each result, but only build diagnostics on failure. Do not
				// call FailNow from a worker; join all workers before native cleanup.
				if err != nil || len(keys) != 3 || len(distances) != 3 || keys[0] != uint64(key) || distances[0] != 0 {
					tb.Errorf("worker %d search %d: key=%d keys=%v distances=%v err=%v", worker, j, key, keys, distances, err)
					return
				}
			}
		}()
	}
	ready.Wait()
	close(start)
	wg.Wait()
}

func TestChecksum(t *testing.T) {
	_, err := CheckSum("abc")
	require.NotNil(t, err)
}

func TestGetConcurrency(t *testing.T) {
	nthread := GetConcurrency(0)
	require.Equal(t, int64(system.GoMaxProcs()), nthread)

	concurrent := int64(64)
	nthread = GetConcurrency(concurrent)
	require.Equal(t, concurrent, nthread)

	nthread = GetConcurrencyForBuild(0)
	require.Equal(t, int64(system.GoMaxProcs()), nthread)

	nthread = GetConcurrencyForBuild(4)
	require.Equal(t, int64(4), nthread)

	// A container can see many host CPUs while its scheduler is quota-limited.
	// Defaults must use that effective limit; explicit settings remain honored.
	require.Equal(t, int64(2), resolveConcurrency(0, 2))
	require.Equal(t, int64(2), resolveConcurrency(-1, 2))
	require.Equal(t, int64(64), resolveConcurrency(64, 2))
	require.Equal(t, int64(1), resolveConcurrency(0, 0))
}

func TestFastMaxHeap(t *testing.T) {
	limit := 3
	keysBuf := make([]int64, limit)
	distsBuf := make([]float32, limit)

	h := NewFastMaxHeap(limit, keysBuf, distsBuf)

	// Add 5 items, we only want the 3 smallest distances
	h.Push(10, float32(10.0))
	h.Push(5, float32(5.0))
	h.Push(20, float32(20.0))
	h.Push(1, float32(1.0))
	h.Push(8, float32(8.0))

	// Expected distances in the heap (the 3 smallest): 1.0, 5.0, 8.0
	// Because it is a max-heap of the minimums, popping should return the largest distance first: 8.0, 5.0, 1.0

	key, dist, ok := h.Pop()
	require.True(t, ok)
	require.Equal(t, int64(8), key)
	require.Equal(t, float32(8.0), dist)

	key, dist, ok = h.Pop()
	require.True(t, ok)
	require.Equal(t, int64(5), key)
	require.Equal(t, float32(5.0), dist)

	key, dist, ok = h.Pop()
	require.True(t, ok)
	require.Equal(t, int64(1), key)
	require.Equal(t, float32(1.0), dist)

	_, _, ok = h.Pop()
	require.False(t, ok)
}

func TestFastMaxHeapSafe(t *testing.T) {
	limit := 5
	keysBuf := make([]int64, limit)
	distsBuf := make([]float32, limit)

	h := NewFastMaxHeapSafe(limit, keysBuf, distsBuf)

	var wg sync.WaitGroup
	// Push 100 elements concurrently. The 5 smallest should be 0, 1, 2, 3, 4
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(val int) {
			defer wg.Done()
			h.Push(int64(val), float32(val))
		}(i)
	}

	wg.Wait()

	// Because it's a bounded max-heap holding the K smallest distances,
	// popping should yield the largest of the top 5 first: 4, 3, 2, 1, 0
	for expected := 4; expected >= 0; expected-- {
		key, dist, ok := h.Pop()
		require.True(t, ok)
		require.Equal(t, int64(expected), key)
		require.Equal(t, float32(expected), dist)
	}

	_, _, ok := h.Pop()
	require.False(t, ok)
}
