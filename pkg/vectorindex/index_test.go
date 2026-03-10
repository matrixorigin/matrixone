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
	"runtime"
	"sync"
	"testing"

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

func TestConcurrent(t *testing.T) {

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

	nthread := 64
	err = index.ChangeThreadsSearch(uint(nthread))
	if err != nil {
		panic("failed to set threads_search")
	}

	for i := 0; i < vectorsCount; i++ {
		err = index.Add(usearch.Key(i), []float32{float32(i), float32(i + 1), float32(i + 2)})
		if err != nil {
			panic("Failed to add")
		}
	}

	var wg sync.WaitGroup

	for i := 0; i < nthread; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20000; j++ {

				keys, distances, err := index.Search([]float32{0.0, 1.0, 2.0}, 3)
				if err != nil {
					panic("Failed to search")
				}
				require.Equal(t, len(keys), 3)
				require.Equal(t, keys[0], uint64(0))
				require.Equal(t, distances[0], float32(0))
				//fmt.Println(keys, distances)
			}
		}()
	}

	wg.Wait()
}

func TestChecksum(t *testing.T) {
	_, err := CheckSum("abc")
	require.NotNil(t, err)
}

func TestGetConcurrency(t *testing.T) {
	nthread := GetConcurrency(0)
	require.Equal(t, int64(runtime.NumCPU()), nthread)

	concurrent := int64(64)
	nthread = GetConcurrency(concurrent)
	require.Equal(t, concurrent, nthread)

	nthread = GetConcurrencyForBuild(0)
	require.Equal(t, int64(runtime.NumCPU()), nthread)

	nthread = GetConcurrencyForBuild(4)
	require.Equal(t, int64(4), nthread)

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
