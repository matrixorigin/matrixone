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

func TestQuantization(t *testing.T) {
	q, ok := QuantizationValid("f16")
	require.True(t, ok)
	require.Equal(t, q, usearch.F16)
	_, ok = QuantizationValid("")
	require.False(t, ok)
}

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
