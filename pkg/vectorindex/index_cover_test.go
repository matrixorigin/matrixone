// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vectorindex

import (
	"container/heap"
	"crypto/md5"
	"encoding/hex"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- index.go: checksums ---------------------------------------------------

// CheckSum over a file and CheckSumFromBuffer over the same bytes agree.
func TestCheckSum(t *testing.T) {
	content := []byte("hnsw model bytes")
	path := filepath.Join(t.TempDir(), "model.bin")
	require.NoError(t, os.WriteFile(path, content, 0o644))

	want := hex.EncodeToString(func() []byte { s := md5.Sum(content); return s[:] }())

	got, err := CheckSum(path)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, want, CheckSumFromBuffer(content))
}

func TestCheckSum_MissingFile(t *testing.T) {
	_, err := CheckSum(filepath.Join(t.TempDir(), "nope.bin"))
	require.Error(t, err)
}

// --- index.go: SearchResultHeap --------------------------------------------

// SearchResultHeap is a min-heap on distance: popping yields nearest-first.
func TestSearchResultHeap(t *testing.T) {
	h := &SearchResultHeap{}
	heap.Init(h)
	for _, d := range []float64{3, 1, 2} {
		heap.Push(h, &SearchResult{Id: int64(d), Distance: d})
	}
	require.Equal(t, 3, h.Len())

	var got []float64
	for h.Len() > 0 {
		got = append(got, heap.Pop(h).(SearchResultIf).GetDistance())
	}
	require.Equal(t, []float64{1, 2, 3}, got)
}

// The any-key variant carries a non-int64 pk unchanged.
func TestSearchResultAnyKey(t *testing.T) {
	r := &SearchResultAnyKey{Id: "pk", Distance: 1.5}
	require.Equal(t, 1.5, r.GetDistance())
	require.Equal(t, "pk", r.Id)
}

// --- index.go: FastMaxHeap -------------------------------------------------

// FastMaxHeap keeps the k smallest distances. Once full it admits an element only
// if it beats the root, the largest kept distance, which Peek reports.
func TestFastMaxHeap_LenFullPeek(t *testing.T) {
	const limit = 3
	h := NewFastMaxHeap[float64, int64](limit, make([]int64, limit), make([]float64, limit))

	require.Equal(t, 0, h.Len())
	require.False(t, h.Full())
	_, _, ok := h.Peek()
	require.False(t, ok, "an empty heap has no threshold")

	for i, d := range []float64{5, 1, 3} {
		h.Push(int64(i), d)
	}
	require.Equal(t, limit, h.Len())
	require.True(t, h.Full())

	_, worst, ok := h.Peek()
	require.True(t, ok)
	require.Equal(t, 5.0, worst, "the root is the largest kept distance")

	// A worse candidate is rejected; a better one evicts the root.
	h.Push(99, 100)
	_, worst, _ = h.Peek()
	require.Equal(t, 5.0, worst)

	h.Push(42, 2)
	_, worst, _ = h.Peek()
	require.Equal(t, 3.0, worst)

	// Draining yields the kept distances largest-first.
	var got []float64
	for {
		_, d, ok := h.Pop()
		if !ok {
			break
		}
		got = append(got, d)
	}
	require.Equal(t, []float64{3, 2, 1}, got)
}

// The mutex wrapper delegates to the same bounded heap.
func TestFastMaxHeapSafe_Delegates(t *testing.T) {
	const limit = 2
	h := NewFastMaxHeapSafe[float64, int64](limit, make([]int64, limit), make([]float64, limit))
	h.Push(1, 5)
	h.Push(2, 1)
	h.Push(3, 9)

	k, d, ok := h.Pop()
	require.True(t, ok)
	require.Equal(t, int64(1), k)
	require.Equal(t, 5.0, d)
}

// --- limit.go --------------------------------------------------------------

// Overflow saturates at MaxUint64.
func TestSaturatingAddUint64(t *testing.T) {
	require.Equal(t, uint64(3), SaturatingAddUint64(1, 2))
	require.Equal(t, uint64(math.MaxUint64), SaturatingAddUint64(math.MaxUint64, 1))
	require.Equal(t, uint64(math.MaxUint64), SaturatingAddUint64(math.MaxUint64, math.MaxUint64))
	require.Equal(t, uint64(math.MaxUint64), SaturatingAddUint64(math.MaxUint64, 0))
}

// SearchResultPreallocate caps the preallocation at 1<<20.
func TestSearchResultPreallocate(t *testing.T) {
	require.Equal(t, 10, SearchResultPreallocate(10))
	require.Equal(t, 1<<20, SearchResultPreallocate(math.MaxUint64))
	require.Equal(t, 0, SearchResultPreallocate(0))
}
