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

// GetIndexSize and Preload for the two CPU brute-force indexes. brute_force.go
// carries no build tag, so these run in the CPU and GPU builds alike.
package brute_force

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// The row-major index charges the vectors plus the per-row slice headers, and
// nothing to the device.
func TestGoBruteForceIndexGetIndexSize(t *testing.T) {
	const dim = 4
	dataset := [][]float32{
		{1, 2, 3, 4},
		{5, 6, 7, 8},
		{9, 10, 11, 12},
	}
	idx := &GoBruteForceIndex[float32, float32]{
		Dataset: dataset, Dimension: dim, Count: uint(len(dataset)),
	}

	wantElems := int64(len(dataset) * dim * 4)
	wantHeaders := int64(len(dataset)) * int64(unsafe.Sizeof([]float32{}))

	host, device := idx.GetIndexSize()
	require.Equal(t, wantElems+wantHeaders, host)
	require.Equal(t, int64(0), device)

	// Preload does not change what GetIndexSize reports.
	require.NoError(t, idx.Preload(nil))
	host2, _ := idx.GetIndexSize()
	require.Equal(t, host, host2)
}

// An empty dataset costs nothing.
func TestGoBruteForceIndexGetIndexSize_Empty(t *testing.T) {
	host, device := (&GoBruteForceIndex[float32, float32]{}).GetIndexSize()
	require.Equal(t, int64(0), host)
	require.Equal(t, int64(0), device)
}

// Ragged rows are charged by their actual lengths.
func TestGoBruteForceIndexGetIndexSize_Ragged(t *testing.T) {
	idx := &GoBruteForceIndex[float64, float64]{
		Dataset: [][]float64{{1, 2}, {3}, nil},
	}
	wantElems := int64(3 * 8)
	wantHeaders := int64(3) * int64(unsafe.Sizeof([]float64{}))

	host, _ := idx.GetIndexSize()
	require.Equal(t, wantElems+wantHeaders, host)
}

// The usearch index holds one flattened buffer; its cost is that buffer.
func TestUsearchBruteForceIndexGetIndexSize(t *testing.T) {
	flat := []float32{1, 2, 3, 4, 5, 6}
	idx := &UsearchBruteForceIndex[float32]{
		Dataset: &flat, Dimension: 3, Count: 2, MoMetric: metric.Metric_L2Distance,
	}

	host, device := idx.GetIndexSize()
	require.Equal(t, int64(len(flat)*4), host)
	require.Equal(t, int64(0), device)

	require.NoError(t, idx.Preload(nil))
}

// A nil dataset costs nothing.
func TestUsearchBruteForceIndexGetIndexSize_NilDataset(t *testing.T) {
	host, device := (&UsearchBruteForceIndex[float32]{}).GetIndexSize()
	require.Equal(t, int64(0), host)
	require.Equal(t, int64(0), device)
}
