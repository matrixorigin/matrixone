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

//go:build gpu

// The two GPU brute-force indexes charge different arenas: the ad-hoc index keeps
// its vectors in Go memory and charges host, the built index owns a device-resident
// cuVS index and charges device. Neither needs a GPU to answer GetIndexSize.
package brute_force

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// The ad-hoc index charges its Go-resident dataset to host and nothing to device;
// the device copy is transient, bounded by one search.
func TestGpuAdhocBruteForceIndexGetIndexSize(t *testing.T) {
	idx := &GpuAdhocBruteForceIndex[float32]{
		dataset:   []float32{1, 2, 3, 4, 5, 6},
		dimension: 3,
		count:     2,
		metric:    metric.Metric_L2Distance,
	}

	host, device := idx.GetIndexSize()
	require.Equal(t, int64(6*4), host)
	require.Equal(t, int64(0), device, "nothing is device resident between queries")

	require.NoError(t, idx.Preload(nil))
}

func TestGpuAdhocBruteForceIndexGetIndexSize_Empty(t *testing.T) {
	host, device := (&GpuAdhocBruteForceIndex[float32]{}).GetIndexSize()
	require.Equal(t, int64(0), host)
	require.Equal(t, int64(0), device)
}

// The built index charges device only; the Go copy is released at build time.
func TestGpuBruteForceIndexGetIndexSize(t *testing.T) {
	idx := &GpuBruteForceIndex[float32]{dimension: 128, count: 1000}

	host, device := idx.GetIndexSize()
	require.Equal(t, int64(0), host, "the Go copy is released at build time")
	require.Equal(t, int64(1000*128*4), device)

	require.NoError(t, idx.Preload(nil))
}

func TestGpuBruteForceIndexGetIndexSize_Empty(t *testing.T) {
	host, device := (&GpuBruteForceIndex[float32]{}).GetIndexSize()
	require.Equal(t, int64(0), host)
	require.Equal(t, int64(0), device)
}
