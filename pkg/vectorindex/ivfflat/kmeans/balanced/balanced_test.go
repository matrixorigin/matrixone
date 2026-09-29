// Copyright 2024 Matrix Origin
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

package balanced

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func Test_NewKMeans_GCStress(t *testing.T) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "info",
		Format: "json",
	})

	vectors := [][]float64{
		{1, 2, 3, 4},
		{1, 2, 4, 5},
		{10, 2, 4, 5},
		{10, 3, 4, 5},
		{10, 5, 4, 5},
		{11, 6, 4, 5},
		{12, 7, 4, 5},
		{13, 8, 4, 5},
	}

	for i := 0; i < 128; i++ {
		km, err := NewKMeans[float64](vectors, 2, 10, 0.01, metric.Metric_L2Distance, false, 1)
		require.NoError(t, err)
		require.NoError(t, km.Close())
		runtime.GC()
	}
}

func TestNewKMeans_Validation(t *testing.T) {
	vectors := [][]float32{{1, 2}, {3, 4}, {5, 6}}

	// Valid
	_, err := NewKMeans(vectors, 2, 10, 0.01, metric.Metric_L2Distance, false, 1)
	require.NoError(t, err)

	// Cluster count too high
	_, err = NewKMeans(vectors, 4, 10, 0.01, metric.Metric_L2Distance, false, 1)
	require.Error(t, err)

	// Dimension mismatch
	mismatch := [][]float32{{1, 2}, {3, 4, 5}}
	_, err = NewKMeans(mismatch, 2, 10, 0.01, metric.Metric_L2Distance, false, 1)
	require.Error(t, err)

	// Empty vectors
	_, err = NewKMeans([][]float32{}, 2, 10, 0.01, metric.Metric_L2Distance, false, 1)
	require.Error(t, err)
}

func TestBalancedKMeans_Basic(t *testing.T) {
	ctx := context.Background()
	// 8 points in 2D
	vectors := [][]float32{
		{1, 1}, {1.1, 1.1}, {0.9, 0.9}, {1, 0.9},
		{10, 10}, {10.1, 10.1}, {9.9, 9.9}, {10, 9.9},
	}

	km, err := NewKMeans(vectors, 2, 10, 0.01, metric.Metric_L2Distance, false, 2)
	require.NoError(t, err)

	res, err := km.Cluster(ctx)
	require.NoError(t, err)

	centroids := res.([][]float32)
	require.Equal(t, 2, len(centroids))

	// Verify assignments
	bkm := km.(*BalancedKMeans[float32])
	counts := make(map[int]int)
	for _, a := range bkm.assignments {
		counts[a]++
	}

	// Should be perfectly balanced: 4 points each
	require.Equal(t, 2, len(counts))
	require.Equal(t, 4, counts[0])
	require.Equal(t, 4, counts[1])

	sse, err := km.SSE()
	require.NoError(t, err)
	require.True(t, sse > 0)
}

func TestBalancedKMeans_K1(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{1, 1}, {2, 2}, {3, 3}}
	km, err := NewKMeans(vectors, 1, 10, 0.01, metric.Metric_L2Distance, false, 1)
	require.NoError(t, err)

	res, err := km.Cluster(ctx)
	require.NoError(t, err)
	centroids := res.([][]float32)
	require.Equal(t, 1, len(centroids))
	require.InDelta(t, 2.0, centroids[0][0], 1e-6)
}

func TestBalancedKMeans_KN(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{1, 1}, {2, 2}, {3, 3}}
	km, err := NewKMeans(vectors, 3, 10, 0.01, metric.Metric_L2Distance, false, 1)
	require.NoError(t, err)

	res, err := km.Cluster(ctx)
	require.NoError(t, err)
	centroids := res.([][]float32)
	require.Equal(t, 3, len(centroids))
}

func TestBalancedKMeans_Spherical(t *testing.T) {
	ctx := context.Background()
	// Vectors on unit circle
	vectors := [][]float32{
		{1, 0}, {0.99, 0.1},
		{0, 1}, {0.1, 0.99},
	}
	km, err := NewKMeans(vectors, 2, 10, 0.01, metric.Metric_CosineDistance, true, 1)
	require.NoError(t, err)

	res, err := km.Cluster(ctx)
	require.NoError(t, err)
	centroids := res.([][]float32)

	// Check if centroids are normalized
	for _, c := range centroids {
		norm := float32(0)
		for _, v := range c {
			norm += v * v
		}
		require.InDelta(t, 1.0, math.Sqrt(float64(norm)), 1e-5)
	}
}

func FakeErrorDistance[T types.RealNumbers](v1, v2 []T) (T, error) {
	return 0, moerr.NewInternalErrorNoCtx("distance calculation failed")
}

func TestBalancedKMeans_DistanceError(t *testing.T) {
	ctx := context.Background()
	vectors := [][]float32{{1, 1}, {2, 2}, {3, 3}, {4, 4}}
	km, err := NewKMeans(vectors, 2, 10, 0.01, metric.Metric_L2Distance, false, 1)
	require.NoError(t, err)

	bkm := km.(*BalancedKMeans[float32])
	bkm.distFn = FakeErrorDistance[float32]

	_, err = km.Cluster(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "distance calculation failed")
}

func TestBalancedKMeans_LargeBalanced(t *testing.T) {
	ctx := context.Background()
	n := 1000
	k := 10
	dim := 16
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = float32(i % (j + 1))
		}
	}

	km, err := NewKMeans(vectors, k, 20, 0.01, metric.Metric_L2Distance, false, 8)
	require.NoError(t, err)

	_, err = km.Cluster(ctx)
	require.NoError(t, err)

	bkm := km.(*BalancedKMeans[float32])
	counts := make(map[int]int)
	for _, a := range bkm.assignments {
		counts[a]++
	}

	require.Equal(t, k, len(counts))
	for i := 0; i < k; i++ {
		// 1000 / 10 = 100 per cluster
		require.Equal(t, 100, counts[i], fmt.Sprintf("Cluster %d is not balanced", i))
	}
}

func BenchmarkBalancedKMeans(b *testing.B) {
	ctx := context.Background()
	n := 10000
	k := 100
	dim := 128
	vectors := make([][]float32, n)
	for i := 0; i < n; i++ {
		vectors[i] = make([]float32, dim)
		for j := 0; j < dim; j++ {
			vectors[i][j] = rand.Float32()
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		km, _ := NewKMeans(vectors, k, 15, 0.01, metric.Metric_L2Distance, false, 8)
		_, _ = km.Cluster(ctx)
		_ = km.Close()
	}
}
