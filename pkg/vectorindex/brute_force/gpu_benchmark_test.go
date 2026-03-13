//go:build gpu

// Copyright 2022 Matrix Origin
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

package brute_force

import (
	"math/rand/v2"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

func BenchmarkGpuBruteForce(b *testing.B) {
	benchmarkBruteForce(b, NewGpuBruteForceIndex[float32])
}

func BenchmarkCentroidSearchGpuBruteForce(b *testing.B) {
	benchmarkCentroidSearch(b, NewGpuBruteForceIndex[float32])
}

func BenchmarkGpuAdhocBruteForce(b *testing.B) {
	benchmarkBruteForce(b, func(dataset [][]float32, dim uint, m metric.MetricType, es uint, nt uint) (cache.VectorIndexSearchIf, error) {
		return NewGpuAdhocBruteForceIndex[float32](dataset, dim, m, es)
	})
}

func BenchmarkCentroidSearchGpuAdhocBruteForce(b *testing.B) {
	benchmarkCentroidSearch(b, func(dataset [][]float32, dim uint, m metric.MetricType, es uint, nt uint) (cache.VectorIndexSearchIf, error) {
		return NewGpuAdhocBruteForceIndex[float32](dataset, dim, m, es)
	})
}

func BenchmarkGpuAdhocBruteForceSingle(b *testing.B) {
	dsize := 10000
	dimension := uint(1024)
	limit := uint(10)
	elemsz := uint(4) // float32

	dataset := make([][]float32, dsize)
	for i := range dataset {
		dataset[i] = make([]float32, dimension)
		for j := range dataset[i] {
			dataset[i][j] = rand.Float32()
		}
	}

	query := make([][]float32, 1)
	query[0] = make([]float32, dimension)
	for j := range query[0] {
		query[0][j] = rand.Float32()
	}

	rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx, err := NewGpuAdhocBruteForceIndex[float32](dataset, dimension, metric.Metric_L2sqDistance, elemsz)
		if err != nil {
			b.Fatal(err)
		}
		_, _, err = idx.Search(nil, query, rt)
		if err != nil {
			b.Fatal(err)
		}
		idx.Destroy()
	}
}
