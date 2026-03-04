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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

func benchmarkBruteForce(b *testing.B, createFn func([][]float32, uint, metric.MetricType, uint, uint) (cache.VectorIndexSearchIf, error)) {
	b.Helper()
	m := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(b, "", m)
	sqlproc := sqlexec.NewSqlProcess(proc)
	dimension := uint(128)
	ncpu := uint(8)
	limit := uint(10)
	elemsz := uint(4) // float32

	dsize := 10000
	dataset := make([][]float32, dsize)
	for i := range dataset {
		dataset[i] = make([]float32, dimension)
		for j := range dataset[i] {
			dataset[i][j] = rand.Float32()
		}
	}

	qsize := 100
	query := make([][]float32, qsize)
	for i := range query {
		query[i] = make([]float32, dimension)
		for j := range query[i] {
			query[i][j] = rand.Float32()
		}
	}

	idx, err := createFn(dataset, dimension, metric.Metric_L2sqDistance, elemsz, ncpu)
	if err != nil {
		b.Fatal(err)
	}
	defer idx.Destroy()

	err = idx.Load(sqlproc)
	if err != nil {
		b.Fatal(err)
	}

	rt := vectorindex.RuntimeConfig{Limit: limit, NThreads: ncpu}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, err := idx.Search(sqlproc, query, rt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGoBruteForce(b *testing.B) {
	benchmarkBruteForce(b, func(dataset [][]float32, dim uint, m metric.MetricType, es uint, nt uint) (cache.VectorIndexSearchIf, error) {
		return NewGoBruteForceIndex[float32](dataset, dim, m, es)
	})
}

func BenchmarkUsearchBruteForce(b *testing.B) {
	benchmarkBruteForce(b, func(dataset [][]float32, dim uint, m metric.MetricType, es uint, nt uint) (cache.VectorIndexSearchIf, error) {
		return NewUsearchBruteForceIndex[float32](dataset, dim, m, es)
	})
}

func BenchmarkGpuBruteForce(b *testing.B) {
	benchmarkBruteForce(b, NewGpuBruteForceIndex[float32])
}
