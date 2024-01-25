// Copyright 2023 Matrix Origin
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

package faiss

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
)

/*
date : 2023-10-31
goos: darwin
goarch: arm64
cpu: Apple M2 Pro
row count: 15_000
dims: 1024
k: 15
Benchmark_kmeans/Spherical_Elkan_Random-10         	       1	10842563917 ns/op
Benchmark_kmeans/Spherical_Elkan_Kmeans++-10       	       1	13850015875 ns/op
*/
func Benchmark_kmeans(b *testing.B) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "debug",
		Format: "console",
	})

	rowCnt := 1000_000
	dims := 128
	k := 1000

	data := make([][]float64, rowCnt)
	populateRandData(rowCnt, dims, data)

	b.Run("Faiss_Clustering", func(b *testing.B) {
		b.ResetTimer()
		clusterRand, _ := NewFaiss(data, k)
		val, err := clusterRand.Cluster()
		if err != nil {
			panic(err)
		}
		assert.Equal(b, len(val), k)
		b.Log("SSE - clusterRand", strconv.FormatFloat(clusterRand.SSE(), 'f', -1, 32))

	})

}

func populateRandData(rowCnt int, dim int, vecs [][]float64) {
	random := rand.New(rand.NewSource(kmeans.DefaultRandSeed))
	for r := 0; r < rowCnt; r++ {
		vecs[r] = make([]float64, dim)
		for c := 0; c < dim; c++ {
			vecs[r][c] = float64(random.Float32() * 1000)
		}
	}
}
