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

package elkans

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"strconv"
	"testing"
)

/*
date : 2023-10-31
goos: darwin
goarch: arm64
cpu: Apple M2 Pro
rows: 10_000
dims: 1024
k: 10
Benchmark_kmeans/KMEANS_-_Random-10         	       1	  1335777583 ns/op (with gonums)
Benchmark_kmeans/KMEANS_-_Kmeans++-10       	       1	  3190817000 ns/op (with gonums)

rows: 100_000
dims: 1024
k: 100
Benchmark_kmeans/KMEANS_-_Random-10         	       1	177648962458 ns/op
*/
func Benchmark_kmeans(b *testing.B) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "debug",
		Format: "console",
	})

	rowCnt := 10_000
	dims := 1024
	k := 10

	data := make([][]float64, rowCnt)
	populateRandData(rowCnt, dims, data)

	clusterRand, _ := NewKMeans(data, k,
		500, 0.01,
		kmeans.L2, kmeans.Random)

	kmeansPlusPlus, _ := NewKMeans(data, k,
		500, 0.01,
		kmeans.L2, kmeans.KmeansPlusPlus)

	b.Run("Elkan_Random", func(b *testing.B) {
		b.ResetTimer()
		_, err := clusterRand.Cluster()
		if err != nil {
			panic(err)
		}
	})
	b.Log("SSE - clusterRand", strconv.FormatFloat(clusterRand.SSE(), 'f', -1, 32))

	b.Run("Elkan_Kmeans++", func(b *testing.B) {
		b.ResetTimer()
		_, err := kmeansPlusPlus.Cluster()
		if err != nil {
			panic(err)
		}

	})
	b.Log("SSE - clusterRand", strconv.FormatFloat(kmeansPlusPlus.SSE(), 'f', -1, 32))

}
