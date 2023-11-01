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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionAgg/algos/kmeans"
	"math/rand"
	"reflect"
	"testing"
)

func TestRandom_InitCentroids(t *testing.T) {
	type args struct {
		vectors [][]float64
		k       int
	}
	tests := []struct {
		name          string
		args          args
		wantCentroids [][]float64
	}{
		{
			name: "TestRandom_InitCentroids",
			args: args{
				vectors: [][]float64{
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4, 5},
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4, 5},
					{10, 2, 4, 5},
					{10, 3, 4, 5},
					{10, 5, 4, 5},
					{10, 2, 4, 5},
					{10, 3, 4, 5},
					{10, 5, 4, 5},
				},
				k: 2,
			},
			wantCentroids: [][]float64{
				// NOTE: values of random initialization need not be farther apart.
				{1, 2, 4, 5},
				{1, 2, 3, 4},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRandomInitializer()
			if gotCentroids := r.InitCentroids(ToGonumsVectors(tt.args.vectors), tt.args.k); !reflect.DeepEqual(gotCentroids, tt.wantCentroids) {
				t.Errorf("InitCentroids() = %v, want %v", gotCentroids, tt.wantCentroids)
			}
		})
	}
}

/*
date : 2023-10-30
goos: darwin
goarch: arm64
cpu: Apple M2 Pro
rows: 10_000
dims: 1024
k : 10
Benchmark_InitCentroids/RANDOM-10         	13193077	        86.51 ns/op  (gonums not applicable)
*/
func Benchmark_InitCentroids(b *testing.B) {
	rowCnt := 10_000
	dims := 1024
	k := 10

	data := make([][]float64, rowCnt)
	populateRandData(rowCnt, dims, data)

	random := NewRandomInitializer()

	b.Run("RANDOM", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = random.InitCentroids(ToGonumsVectors(data), k)
		}
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
