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
			if gotCentroids := r.InitCentroids(tt.args.vectors, tt.args.k); !reflect.DeepEqual(gotCentroids, tt.wantCentroids) {
				t.Errorf("InitCentroids() = %v, want %v", gotCentroids, tt.wantCentroids)
			}
		})
	}
}

func TestKMeansPlusPlus_InitCentroids(t *testing.T) {
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
			name: "TestKMeansPlusPlus_InitCentroids",
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
			// Kmeans++ picked the relatively farthest points as the initial centroids
			wantCentroids: [][]float64{
				{1, 2, 4, 5},
				{10, 5, 4, 5},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewKMeansPlusPlusInitializer(L2Distance)
			if gotCentroids := r.InitCentroids(tt.args.vectors, tt.args.k); !reflect.DeepEqual(gotCentroids, tt.wantCentroids) {
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
rows: 1000
dims: 1024

k:10
Benchmark_InitCentroids/RANDOM-10         	13396052	        85.93 ns/op
Benchmark_InitCentroids/KMEANS++-10       	       2	 	948731542 ns/op

k: 100
Benchmark_InitCentroids/RANDOM-10         	 1778432	       	 627.9 ns/op
Benchmark_InitCentroids/KMEANS++-10       	       1	104728047959.0 ns/op
*/
func Benchmark_InitCentroids(b *testing.B) {
	rowCnt := 1_000
	dims := 1024
	k := 10
	data := make([][]float64, rowCnt)
	loadData(rowCnt, dims, data)

	random := NewRandomInitializer()
	kmeanspp := NewKMeansPlusPlusInitializer(L2Distance)

	b.Run("RANDOM", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = random.InitCentroids(data, k)
		}
	})

	b.Run("KMEANS++", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = kmeanspp.InitCentroids(data, k)
		}
	})
}

func loadData(nb int, d int, xb [][]float64) {
	for r := 0; r < nb; r++ {
		xb[r] = make([]float64, d)
		for c := 0; c < d; c++ {
			xb[r][c] = float64(rand.Float32() * 1000)
		}
	}
}
