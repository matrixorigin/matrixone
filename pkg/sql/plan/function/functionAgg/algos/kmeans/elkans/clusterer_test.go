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
	"reflect"
	"strconv"
	"testing"
)

func Test_NewKMeans(t *testing.T) {
	type kmeansArg struct {
		vectorList     [][]float64
		clusterCnt     int
		maxIterations  int
		deltaThreshold float64
		distType       kmeans.DistanceType
		initType       kmeans.InitType
	}
	tests := []struct {
		name    string
		fields  kmeansArg
		wantErr bool
	}{
		{
			name: "Test 1 - Dimension mismatch",
			fields: kmeansArg{
				vectorList: [][]float64{
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4},
				},
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2,
				initType:       kmeans.Random,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewKMeans(tt.fields.vectorList, tt.fields.clusterCnt,
				tt.fields.maxIterations, tt.fields.deltaThreshold,
				tt.fields.distType, tt.fields.initType)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewKMeans() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_Cluster(t *testing.T) {
	logutil.SetupMOLogger(&logutil.LogConfig{
		Level:  "debug",
		Format: "console",
	})
	type kmeansArg struct {
		vectorList     [][]float64
		clusterCnt     int
		maxIterations  int
		deltaThreshold float64
		distType       kmeans.DistanceType
		initType       kmeans.InitType
	}
	tests := []struct {
		name    string
		fields  kmeansArg
		want    [][]float64
		wantErr bool
		wantSSE float64
	}{
		{
			name: "Test 1 - Skewed data (Random Init)",
			fields: kmeansArg{
				vectorList: [][]float64{
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
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2,
				initType:       kmeans.Random,
			},
			want: [][]float64{
				{10, 3.3333333333333335, 4, 5},
				{1, 2, 3.6666666666666665, 4.666666666666667},
			},
			wantSSE: 12,
			wantErr: false,
		},
		{
			name: "Test 2 - Skewed data (Kmeans++ Init)",
			fields: kmeansArg{
				vectorList: [][]float64{
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
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2,
				initType:       kmeans.KmeansPlusPlus,
			},
			want: [][]float64{
				{1, 2, 3.6666666666666665, 4.666666666666667},
				{10, 3.3333333333333335, 4, 5},
			},
			wantSSE: 12,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterer, _ := NewKMeans(tt.fields.vectorList, tt.fields.clusterCnt,
				tt.fields.maxIterations, tt.fields.deltaThreshold,
				tt.fields.distType, tt.fields.initType)
			got, err := clusterer.Cluster()
			if (err != nil) != tt.wantErr {
				t.Errorf("Cluster() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Cluster() got = %v, want %v", got, tt.want)
			}

			if tt.wantSSE != clusterer.SSE() {
				t.Errorf("SSE() got = %v, want %v", clusterer.SSE(), tt.wantSSE)
			}
		})
	}
}

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

func TestElkanClusterer_recalculateCentroids(t *testing.T) {
	type kmeansArg struct {
		vectorList     [][]float64
		clusterCnt     int
		maxIterations  int
		deltaThreshold float64
		distType       kmeans.DistanceType
		initType       kmeans.InitType
	}
	tests := []struct {
		name   string
		fields kmeansArg
		want   [][]float64
	}{
		{
			name: "Test 1 - Skewed data (Random Init)",
			fields: kmeansArg{
				vectorList: [][]float64{
					{1, 2, 3, 4},
					{1, 2, 4, 5},
					{1, 2, 4, 5},
					{1, 2, 3, 4},
					{1, 2, 4, 5},
				},
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2,
				initType:       kmeans.Random,
			},
			want: [][]float64{
				{1, 2, 3, 4},
				{1, 2, 4, 5},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			km, err := NewKMeans(tt.fields.vectorList, tt.fields.clusterCnt,
				tt.fields.maxIterations, tt.fields.deltaThreshold,
				tt.fields.distType, tt.fields.initType)

			if err != nil {
				t.Errorf("Error while creating KMeans object %v", err)
			}
			if ekm, ok := km.(*ElkanClusterer); ok {
				ekm.InitCentroids()
				ekm.initBounds()
				if got := ekm.recalculateCentroids(); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("recalculateCentroids() = %v, want %v", got, tt.want)
				}
			} else if !ok {
				t.Errorf("km not of type ElkanClusterer")
			}

		})
	}
}
