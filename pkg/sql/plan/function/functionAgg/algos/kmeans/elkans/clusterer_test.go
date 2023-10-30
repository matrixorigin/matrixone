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
	"reflect"
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
	}{
		{
			name: "Test 1 - Skewed data",
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
				},
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2,
				initType:       kmeans.Random,
			},
			want: [][]float64{
				{1, 2, 3.611111111111111, 4.611111111111112},
				{10, 3.777777777777778, 4, 5},
			},
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
		})
	}
}
