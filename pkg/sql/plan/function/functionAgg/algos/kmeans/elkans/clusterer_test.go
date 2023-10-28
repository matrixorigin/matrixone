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

func TestElkansKMeansClusterer_Cluster(t *testing.T) {
	type kmeansArg struct {
		vectorList     [][]float64
		clusterCnt     int
		maxIterations  int
		deltaThreshold float64
		distType       kmeans.DistanceType
	}
	tests := []struct {
		name    string
		fields  kmeansArg
		want    [][]float64
		wantErr bool
	}{
		{
			name: "Test 1",
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
					//{7, 7, 2, 6},
					//{3, 9, 1, 6},
					//{8, 1, 2, 6},
					//{8, 5, 3, 1},
					//{1, 4, 2, 7},
					//{10, 5, 9, 3},
					//{10, 9, 2, 1},
					//{2, 4, 6, 9},
					//{9, 9, 2, 7},
					//{5, 10, 8, 7},
					//{10, 1, 9, 5},
					//{7, 9, 2, 1},
				},
				clusterCnt:     2,
				maxIterations:  500,
				deltaThreshold: 0.01,
				distType:       kmeans.L2,
			},
			want: [][]float64{
				{1, 2, 3.6666666666666665, 4.666666666666666},
				{10, 3.333333333333333, 4, 5},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kmeans, err := NewElkansKMeans(tt.fields.vectorList, tt.fields.clusterCnt, tt.fields.maxIterations, tt.fields.deltaThreshold, tt.fields.distType)
			got, err := kmeans.Cluster()
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
