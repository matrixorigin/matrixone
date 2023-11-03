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
	"github.com/matrixorigin/matrixone/pkg/vectorize/moarray"
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
				// NOTE: values of random initialization need not be farther apart, it is random.
				{1, 2, 4, 5},
				{1, 2, 3, 4},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRandomInitializer()
			if gotCentroids := r.InitCentroids(moarray.ToGonumVectors[float64](tt.args.vectors), tt.args.k); !reflect.DeepEqual(moarray.ToMoArrays[float64](gotCentroids), tt.wantCentroids) {
				t.Errorf("InitCentroids() = %v, want %v", moarray.ToMoArrays[float64](gotCentroids), tt.wantCentroids)
			}
		})
	}
}
