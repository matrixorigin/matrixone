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

package moarray

import (
	"gonum.org/v1/gonum/mat"
	"reflect"
	"testing"
)

func Test_ToGonumVectors(t *testing.T) {
	type args struct {
		vectors [][]float64
	}
	tests := []struct {
		name string
		args args
		want []*mat.VecDense
	}{
		{
			name: "Test1",
			args: args{
				vectors: [][]float64{{1, 2, 3}, {4, 5, 6}},
			},
			want: []*mat.VecDense{
				mat.NewVecDense(3, []float64{1, 2, 3}),
				mat.NewVecDense(3, []float64{4, 5, 6}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := ToGonumVectors[float64](tt.args.vectors...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToGonumVectors() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_ToMoArrays(t *testing.T) {
	type args struct {
		vectors []*mat.VecDense
	}
	tests := []struct {
		name string
		args args
		want [][]float64
	}{
		{
			name: "Test1",
			args: args{
				vectors: []*mat.VecDense{
					mat.NewVecDense(3, []float64{1, 2, 3}),
					mat.NewVecDense(3, []float64{4, 5, 6}),
				},
			},
			want: [][]float64{{1, 2, 3}, {4, 5, 6}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, _ := ToMoArrays[float64](tt.args.vectors); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToMoArrays() = %v, want %v", got, tt.want)
			}
		})
	}
}
