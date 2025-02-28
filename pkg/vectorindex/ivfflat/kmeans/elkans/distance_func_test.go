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
	"testing"
)

func Test_L2Distance(t *testing.T) {
	type args struct {
		v1 []float64
		v2 []float64
	}
	tests := []struct {
		name string
		args args
		want float64
	}{
		{
			name: "Test 1",
			args: args{
				v1: []float64{1, 2, 3, 4},
				v2: []float64{1, 2, 4, 5},
			},
			want: 1.4142135623730951,
		},
		{
			name: "Test 2",
			args: args{
				v1: []float64{10, 20, 30, 40},
				v2: []float64{10.5, 21.5, 31.5, 43.5},
			},
			want: 4.123105625617661,
		},
		{
			name: "Test 3.a",
			args: args{
				v1: []float64{1, 1},
				v2: []float64{4, 1},
			},
			want: 3,
		},
		{
			name: "Test 3.b",
			args: args{
				v1: []float64{4, 1},
				v2: []float64{1, 4},
			},
			want: 4.242640687119286,
		},
		{
			name: "Test 3.c",
			args: args{
				v1: []float64{1, 4},
				v2: []float64{1, 1},
			},
			want: 3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := L2Distance(moarray.ToGonumVector[float64](tt.args.v1), moarray.ToGonumVector[float64](tt.args.v2)); got != tt.want {
				t.Errorf("L2Distance() = %v, want %v", got, tt.want)
			}
		})
	}
}

//func Test_AngularDistance(t *testing.T) {
//	type args struct {
//		v1 []float64
//		v2 []float64
//	}
//	tests := []struct {
//		name string
//		args args
//		want float64
//	}{
//		{
//			name: "Test 1",
//			args: args{
//				v1: []float64{1, 2, 3, 4},
//				v2: []float64{1, 2, 4, 5},
//			},
//			want: 0,
//		},
//		{
//			name: "Test 2",
//			args: args{
//				v1: []float64{10, 20, 30, 40},
//				v2: []float64{10.5, 21.5, 31.5, 43.5},
//			},
//			want: 0,
//		},
//		// Test 3:  Triangle Inequality check on **un-normalized** vector
//		// A(1,0),B(2,2), C(0,1) => AB + AC !>= BC => 0 + 0 !>= 0.5
//		{
//			name: "Test 3.a",
//			args: args{
//				v1: []float64{1, 0},
//				v2: []float64{2, 2},
//			},
//			want: 0,
//		},
//		{
//			name: "Test 3.b",
//			args: args{
//				v1: []float64{2, 2},
//				v2: []float64{0, 1},
//			},
//			want: 0,
//		},
//		{
//			name: "Test 3.c",
//			args: args{
//				v1: []float64{0, 1},
//				v2: []float64{1, 0},
//			},
//			want: 0.5,
//		},
//		// Test 4: Triangle Inequality check on **normalized** vector
//		// A(1,0),B(2,2), C(0,1) => AB + AC >= BC => 0.25 + 0.25 >= 0.5
//		//{
//		//	name: "Test 4.a",
//		//	args: args{
//		//		v1: moarray.NormalizeMoVecf64([]float64{1, 0}),
//		//		v2: moarray.NormalizeMoVecf64([]float64{2, 2}),
//		//	},
//		//	want: 0.25000000000000006,
//		//},
//		//{
//		//	name: "Test 4.b",
//		//	args: args{
//		//		v1: moarray.NormalizeMoVecf64([]float64{2, 2}),
//		//		v2: moarray.NormalizeMoVecf64([]float64{0, 1}),
//		//	},
//		//	want: 0.25000000000000006,
//		//},
//		//{
//		//	name: "Test 4.c",
//		//	args: args{
//		//		v1: moarray.NormalizeMoVecf64([]float64{0, 1}),
//		//		v2: moarray.NormalizeMoVecf64([]float64{1, 0}),
//		//	},
//		//	want: 0.5,
//		//},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//
//			if got := SphericalDistance(moarray.ToGonumVector[float64](tt.args.v1), moarray.ToGonumVector[float64](tt.args.v2)); !assertx.InEpsilonF64(got, tt.want) {
//				t.Errorf("SphericalDistance() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
