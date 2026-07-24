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

package metric

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/assertx"
)

func Test_L2Distance_F32(t *testing.T) {
	type args struct {
		v1 []float32
		v2 []float32
	}
	tests := []struct {
		name string
		args args
		want float32
	}{
		{
			name: "Test 1",
			args: args{
				v1: []float32{1, 2, 3, 4},
				v2: []float32{1, 2, 4, 5},
			},
			want: 1.4142135623730951,
		},
		{
			name: "Test 2",
			args: args{
				v1: []float32{10, 20, 30, 40},
				v2: []float32{10.5, 21.5, 31.5, 43.5},
			},
			want: 4.123105625617661,
		},
		{
			name: "Test 3.a",
			args: args{
				v1: []float32{1, 1},
				v2: []float32{4, 1},
			},
			want: 3,
		},
		{
			name: "Test 3.b",
			args: args{
				v1: []float32{4, 1},
				v2: []float32{1, 4},
			},
			want: 4.242640687119285,
		},
		{
			name: "Test 3.c",
			args: args{
				v1: []float32{1, 4},
				v2: []float32{1, 1},
			},
			want: 3,
		},
		{
			name: "Test 4",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			},
			want: 3.1622776601683795,
		},
		{
			name: "Test 5",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8},
			},
			want: 5.196152422706632,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := L2Distance[float32](tt.args.v1, tt.args.v2); err != nil || got != tt.want {
				t.Errorf("L2Distance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_L1Distance_F32(t *testing.T) {
	type args struct {
		v1 []float32
		v2 []float32
	}
	tests := []struct {
		name string
		args args
		want float32
	}{
		{
			name: "Test 1",
			args: args{
				v1: []float32{1, 2, 3, 4},
				v2: []float32{1, 2, 4, 5},
			},
			want: 2,
		},
		{
			name: "Test 2",
			args: args{
				v1: []float32{10, 20, 30, 40},
				v2: []float32{10.5, 21.5, 31.5, 43.5},
			},
			want: 7,
		},
		{
			name: "Test 3.a",
			args: args{
				v1: []float32{1, 1},
				v2: []float32{4, 1},
			},
			want: 3,
		},
		{
			name: "Test 3.b",
			args: args{
				v1: []float32{4, 1},
				v2: []float32{1, 4},
			},
			want: 6,
		},
		{
			name: "Test 3.c",
			args: args{
				v1: []float32{1, 4},
				v2: []float32{1, 1},
			},
			want: 3,
		},
		{
			name: "Test 4",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			},
			want: 10,
		},
		{
			name: "Test 5",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8},
			},
			want: 27,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := L1Distance[float32](tt.args.v1, tt.args.v2); err != nil || got != tt.want {
				t.Errorf("L1Distance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_CosineDistance_F32(t *testing.T) {
	type args struct {
		v1 []float32
		v2 []float32
	}
	tests := []struct {
		name string
		args args
		want float32
	}{
		{
			name: "Test 1",
			args: args{
				v1: []float32{1, 2, 3, 4},
				v2: []float32{1, 2, 4, 5},
			},
			want: 0.003993481192393733,
		},
		{
			name: "Test 2",
			args: args{
				v1: []float32{10, 20, 30, 40},
				v2: []float32{10.5, 21.5, 31.5, 43.5},
			},
			want: 0.0001253573895874105,
		},
		{
			name: "Test 3.a",
			args: args{
				v1: []float32{1, 1},
				v2: []float32{4, 1},
			},
			want: 0.1425070742874559,
		},
		{
			name: "Test 3.b",
			args: args{
				v1: []float32{4, 1},
				v2: []float32{1, 4},
			},
			want: 0.5294117647058824,
		},
		{
			name: "Test 3.c",
			args: args{
				v1: []float32{1, 4},
				v2: []float32{1, 1},
			},
			want: 0.1425070742874559,
		},
		{
			name: "Test 4",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			},
			want: 0.0021238962030426523,
		},
		{
			name: "Test 5",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8},
			},
			want: 0.0025062434610066964,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := CosineDistance[float32](tt.args.v1, tt.args.v2); err != nil || got != tt.want {
				t.Errorf("CosineDistance() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_CosineSimilarity_F32(t *testing.T) {
	type args struct {
		v1 []float32
		v2 []float32
	}
	tests := []struct {
		name string
		args args
		want float32
	}{
		{
			name: "Test 1",
			args: args{
				v1: []float32{1, 2, 3, 4},
				v2: []float32{1, 2, 4, 5},
			},
			want: 0.9960065188076063,
		},
		{
			name: "Test 2",
			args: args{
				v1: []float32{10, 20, 30, 40},
				v2: []float32{10.5, 21.5, 31.5, 43.5},
			},
			want: 0.9998746426104126,
		},
		{
			name: "Test 3.a",
			args: args{
				v1: []float32{1, 1},
				v2: []float32{4, 1},
			},
			want: 0.8574929257125441,
		},
		{
			name: "Test 3.b",
			args: args{
				v1: []float32{4, 1},
				v2: []float32{1, 4},
			},
			want: 0.47058823529411764,
		},
		{
			name: "Test 3.c",
			args: args{
				v1: []float32{1, 4},
				v2: []float32{1, 1},
			},
			want: 0.8574929257125441,
		},
		{
			name: "Test 4",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			},
			want: 0.9978761037969573,
		},
		{
			name: "Test 5",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8},
			},
			want: 0.9974937565389933,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := CosineSimilarity[float32](tt.args.v1, tt.args.v2); err != nil || got != tt.want {
				t.Errorf("CosineSimilarity() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_InnerProduct_F32(t *testing.T) {
	type args struct {
		v1 []float32
		v2 []float32
	}
	tests := []struct {
		name string
		args args
		want float32
	}{
		{
			name: "Test 1",
			args: args{
				v1: []float32{1, 2, 3, 4},
				v2: []float32{1, 2, 4, 5},
			},
			want: -37,
		},
		{
			name: "Test 2",
			args: args{
				v1: []float32{10, 20, 30, 40},
				v2: []float32{10.5, 21.5, 31.5, 43.5},
			},
			want: -3220,
		},
		{
			name: "Test 3.a",
			args: args{
				v1: []float32{1, 1},
				v2: []float32{4, 1},
			},
			want: -5,
		},
		{
			name: "Test 3.b",
			args: args{
				v1: []float32{4, 1},
				v2: []float32{1, 4},
			},
			want: -8,
		},
		{
			name: "Test 3.c",
			args: args{
				v1: []float32{1, 4},
				v2: []float32{1, 1},
			},
			want: -5,
		},
		{
			name: "Test 4",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			},
			want: -440,
		},
		{
			name: "Test 5",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8},
			},
			want: -1048,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := InnerProduct[float32](tt.args.v1, tt.args.v2); err != nil || got != tt.want {
				t.Errorf("InnerProduct() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_L2DistanceSq_F32(t *testing.T) {
	type args struct {
		v1 []float32
		v2 []float32
	}
	tests := []struct {
		name string
		args args
		want float32
	}{
		{
			name: "Test 1",
			args: args{
				v1: []float32{1, 2, 3, 4},
				v2: []float32{1, 2, 4, 5},
			},
			want: 2,
		},
		{
			name: "Test 2",
			args: args{
				v1: []float32{10, 20, 30, 40},
				v2: []float32{10.5, 21.5, 31.5, 43.5},
			},
			want: 17,
		},
		{
			name: "Test 3.a",
			args: args{
				v1: []float32{1, 1},
				v2: []float32{4, 1},
			},
			want: 9,
		},
		{
			name: "Test 3.b",
			args: args{
				v1: []float32{4, 1},
				v2: []float32{1, 4},
			},
			want: 18,
		},
		{
			name: "Test 3.c",
			args: args{
				v1: []float32{1, 4},
				v2: []float32{1, 1},
			},
			want: 9,
		},
		{
			name: "Test 4",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			},
			want: 10,
		},
		{
			name: "Test 5",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8},
			},
			want: 27,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := L2DistanceSq[float32](tt.args.v1, tt.args.v2); err != nil || got != tt.want {
				t.Errorf("L2DistanceSq() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_AngularDistance_F32(t *testing.T) {
	type args struct {
		v1 []float32
		v2 []float32
	}
	tests := []struct {
		name string
		args args
		want float32
	}{
		{
			name: "Test 1",
			args: args{
				v1: []float32{1, 2, 3, 4},
				v2: []float32{1, 2, 4, 5},
			},
			want: 0,
		},
		{
			name: "Test 2",
			args: args{
				v1: []float32{10, 20, 30, 40},
				v2: []float32{10.5, 21.5, 31.5, 43.5},
			},
			want: 0,
		},
		// Test 3:  Triangle Inequality check on **un-normalized** vector
		// A(1,0),B(2,2), C(0,1) => AB + AC !>= BC => 0 + 0 !>= 0.5
		{
			name: "Test 3.a",
			args: args{
				v1: []float32{1, 0},
				v2: []float32{2, 2},
			},
			want: 0,
		},
		{
			name: "Test 3.b",
			args: args{
				v1: []float32{2, 2},
				v2: []float32{0, 1},
			},
			want: 0,
		},
		{
			name: "Test 3.c",
			args: args{
				v1: []float32{0, 1},
				v2: []float32{1, 0},
			},
			want: 0.5,
		},
		{
			name: "Test 4",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			},
			want: 0,
		},
		{
			name: "Test 5",
			args: args{
				v1: []float32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5, 6, 7},
				v2: []float32{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 2, 3, 4, 5, 6, 7, 8},
			},
			want: 0,
		},

		// Test 4: Triangle Inequality check on **normalized** vector
		// A(1,0),B(2,2), C(0,1) => AB + AC >= BC => 0.25 + 0.25 >= 0.5
		//{
		//	name: "Test 4.a",
		//	args: args{
		//		v1: moarray.NormalizeMoVecf64([]float32{1, 0}),
		//		v2: moarray.NormalizeMoVecf64([]float32{2, 2}),
		//	},
		//	want: 0.25000000000000006,
		//},
		//{
		//	name: "Test 4.b",
		//	args: args{
		//		v1: moarray.NormalizeMoVecf64([]float32{2, 2}),
		//		v2: moarray.NormalizeMoVecf64([]float32{0, 1}),
		//	},
		//	want: 0.25000000000000006,
		//},
		//{
		//	name: "Test 4.c",
		//	args: args{
		//		v1: moarray.NormalizeMoVecf64([]float32{0, 1}),
		//		v2: moarray.NormalizeMoVecf64([]float32{1, 0}),
		//	},
		//	want: 0.5,
		//},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if got, err := SphericalDistance[float32](tt.args.v1, tt.args.v2); err != nil || !assertx.InEpsilonF64(float64(got), float64(tt.want)) {
				t.Errorf("SphericalDistance() = %v, want %v", got, tt.want)
			}
		})
	}
}
