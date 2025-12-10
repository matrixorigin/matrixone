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

package assertx

import "testing"

func TestInEpsilonF32(t *testing.T) {
	type args struct {
		want float32
		got  float32
	}
	tests := []struct {
		name string
		args args
		want bool
	}{

		{
			name: "Test 1 - difference is epsilon",
			args: args{
				want: 1.0,
				got:  1.0 + float32EqualityThreshold,
			},
			want: false,
		},
		{
			name: "Test 2.a - difference is less than epsilon",
			args: args{
				want: 2.0,
				got:  2.0 + 0.5*float32EqualityThreshold,
			},
			want: true,
		},
		{
			name: "Test 2.b - difference is more than epsilon",
			args: args{
				want: 2.0,
				got:  2.0 + 2*float32EqualityThreshold,
			},
			want: false,
		},
		{
			name: "Test 2.c - difference is -ve of epsilon",
			args: args{
				want: 2.0,
				got:  2.0 - float32EqualityThreshold,
			},
			want: false,
		},
		{
			name: "Test 3 - till 9th digit is same, 10th digit is different",
			args: args{
				want: 1.732050800_0,
				got:  1.732050800_9,
			},
			want: true,
		},
		{
			name: "Test 4 - 7th digit is different",
			args: args{
				want: 1.732050_0,
				got:  1.732050_9,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InEpsilonF32(tt.args.want, tt.args.got); got != tt.want {
				t.Errorf("%s InEpsilonF32() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}

func TestInEpsilonF32Slice(t *testing.T) {
	type args struct {
		want []float32
		got  []float32
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Test 1 - difference is epsilon",
			args: args{
				want: []float32{2.0, 3.0},
				got:  []float32{2.0 + float32EqualityThreshold, 3.0 + float32EqualityThreshold},
			},
			want: false,
		},
		{
			name: "Test 2 - till 9th digit is same, 10th digit is different)",
			args: args{
				want: []float32{1.732050800_0},
				got:  []float32{1.732050800_9},
			},
			want: true,
		},
		{
			name: "Test 3 - 9th digit is different",
			args: args{
				want: []float32{1.73205080_0},
				got:  []float32{1.73205080_9},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InEpsilonF32Slice(tt.args.want, tt.args.got); got != tt.want {
				t.Errorf("InEpsilonF32Slice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInEpsilonF32Slices(t *testing.T) {
	type args struct {
		want [][]float32
		got  [][]float32
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Test 1 - difference is epsilon",
			args: args{
				want: [][]float32{{2.0, 3.0}, {4.0}},
				got:  [][]float32{{2.0 + float32EqualityThreshold, 3.0 + float32EqualityThreshold}, {4.0}},
			},
			want: false,
		},
		{
			name: "Test 2 - difference is less than epsilon (next neg-power of epsilon & epsilon/2)",
			args: args{
				want: [][]float32{{2.0, 3.0}, {4.0}},
				got:  [][]float32{{2.0 + 1e-1*float32EqualityThreshold, 3.0 + float32EqualityThreshold/2}, {4.0}},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := InEpsilonF32Slices(tt.args.want, tt.args.got); got != tt.want {
				t.Errorf("InEpsilonF32Slices() = %v, want %v", got, tt.want)
			}
		})
	}
}
