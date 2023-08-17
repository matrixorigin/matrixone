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
	"reflect"
	"testing"
)

func TestCompare(t *testing.T) {
	type args struct {
		leftArgF32  []float32
		rightArgF32 []float32

		leftArgF64  []float64
		rightArgF64 []float64
	}
	type testCase struct {
		name string
		args args
		want int
	}
	tests := []testCase{
		{
			name: "Test1 - float32 - less",
			args: args{leftArgF32: []float32{1, 2, 3}, rightArgF32: []float32{2, 3, 4}},
			want: -1,
		},
		{
			name: "Test1 - float32 - large",
			args: args{leftArgF32: []float32{3, 2, 3}, rightArgF32: []float32{2, 3, 4}},
			want: 1,
		},
		{
			name: "Test1 - float32 - equal",
			args: args{leftArgF32: []float32{3, 2, 3}, rightArgF32: []float32{3, 2, 3}},
			want: 0,
		},
		{
			name: "Test1 - float64 - less",
			args: args{leftArgF64: []float64{1, 2, 3}, rightArgF64: []float64{2, 3, 4}},
			want: -1,
		},
		{
			name: "Test1 - float64 - large",
			args: args{leftArgF64: []float64{3, 2, 3}, rightArgF64: []float64{2, 3, 4}},
			want: 1,
		},
		{
			name: "Test1 - float64 - equal",
			args: args{leftArgF64: []float64{3, 2, 3}, rightArgF64: []float64{3, 2, 3}},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.rightArgF32 != nil {
				if gotRes := Compare[float32](tt.args.leftArgF32, tt.args.rightArgF32); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("CompareArray() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.rightArgF64 != nil {
				if gotRes := Compare[float64](tt.args.leftArgF64, tt.args.rightArgF64); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("CompareArray() = %v, want %v", gotRes, tt.want)
				}
			}

		})
	}
}
