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

package types

import (
	"reflect"
	"testing"
)

func TestBytesToArray(t *testing.T) {
	type args struct {
		input []byte
	}
	type testCase struct {
		name       string
		args       args
		wantResF32 []float32
		wantResF64 []float64
	}
	tests := []testCase{
		{
			name:       "Test1 - float32",
			args:       args{input: []byte{0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64}},
			wantResF32: []float32{1, 2, 3},
		},
		{
			name:       "Test1 - float64",
			args:       args{input: []byte{0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 8, 64}},
			wantResF64: []float64{1, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.wantResF32 != nil {
				if gotRes := BytesToArray[float32](tt.args.input); !reflect.DeepEqual(gotRes, tt.wantResF32) {
					t.Errorf("BytesToArray() = %v, want %v", gotRes, tt.wantResF32)
				}
			}
			if tt.wantResF64 != nil {
				if gotRes := BytesToArray[float64](tt.args.input); !reflect.DeepEqual(gotRes, tt.wantResF64) {
					t.Errorf("BytesToArray() = %v, want %v", gotRes, tt.wantResF64)
				}
			}
		})
	}
}

func TestArrayToBytes(t *testing.T) {

	type testCase struct {
		name    string
		argsF32 []float32
		argsF64 []float64
		want    []byte
	}
	tests := []testCase{
		{
			name:    "Test1 - Float32",
			argsF32: []float32{1, 2, 3},
			want:    []byte{0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64},
		},
		{
			name:    "Test1 - Float64",
			argsF64: []float64{1, 2, 3},
			want:    []byte{0, 0, 0, 0, 0, 0, 240, 63, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 8, 64},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.argsF32 != nil {
				if got := ArrayToBytes[float32](tt.argsF32); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ArrayToBytes() = %v, want %v", got, tt.want)
				}
			}

			if tt.argsF64 != nil {
				if got := ArrayToBytes[float64](tt.argsF64); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ArrayToBytes() = %v, want %v", got, tt.want)
				}
			}

		})
	}
}

func TestArrayToString(t *testing.T) {
	type testCase struct {
		name    string
		argsF32 []float32
		argsF64 []float64
		want    string
	}
	tests := []testCase{
		{
			name:    "Test1 - Float32",
			argsF32: []float32{1, 2, 3, 4},
			want:    "[1, 2, 3, 4]",
		},
		{
			name:    "Test1 - Float64",
			argsF64: []float64{1, 2, 3},
			want:    "[1, 2, 3]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.argsF32 != nil {
				if got := ArrayToString[float32](tt.argsF32); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ArrayToString() = %v, want %v", got, tt.want)
				}
			}

			if tt.argsF64 != nil {
				if got := ArrayToString[float64](tt.argsF64); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ArrayToString() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestArraysToString(t *testing.T) {
	type testCase struct {
		name    string
		argsF32 [][]float32
		argsF64 [][]float64
		want    string
	}
	tests := []testCase{
		{
			name:    "Test1 - Float32",
			argsF32: [][]float32{{1, 2, 3, 4}, {0, 0, 0, 0}},
			want:    "[1, 2, 3, 4] [0, 0, 0, 0]",
		},
		{
			name:    "Test1 - Float64",
			argsF64: [][]float64{{1, -2, 3, 4}, {0, 0, 0, 0}},
			want:    "[1, -2, 3, 4] [0, 0, 0, 0]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.argsF32 != nil {
				if got := ArraysToString[float32](tt.argsF32); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ArraysToString() = %v, want %v", got, tt.want)
				}
			}

			if tt.argsF64 != nil {
				if got := ArraysToString[float64](tt.argsF64); !reflect.DeepEqual(got, tt.want) {
					t.Errorf("ArraysToString() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestStringToArray(t *testing.T) {
	type args struct {
		input string
	}
	type testCase struct {
		name       string
		args       args
		wantResF32 []float32
		wantResF64 []float64
	}
	tests := []testCase{
		{
			name:       "Test1 - float32",
			args:       args{input: "[1,2,3,-2]"},
			wantResF32: []float32{1, 2, 3, -2},
		},
		{
			name:       "Test1 - float64",
			args:       args{input: "[1,2,3,30]"},
			wantResF64: []float64{1, 2, 3, 30},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.wantResF32 != nil {
				if gotRes, err := StringToArray[float32](tt.args.input); err != nil || !reflect.DeepEqual(gotRes, tt.wantResF32) {
					t.Errorf("StringToArray() = %v, want %v", gotRes, tt.wantResF32)
				}
			}
			if tt.wantResF64 != nil {
				if gotRes, err := StringToArray[float64](tt.args.input); err != nil || !reflect.DeepEqual(gotRes, tt.wantResF64) {
					t.Errorf("StringToArray() = %v, want %v", gotRes, tt.wantResF64)
				}
			}
		})
	}
}

func TestCompareArray(t *testing.T) {
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
				if gotRes := CompareArray[float32](tt.args.leftArgF32, tt.args.rightArgF32); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("CompareArray() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.rightArgF64 != nil {
				if gotRes := CompareArray[float64](tt.args.leftArgF64, tt.args.rightArgF64); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("CompareArray() = %v, want %v", gotRes, tt.want)
				}
			}

		})
	}
}
