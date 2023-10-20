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

func TestAdd(t *testing.T) {
	type args struct {
		leftArgF32  []float32
		rightArgF32 []float32

		leftArgF64  []float64
		rightArgF64 []float64
	}
	type testCase struct {
		name    string
		args    args
		wantF32 []float32
		wantF64 []float64
	}
	tests := []testCase{
		{
			name:    "Test1 - float32",
			args:    args{leftArgF32: []float32{1, 2, 3}, rightArgF32: []float32{2, 3, 4}},
			wantF32: []float32{3, 5, 7},
		},
		{
			name:    "Test2 - float64",
			args:    args{leftArgF64: []float64{1, 2, 3}, rightArgF64: []float64{2, 3, 4}},
			wantF64: []float64{3, 5, 7},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.rightArgF32 != nil {
				if gotRes, _ := Add[float32](tt.args.leftArgF32, tt.args.rightArgF32); !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Add() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.rightArgF64 != nil {
				if gotRes, _ := Add[float64](tt.args.leftArgF64, tt.args.rightArgF64); !reflect.DeepEqual(gotRes, tt.wantF64) {
					t.Errorf("Add() = %v, want %v", gotRes, tt.wantF64)
				}
			}
		})
	}
}

func TestSubtract(t *testing.T) {
	type args struct {
		leftArgF32  []float32
		rightArgF32 []float32

		leftArgF64  []float64
		rightArgF64 []float64
	}
	type testCase struct {
		name    string
		args    args
		wantF32 []float32
		wantF64 []float64
	}
	tests := []testCase{
		{
			name:    "Test1 - float32",
			args:    args{leftArgF32: []float32{1, 2, 3}, rightArgF32: []float32{2, 3, 4}},
			wantF32: []float32{-1, -1, -1},
		},
		{
			name:    "Test2 - float64",
			args:    args{leftArgF64: []float64{1, 4, 3}, rightArgF64: []float64{1, 3, 4}},
			wantF64: []float64{0, 1, -1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.rightArgF32 != nil {
				if gotRes, _ := Subtract[float32](tt.args.leftArgF32, tt.args.rightArgF32); !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Subtract() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.rightArgF64 != nil {
				if gotRes, _ := Subtract[float64](tt.args.leftArgF64, tt.args.rightArgF64); !reflect.DeepEqual(gotRes, tt.wantF64) {
					t.Errorf("Subtract() = %v, want %v", gotRes, tt.wantF64)
				}
			}
		})
	}
}

func TestMultiply(t *testing.T) {
	type args struct {
		leftArgF32  []float32
		rightArgF32 []float32

		leftArgF64  []float64
		rightArgF64 []float64
	}
	type testCase struct {
		name    string
		args    args
		wantF32 []float32
		wantF64 []float64
	}
	tests := []testCase{
		{
			name:    "Test1 - float32",
			args:    args{leftArgF32: []float32{1, 2, 3}, rightArgF32: []float32{2, 3, 4}},
			wantF32: []float32{2, 6, 12},
		},
		{
			name:    "Test2 - float64",
			args:    args{leftArgF64: []float64{1, 4, 3}, rightArgF64: []float64{1, 3, 4}},
			wantF64: []float64{1, 12, 12},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.rightArgF32 != nil {
				if gotRes, _ := Multiply[float32](tt.args.leftArgF32, tt.args.rightArgF32); !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Multiply() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.rightArgF64 != nil {
				if gotRes, _ := Multiply[float64](tt.args.leftArgF64, tt.args.rightArgF64); !reflect.DeepEqual(gotRes, tt.wantF64) {
					t.Errorf("Multiply() = %v, want %v", gotRes, tt.wantF64)
				}
			}
		})
	}
}

func TestDivide(t *testing.T) {
	type args struct {
		leftArgF32  []float32
		rightArgF32 []float32

		leftArgF64  []float64
		rightArgF64 []float64
	}
	type testCase struct {
		name    string
		args    args
		wantF32 []float32
		wantF64 []float64
		wantErr bool
	}
	tests := []testCase{
		{
			name:    "Test1 - float32",
			args:    args{leftArgF32: []float32{1, 2, 3}, rightArgF32: []float32{2, 3, 4}},
			wantF32: []float32{0.5, 0.6666667, 0.75},
		},
		{
			name:    "Test2 - float32 - div by zero",
			args:    args{leftArgF32: []float32{1, 4, 3}, rightArgF32: []float32{1, 0, 4}},
			wantErr: true,
		},
		{
			name:    "Test3 - float64",
			args:    args{leftArgF64: []float64{1, 4, 3}, rightArgF64: []float64{1, 3, 4}},
			wantF64: []float64{1, 1.3333333333333333, 0.75},
		},
		{
			name:    "Test4 - float64 - div by zero",
			args:    args{leftArgF64: []float64{1, 4, 3}, rightArgF64: []float64{1, 0, 4}},
			wantErr: true,
		},
		{
			name:    "Test5 - float64 - dimension mismatch",
			args:    args{leftArgF64: []float64{1, 4}, rightArgF64: []float64{1, 1, 4}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.rightArgF32 != nil {
				if tt.wantErr {
					if _, gotErr := Divide[float32](tt.args.leftArgF32, tt.args.rightArgF32); gotErr == nil {
						t.Errorf("Divide() should throw error")
					}
				} else if gotRes, _ := Divide[float32](tt.args.leftArgF32, tt.args.rightArgF32); !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Divide() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.rightArgF64 != nil {
				if tt.wantErr {
					if _, gotErr := Divide[float64](tt.args.leftArgF64, tt.args.rightArgF64); gotErr == nil {
						t.Errorf("Divide() should throw error")
					}
				} else if gotRes, _ := Divide[float64](tt.args.leftArgF64, tt.args.rightArgF64); !reflect.DeepEqual(gotRes, tt.wantF64) {
					t.Errorf("Divide() = %v, want %v", gotRes, tt.wantF64)
				}
			}
		})
	}
}

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
			name: "Test1 - float32-less",
			args: args{leftArgF32: []float32{1, 2, 3}, rightArgF32: []float32{2, 3, 4}},
			want: -1,
		},
		{
			name: "Test2 - float32-large",
			args: args{leftArgF32: []float32{3, 2, 3}, rightArgF32: []float32{2, 3, 4}},
			want: 1,
		},
		{
			name: "Test3 - float32-equal",
			args: args{leftArgF32: []float32{3, 2, 3}, rightArgF32: []float32{3, 2, 3}},
			want: 0,
		},
		{
			name: "Test4 - float64-less",
			args: args{leftArgF64: []float64{1, 2, 3}, rightArgF64: []float64{2, 3, 4}},
			want: -1,
		},
		{
			name: "Test5 - float64-large",
			args: args{leftArgF64: []float64{3, 2, 3}, rightArgF64: []float64{2, 3, 4}},
			want: 1,
		},
		{
			name: "Test6 - float64-equal",
			args: args{leftArgF64: []float64{3, 2, 3}, rightArgF64: []float64{3, 2, 3}},
			want: 0,
		},
		{
			name: "Test7 - float64 difference dims",
			args: args{leftArgF64: []float64{3, 2}, rightArgF64: []float64{3, 2, 3}},
			want: -1,
		},
		{
			name: "Test7 - float64 difference dims",
			args: args{leftArgF64: []float64{3, 2, 3}, rightArgF64: []float64{3, 2}},
			want: 1,
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

func TestCast(t *testing.T) {
	type args struct {
		argF32 []float32
		argF64 []float64
	}
	type testCase struct {
		name    string
		args    args
		wantF32 []float32
		wantF64 []float64
	}
	tests := []testCase{
		{
			name:    "Test1 - float32 to float64",
			args:    args{argF32: []float32{1, 2, 3}},
			wantF64: []float64{1, 2, 3},
		},
		{
			name:    "Test2 - float64 to float32",
			args:    args{argF64: []float64{1, 4, 3}},
			wantF32: []float32{1, 4, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argF32 != nil && tt.wantF64 != nil {
				if gotRes := Cast[float32, float64](tt.args.argF32); !reflect.DeepEqual(gotRes, tt.wantF64) {
					t.Errorf("Cast() = %v, want %v", gotRes, tt.wantF64)
				}
			}
			if tt.args.argF64 != nil && tt.wantF32 != nil {
				if gotRes := Cast[float64, float32](tt.args.argF64); !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Cast() = %v, want %v", gotRes, tt.wantF32)
				}
			}
		})
	}
}

func TestAbs(t *testing.T) {
	type args struct {
		argF32 []float32
		argF64 []float64
	}
	type testCase struct {
		name string
		args args

		wantF32 []float32
		wantF64 []float64
	}
	tests := []testCase{
		{
			name:    "Test1 - float32",
			args:    args{argF32: []float32{-1, 0, -3.4e+38}},
			wantF32: []float32{1, 0, 3.4e+38},
		},
		{
			name:    "Test2 - float64",
			args:    args{argF64: []float64{-1, 0, 3}},
			wantF64: []float64{1, 0, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argF32 != nil {
				if gotRes, err := Abs[float32](tt.args.argF32); err != nil && !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Abs() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.argF64 != nil {
				if gotRes, err := Abs[float64](tt.args.argF64); err != nil && !reflect.DeepEqual(gotRes, tt.wantF64) {
					t.Errorf("Abs() = %v, want %v", gotRes, tt.wantF64)
				}
			}
		})
	}
}

func TestSqrt(t *testing.T) {
	type args struct {
		argF32 []float32
		argF64 []float64
	}
	type testCase struct {
		name string
		args args

		wantF32 []float32
		wantF64 []float64
		wantErr bool
	}
	tests := []testCase{
		{
			name:    "Test1 - float32",
			args:    args{argF32: []float32{1, 0, 4}},
			wantF32: []float32{1, 0, 2},
		},
		{
			name:    "Test2 - float32 error case",
			args:    args{argF32: []float32{-1, 0, 4}},
			wantErr: true,
		},
		{
			name:    "Test3 - float64",
			args:    args{argF64: []float64{1, 0, 4}},
			wantF64: []float64{1, 0, 2},
		},
		{
			name:    "Test4 - float64 error case",
			args:    args{argF64: []float64{-1, 0, 4}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argF32 != nil {
				if tt.wantErr {
					if _, err := Sqrt[float32](tt.args.argF32); err == nil {
						t.Errorf("Sqrt() should throw error")
					}
				} else if gotRes, err := Sqrt[float32](tt.args.argF32); err != nil && !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Sqrt() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.argF64 != nil {
				if tt.wantErr {
					if _, err := Sqrt[float64](tt.args.argF64); err == nil {
						t.Errorf("Sqrt() should throw error")
					}
				} else if gotRes, err := Sqrt[float64](tt.args.argF64); err != nil && !reflect.DeepEqual(gotRes, tt.wantF64) {
					t.Errorf("Sqrt() = %v, want %v", gotRes, tt.wantF64)
				}
			}

		})
	}
}

func TestSummation(t *testing.T) {
	type args struct {
		argF32 []float32
		argF64 []float64
	}
	type testCase struct {
		name string
		args args
		want float64
	}
	tests := []testCase{
		{
			name: "Test1 - float32",
			args: args{argF32: []float32{1, 2, 3}},
			want: 6,
		},
		{
			name: "Test2 - float64",
			args: args{argF64: []float64{1, 2, 3}},
			want: 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argF32 != nil {
				if gotRes := Summation[float32](tt.args.argF32); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("Summation() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argF64 != nil {
				if gotRes := Summation[float64](tt.args.argF64); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("Summation() = %v, want %v", gotRes, tt.want)
				}
			}

		})
	}
}

func TestInnerProduct(t *testing.T) {
	type args struct {
		argLeftF32  []float32
		argRightF32 []float32

		argLeftF64  []float64
		argRightF64 []float64
	}
	type testCase struct {
		name string
		args args
		want float64
	}
	tests := []testCase{
		{
			name: "Test1 - float32",
			args: args{argLeftF32: []float32{1, 2, 3}, argRightF32: []float32{1, 2, 3}},
			want: 14,
		},
		{
			name: "Test2 - float64",
			args: args{argLeftF64: []float64{1, 2, 3}, argRightF64: []float64{1, 2, 3}},
			want: 14,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argLeftF32 != nil {
				if gotRes, _ := InnerProduct[float32](tt.args.argLeftF32, tt.args.argRightF32); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("InnerProduct() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argLeftF64 != nil {
				if gotRes, _ := InnerProduct[float64](tt.args.argLeftF64, tt.args.argRightF64); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("InnerProduct() = %v, want %v", gotRes, tt.want)
				}
			}

		})
	}
}

func TestL1Norm(t *testing.T) {
	type args struct {
		argF32 []float32
		argF64 []float64
	}
	type testCase struct {
		name string
		args args
		want float64
	}
	tests := []testCase{
		{
			name: "Test1 - float32",
			args: args{argF32: []float32{1, 2, 3}},
			want: 6,
		},
		{
			name: "Test2 - float64",
			args: args{argF64: []float64{1, 2, 3}},
			want: 6,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argF32 != nil {
				if gotRes, _ := L1Norm[float32](tt.args.argF32); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("L1Norm() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argF64 != nil {
				if gotRes, _ := L1Norm[float64](tt.args.argF64); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("L1Norm() = %v, want %v", gotRes, tt.want)
				}
			}

		})
	}
}

func TestL2Norm(t *testing.T) {
	type args struct {
		argF32 []float32
		argF64 []float64
	}
	type testCase struct {
		name string
		args args
		want float64
	}
	tests := []testCase{
		{
			name: "Test1 - float32",
			args: args{argF32: []float32{1, 2, 3}},
			want: 3.7416573867739413,
		},
		{
			name: "Test2 - float64",
			args: args{argF64: []float64{1, 2, 3}},
			want: 3.7416573867739413,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argF32 != nil {
				if gotRes, _ := L2Norm[float32](tt.args.argF32); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("L1Norm() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argF64 != nil {
				if gotRes, _ := L2Norm[float64](tt.args.argF64); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("L1Norm() = %v, want %v", gotRes, tt.want)
				}
			}

		})
	}
}

func TestCosineSimilarity(t *testing.T) {
	type args struct {
		argLeftF32  []float32
		argRightF32 []float32

		argLeftF64  []float64
		argRightF64 []float64
	}
	type testCase struct {
		name string
		args args
		want float32
	}
	tests := []testCase{
		{
			name: "Test1 - float32",
			args: args{argLeftF32: []float32{1, 2, 3}, argRightF32: []float32{1, 2, 3}},
			want: 1,
		},
		{
			name: "Test2 - float64",
			args: args{argLeftF64: []float64{1, 2, 3}, argRightF64: []float64{1, 2, 3}},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argLeftF32 != nil {
				if gotRes, _ := CosineSimilarity[float32](tt.args.argLeftF32, tt.args.argRightF32); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("CosineSimilarity() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argLeftF64 != nil {
				if gotRes, _ := CosineSimilarity[float64](tt.args.argLeftF64, tt.args.argRightF64); !reflect.DeepEqual(gotRes, tt.want) {
					t.Errorf("CosineSimilarity() = %v, want %v", gotRes, tt.want)
				}
			}

		})
	}
}
