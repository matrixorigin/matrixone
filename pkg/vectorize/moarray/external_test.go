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
	"github.com/matrixorigin/matrixone/pkg/common/assertx"
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
				if gotRes, err := Add[float32](tt.args.leftArgF32, tt.args.rightArgF32); err != nil || !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Add() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.rightArgF64 != nil {
				if gotRes, err := Add[float64](tt.args.leftArgF64, tt.args.rightArgF64); err != nil || !assertx.InEpsilonF64Slice(gotRes, tt.wantF64) {
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
				if gotRes, err := Subtract[float32](tt.args.leftArgF32, tt.args.rightArgF32); err != nil || !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Subtract() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.rightArgF64 != nil {
				if gotRes, err := Subtract[float64](tt.args.leftArgF64, tt.args.rightArgF64); err != nil || !assertx.InEpsilonF64Slice(tt.wantF64, gotRes) {
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
		{
			name:    "Test3 - float64",
			args:    args{leftArgF64: []float64{0.66616553}, rightArgF64: []float64{0.66616553}},
			wantF64: []float64{0.4437765133601809},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.rightArgF32 != nil {
				if gotRes, err := Multiply[float32](tt.args.leftArgF32, tt.args.rightArgF32); err != nil || !reflect.DeepEqual(tt.wantF32, gotRes) {
					t.Errorf("Multiply() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.rightArgF64 != nil {
				if gotRes, err := Multiply[float64](tt.args.leftArgF64, tt.args.rightArgF64); err != nil || !assertx.InEpsilonF64Slice(tt.wantF64, gotRes) {
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
				} else if gotRes, err := Divide[float32](tt.args.leftArgF32, tt.args.rightArgF32); err != nil || !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Divide() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.rightArgF64 != nil {
				if tt.wantErr {
					if _, gotErr := Divide[float64](tt.args.leftArgF64, tt.args.rightArgF64); gotErr == nil {
						t.Errorf("Divide() should throw error")
					}
				} else if gotRes, err := Divide[float64](tt.args.leftArgF64, tt.args.rightArgF64); err != nil || !assertx.InEpsilonF64Slice(tt.wantF64, gotRes) {
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
				if gotResF64, err := Cast[float32, float64](tt.args.argF32); err != nil || !assertx.InEpsilonF64Slice(gotResF64, tt.wantF64) {
					t.Errorf("Cast() = %v, want %v", gotResF64, tt.wantF64)
				}
			}
			if tt.args.argF64 != nil && tt.wantF32 != nil {
				if gotResF32, err := Cast[float64, float32](tt.args.argF64); err != nil || !reflect.DeepEqual(gotResF32, tt.wantF32) {
					t.Errorf("Cast() = %v, want %v", gotResF32, tt.wantF32)
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
				if gotRes, err := Abs[float32](tt.args.argF32); err != nil || !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Abs() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.argF64 != nil {
				if gotRes, err := Abs[float64](tt.args.argF64); err != nil || !assertx.InEpsilonF64Slice(tt.wantF64, gotRes) {
					t.Errorf("Abs() = %v, want %v", gotRes, tt.wantF64)
				}
			}
		})
	}
}

func TestNormalizeL2(t *testing.T) {
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
			name:    "Test1 - float32 - zero vector",
			args:    args{argF32: []float32{0, 0, 0}},
			wantF32: []float32{0, 0, 0},
		},
		{
			name:    "Test1.b - float32",
			args:    args{argF32: []float32{1, 2, 3}},
			wantF32: []float32{0.26726124, 0.5345225, 0.80178374},
		},
		{
			name:    "Test1.c - float32",
			args:    args{argF32: []float32{10, 3.333333333333333, 4, 5}},
			wantF32: []float32{0.8108108, 0.27027026, 0.32432434, 0.4054054},
		},
		{
			name:    "Test2 - float64 - zero vector",
			args:    args{argF64: []float64{0, 0, 0}},
			wantF64: []float64{0, 0, 0},
		},
		{
			name:    "Test3 - float64",
			args:    args{argF64: []float64{1, 2, 3}},
			wantF64: []float64{0.2672612419124244, 0.5345224838248488, 0.8017837257372732},
		},
		{
			name:    "Test4 - float64",
			args:    args{argF64: []float64{-1, 2, 3}},
			wantF64: []float64{-0.2672612419124244, 0.5345224838248488, 0.8017837257372732},
		},
		{
			name:    "Test5 - float64",
			args:    args{argF64: []float64{10, 3.333333333333333, 4, 5}},
			wantF64: []float64{0.8108108108108107, 0.27027027027027023, 0.3243243243243243, 0.4054054054054054},
		},
		{
			name:    "Test6 - float64",
			args:    args{argF64: []float64{1, 2, 3.6666666666666665, 4.666666666666666}},
			wantF64: []float64{0.15767649936829103, 0.31535299873658207, 0.5781471643504004, 0.7358236637186913},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argF32 != nil {
				if tt.wantErr {
					if _, err := NormalizeL2[float32](tt.args.argF32); err == nil {
						t.Errorf("NormalizeL2() should throw error")
					}
				} else if gotRes, err := NormalizeL2[float32](tt.args.argF32); err != nil || !reflect.DeepEqual(tt.wantF32, gotRes) {
					t.Errorf("NormalizeL2() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.argF64 != nil {
				if tt.wantErr {
					if _, err := NormalizeL2[float64](tt.args.argF64); err == nil {
						t.Errorf("NormalizeL2() should throw error")
					}
				} else if gotRes, err := NormalizeL2[float64](tt.args.argF64); err != nil || !assertx.InEpsilonF64Slice(tt.wantF64, gotRes) {
					t.Errorf("NormalizeL2() = %v, want %v", gotRes, tt.wantF64)
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

		wantF32 []float64 // ie result for argF32 is []float32
		wantF64 []float64 // ie result for argF64 is []float64
		wantErr bool
	}
	tests := []testCase{
		{
			name:    "Test1 - float32",
			args:    args{argF32: []float32{1, 0, 4}},
			wantF32: []float64{1, 0, 2},
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
				} else if gotRes, err := Sqrt[float32](tt.args.argF32); err != nil || !reflect.DeepEqual(gotRes, tt.wantF32) {
					t.Errorf("Sqrt() = %v, want %v", err, tt.wantErr)
					t.Errorf("Sqrt() = %v, want %v", gotRes, tt.wantF32)
				}
			}
			if tt.args.argF64 != nil {
				if tt.wantErr {
					if _, err := Sqrt[float64](tt.args.argF64); err == nil {
						t.Errorf("Sqrt() should throw error")
					}
				} else if gotRes, err := Sqrt[float64](tt.args.argF64); err != nil || !assertx.InEpsilonF64Slice(tt.wantF64, gotRes) {
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
				if gotRes, err := Summation[float32](tt.args.argF32); err != nil || !assertx.InEpsilonF64(tt.want, gotRes) {
					t.Errorf("Summation() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argF64 != nil {
				if gotRes, err := Summation[float64](tt.args.argF64); err != nil || !assertx.InEpsilonF64(tt.want, gotRes) {
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
				if gotRes, _ := InnerProduct[float32](tt.args.argLeftF32, tt.args.argRightF32); !assertx.InEpsilonF64(tt.want, gotRes) {
					t.Errorf("InnerProduct() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argLeftF64 != nil {
				if gotRes, _ := InnerProduct[float64](tt.args.argLeftF64, tt.args.argRightF64); !assertx.InEpsilonF64(tt.want, gotRes) {
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
				if gotRes, _ := L1Norm[float32](tt.args.argF32); !assertx.InEpsilonF64(tt.want, gotRes) {
					t.Errorf("L1Norm() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argF64 != nil {
				if gotRes, _ := L1Norm[float64](tt.args.argF64); !assertx.InEpsilonF64(tt.want, gotRes) {
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
			want: 3.741657386773941,
		},
		{
			name: "Test2 - float64",
			args: args{argF64: []float64{1, 2, 3}},
			want: 3.741657386773941,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argF32 != nil {
				if gotRes, _ := L2Norm[float32](tt.args.argF32); !assertx.InEpsilonF64(tt.want, gotRes) {
					t.Errorf("L2Norm() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argF64 != nil {
				if gotRes, _ := L2Norm[float64](tt.args.argF64); !assertx.InEpsilonF64(tt.want, gotRes) {
					t.Errorf("L2Norm() = %v, want %v", gotRes, tt.want)
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
		want float64
	}
	tests := []testCase{
		{
			name: "Test1.a - float32",
			args: args{argLeftF32: []float32{1, 2, 3}, argRightF32: []float32{1, 2, 3}},
			want: 1,
		},
		{
			name: "Test1.b - float32",
			args: args{argLeftF32: []float32{0.46323407, 23.498016, 563.923, 56.076736, 8732.958}, argRightF32: []float32{0.46323407, 23.498016, 563.923, 56.076736, 8732.958}},
			want: 1,
		},
		{
			name: "Test2.a - float64",
			args: args{argLeftF64: []float64{1, 2, 3}, argRightF64: []float64{1, 2, 3}},
			want: 1,
		},
		{
			name: "Test2.b - float64",
			args: args{argLeftF64: []float64{0.46323407, 23.498016, 563.923, 56.076736, 8732.958}, argRightF64: []float64{0.46323407, 23.498016, 563.923, 56.076736, 8732.958}},
			want: 1,
		},
		{
			name: "Test2.c - float64",
			args: args{argLeftF64: []float64{0.8166459, 0.66616553, 0.4886152}, argRightF64: []float64{0.8166459, 0.66616553, 0.4886152}},
			want: 1,
		},
		{
			name: "Test2.d - float64",
			args: args{argLeftF64: []float64{8.5606893, 6.7903588, 821.977768}, argRightF64: []float64{8.5606893, 6.7903588, 821.977768}},
			want: 1,
		},
		{
			name: "Test2.e - float64",
			args: args{argLeftF64: []float64{0.9260021, 0.26637346, 0.06567037}, argRightF64: []float64{0.9260021, 0.26637346, 0.06567037}},
			want: 1,
		},
		{
			name: "Test2.f - float64",
			args: args{argLeftF64: []float64{0.45756745, 65.2996871, 321.623636, 3.60082066, 87.58445764}, argRightF64: []float64{0.45756745, 65.2996871, 321.623636, 3.60082066, 87.58445764}},
			want: 1,
		},
		{
			name: "Test2.g - float64",
			args: args{argLeftF64: []float64{0.46323407, 23.49801546, 563.9229458, 56.07673508, 8732.9583881}, argRightF64: []float64{0.46323407, 23.49801546, 563.9229458, 56.07673508, 8732.9583881}},
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argLeftF32 != nil {
				if gotRes, _ := CosineSimilarity[float32](tt.args.argLeftF32, tt.args.argRightF32); !reflect.DeepEqual(tt.want, gotRes) {
					t.Errorf("CosineSimilarity() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argLeftF64 != nil {
				if gotRes, _ := CosineSimilarity[float64](tt.args.argLeftF64, tt.args.argRightF64); !reflect.DeepEqual(tt.want, gotRes) {
					t.Errorf("CosineSimilarity() = %v, want %v", gotRes, tt.want)
				}
			}

		})
	}
}

func TestL2Distance(t *testing.T) {
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
			args: args{argLeftF32: []float32{1, 2, 3}, argRightF32: []float32{10, 20, 30}},
			want: 33.67491648096547,
		},
		{
			name: "Test2 - float64",
			args: args{argLeftF64: []float64{1, 2, 3}, argRightF64: []float64{10, 20, 30}},
			want: 33.67491648096547,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argLeftF32 != nil {
				if gotRes, _ := L2Distance[float32](tt.args.argLeftF32, tt.args.argRightF32); !assertx.InEpsilonF64(tt.want, gotRes) {
					t.Errorf("L2Distance() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argLeftF64 != nil {
				if gotRes, _ := L2Distance[float64](tt.args.argLeftF64, tt.args.argRightF64); !assertx.InEpsilonF64(tt.want, gotRes) {
					t.Errorf("L2Distance() = %v, want %v", gotRes, tt.want)
				}
			}

		})
	}
}

func TestCosineDistance(t *testing.T) {
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
			args: args{argLeftF32: []float32{1, 2, 3}, argRightF32: []float32{-1, -2, -3}},
			want: 2,
		},
		{
			name: "Test2 - float64",
			args: args{argLeftF64: []float64{1, 2, 3}, argRightF64: []float64{1, 2, 3}},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argLeftF32 != nil {
				if gotRes, _ := CosineDistance[float32](tt.args.argLeftF32, tt.args.argRightF32); !assertx.InEpsilonF64(tt.want, gotRes) {
					t.Errorf("CosineDistance() = %v, want %v", gotRes, tt.want)
				}
			}
			if tt.args.argLeftF64 != nil {
				if gotRes, _ := CosineDistance[float64](tt.args.argLeftF64, tt.args.argRightF64); !assertx.InEpsilonF64(tt.want, gotRes) {
					t.Errorf("CosineDistance() = %v, want %v", gotRes, tt.want)
				}
			}

		})
	}
}

func TestScalarOp(t *testing.T) {
	type args struct {
		argVecF32 []float32
		argVecF64 []float64
		argOp     string
		argSca    float64
	}
	type testCase struct {
		name       string
		args       args
		wantVecF32 []float32
		wantVecF64 []float64
	}
	tests := []testCase{
		{
			name:       "Test1 - float32",
			args:       args{argVecF32: []float32{1, 2, 3}, argOp: "+", argSca: 2},
			wantVecF32: []float32{3, 4, 5},
		},
		{
			name:       "Test2 - float32",
			args:       args{argVecF32: []float32{1, 2, 3}, argOp: "-", argSca: 2},
			wantVecF32: []float32{-1, 0, 1},
		},
		{
			name:       "Test3 - float32",
			args:       args{argVecF32: []float32{1, 2, 3}, argOp: "*", argSca: 2},
			wantVecF32: []float32{2, 4, 6},
		},
		{
			name:       "Test4 - float32",
			args:       args{argVecF32: []float32{1, 2, 3}, argOp: "/", argSca: 2},
			wantVecF32: []float32{0.5, 1, 1.5},
		},

		{
			name:       "Test5 - float64",
			args:       args{argVecF64: []float64{1, 2, 3}, argOp: "+", argSca: 2},
			wantVecF64: []float64{3, 4, 5},
		},
		{
			name:       "Test6 - float64",
			args:       args{argVecF64: []float64{1, 2, 3}, argOp: "-", argSca: 2},
			wantVecF64: []float64{-1, 0, 1},
		},
		{
			name:       "Test7 - float64",
			args:       args{argVecF64: []float64{1, 2, 3}, argOp: "*", argSca: 2},
			wantVecF64: []float64{2, 4, 6},
		},
		{
			name:       "Test8 - float64",
			args:       args{argVecF64: []float64{1, 2, 3}, argOp: "/", argSca: 2},
			wantVecF64: []float64{0.5, 1, 1.5},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			if tt.args.argVecF32 != nil {
				if gotRes, _ := ScalarOp[float32](tt.args.argVecF32, tt.args.argOp, tt.args.argSca); !reflect.DeepEqual(gotRes, tt.wantVecF32) {
					t.Errorf("ScalarOp() = %v, want %v", gotRes, tt.wantVecF32)
				}
			} else if tt.args.argVecF64 != nil {
				if gotRes, _ := ScalarOp[float64](tt.args.argVecF64, tt.args.argOp, tt.args.argSca); !reflect.DeepEqual(gotRes, tt.wantVecF64) {
					t.Errorf("ScalarOp() = %v, want %v", gotRes, tt.wantVecF64)
				}
			}

		})
	}
}
