// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"testing"
)

func Test_argumentCheck(t *testing.T) {
	// a simple function just using for test
	makeSimpleArgs := func(ts []types.T) []Arg {
		ret := make([]Arg, 0, len(ts))
		for i := 0; i < len(ts); i++ {
			ret = append(ret, Arg{Name: fmt.Sprintf("arg_%d", i), Typ: ts[i]})
		}
		return ret
	}

	type input struct {
		args  []types.T // input arguments
		fArgs ArgList   // function required arguments
	}
	tests := []struct {
		name  string
		input input
		want  bool
	}{
		// 1. argument number is constant and can not be NULL
		{
			name: "test_constant_1",
			input: input{
				args:  []types.T{types.T_int64},
				fArgs: MakeLimitArgList(false, makeSimpleArgs([]types.T{types.T_int64})),
			},
			want: true,
		},
		{
			name: "test_constant_2",
			input: input{
				args:  []types.T{types.T_int64},
				fArgs: MakeLimitArgList(false, makeSimpleArgs([]types.T{types.T_float64})),
			},
			want: false,
		},
		{
			name: "test_constant_3",
			input: input{
				args:  []types.T{types.T_int64, types.T_int32},
				fArgs: MakeLimitArgList(false, makeSimpleArgs([]types.T{types.T_int64, types.T_int32})),
			},
			want: true,
		},
		{
			name: "test_constant_4",
			input: input{
				args:  []types.T{types.T_int64, types.T_uint8},
				fArgs: MakeLimitArgList(false, makeSimpleArgs([]types.T{types.T_uint8, types.T_int64})),
			},
			want: false,
		},
		{
			name: "test_constant_5",
			input: input{
				args:  []types.T{NullValueType, types.T_int32},
				fArgs: MakeLimitArgList(false, makeSimpleArgs([]types.T{types.T_int64, types.T_int32})),
			},
			want: false,
		},
		// 2. argument number is constant and can be NULL
		{
			name: "test_constant_null_1",
			input: input{
				args:  []types.T{NullValueType},
				fArgs: MakeLimitArgList(true, makeSimpleArgs([]types.T{types.T_int64})),
			},
			want: true,
		},
		{
			name: "test_constant_null_2",
			input: input{
				args:  []types.T{NullValueType, NullValueType},
				fArgs: MakeLimitArgList(true, makeSimpleArgs([]types.T{types.T_int64, types.T_float32})),
			},
			want: true,
		},
		{
			name: "test_constant_null_3",
			input: input{
				args:  []types.T{NullValueType, types.T_int32},
				fArgs: MakeLimitArgList(true, makeSimpleArgs([]types.T{types.T_int64, types.T_int64})),
			},
			want: false,
		},
		// 3. argument number is variadic and can not be NULL
		{
			name: "test_variadic_1",
			input: input{
				args:  []types.T{types.T_int32, types.T_varchar, types.T_varchar},
				fArgs: MakeUnLimitArgList(false, makeSimpleArgs([]types.T{types.T_int32, types.T_varchar}), []int{1, NoLimit}, 10),
			},
			want: true,
		},
		{
			name: "test_variadic_2",
			input: input{
				args:  []types.T{types.T_int32, types.T_varchar, types.T_varchar},
				fArgs: MakeUnLimitArgList(false, makeSimpleArgs([]types.T{types.T_int32, types.T_varchar}), []int{NoLimit, 1}, 10),
			},
			want: false,
		},
		{
			name: "test_variadic_3",
			input: input{
				args:  []types.T{types.T_varchar, types.T_varchar},
				fArgs: MakeUnLimitArgList(false, makeSimpleArgs([]types.T{types.T_varchar, types.T_varchar}), []int{1, NoLimit}, 10),
			},
			want: true,
		},
		{
			name: "test_variadic_4",
			input: input{
				args:  nil,
				fArgs: MakeUnLimitArgList(false, makeSimpleArgs([]types.T{types.T_varchar, types.T_varchar}), []int{1, NoLimit}, 10),
			},
			want: false,
		},
		{
			name: "test_variadic_5",
			input: input{
				args: []types.T{types.T_int64, types.T_float64, types.T_float64, types.T_int64},
				fArgs: MakeUnLimitArgList(false, makeSimpleArgs([]types.T{types.T_int64, types.T_float64, types.T_float64, types.T_int64}),
					[]int{1, 1, NoLimit, 1}, 20),
			},
			want: true,
		},
		{
			name: "test_variadic_6",
			input: input{
				args: []types.T{types.T_float64, types.T_float64, types.T_int64},
				fArgs: MakeUnLimitArgList(false, makeSimpleArgs([]types.T{types.T_float64, types.T_int64}),
					[]int{NoLimit, 1}, 20),
			},
			want: true,
		},
		{
			name: "test_variadic_7",
			input: input{
				args: []types.T{types.T_float64, types.T_float64, types.T_float64},
				fArgs: MakeUnLimitArgList(false, makeSimpleArgs([]types.T{types.T_float64, types.T_float64}),
					[]int{1, NoLimit}, 2),
			},
			want: false,
		},
		{
			name: "test_variadic_8",
			input: input{
				args: []types.T{types.T_float64, types.T_float64, NullValueType},
				fArgs: MakeUnLimitArgList(false, makeSimpleArgs([]types.T{types.T_float64, types.T_int64}),
					[]int{NoLimit, 1}, 20),
			},
			want: false,
		},
		// 4. argument number is variadic and can be NULL
		{
			name: "test_variadic_null_1",
			input: input{
				args: []types.T{types.T_float64, NullValueType},
				fArgs: MakeUnLimitArgList(true, makeSimpleArgs([]types.T{types.T_float64, types.T_float64}),
					[]int{1, NoLimit}, 10),
			},
			want: true,
		},
		{
			name: "test_variadic_null_2",
			input: input{
				args: []types.T{NullValueType, NullValueType},
				fArgs: MakeUnLimitArgList(true, makeSimpleArgs([]types.T{types.T_float64, types.T_float64}),
					[]int{1, NoLimit}, 10),
			},
			want: true,
		},
		{
			name: "test_variadic_null_3",
			input: input{
				args: []types.T{types.T_float64, types.T_float64},
				fArgs: MakeUnLimitArgList(true, makeSimpleArgs([]types.T{types.T_float64, types.T_float64}),
					[]int{1, NoLimit}, 10),
			},
			want: true,
		},
		{
			name: "test_variadic_null_4",
			input: input{
				args: nil,
				fArgs: MakeUnLimitArgList(true, makeSimpleArgs([]types.T{types.T_float64, types.T_float64}),
					[]int{1, NoLimit}, 10),
			},
			want: false,
		},
		{
			name: "test_variadic_null_5",
			input: input{
				args: []types.T{NullValueType},
				fArgs: MakeUnLimitArgList(true, makeSimpleArgs([]types.T{types.T_float64, types.T_float64, types.T_float64}),
					[]int{1, 1, NoLimit}, 10),
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := argumentCheck(tt.input.args, tt.input.fArgs); got != tt.want {
				t.Errorf("case name: '%s', argumentCheck() = %v, want %v", tt.name, got, tt.want)
			}
		})
	}
}
