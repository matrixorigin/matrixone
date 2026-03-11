// Copyright 2022 Matrix Origin
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

package operator

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type arg struct {
	info   string
	proc   *process.Process
	vs     []*vector.Vector
	match  bool // if false, the case shouldn't meet the function requirement
	err    bool
	expect *vector.Vector
}

func TestCwFn1(t *testing.T) {
	testCases := []arg{
		{
			info: "when a = 1 then 1, when a = 2 then 2, else 3", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeBoolVector([]bool{true, true, false, false, false}),  // a = 1
				testutil.MakeScalarInt64(1, 5),                                    // 1
				testutil.MakeBoolVector([]bool{false, false, false, true, false}), // a = 2
				testutil.MakeScalarInt64(2, 5),                                    // 2
				testutil.MakeScalarInt64(3, 5),                                    // 3
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{1, 1, 3, 2, 3}, nil),
		},

		{
			info: "when a = 1 then 1, when a = 2 then 2, else null", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeBoolVector([]bool{false, false, false, false}), // a = 1
				testutil.MakeScalarInt64(1, 4),                              // 1
				testutil.MakeBoolVector([]bool{true, false, false, false}),  // a = 2
				testutil.MakeScalarInt64(2, 4),                              // 2
				testutil.MakeScalarNull(types.T_int64, 4),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{2, 0, 0, 0}, []uint64{1, 2, 3}),
		},

		{
			info: "when a = 1 then 1, when a = 2 then 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeBoolVector([]bool{false, false, false, false}), // a = 1
				testutil.MakeScalarInt64(1, 4),                              // 1
				testutil.MakeBoolVector([]bool{true, false, false, false}),  // a = 2
				testutil.MakeScalarInt64(2, 4),                              // 2
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{2, 0, 0, 0}, []uint64{1, 2, 3}),
		},

		{
			info: "when a = 1 then c1, when a = 2 then c2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeBoolVector([]bool{true, true, false, false, false}),    // a = 1
				testutil.MakeInt64Vector([]int64{1, 0, 3, 4, 5}, []uint64{1}),       // 1, null, 3, 4, 5
				testutil.MakeBoolVector([]bool{false, false, true, true, false}),    // a = 2
				testutil.MakeInt64Vector([]int64{0, 0, 0, 0, 0}, []uint64{1, 2, 3}), // 0, null, null, null, 0
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{1, 0, 0, 0, 0}, []uint64{1, 2, 3, 4}), // 1, null, null, null, null
		},

		{
			info: "when a = 1 then c1, when a = 2 then c2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeBoolVector([]bool{true, true, false, false, false}),    // a = 1
				testutil.MakeInt64Vector([]int64{1, 0, 3, 4, 5}, []uint64{1}),       // 1, null, 3, 4, 5
				testutil.MakeBoolVector([]bool{false, false, true, true, false}),    // a = 2
				testutil.MakeInt64Vector([]int64{0, 0, 0, 0, 0}, []uint64{1, 2, 3}), // 0, null, null, null, 0
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{1, 0, 0, 0, 0}, []uint64{1, 2, 3, 4}), // 1, null, null, null, null
		},

		{
			info: "when a = 1 then c1, when a = 2 then c2, else null", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeBoolVector([]bool{true, true, false, false, false}),    // a = 1
				testutil.MakeInt64Vector([]int64{1, 0, 3, 4, 5}, []uint64{1}),       // 1, null, 3, 4, 5
				testutil.MakeBoolVector([]bool{false, false, true, true, false}),    // a = 2
				testutil.MakeInt64Vector([]int64{0, 0, 0, 0, 0}, []uint64{1, 2, 3}), // 0, null, null, null, 0
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{1, 0, 0, 0, 0}, []uint64{1, 2, 3, 4}), // 1, null, null, null, null
		},

		{
			info: "when a = 1 then c1, when a = 2 then c2, else c3", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeBoolVector([]bool{true, true, false, false, false}),      // a = 1
				testutil.MakeInt64Vector([]int64{1, 0, 3, 4, 5}, []uint64{1}),         // 1, null, 3, 4, 5
				testutil.MakeBoolVector([]bool{false, false, true, true, false}),      // a = 2
				testutil.MakeInt64Vector([]int64{0, 0, 0, 0, 0}, []uint64{1, 2, 3}),   // 0, null, null, null, 0
				testutil.MakeInt64Vector([]int64{100, 200, 300, 0, 500}, []uint64{3}), // 100, 200, 300, null, 500
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{1, 0, 0, 0, 500}, []uint64{1, 2, 3}), // 1, null, null, null, 500
		},

		{
			info: "when true then c1, when false then c2, else null", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarBool(true, 4),
				testutil.MakeInt64Vector([]int64{1, 2, 3, 4}, nil), // 1, 2, 3, 4
				testutil.MakeScalarBool(false, 4),
				testutil.MakeInt64Vector([]int64{4, 3, 2, 1}, nil),
				testutil.MakeScalarNull(types.T_int64, 4),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{1, 2, 3, 4}, nil), // 1, 2, 3, 4
		},

		{
			info: "when true then 1, when false then 2, else null", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarBool(true, 4),
				testutil.MakeScalarInt64(1, 4),
				testutil.MakeScalarBool(false, 4),
				testutil.MakeScalarInt64(2, 4),
				testutil.MakeScalarNull(types.T_int64, 4),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(1, 4),
		},

		{
			info: "when a = 1 then 10, when a = 2 then true, else null", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeBoolVector([]bool{false, false, false, false}),
				testutil.MakeScalarInt64(10, 4),
				testutil.MakeBoolVector([]bool{false, false, false, false}),
				testutil.MakeScalarBool(true, 4),
				testutil.MakeScalarNull(types.T_bool, 4),
			},
			match: false,
		},

		{
			// special case 1
			info: "when a = 1 then 1, when a = 1 then 2, else null", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeBoolVector([]bool{true, true, false, false}),
				testutil.MakeScalarInt64(1, 4),
				testutil.MakeBoolVector([]bool{false, true, true, false}),
				testutil.MakeScalarInt64(2, 4),
				testutil.MakeScalarNull(types.T_int64, 4),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{1, 1, 2, 0}, []uint64{3}),
		},

		{
			// special case 2
			info: "when true then null else null", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarBool(true, 5),
				testutil.MakeScalarNull(types.T_any, 5),
				testutil.MakeScalarNull(types.T_any, 5),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarNull(types.T_any, 5),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			{
				inputTypes := make([]types.T, len(tc.vs))
				for i := range inputTypes {
					inputTypes[i] = tc.vs[i].GetType().Oid
				}
				b := CwTypeCheckFn(inputTypes, nil, types.T_int64)
				if !tc.match {
					require.False(t, b, fmt.Sprintf("case '%s' shouldn't meet the function's requirement but it meets.", tc.info))
					return
				}
				require.True(t, b)
			}

			got, ergot := cwGeneral[int64](tc.vs, tc.proc, types.Type{Oid: types.T_int64})
			if tc.err {
				require.Errorf(t, ergot, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, ergot)
				require.True(t, testutil.CompareVectors(tc.expect, got), "got vector is different with expected")
			}
		})
	}
}
