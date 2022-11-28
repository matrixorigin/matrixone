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
	"github.com/stretchr/testify/require"
)

func TestCoalesceGeneral(t *testing.T) {
	testCases := []arg{
		{
			info: "coalesce(null, 1)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarInt64(1, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(1, 1),
		},

		{
			info: "coalesce(null, 1, null, 3)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarInt64(1, 3),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(1, 1),
		},

		{
			info: "coalesce(a, 1, null, 3)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarInt64(1, 3),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{1, 1, 3, 1}, nil),
		},

		{
			info: "coalesce(a, null, b)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeInt64Vector([]int64{1, 11, 3, 0}, []uint64{3}),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{1, 11, 3, 0}, []uint64{3}),
		},

		{
			info: "coalesce(a, null, 33)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarInt64(33, 4),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{1, 33, 3, 33}, nil),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			{
				inputTypes := make([]types.T, len(tc.vs))
				for i := range inputTypes {
					inputTypes[i] = tc.vs[i].Typ.Oid
				}
				b := CoalesceTypeCheckFn(inputTypes, nil, types.T_int64)
				if !tc.match {
					require.False(t, b, fmt.Sprintf("case '%s' shouldn't meet the function's requirement but it meets.", tc.info))
					return
				}
				require.True(t, b)
			}

			got, ergot := coalesceGeneral[int64](tc.vs, tc.proc, types.Type{Oid: types.T_int64})
			if tc.err {
				require.Errorf(t, ergot, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, ergot)
				require.True(t, testutil.CompareVectors(tc.expect, got), "got vector is different with expected")
			}
		})
	}
}

func TestCoalesceString(t *testing.T) {
	testCases := []arg{
		{
			info: "coalesce(null, 'a', null, 'b')", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarVarchar("a", 1),
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarVarchar("b", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarVarchar("a", 1),
		},

		{
			info: "coalesce(a, 'a')", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"x", "y", "z"}, nil),
				testutil.MakeScalarVarchar("a", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"x", "y", "z"}, nil),
		},

		{
			info: "coalesce(a, 'a', null, 'b')", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"kk", "", "ss", ""}, []uint64{1, 3}),
				testutil.MakeScalarVarchar("a", 1),
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarVarchar("b", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"kk", "a", "ss", "a"}, nil),
		},

		{
			info: "coalesce(a, null, 'b')", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"kk", "", "ss", ""}, []uint64{1, 3}),
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarVarchar("b", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"kk", "b", "ss", "b"}, nil),
		},

		{
			info: "coalesce(a, null, b)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"a1", "", "a2", ""}, []uint64{1, 3}),
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeVarcharVector([]string{"b1", "b2", "", ""}, []uint64{2, 3}),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"a1", "b2", "a2", ""}, []uint64{3}),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			{
				inputTypes := make([]types.T, len(tc.vs))
				for i := range inputTypes {
					inputTypes[i] = tc.vs[i].Typ.Oid
				}
				b := CoalesceTypeCheckFn(inputTypes, nil, types.T_varchar)
				if !tc.match {
					require.False(t, b, fmt.Sprintf("case '%s' shouldn't meet the function's requirement but it meets.", tc.info))
					return
				}
				require.True(t, b)
			}

			got, ergot := coalesceString(tc.vs, tc.proc, types.Type{Oid: types.T_varchar, Width: types.MaxVarcharLen})
			if tc.err {
				require.Errorf(t, ergot, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, ergot)
				require.True(t, testutil.CompareVectors(tc.expect, got), "got vector is different with expected")
			}
		})
	}
}
