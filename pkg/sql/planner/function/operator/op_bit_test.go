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

func TestOpXorGeneral(t *testing.T) {
	testCases := []arg{
		{
			info: "1 ^ 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(3, 1),
		},

		{
			info: "-1 ^ 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(-1, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(-3, 1),
		},

		{
			info: "null ^ 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_int64, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarNull(types.T_int64, 1),
		},

		{
			info: "a ^ 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{-1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{-3, 0, 1, 0}, []uint64{1, 3}),
		},

		{
			info: "a ^ b", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{-1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeInt64Vector([]int64{2, 3, 2, 4}, []uint64{1, 3}),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{-3, 0, 1, 0}, []uint64{1, 3}),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			got, ergot := OpBitXorFun[int64](tc.vs, tc.proc)
			if tc.err {
				require.Errorf(t, ergot, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, ergot)
				require.True(t, testutil.CompareVectors(tc.expect, got), "got vector is different with expected")
			}
		})
	}
}

func TestOpOrGeneral(t *testing.T) {
	testCases := []arg{
		{
			info: "1 | 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(3, 1),
		},

		{
			info: "-1 | 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(-1, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(-1, 1),
		},

		{
			info: "null | 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_int64, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarNull(types.T_int64, 1),
		},

		{
			info: "a | 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{3, 0, 3, 0}, []uint64{1, 3}),
		},

		{
			info: "a | b", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeInt64Vector([]int64{2, 3, 2, 4}, []uint64{1, 3}),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{3, 0, 3, 0}, []uint64{1, 3}),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			got, ergot := OpBitOrFun[int64](tc.vs, tc.proc)
			if tc.err {
				require.Errorf(t, ergot, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, ergot)
				require.True(t, testutil.CompareVectors(tc.expect, got), "got vector is different with expected")
			}
		})
	}
}

func TestOpAndGeneral(t *testing.T) {
	testCases := []arg{
		{
			info: "1 & 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(0, 1),
		},

		{
			info: "-1 & 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(-1, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(2, 1),
		},

		{
			info: "null & 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_int64, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarNull(types.T_int64, 1),
		},

		{
			info: "a & 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{0, 0, 2, 0}, []uint64{1, 3}),
		},

		{
			info: "a & b", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeInt64Vector([]int64{2, 3, 2, 4}, []uint64{1, 3}),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{0, 0, 2, 0}, []uint64{1, 3}),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			got, ergot := OpBitAndFun[int64](tc.vs, tc.proc)
			if tc.err {
				require.Errorf(t, ergot, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, ergot)
				require.True(t, testutil.CompareVectors(tc.expect, got), "got vector is different with expected")
			}
		})
	}
}

func TestOpRightShiftGeneral(t *testing.T) {
	testCases := []arg{
		{
			info: "1024 >> 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(1024, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(256, 1),
		},

		{
			info: "-5 >> 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(-5, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(-2, 1),
		},

		{
			info: "2 >> -2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(2, 1),
				testutil.MakeScalarInt64(-2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(0, 1),
		},

		{
			info: "null >> 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarNull(types.T_any, 1),
		},

		{
			info: "a >> 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{-5, 0, 1024, 0}, []uint64{1, 3}),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{-2, 0, 256, 0}, []uint64{1, 3}),
		},

		{
			info: "a >> b", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{-5, 0, 1024, 0}, []uint64{1, 3}),
				testutil.MakeInt64Vector([]int64{2, 3, 2, 4}, []uint64{1, 3}),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{-2, 0, 256, 0}, []uint64{1, 3}),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			got, ergot := OpBitRightShiftFun[int64](tc.vs, tc.proc)
			if tc.err {
				require.Errorf(t, ergot, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, ergot)
				require.True(t, testutil.CompareVectors(tc.expect, got), "got vector is different with expected")
			}
		})
	}
}

func TestOpLeftShiftGeneral(t *testing.T) {
	testCases := []arg{
		{
			info: "1 << 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(4, 1),
		},

		{
			info: "-1 << 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(-1, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(-4, 1),
		},

		{
			info: "2 << -2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(2, 1),
				testutil.MakeScalarInt64(-2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarInt64(0, 1),
		},

		{
			info: "null << 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarNull(types.T_any, 1),
		},

		{
			info: "a << 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{-1, 0, 1, 0}, []uint64{1, 3}),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{-4, 0, 4, 0}, []uint64{1, 3}),
		},

		{
			info: "a << b", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeInt64Vector([]int64{-1, 0, 1, 0}, []uint64{1, 3}),
				testutil.MakeInt64Vector([]int64{2, 3, 2, 4}, []uint64{1, 3}),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeInt64Vector([]int64{-4, 0, 4, 0}, []uint64{1, 3}),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			got, ergot := OpBitLeftShiftFun[int64](tc.vs, tc.proc)
			if tc.err {
				require.Errorf(t, ergot, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, ergot)
				require.True(t, testutil.CompareVectors(tc.expect, got), "got vector is different with expected")
			}
		})
	}
}
