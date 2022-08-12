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

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestOpXorGeneral(t *testing.T) {
	testCases := []arg{
		{
			info: "1 ^ 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarUint64(1, 1),
				testutil.MakeScalarUint64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(3, 1),
		},

		{
			info: "null ^ 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(1),
				testutil.MakeScalarUint64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarNull(1),
		},

		{
			info: "a ^ 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeUint64Vector([]uint64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeScalarUint64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeUint64Vector([]uint64{3, 0, 1, 0}, []uint64{1, 3}),
		},

		{
			info: "a ^ b", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeUint64Vector([]uint64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeUint64Vector([]uint64{2, 3, 2, 4}, []uint64{1, 3}),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeUint64Vector([]uint64{3, 0, 1, 0}, []uint64{1, 3}),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			got, ergot := OpBitXorFun[uint64](tc.vs, tc.proc)
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
				testutil.MakeScalarUint64(1, 1),
				testutil.MakeScalarUint64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(3, 1),
		},

		{
			info: "null | 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(1),
				testutil.MakeScalarUint64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarNull(1),
		},

		{
			info: "a | 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeUint64Vector([]uint64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeScalarUint64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeUint64Vector([]uint64{3, 0, 3, 0}, []uint64{1, 3}),
		},

		{
			info: "a | b", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeUint64Vector([]uint64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeUint64Vector([]uint64{2, 3, 2, 4}, []uint64{1, 3}),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeUint64Vector([]uint64{3, 0, 3, 0}, []uint64{1, 3}),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			got, ergot := OpBitOrFun[uint64](tc.vs, tc.proc)
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
				testutil.MakeScalarUint64(1, 1),
				testutil.MakeScalarUint64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(0, 1),
		},

		{
			info: "null & 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(1),
				testutil.MakeScalarUint64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarNull(1),
		},

		{
			info: "a & 2", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeUint64Vector([]uint64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeScalarUint64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeUint64Vector([]uint64{0, 0, 2, 0}, []uint64{1, 3}),
		},

		{
			info: "a & b", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeUint64Vector([]uint64{1, 0, 3, 0}, []uint64{1, 3}),
				testutil.MakeUint64Vector([]uint64{2, 3, 2, 4}, []uint64{1, 3}),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeUint64Vector([]uint64{0, 0, 2, 0}, []uint64{1, 3}),
		},
	}

	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			got, ergot := OpBitAndFun[uint64](tc.vs, tc.proc)
			if tc.err {
				require.Errorf(t, ergot, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, ergot)
				require.True(t, testutil.CompareVectors(tc.expect, got), "got vector is different with expected")
			}
		})
	}
}
