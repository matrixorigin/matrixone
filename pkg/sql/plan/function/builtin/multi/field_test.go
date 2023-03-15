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

package multi

import (
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

func TestFieldNumber(t *testing.T) {
	testCases := []arg{
		{
			info: "field(null, 1)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarInt64(1, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(0, 1),
		},

		{
			info: "field(null, 1, 1)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarInt64(1, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(0, 1),
		},

		{
			info: "field(1, 1, 2)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarInt64(2, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(1, 1),
		},

		{
			info: "field(1, 2, 1)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarInt64(2, 1),
				testutil.MakeScalarInt64(1, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(2, 1),
		},

		{
			info: "field(1, 2, 3, 4)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarInt64(2, 1),
				testutil.MakeScalarInt64(3, 1),
				testutil.MakeScalarInt64(4, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(0, 1),
		},

		{
			info: "field(1, null, 1)", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarInt64(1, 1),
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarInt64(1, 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(2, 1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			gotV, err := FieldNumber[int64](tc.vs, tc.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, vector.MustFixedCol[uint64](tc.expect), vector.MustFixedCol[uint64](gotV))
		})
	}
}

func TestFieldString(t *testing.T) {
	testCases := []arg{
		{
			info: "field(null, 'a')", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarVarchar("a", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(0, 1),
		},

		{
			info: "field(null, 'a', 'a')", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarVarchar("a", 1),
				testutil.MakeScalarVarchar("a", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(0, 1),
		},

		{
			info: "field('Bb', 'Bb', 'Aa')", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("Bb", 1),
				testutil.MakeScalarVarchar("Bb", 1),
				testutil.MakeScalarVarchar("Aa", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(1, 1),
		},

		{
			info: "field('Bb', 'Aa', 'Bb')", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("Bb", 1),
				testutil.MakeScalarVarchar("Aa", 1),
				testutil.MakeScalarVarchar("Bb", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(2, 1),
		},

		{
			info: "field('Gg', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff')", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("Gg", 1),
				testutil.MakeScalarVarchar("Aa", 1),
				testutil.MakeScalarVarchar("Bb", 1),
				testutil.MakeScalarVarchar("Cc", 1),
				testutil.MakeScalarVarchar("Dd", 1),
				testutil.MakeScalarVarchar("Ff", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(0, 1),
		},

		{
			info: "field('Bb', null, 'Bb')", proc: testutil.NewProc(),
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("Bb", 1),
				testutil.MakeScalarNull(types.T_any, 1),
				testutil.MakeScalarVarchar("Bb", 1),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeScalarUint64(2, 1),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			gotV, err := FieldString(tc.vs, tc.proc)
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, vector.MustFixedCol[uint64](tc.expect), vector.MustFixedCol[uint64](gotV))
		})
	}
}
