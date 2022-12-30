// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package multi

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestReplace(t *testing.T) {
	testCases := []arg{
		{
			info: "Single string case1",
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"abc"}, nil),
				testutil.MakeVarcharVector([]string{"a"}, nil),
				testutil.MakeVarcharVector([]string{"d"}, nil),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"dbc"}, nil),
		},

		{
			info: "Single string case2",
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{".*.*.*"}, nil),
				testutil.MakeVarcharVector([]string{".*"}, nil),
				testutil.MakeVarcharVector([]string{"n"}, nil),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"nnn"}, nil),
		},

		{
			info: "Single string case3",
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"当时明月 在 当时"}, nil),
				testutil.MakeVarcharVector([]string{"当时"}, nil),
				testutil.MakeVarcharVector([]string{"此时"}, nil),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"此时明月 在 此时"}, nil),
		},

		{
			info: "Single string case4",
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"123"}, nil),
				testutil.MakeVarcharVector([]string{""}, nil),
				testutil.MakeVarcharVector([]string{"n"}, nil),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"123"}, nil),
		},

		{
			info: "Multi string case1",
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"firststring", "secondstring"}, nil),
				testutil.MakeVarcharVector([]string{"st"}, nil),
				testutil.MakeVarcharVector([]string{"re"}, nil),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"firrerering", "secondrering"}, nil),
		},

		{
			info: "Multi string case2",
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"Oneinput"}, nil),
				testutil.MakeVarcharVector([]string{"n"}, nil),
				testutil.MakeVarcharVector([]string{"e", "b"}, nil),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"Oeeieput", "Obeibput"}, nil),
		},

		{
			info: "Multi string case3",
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"aaabbb"}, nil),
				testutil.MakeVarcharVector([]string{"a", "b"}, nil),
				testutil.MakeVarcharVector([]string{"n"}, nil),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"nnnbbb", "aaannn"}, nil),
		},

		{
			info: "Multi string case4",
			vs: []*vector.Vector{
				testutil.MakeVarcharVector([]string{"Matrix", "Origin"}, nil),
				testutil.MakeVarcharVector([]string{"a", "i"}, nil),
				testutil.MakeVarcharVector([]string{"b", "d"}, nil),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"Mbtrix", "Ordgdn"}, nil),
		},

		{
			info: "Scalar case1",
			vs: []*vector.Vector{
				testutil.MakeScalarVarchar("cool", 1),
				testutil.MakeVarcharVector([]string{"o"}, nil),
				testutil.MakeVarcharVector([]string{"a"}, nil),
			},
			match:  true,
			err:    false,
			expect: testutil.MakeVarcharVector([]string{"caal"}, nil),
		},
	}

	proc := testutil.NewProcess()
	for i, tc := range testCases {
		t.Run(tc.info, func(t *testing.T) {
			resultVector, rerr := Replace(tc.vs, proc)
			if tc.err {
				require.Errorf(t, rerr, fmt.Sprintf("case '%d' expected error, but no error happens", i))
			} else {
				require.NoError(t, rerr)
				require.True(t, testutil.CompareVectors(tc.expect, resultVector), "got vector is different with expected")
			}
		})
	}
}
