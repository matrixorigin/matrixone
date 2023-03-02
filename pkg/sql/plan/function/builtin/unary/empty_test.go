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

package unary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestEmpty(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name     string
		proc     *process.Process
		inputstr []string
		inputNsp []uint64
		expected []bool
		isScalar bool
	}{
		{
			name:     "Empty string",
			proc:     procs,
			inputstr: []string{""},
			expected: []bool{true},
			isScalar: false,
		},
		{
			name:     "Empty scalar string",
			proc:     procs,
			inputstr: []string{""},
			expected: []bool{true},
			isScalar: true,
		},
		{
			name:     "Non-empty scalar string",
			proc:     procs,
			inputstr: []string{"ab"},
			expected: []bool{false},
			isScalar: true,
		},
		{
			name:     "String with empty element",
			proc:     procs,
			inputstr: []string{"ab", "cd", "", "ef", " ", "\n"},
			expected: []bool{false, false, true, false, false, false},
			isScalar: false,
		},
		{
			name:     "Non-empty string",
			proc:     procs,
			inputstr: []string{"ab", "cd", " ", "\t", "\n", "\r"},
			expected: []bool{false, false, false, false, false, false},
			isScalar: false,
		},
		{
			name:     "Non-empty string with null list",
			proc:     procs,
			inputstr: []string{"ab", "", " ", "\t", "\n", "\r", ""},
			inputNsp: []uint64{1, 4},
			expected: []bool{false, true, false, false, false, false, true},
			isScalar: false,
		},
		{
			name:     "Null",
			proc:     procs,
			isScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vecs := makeEmptyTestVectors(c.inputstr, c.inputNsp, c.isScalar)
			result, err := Empty(vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			col := vector.MustFixedCol[bool](result)
			require.Equal(t, c.expected, col)
			require.Equal(t, c.isScalar, result.IsConst())
		})
	}
}

func makeEmptyTestVectors(data []string, nsp []uint64, isScalar bool) []*vector.Vector {
	vec := make([]*vector.Vector, 1)
	if data != nil {
		vec[0] = testutil.MakeCharVector(data, nsp)
		if isScalar {
			vec[0].SetClass(vector.CONSTANT)
			vec[0].SetLength(1)
		}
	} else {
		vec[0] = testutil.MakeScalarNull(types.T_char, 1)
	}

	return vec
}
