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
		expected []uint8
		isScalar bool
	}{
		{
			name:     "Empty string",
			proc:     procs,
			inputstr: []string{""},
			expected: []uint8{1},
			isScalar: false,
		},
		{
			name:     "Empty scalar string",
			proc:     procs,
			inputstr: []string{""},
			expected: []uint8{1},
			isScalar: true,
		},
		{
			name:     "Non-empty scalar string",
			proc:     procs,
			inputstr: []string{"ab"},
			expected: []uint8{0},
			isScalar: true,
		},
		{
			name:     "String with empty element",
			proc:     procs,
			inputstr: []string{"ab", "cd", "", "ef", " ", "\n"},
			expected: []uint8{0, 0, 1, 0, 0, 0},
			isScalar: false,
		},
		{
			name:     "Non-empty string",
			proc:     procs,
			inputstr: []string{"ab", "cd", " ", "\t", "\n", "\r"},
			expected: []uint8{0, 0, 0, 0, 0, 0},
			isScalar: false,
		},
		{
			name:     "Non-empty string with null list",
			proc:     procs,
			inputstr: []string{"ab", "", " ", "\t", "\n", "\r", ""},
			inputNsp: []uint64{1, 4},
			expected: []uint8{0, 1, 0, 0, 0, 0, 1},
			isScalar: false,
		},
		{
			name:     "Null",
			proc:     procs,
			expected: []uint8{0},
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
			col := vector.MustTCols[uint8](result)
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
			vec[0].MakeScalar(1)
		}
	} else {
		vec[0] = testutil.MakeScalarNull(types.T_char, 0)
	}

	return vec
}
