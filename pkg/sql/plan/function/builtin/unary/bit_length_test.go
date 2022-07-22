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

package unary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestBitLength(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name     string
		proc     *process.Process
		inputstr []string
		inputNsp []uint64
		expected []int64
		isScalar bool
	}{
		{
			name:     "Empty string",
			proc:     procs,
			inputstr: []string{""},
			expected: []int64{0},
			isScalar: false,
		},
		{
			name:     "Scalar empty string",
			proc:     procs,
			inputstr: []string{""},
			expected: []int64{0},
			isScalar: true,
		},
		{
			name:     "Non-empty scalar string",
			proc:     procs,
			inputstr: []string{"matrix", "origin", "=", "mo", " ", "\t", ""},
			expected: []int64{48, 48, 8, 16, 8, 8, 0},
			isScalar: false,
		},
		{
			name:     "Null",
			proc:     procs,
			expected: []int64{0},
			isScalar: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vecs := makeBitLenTestVectors(c.inputstr, c.inputNsp, c.isScalar)
			result, err := BitLengthFunc(vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			col := result.Col.([]int64)
			require.Equal(t, c.expected, col)
			require.Equal(t, c.isScalar, result.IsScalar())
		})
	}
}

func makeBitLenTestVectors(data []string, nsp []uint64, isScalar bool) []*vector.Vector {
	vec := make([]*vector.Vector, 1)
	if data != nil {
		vec[0] = testutil.MakeCharVector(data, nsp)
		vec[0].IsConst = isScalar
	} else {
		vec[0] = testutil.MakeScalarNull(0)
	}

	return vec
}
