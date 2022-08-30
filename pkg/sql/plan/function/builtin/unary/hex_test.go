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

	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestHex(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name     string
		proc     *process.Process
		inputstr []string
		inputNsp []uint64
		expected []string
		isScalar bool
	}{
		{
			name:     "Empty string",
			proc:     procs,
			inputstr: []string{""},
			expected: []string{""},
			isScalar: false,
		},
		{
			name:     "Scalar empty string",
			proc:     procs,
			inputstr: []string{""},
			expected: []string{""},
			isScalar: true,
		},
		{
			name:     "Non-empty scalar string",
			proc:     procs,
			inputstr: []string{"hello"},
			expected: []string{"68656c6c6f"},
			isScalar: false,
		},
		{
			name:     "Null",
			proc:     procs,
			expected: []string{""},
			isScalar: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vecs := makeEmptyTestVectors(c.inputstr, c.inputNsp, c.isScalar)
			result, err := Hex(vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			col := result.Col.([]uint8)
			require.Equal(t, c.expected, col)
			require.Equal(t, c.isScalar, result.IsScalar())
		})
	}
}
