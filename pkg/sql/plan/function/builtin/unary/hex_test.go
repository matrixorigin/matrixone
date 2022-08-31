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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
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
		expected []string
		isScalar bool
	}{
		{
			name:     "Normal test",
			proc:     procs,
			inputstr: []string{"a"},
			expected: []string{"61"},
			isScalar: true,
		},
		{
			name:     "Scalar empty string",
			proc:     procs,
			inputstr: []string{""},
			expected: []string{""},
			isScalar: true,
		},
		{
			name:     "multi row test",
			proc:     procs,
			inputstr: []string{"Hello", "Gopher!"},
			expected: []string{"48656c6c6f", "476f7068657221"},
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
			inVector := testutil.MakeVarcharVector(c.inputstr, nil)
			result, err := Hex([]*vector.Vector{inVector}, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			col := result.Col.(*types.Bytes)
			s := string(col.Data)
			require.Equal(t, c.expected, s)
			require.Equal(t, c.isScalar, result.IsScalar())
		})
	}
}
