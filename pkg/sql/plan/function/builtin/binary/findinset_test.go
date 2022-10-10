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

package binary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestFindInSetLength(t *testing.T) {
	procs := testutil.NewProc()
	cases := []struct {
		name      string
		proc      *process.Process
		left      []string
		right     []string
		isScalarL bool
		isScalarR bool
		expected  []uint64
	}{
		{
			name:      "normal string test 01",
			proc:      procs,
			left:      []string{"abc"},
			right:     []string{"abc,def"},
			expected:  []uint64{1},
			isScalarL: false,
			isScalarR: false,
		},
		{
			name:      "normal string test 02",
			proc:      procs,
			left:      []string{"xyz"},
			right:     []string{"dec,xyz,abc"},
			expected:  []uint64{2},
			isScalarL: false,
			isScalarR: false,
		},
		{
			name:      "left scalar test",
			proc:      procs,
			left:      []string{"z"},
			right:     []string{"a,e,c,z"},
			expected:  []uint64{4},
			isScalarL: false,
			isScalarR: false,
		},
		{
			name:      "left scalar test",
			proc:      procs,
			left:      []string{"abc"},
			right:     []string{"abc,def"},
			expected:  []uint64{1},
			isScalarL: true,
			isScalarR: false,
		},
		{
			name:      "right scalar test",
			proc:      procs,
			left:      []string{"abc"},
			right:     []string{"abc,def"},
			expected:  []uint64{1},
			isScalarL: false,
			isScalarR: true,
		},
		{
			name:      "both scalar test",
			proc:      procs,
			left:      []string{"abc"},
			right:     []string{"abc,def"},
			expected:  []uint64{1},
			isScalarL: true,
			isScalarR: true,
		},
		{
			name:      "left null test",
			proc:      procs,
			right:     []string{"abc"},
			expected:  []uint64{0},
			isScalarL: true,
			isScalarR: true,
		},
		{
			name:      "right null test",
			proc:      procs,
			left:      []string{"abc"},
			expected:  []uint64{0},
			isScalarL: true,
			isScalarR: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			vecs := makeFindInSetTestVectors(c.left, c.right, c.isScalarL, c.isScalarR)
			result, err := FindInSet(vecs, c.proc)
			if err != nil {
				t.Fatal(err)
			}
			col := result.Col.([]uint64)
			require.Equal(t, c.expected, col)
			require.Equal(t, c.isScalarL && c.isScalarR, result.IsScalar())
		})
	}
}

func makeFindInSetTestVectors(left []string, right []string, isScalarL bool, isScalarR bool) []*vector.Vector {
	mp := mpool.MustNewZero()
	vec := make([]*vector.Vector, 2)
	if left != nil {
		if isScalarL {
			vec[0] = vector.NewConstString(types.T_varchar.ToType(), 1, left[0], mp)
		} else {
			vec[0] = testutil.MakeVarcharVector(left, nil)
		}
	} else {
		vec[0] = testutil.MakeScalarNull(types.T_varchar, 0)
	}

	if right != nil {
		if isScalarR {
			vec[1] = vector.NewConstString(types.T_varchar.ToType(), 1, right[0], mp)
		} else {
			vec[1] = testutil.MakeVarcharVector(right, nil)
		}
	} else {
		vec[1] = testutil.MakeScalarNull(types.T_varchar, 0)
	}

	return vec
}
