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

package binary

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestInstr(t *testing.T) {
	proc := testutil.NewProc()
	kases := []struct {
		strs    []string
		substrs []string
		wants   []int64
	}{
		{
			strs:    []string{"abc", "abc", "abc", "abc", "abc"},
			substrs: []string{"bc", "b", "abc", "a", "dca"},
			wants:   []int64{2, 2, 1, 1, 0},
		},
		{
			strs:    []string{"abc", "abc", "abc", "abc", "abc"},
			substrs: []string{"", "", "a", "b", "c"},
			wants:   []int64{1, 1, 1, 2, 3},
		},
		{
			strs:    []string{"abc", "abc", "abc", "abc", "abc"},
			substrs: []string{"bc"},
			wants:   []int64{2, 2, 2, 2, 2},
		},
		{
			strs:    []string{"abc"},
			substrs: []string{"bc", "b", "abc", "a", "dca"},
			wants:   []int64{2, 2, 1, 1, 0},
		},
	}
	for _, k := range kases {
		inVec := testutil.MakeVarcharVector(k.strs, nil)
		subVec := testutil.MakeVarcharVector(k.substrs, nil)
		if len(k.strs) == 1 {
			inVec.SetClass(vector.CONSTANT)
			inVec.SetLength(1)
		}
		if len(k.substrs) == 1 {
			subVec.SetClass(vector.CONSTANT)
			subVec.SetLength(1)
		}
		v, err := Instr([]*vector.Vector{inVec, subVec}, proc)
		require.NoError(t, err)
		vSlice := vector.MustFixedCol[int64](v)
		require.Equal(t, k.wants, vSlice)
		if inVec.IsConst() {
			inVec.GetNulls().Set(0)
			_, err = Instr([]*vector.Vector{inVec, subVec}, proc)
			require.NoError(t, err)
		}
	}

}
