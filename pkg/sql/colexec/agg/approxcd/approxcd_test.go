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

package approxcd

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/agg"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/mheap"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/guest"
	"github.com/matrixorigin/matrixone/pkg/vm/mmu/host"
	"github.com/stretchr/testify/require"
)

const (
	Rows = 10
)

func TestApproxcdCount(t *testing.T) {
	testTyp := types.New(types.T_int8, 0, 0, 0)
	retTyp := types.New(types.T_uint64, 0, 0, 0)
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vs := []int8{1, 1, 2, 2, 3, 3, 4, 4, 5, 5}
	vs2 := []int8{5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10}
	vec := testutil.NewVector(Rows, testTyp, m, false, vs)
	vec2 := testutil.NewVector(Rows, testTyp, m, false, vs2)
	{
		a := NewApproxc[int8]()
		agg := agg.NewUnaryAgg(a, true, testTyp, retTyp, a.Grows, a.Eval, a.Merge, a.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, []uint64{5}, vector.GetColumn[uint64](v))
		v.Free(m)
	}
	{
		a1 := NewApproxc[int8]()
		a2 := NewApproxc[int8]()
		agg0 := agg.NewUnaryAgg(a1, true, testTyp, retTyp, a1.Grows, a1.Eval, a1.Merge, a1.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(a2, true, testTyp, retTyp, a2.Grows, a2.Eval, a2.Merge, a2.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []uint64{9}, vector.GetColumn[uint64](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []uint64{5}, vector.GetColumn[uint64](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}
