// Copyright 2021 Matrix Origin
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

package bit_or

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

func TestBitOr(t *testing.T) {
	bo := New[int8]()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vec := testutil.NewVector(Rows, types.New(types.T_int8, 0, 0, 0), m, false, nil)
	{
		agg := agg.NewUnaryAgg(nil, true, types.New(types.T_int8, 0, 0, 0), types.New(types.T_int64, 0, 0, 0), bo.Grows, bo.Eval, bo.Merge, bo.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, []int64{15}, vector.GetColumn[int64](v))
		v.Free(m)
	}
	{
		vec0 := testutil.NewVector(2, types.New(types.T_int8, 0, 0, 0), m, false, []int8{2, 2})
		agg := agg.NewUnaryAgg(nil, true, types.New(types.T_int8, 0, 0, 0), types.New(types.T_int64, 0, 0, 0), bo.Grows, bo.Eval, bo.Merge, bo.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		agg.Fill(0, int64(0), 2, []*vector.Vector{vec0})
		v, err := agg.Eval(m)
		require.NoError(t, err)
		// 2 BIT_OR 2 --> 2
		require.Equal(t, []int64{2}, vector.GetColumn[int64](v))
		v.Free(m)
		vec0.Free(m)
	}
	{
		agg0 := agg.NewUnaryAgg(nil, true, types.New(types.T_int8, 0, 0, 0), types.New(types.T_int64, 0, 0, 0), bo.Grows, bo.Eval, bo.Merge, bo.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(nil, true, types.New(types.T_int8, 0, 0, 0), types.New(types.T_int64, 0, 0, 0), bo.Grows, bo.Eval, bo.Merge, bo.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{15}, vector.GetColumn[int64](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{15}, vector.GetColumn[int64](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDist(t *testing.T) {
	bo := New[int8]()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vec := testutil.NewVector(Rows, types.New(types.T_int8, 0, 0, 0), m, false, nil)
	{
		agg := agg.NewUnaryDistAgg(true, types.New(types.T_int8, 0, 0, 0), types.New(types.T_int64, 0, 0, 0), bo.Grows, bo.Eval, bo.Merge, bo.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, []int64{15}, vector.GetColumn[int64](v))
		v.Free(m)
	}
	{
		agg0 := agg.NewUnaryDistAgg(true, types.New(types.T_int8, 0, 0, 0), types.New(types.T_int64, 0, 0, 0), bo.Grows, bo.Eval, bo.Merge, bo.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryDistAgg(true, types.New(types.T_int8, 0, 0, 0), types.New(types.T_int64, 0, 0, 0), bo.Grows, bo.Eval, bo.Merge, bo.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{15}, vector.GetColumn[int64](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{15}, vector.GetColumn[int64](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	require.Equal(t, int64(0), m.Size())
}
