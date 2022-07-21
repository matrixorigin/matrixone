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

package sum

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

//TODO: add distict decimal128 test
func TestSum(t *testing.T) {
	testTyp := types.New(types.T_int8, 0, 0, 0)
	s := NewSum[int8, int64]()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vs := []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	vec := testutil.NewVector(Rows, testTyp, m, false, vs)
	expected := []int64{45}
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(nil, true, testTyp, types.New(types.T_int64, 0, 0, 0), s.Grows, s.Eval, s.Merge, s.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, expected, vector.GetColumn[int64](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(nil, true, testTyp, types.New(types.T_int64, 0, 0, 0), s.Grows, s.Eval, s.Merge, s.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(nil, true, testTyp, types.New(types.T_int64, 0, 0, 0), s.Grows, s.Eval, s.Merge, s.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{90}, vector.GetColumn[int64](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, expected, vector.GetColumn[int64](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDecimal128Sum(t *testing.T) {
	testTyp := types.New(types.T_decimal128, 0, 0, 0)
	ds := NewD128Sum()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	vec := testutil.MakeDecimal128Vector(input1, nil, testTyp)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(nil, true, testTyp, testTyp, ds.Grows, ds.Eval, ds.Merge, ds.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, MakeDecimal128Arr([]int64{45}), vector.GetColumn[types.Decimal128](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, ds.Grows, ds.Eval, ds.Merge, ds.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, ds.Grows, ds.Eval, ds.Merge, ds.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, MakeDecimal128Arr([]int64{90}), vector.GetColumn[types.Decimal128](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, MakeDecimal128Arr([]int64{45}), vector.GetColumn[types.Decimal128](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDistincSum(t *testing.T) {
	testTyp := types.New(types.T_int8, 0, 0, 0)
	s := NewSum[int8, int64]()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vs := []int8{1, 1, 2, 2, 3, 3, 4, 4, 5, 5}
	vs2 := []int8{6, 6, 7, 7, 8, 8, 9, 9, 10, 10}
	vec := testutil.NewVector(Rows, testTyp, m, false, vs)
	vec2 := testutil.NewVector(Rows, testTyp, m, false, vs2)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryDistAgg(true, testTyp, types.New(types.T_int64, 0, 0, 0), s.Grows, s.Eval, s.Merge, s.Fill)
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
		// test two agg with Merge function
		agg0 := agg.NewUnaryDistAgg(true, testTyp, types.New(types.T_int64, 0, 0, 0), s.Grows, s.Eval, s.Merge, s.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryDistAgg(true, testTyp, types.New(types.T_int64, 0, 0, 0), s.Grows, s.Eval, s.Merge, s.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{55}, vector.GetColumn[int64](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{40}, vector.GetColumn[int64](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func MakeDecimal128Arr(input []int64) []types.Decimal128 {
	ret := make([]types.Decimal128, len(input))
	for i, v := range input {
		ret[i] = types.InitDecimal128(v)
	}

	return ret
}
