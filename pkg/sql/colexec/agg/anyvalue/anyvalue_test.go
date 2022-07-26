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

package anyvalue

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

//TODO: add decimal128 and more type test.
func TestAnyvalue(t *testing.T) {
	testTyp := types.New(types.T_int64, 0, 0, 0)
	a := NewAnyvalue[int64]()
	a2 := NewAnyvalue[int64]()
	a3 := NewAnyvalue[int64]()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vs := []int64{0, 1, -2, 3, 14, 5, -6, 7, 8, 9}
	vs2 := []int64{29, 8, 7, -6, 5, 14, -3, 2, 1, 0}
	vec := testutil.NewVector(Rows, testTyp, m, false, vs)
	vec2 := testutil.NewVector(Rows, testTyp, m, false, vs2)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(a, true, testTyp, testTyp, a.Grows, a.Eval, a.Merge, a.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, []int64{0}, vector.GetColumn[int64](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(a2, true, testTyp, testTyp, a2.Grows, a2.Eval, a2.Merge, a2.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(a3, true, testTyp, testTyp, a3.Grows, a3.Eval, a3.Merge, a3.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{0}, vector.GetColumn[int64](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{29}, vector.GetColumn[int64](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDecimalAnyva(t *testing.T) {
	testTyp := types.New(types.T_decimal128, 0, 0, 0)
	da := NewAnyvalue[types.Decimal128]()
	da2 := NewAnyvalue[types.Decimal128]()
	da3 := NewAnyvalue[types.Decimal128]()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []int64{1, 12, 3, 4, 5, 26, 7, 8, 9, 10}
	input2 := []int64{14, 5, 6, 7, 8, 29, 30, 1, 2, 3}
	vec := testutil.MakeDecimal128Vector(input1, nil, testTyp)
	vec2 := testutil.MakeDecimal128Vector(input2, nil, testTyp)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(da, true, testTyp, testTyp, da.Grows, da.Eval, da.Merge, da.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, MakeDecimal128Arr([]int64{1}), vector.GetColumn[types.Decimal128](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(da2, true, testTyp, testTyp, da2.Grows, da2.Eval, da2.Merge, da2.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(da3, true, testTyp, testTyp, da3.Grows, da3.Eval, da3.Merge, da3.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, MakeDecimal128Arr([]int64{1}), vector.GetColumn[types.Decimal128](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, MakeDecimal128Arr([]int64{14}), vector.GetColumn[types.Decimal128](v))
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
