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

package avg

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
func TestAvg(t *testing.T) {
	testTyp := types.New(types.T_int8, 0, 0, 0)
	retTyp := types.New(types.T_float64, 0, 0, 0)
	a1 := NewAvg[int8]()
	a2 := NewAvg[int8]()
	a3 := NewAvg[int8]()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vs1 := []int8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	vs2 := []int8{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	vec1 := testutil.NewVector(Rows, testTyp, m, false, vs1)
	vec2 := testutil.NewVector(Rows, testTyp, m, false, vs2)
	expected1 := []float64{4.5}
	expected2 := []float64{14.5}
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(a1, true, testTyp, retTyp, a1.Grows, a1.Eval, a1.Merge, a1.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec1})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, expected1, vector.GetColumn[float64](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(a2, true, testTyp, retTyp, a2.Grows, a2.Eval, a2.Merge, a2.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec1})
		}
		agg1 := agg.NewUnaryAgg(a3, true, testTyp, retTyp, a3.Grows, a3.Eval, a3.Merge, a3.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []float64{9.5}, vector.GetColumn[float64](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, expected2, vector.GetColumn[float64](v))
			v.Free(m)
		}
	}
	vec1.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDecimal128Avg(t *testing.T) {
	testTyp := types.New(types.T_decimal128, 0, 0, 0)
	a1 := NewD128Avg()
	a2 := NewD128Avg()
	a3 := NewD128Avg()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	input2 := []int64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}
	vec := testutil.MakeDecimal128Vector(input1, nil, testTyp)
	vec2 := testutil.MakeDecimal128Vector(input2, nil, testTyp)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(a1, true, testTyp, testTyp, a1.Grows, a1.Eval, a1.Merge, a1.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, MakeDecimal128Arr([]float64{4.5}), vector.GetColumn[types.Decimal128](v))
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
			require.Equal(t, MakeDecimal128Arr([]float64{9.5}), vector.GetColumn[types.Decimal128](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, MakeDecimal128Arr([]float64{14.5}), vector.GetColumn[types.Decimal128](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDistincAvg(t *testing.T) {
	testTyp := types.New(types.T_int8, 0, 0, 0)
	retTyp := types.New(types.T_float64, 0, 0, 0)
	a1 := NewAvg[int8]()
	a2 := NewAvg[int8]()
	a3 := NewAvg[int8]()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vs1 := []int8{0, 0, 1, 1, 2, 2, 3, 3, 4, 4}
	vs2 := []int8{15, 15, 16, 16, 17, 17, 18, 18, 19, 19}
	vec1 := testutil.NewVector(Rows, testTyp, m, false, vs1)
	vec2 := testutil.NewVector(Rows, testTyp, m, false, vs2)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryDistAgg(true, testTyp, retTyp, a1.Grows, a1.Eval, a1.Merge, a1.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec1})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, []float64{2}, vector.GetColumn[float64](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryDistAgg(true, testTyp, retTyp, a2.Grows, a2.Eval, a2.Merge, a2.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec1})
		}
		agg1 := agg.NewUnaryDistAgg(true, testTyp, retTyp, a3.Grows, a3.Eval, a3.Merge, a3.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []float64{9.5}, vector.GetColumn[float64](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []float64{17}, vector.GetColumn[float64](v))
			v.Free(m)
		}
	}
	vec1.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func MakeDecimal128Arr(input []float64) []types.Decimal128 {
	ret := make([]types.Decimal128, len(input))
	for i, v := range input {
		ret[i] = types.Decimal128FromFloat64(v)
	}

	return ret
}
