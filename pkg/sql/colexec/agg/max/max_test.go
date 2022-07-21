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

package max

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

//TODO: add distinc decimal128 test

func TestMax(t *testing.T) {
	testTyp := types.New(types.T_int64, 0, 0, 0)
	mx := NewMax[int64]()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vs := []int64{0, 1, -2, 3, 14, 5, -6, 7, 8, 9}
	vs2 := []int64{0, 1, -2, 3, -14, 5, -6, 7, 8, 29}
	vec := testutil.NewVector(Rows, testTyp, m, false, vs)
	vec2 := testutil.NewVector(Rows, testTyp, m, false, vs2)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(nil, true, testTyp, testTyp, mx.Grows, mx.Eval, mx.Merge, mx.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, []int64{14}, vector.GetColumn[int64](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, mx.Grows, mx.Eval, mx.Merge, mx.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, mx.Grows, mx.Eval, mx.Merge, mx.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{29}, vector.GetColumn[int64](v))
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

func TestDecimalMax(t *testing.T) {
	testTyp := types.New(types.T_decimal128, 0, 0, 0)
	dmx := NewD128Max()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []int64{0, 1, 12, 3, 4, 5, 26, 7, 8, 9}
	input2 := []int64{0, 1, 2, 3, 14, 5, 6, 7, 8, 29}
	vec := testutil.MakeDecimal128Vector(input1, nil, testTyp)
	vec2 := testutil.MakeDecimal128Vector(input2, nil, testTyp)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmx.Grows, dmx.Eval, dmx.Merge, dmx.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, MakeDecimal128Arr([]int64{26}), vector.GetColumn[types.Decimal128](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmx.Grows, dmx.Eval, dmx.Merge, dmx.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmx.Grows, dmx.Eval, dmx.Merge, dmx.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, MakeDecimal128Arr([]int64{29}), vector.GetColumn[types.Decimal128](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, MakeDecimal128Arr([]int64{29}), vector.GetColumn[types.Decimal128](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestBoollMax(t *testing.T) {
	testTyp := types.New(types.T_decimal128, 0, 0, 0)
	dmx := NewBoolMax()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []bool{false, true, false, true, false, true, false, true, false, true}
	input2 := []bool{false, false, false, false, false, false, false, false, false, false}
	vec := testutil.MakeBoolVector(input1)
	vec2 := testutil.MakeBoolVector(input2)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmx.Grows, dmx.Eval, dmx.Merge, dmx.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, []bool{true}, vector.GetColumn[bool](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmx.Grows, dmx.Eval, dmx.Merge, dmx.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmx.Grows, dmx.Eval, dmx.Merge, dmx.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []bool{true}, vector.GetColumn[bool](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []bool{false}, vector.GetColumn[bool](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestStrlMax(t *testing.T) {
	testTyp := types.New(types.T_varchar, 0, 0, 0)
	smx := NewStrMax()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []string{"ab", "ac", "bc", "bcdd", "c", "za", "mo", "momo", "zb", "z"}
	input2 := []string{"ab", "ac", "bc", "bcdd", "c", "za", "mo", "momo", "zb", "zzz"}
	vec := testutil.MakeVarcharVector(input1, nil)
	vec2 := testutil.MakeVarcharVector(input2, nil)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(nil, true, testTyp, testTyp, smx.Grows, smx.Eval, smx.Merge, smx.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, makeBytes([]string{"zb"}), vector.GetStrColumn(v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, smx.Grows, smx.Eval, smx.Merge, smx.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, smx.Grows, smx.Eval, smx.Merge, smx.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, makeBytes([]string{"zzz"}), vector.GetStrColumn(v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, makeBytes([]string{"zzz"}), vector.GetStrColumn(v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDistincMax(t *testing.T) {
	testTyp := types.New(types.T_int64, 0, 0, 0)
	mx := NewMax[int64]()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vs := []int64{0, 1, -2, 3, 14, 5, -6, 7, 8, 9}
	vs2 := []int64{0, 1, -2, 3, -14, 5, -6, 7, 8, 29}
	vec := testutil.NewVector(Rows, testTyp, m, false, vs)
	vec2 := testutil.NewVector(Rows, testTyp, m, false, vs2)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryDistAgg(true, testTyp, testTyp, mx.Grows, mx.Eval, mx.Merge, mx.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, []int64{14}, vector.GetColumn[int64](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryDistAgg(true, testTyp, testTyp, mx.Grows, mx.Eval, mx.Merge, mx.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryDistAgg(true, testTyp, testTyp, mx.Grows, mx.Eval, mx.Merge, mx.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{29}, vector.GetColumn[int64](v))
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

func TestDisctincBoollMax(t *testing.T) {
	testTyp := types.New(types.T_decimal128, 0, 0, 0)
	dmx := NewBoolMax()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []bool{false, true, false, true, false, true, false, true, false, true}
	input2 := []bool{false, false, false, false, false, false, false, false, false, false}
	vec := testutil.MakeBoolVector(input1)
	vec2 := testutil.MakeBoolVector(input2)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryDistAgg(true, testTyp, testTyp, dmx.Grows, dmx.Eval, dmx.Merge, dmx.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, []bool{true}, vector.GetColumn[bool](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryDistAgg(true, testTyp, testTyp, dmx.Grows, dmx.Eval, dmx.Merge, dmx.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryDistAgg(true, testTyp, testTyp, dmx.Grows, dmx.Eval, dmx.Merge, dmx.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []bool{true}, vector.GetColumn[bool](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []bool{false}, vector.GetColumn[bool](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDiscincStrlMax(t *testing.T) {
	testTyp := types.New(types.T_varchar, 0, 0, 0)
	smx := NewStrMax()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []string{"ab", "ab", "ab", "bcdd", "c", "za", "mo", "momo", "zb", "z"}
	input2 := []string{"ab", "ac", "mo", "bcdd", "c", "mo", "mo", "momo", "zb", "zzz"}
	vec := testutil.MakeVarcharVector(input1, nil)
	vec2 := testutil.MakeVarcharVector(input2, nil)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryDistAgg(true, testTyp, testTyp, smx.Grows, smx.Eval, smx.Merge, smx.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, makeBytes([]string{"zb"}), vector.GetStrColumn(v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryDistAgg(true, testTyp, testTyp, smx.Grows, smx.Eval, smx.Merge, smx.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryDistAgg(true, testTyp, testTyp, smx.Grows, smx.Eval, smx.Merge, smx.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, makeBytes([]string{"zzz"}), vector.GetStrColumn(v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, makeBytes([]string{"zzz"}), vector.GetStrColumn(v))
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

func makeBytes(values []string) *types.Bytes {
	next := uint32(0)
	bs := &types.Bytes{
		Lengths: make([]uint32, len(values)),
		Offsets: make([]uint32, len(values)),
	}
	for i := range values {
		s := values[i]
		l := uint32(len(s))
		bs.Data = append(bs.Data, []byte(s)...)
		bs.Lengths[i] = l
		bs.Offsets[i] = next
		next += l
	}

	return bs
}
