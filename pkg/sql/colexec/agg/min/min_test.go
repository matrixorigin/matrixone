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

package min

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
func TestMin(t *testing.T) {
	testTyp := types.New(types.T_int64, 0, 0, 0)
	mn := NewMin[int64]()
	mn2 := NewMin[int64]()
	mn3 := NewMin[int64]()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	vs := []int64{0, 1, -2, 3, 14, 5, -6, 7, 8, 9}
	vs2 := []int64{0, 1, -2, 3, -14, 5, -6, 7, 8, 29}
	vec := testutil.NewVector(Rows, testTyp, m, false, vs)
	vec2 := testutil.NewVector(Rows, testTyp, m, false, vs2)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(nil, true, testTyp, testTyp, mn.Grows, mn.Eval, mn.Merge, mn.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, []int64{-6}, vector.GetColumn[int64](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, mn2.Grows, mn2.Eval, mn2.Merge, mn2.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, mn3.Grows, mn3.Eval, mn3.Merge, mn3.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{-14}, vector.GetColumn[int64](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{-14}, vector.GetColumn[int64](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDecimalMin(t *testing.T) {
	testTyp := types.New(types.T_decimal128, 0, 0, 0)
	dmn := NewD128Min()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []int64{10, 1, 12, 3, 4, 5, 26, 7, 8, 9}
	input2 := []int64{0, 1, 2, 3, 14, 5, 6, 7, 8, 29}
	vec := testutil.MakeDecimal128Vector(input1, nil, testTyp)
	vec2 := testutil.MakeDecimal128Vector(input2, nil, testTyp)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmn.Grows, dmn.Eval, dmn.Merge, dmn.Fill)
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
		agg0 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmn.Grows, dmn.Eval, dmn.Merge, dmn.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmn.Grows, dmn.Eval, dmn.Merge, dmn.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, MakeDecimal128Arr([]int64{0}), vector.GetColumn[types.Decimal128](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, MakeDecimal128Arr([]int64{0}), vector.GetColumn[types.Decimal128](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestBoollMin(t *testing.T) {
	testTyp := types.New(types.T_decimal128, 0, 0, 0)
	dmn := NewBoolMin()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []bool{false, true, false, true, false, true, false, true, false, true}
	input2 := []bool{true, true, true, true, true, true, true, true, true, true}
	vec := testutil.MakeBoolVector(input1)
	vec2 := testutil.MakeBoolVector(input2)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmn.Grows, dmn.Eval, dmn.Merge, dmn.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, []bool{false}, vector.GetColumn[bool](v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmn.Grows, dmn.Eval, dmn.Merge, dmn.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, dmn.Grows, dmn.Eval, dmn.Merge, dmn.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []bool{false}, vector.GetColumn[bool](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []bool{true}, vector.GetColumn[bool](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestStrlMin(t *testing.T) {
	testTyp := types.New(types.T_varchar, 0, 0, 0)
	smn := NewStrMin()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []string{"ab", "ac", "bc", "bcdd", "c", "a", "mo", "momo", "zb", "z"}
	input2 := []string{"ab", "ac", "bc", "bcdd", "c", "za", "mo", "momo", "zb", "zzz"}
	vec := testutil.MakeVarcharVector(input1, nil)
	vec2 := testutil.MakeVarcharVector(input2, nil)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryAgg(nil, true, testTyp, testTyp, smn.Grows, smn.Eval, smn.Merge, smn.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, makeBytes([]string{"a"}), vector.GetStrColumn(v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, smn.Grows, smn.Eval, smn.Merge, smn.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryAgg(nil, true, testTyp, testTyp, smn.Grows, smn.Eval, smn.Merge, smn.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, makeBytes([]string{"a"}), vector.GetStrColumn(v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, makeBytes([]string{"ab"}), vector.GetStrColumn(v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDistincMin(t *testing.T) {
	testTyp := types.New(types.T_int64, 0, 0, 0)
	mx := NewMin[int64]()
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
		require.Equal(t, []int64{-6}, vector.GetColumn[int64](v))
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
			require.Equal(t, []int64{-14}, vector.GetColumn[int64](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []int64{-14}, vector.GetColumn[int64](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDisctincBoollMin(t *testing.T) {
	testTyp := types.New(types.T_decimal128, 0, 0, 0)
	dmx := NewBoolMin()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []bool{false, true, false, true, false, true, false, true, false, true}
	input2 := []bool{true, true, true, true, true, true, true, true, true, true}
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
		require.Equal(t, []bool{false}, vector.GetColumn[bool](v))
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
			require.Equal(t, []bool{false}, vector.GetColumn[bool](v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, []bool{true}, vector.GetColumn[bool](v))
			v.Free(m)
		}
	}
	vec.Free(m)
	vec2.Free(m)
	require.Equal(t, int64(0), m.Size())
}

func TestDiscincStrlMin(t *testing.T) {
	testTyp := types.New(types.T_varchar, 0, 0, 0)
	smn := NewStrMin()
	m := mheap.New(guest.New(1<<30, host.New(1<<30)))
	input1 := []string{"ab", "ab", "ab", "bcdd", "c", "a", "mo", "momo", "a", "z"}
	input2 := []string{"ab", "ac", "mo", "bcdd", "c", "mo", "mo", "ab", "zb", "zzz"}
	vec := testutil.MakeVarcharVector(input1, nil)
	vec2 := testutil.MakeVarcharVector(input2, nil)
	{
		// test single agg with Grow & Fill function
		agg := agg.NewUnaryDistAgg(true, testTyp, testTyp, smn.Grows, smn.Eval, smn.Merge, smn.Fill)
		err := agg.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		v, err := agg.Eval(m)
		require.NoError(t, err)
		require.Equal(t, makeBytes([]string{"a"}), vector.GetStrColumn(v))
		v.Free(m)
	}
	{
		// test two agg with Merge function
		agg0 := agg.NewUnaryDistAgg(true, testTyp, testTyp, smn.Grows, smn.Eval, smn.Merge, smn.Fill)
		err := agg0.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg0.Fill(0, int64(i), 1, []*vector.Vector{vec})
		}
		agg1 := agg.NewUnaryDistAgg(true, testTyp, testTyp, smn.Grows, smn.Eval, smn.Merge, smn.Fill)
		err = agg1.Grows(1, m)
		require.NoError(t, err)
		for i := 0; i < Rows; i++ {
			agg1.Fill(0, int64(i), 1, []*vector.Vector{vec2})
		}
		agg0.Merge(agg1, 0, 0)
		{
			v, err := agg0.Eval(m)
			require.NoError(t, err)
			require.Equal(t, makeBytes([]string{"a"}), vector.GetStrColumn(v))
			v.Free(m)
		}
		{
			v, err := agg1.Eval(m)
			require.NoError(t, err)
			require.Equal(t, makeBytes([]string{"ab"}), vector.GetStrColumn(v))
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
