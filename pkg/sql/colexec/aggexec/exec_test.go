// Copyright 2024 Matrix Origin
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

package aggexec

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

// testSingleAggPrivate1 is a structure that implements singleAggPrivateStructure1.
// it counts the number of input values (including NULLs) and it returns the count.
// just for testing purposes.
type testSingleAggPrivate1 struct{}

func gTesSingleAggPrivate1() singleAggPrivateStructure1[int32, int64] {
	return &testSingleAggPrivate1{}
}
func (t *testSingleAggPrivate1) init() {}
func (t *testSingleAggPrivate1) fill(from int32, getter aggGetter[int64], setter aggSetter[int64]) {
	setter(getter() + 1)
}
func (t *testSingleAggPrivate1) fillNull(getter aggGetter[int64], setter aggSetter[int64]) {
	setter(getter() + 1)
}
func (t *testSingleAggPrivate1) fills(value int32, isNull bool, count int, getter aggGetter[int64], setter aggSetter[int64]) {
	setter(getter() + int64(count))
}
func (t *testSingleAggPrivate1) flush(getter aggGetter[int64], setter aggSetter[int64]) {}

func TestSingleAggFuncExec1(t *testing.T) {
	proc := testutil.NewProcess()

	info := singleAggInfo{
		aggID:   1,
		argType: types.T_int32.ToType(),
		retType: types.T_int64.ToType(),
	}
	opt := singleAggOptimizedInfo{
		receiveNull: true,
	}
	executor := newSingleAggFuncExec1(info.argType, info.retType).(*singleAggFuncExec1[int32, int64])
	executor.Init(proc, info, opt, gTesSingleAggPrivate1)

	// input first row of [3, null, 4, 5] - count 1
	// input second row of [3, null, 4, 5] - count 1
	// input [null * 2] - count 2
	// input [1 * 3] - count 3
	// input [1, 2, 3, 4] - count 4
	// and the total count is 1+1+2+3+4 = 11
	inputType := info.argType
	inputs := make([]*vector.Vector, 5)
	{
		// prepare the input data.
		var err error

		vec := testutil.NewInt32Vector(4, inputType, proc.Mp(), false, []int32{3, 3, 0, 1, 1})
		vec.SetNulls(nulls.Build(4, 1))
		inputs[0] = vec
		inputs[1] = vec
		inputs[2] = vector.NewConstNull(inputType, 2, proc.Mp())
		inputs[3], err = vector.NewConstFixed[int32](inputType, 1, 3, proc.Mp())
		require.NoError(t, err)
		inputs[4] = testutil.NewInt32Vector(4, inputType, proc.Mp(), false, []int32{1, 2, 3, 4})
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data fill.
		require.NoError(t, executor.Fill(0, 0, []*vector.Vector{inputs[0]}))
		require.NoError(t, executor.Fill(0, 1, []*vector.Vector{inputs[1]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[2]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[3]}))
		require.NoError(t, executor.BulkFill(0, []*vector.Vector{inputs[4]}))
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
			require.Equal(t, 1, v.Length())
			require.Equal(t, int64(11), vector.MustFixedCol[int64](v)[0])
		}
		v.Free(proc.Mp())
	}
	{
		// memory check.
		for _, v := range inputs {
			v.Free(proc.Mp())
		}
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}
}

// testMultiAggPrivate1 is a structure that implements multiAggPrivateStructure1.
// it counts the amount of the second input value (including NULLs) when the first value was not null.
type testMultiAggPrivate1 struct {
	firstIsNull bool
}

func gTesMultiAggPrivate1() multiAggPrivateStructure1[int64] {
	return &testMultiAggPrivate1{}
}
func (t *testMultiAggPrivate1) init() {}
func (t *testMultiAggPrivate1) getFillWhich(idx int) any {
	switch idx {
	case 0:
		return func(k multiAggPrivateStructure1[int64], value int64) {
			k.(*testMultiAggPrivate1).firstIsNull = false
		}
	case 1:
		return func(k multiAggPrivateStructure1[int64], value bool) {}
	}
	panic("invalid idx")
}
func (t *testMultiAggPrivate1) getFillNullWhich(idx int) any {
	switch idx {
	case 0:
		return func(k multiAggPrivateStructure1[int64]) {
			k.(*testMultiAggPrivate1).firstIsNull = true
		}
	case 1:
		return func(k multiAggPrivateStructure1[int64]) {}
	}
	panic("invalid idx")
}
func (t *testMultiAggPrivate1) eval(getter aggGetter[int64], setter aggSetter[int64]) {
	if t.firstIsNull {
		return
	}
	setter(getter() + 1)
}
func (t *testMultiAggPrivate1) flush(getter aggGetter[int64], setter aggSetter[int64]) {}

func TestMultiAggFuncExec1(t *testing.T) {
	proc := testutil.NewProcess()

	info := multiAggInfo{
		aggID:    2,
		argTypes: []types.Type{types.T_int64.ToType(), types.T_bool.ToType()},
		retType:  types.T_int64.ToType(),
	}
	executor := newMultiAggFuncExec(info.retType).(*multiAggFuncExec1[int64])
	executor.Init(proc, info, gTesMultiAggPrivate1)

	// input first row of [{null, false}, {1, true}] - count 0
	// input second row of [{null, false}, {1, true}] - count 1
	// input [{null, false} * 2] - count 0
	// input [{1, true} * 3] - count 3
	// input [{1, null}, {null, false}, {3, true}, {null, false}] - count 2
	// and the total count is 0+1+0+3+2 = 6
	inputs := make([][2]*vector.Vector, 5)
	inputType1, inputType2 := info.argTypes[0], info.argTypes[1]
	{
		var err error

		// prepare the input data.
		vec1 := testutil.NewInt64Vector(2, inputType1, proc.Mp(), false, []int64{0, 1})
		vec1.SetNulls(nulls.Build(2, 0))
		vec2 := testutil.NewBoolVector(2, inputType2, proc.Mp(), false, []bool{false, true})
		inputs[0] = [2]*vector.Vector{vec1, vec2}
		inputs[1] = [2]*vector.Vector{vec1, vec2}

		inputs[2] = [2]*vector.Vector{nil, nil}
		inputs[2][0] = vector.NewConstNull(inputType1, 2, proc.Mp())
		inputs[2][1], err = vector.NewConstFixed[bool](inputType2, false, 2, proc.Mp())
		require.NoError(t, err)

		inputs[3] = [2]*vector.Vector{nil, nil}
		inputs[3][0], err = vector.NewConstFixed[int64](inputType1, 1, 3, proc.Mp())
		require.NoError(t, err)
		inputs[3][1], err = vector.NewConstFixed[bool](inputType2, true, 3, proc.Mp())
		require.NoError(t, err)

		inputs[4][0] = testutil.NewInt64Vector(4, inputType1, proc.Mp(), false, []int64{1, 0, 3, 0})
		inputs[4][0].SetNulls(nulls.Build(4, 1, 3))
		inputs[4][1] = testutil.NewBoolVector(4, inputType2, proc.Mp(), false, []bool{true, false, true, false})
		inputs[4][1].SetNulls(nulls.Build(4, 0))
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data fill.
		require.NoError(t, executor.Fill(0, 0, inputs[0][:]))
		require.NoError(t, executor.Fill(0, 1, inputs[1][:]))
		require.NoError(t, executor.BulkFill(0, inputs[2][:]))
		require.NoError(t, executor.BulkFill(0, inputs[3][:]))
		require.NoError(t, executor.BulkFill(0, inputs[4][:]))
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
			require.Equal(t, 1, v.Length())
			require.Equal(t, int64(6), vector.MustFixedCol[int64](v)[0])
		}
		v.Free(proc.Mp())
	}
	{
		// memory check.
		for _, v := range inputs {
			for _, vv := range v {
				vv.Free(proc.Mp())
			}
		}
		require.Equal(t, int64(0), proc.Mp().CurrNB())
	}
}
