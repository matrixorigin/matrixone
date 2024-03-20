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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

type testAggMemoryManager struct {
	mp *mpool.MPool
}

func (m *testAggMemoryManager) Mp() *mpool.MPool {
	return m.mp
}
func (m *testAggMemoryManager) GetVector(typ types.Type) *vector.Vector {
	return vector.NewVec(typ)
}
func (m *testAggMemoryManager) PutVector(v *vector.Vector) {
	v.Free(m.mp)
}
func newTestAggMemoryManager() AggMemoryManager {
	return &testAggMemoryManager{mp: mpool.MustNewNoFixed("test_agg_exec")}
}

// testSingleAggPrivate1 is a structure that implements SingleAggFromFixedRetFixed.
// it counts the number of input values (including NULLs) and it returns the count.
// just for testing purposes.
type testSingleAggPrivate1 struct{}

func gTesSingleAggPrivate1() SingleAggFromFixedRetFixed[int32, int64] {
	return &testSingleAggPrivate1{}
}
func (t *testSingleAggPrivate1) Init(AggSetter[int64], types.Type, types.Type) {}
func (t *testSingleAggPrivate1) Fill(from int32, getter AggGetter[int64], setter AggSetter[int64]) {
	setter(getter() + 1)
}
func (t *testSingleAggPrivate1) FillNull(getter AggGetter[int64], setter AggSetter[int64]) {
	setter(getter() + 1)
}
func (t *testSingleAggPrivate1) Fills(value int32, isNull bool, count int, getter AggGetter[int64], setter AggSetter[int64]) {
	setter(getter() + int64(count))
}
func (t *testSingleAggPrivate1) Merge(other SingleAggFromFixedRetFixed[int32, int64], getter1 AggGetter[int64], getter2 AggGetter[int64], setter AggSetter[int64]) {
	setter(getter1() + getter2())
}
func (t *testSingleAggPrivate1) Flush(getter AggGetter[int64], setter AggSetter[int64]) {}
func (t *testSingleAggPrivate1) Marshal() []byte {
	return nil
}
func (t *testSingleAggPrivate1) Unmarshal([]byte) {}

func TestSingleAggFuncExec1(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     1,
		distinct:  false,
		argType:   types.T_int32.ToType(),
		retType:   types.T_int64.ToType(),
		emptyNull: false,
	}
	RegisterDeterminedSingleAgg(
		MakeDeterminedSingleAggInfo(
			info.aggID, info.argType, info.retType, true, info.emptyNull),
		gTesSingleAggPrivate1)
	executor := MakeAgg(
		mg,
		info.aggID, info.distinct, info.argType)

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

		vec := vector.NewVec(inputType)
		require.NoError(t, vector.AppendFixedList[int32](vec, []int32{3, 0, 4, 5}, []bool{false, true, false, false}, mg.Mp()))
		inputs[0] = vec
		inputs[1] = vec
		inputs[2] = vector.NewConstNull(inputType, 2, mg.Mp())
		inputs[3], err = vector.NewConstFixed[int32](inputType, 1, 3, mg.Mp())
		require.NoError(t, err)
		inputs[4] = vector.NewVec(inputType)
		require.NoError(t, vector.AppendFixedList[int32](inputs[4], []int32{1, 2, 3, 4}, nil, mg.Mp()))
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
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
		v.Free(mg.Mp())
	}
	{
		executor.Free()
		// memory check.
		for _, v := range inputs {
			v.Free(mg.Mp())
		}
		require.Equal(t, int64(0), mg.Mp().CurrNB())
	}
}

// testMultiAggPrivate1 is a structure that implements MultiAggRetFixed.
// it counts the amount of the second input value (including NULLs) when the first value was not null.
type testMultiAggPrivate1 struct {
	firstIsNull bool
}

func gTesMultiAggPrivate1() MultiAggRetFixed[int64] {
	return &testMultiAggPrivate1{}
}
func (t *testMultiAggPrivate1) Init(AggSetter[int64], []types.Type, types.Type) {}
func (t *testMultiAggPrivate1) GetWhichFill(idx int) any {
	switch idx {
	case 0:
		return func(k MultiAggRetFixed[int64], value int64) {
			k.(*testMultiAggPrivate1).firstIsNull = false
		}
	case 1:
		return func(k MultiAggRetFixed[int64], value bool) {}
	}
	panic("invalid idx")
}
func (t *testMultiAggPrivate1) GetWhichFillNull(idx int) any {
	switch idx {
	case 0:
		return func(k MultiAggRetFixed[int64]) {
			k.(*testMultiAggPrivate1).firstIsNull = true
		}
	case 1:
		return func(k MultiAggRetFixed[int64]) {}
	}
	panic("invalid idx")
}
func (t *testMultiAggPrivate1) Valid() bool {
	return !t.firstIsNull
}
func (t *testMultiAggPrivate1) Eval(getter AggGetter[int64], setter AggSetter[int64]) {
	setter(getter() + 1)
}
func (t *testMultiAggPrivate1) Merge(other MultiAggRetFixed[int64], getter1 AggGetter[int64], getter2 AggGetter[int64], setter AggSetter[int64]) {
	setter(getter1() + getter2())
}
func (t *testMultiAggPrivate1) Flush(getter AggGetter[int64], setter AggSetter[int64]) {}
func (t *testMultiAggPrivate1) Marshal() []byte {
	return nil
}
func (t *testMultiAggPrivate1) Unmarshal([]byte) {}

func TestMultiAggFuncExec1(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := multiAggInfo{
		aggID:     2,
		distinct:  false,
		argTypes:  []types.Type{types.T_int64.ToType(), types.T_bool.ToType()},
		retType:   types.T_int64.ToType(),
		emptyNull: false,
	}
	RegisterDeterminedMultiAgg(
		MakeDeterminedMultiAggInfo(
			info.aggID, info.argTypes, info.retType, false),
		gTesMultiAggPrivate1)
	executor := MakeAgg(
		mg,
		info.aggID, info.distinct, info.argTypes...)

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
		vec1 := vector.NewVec(inputType1)
		require.NoError(t, vector.AppendFixedList[int64](vec1, []int64{0, 1}, []bool{true, false}, mg.Mp()))
		vec2 := vector.NewVec(inputType2)
		require.NoError(t, vector.AppendFixedList[bool](vec2, []bool{false, true}, nil, mg.Mp()))
		inputs[0] = [2]*vector.Vector{vec1, vec2}
		inputs[1] = [2]*vector.Vector{vec1, vec2}

		inputs[2] = [2]*vector.Vector{nil, nil}
		inputs[2][0] = vector.NewConstNull(inputType1, 2, mg.Mp())
		inputs[2][1], err = vector.NewConstFixed[bool](inputType2, false, 2, mg.Mp())
		require.NoError(t, err)

		inputs[3] = [2]*vector.Vector{nil, nil}
		inputs[3][0], err = vector.NewConstFixed[int64](inputType1, 1, 3, mg.Mp())
		require.NoError(t, err)
		inputs[3][1], err = vector.NewConstFixed[bool](inputType2, true, 3, mg.Mp())
		require.NoError(t, err)

		inputs[4][0] = vector.NewVec(inputType1)
		require.NoError(t, vector.AppendFixedList[int64](inputs[4][0], []int64{1, 0, 3, 0}, []bool{false, true, false, true}, mg.Mp()))
		inputs[4][1] = vector.NewVec(inputType2)
		require.NoError(t, vector.AppendFixedList[bool](inputs[4][1], []bool{true, false, true, false}, []bool{true, false, false, false}, mg.Mp()))
	}
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
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
		v.Free(mg.Mp())
	}
	{
		executor.Free()
		// memory check.
		for _, v := range inputs {
			for _, vv := range v {
				vv.Free(mg.Mp())
			}
		}
		require.Equal(t, int64(0), mg.Mp().CurrNB())
	}
}

func TestGroupConcatExec(t *testing.T) {
	mg := newTestAggMemoryManager()
	RegisterGroupConcatAgg(3, ",")
	info := multiAggInfo{
		aggID:    3,
		distinct: false,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_char.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: false,
	}
	executor := MakeAgg(mg, info.aggID, info.distinct, info.argTypes...)

	// group concat the vector1 and vector2.
	// vector1: ["a", "b", "c", "d"].
	// vector2: ["d", "c", "b", "a"].
	// the result is ["ad,bc,cb,da"].
	inputs := make([]*vector.Vector, 2)
	inputs[0] = vector.NewVec(info.argTypes[0])
	inputs[1] = vector.NewVec(info.argTypes[1])
	require.NoError(t, vector.AppendStringList(inputs[0], []string{"a", "b", "c", "d"}, nil, mg.Mp()))
	require.NoError(t, vector.AppendStringList(inputs[1], []string{"d", "c", "b", "a"}, nil, mg.Mp()))
	{
		require.NoError(t, executor.GroupGrow(1))
		// data Fill.
		require.NoError(t, executor.Fill(0, 0, inputs))
		require.NoError(t, executor.Fill(0, 1, inputs))
		require.NoError(t, executor.Fill(0, 2, inputs))
		require.NoError(t, executor.Fill(0, 3, inputs))
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
			require.Equal(t, 1, v.Length())
			bs := vector.MustStrCol(v)
			require.Equal(t, 1, len(bs))
			require.Equal(t, "ad,bc,cb,da", bs[0])
		}
		v.Free(mg.Mp())
	}
	{
		executor.Free()
		// memory check.
		for _, v := range inputs {
			v.Free(mg.Mp())
		}
		require.Equal(t, int64(0), mg.Mp().CurrNB())
	}
}

// TestEmptyNullFlag test if the emptyNull flag is working.
// the emptyNull flag is used to determine whether the NULL value is included in the aggregation result.
// if the emptyNull flag is true, the NULL value will be returned for empty groups.
func TestEmptyNullFlag(t *testing.T) {
	mg := newTestAggMemoryManager()
	{
		RegisterDeterminedSingleAgg(
			MakeDeterminedSingleAggInfo(
				1, types.T_int32.ToType(), types.T_int64.ToType(), false, true),
			gTesSingleAggPrivate1)
		executor := MakeAgg(
			mg,
			1, false, types.T_int32.ToType())
		require.NoError(t, executor.GroupGrow(1))
		v, err := executor.Flush()
		require.NoError(t, err)
		require.True(t, v.IsNull(0))
		v.Free(mg.Mp())
		executor.Free()
	}
	{
		RegisterDeterminedSingleAgg(
			MakeDeterminedSingleAggInfo(
				1, types.T_int32.ToType(), types.T_int64.ToType(), false, false),
			gTesSingleAggPrivate1)
		executor := MakeAgg(
			mg,
			1, false, types.T_int32.ToType())
		require.NoError(t, executor.GroupGrow(1))
		v, err := executor.Flush()
		require.NoError(t, err)
		require.False(t, v.IsNull(0))
		require.Equal(t, int64(0), vector.MustFixedCol[int64](v)[0])
		v.Free(mg.Mp())
		executor.Free()
	}
	{
		RegisterDeterminedMultiAgg(
			MakeDeterminedMultiAggInfo(2, []types.Type{types.T_int64.ToType(), types.T_bool.ToType()}, types.T_int64.ToType(), true), gTesMultiAggPrivate1)
		executor := MakeAgg(
			mg,
			2, false, []types.Type{types.T_int64.ToType(), types.T_bool.ToType()}...)
		require.NoError(t, executor.GroupGrow(1))
		v, err := executor.Flush()
		require.NoError(t, err)
		require.True(t, v.IsNull(0))
		v.Free(mg.Mp())
		executor.Free()
	}
	{
		RegisterDeterminedMultiAgg(
			MakeDeterminedMultiAggInfo(2, []types.Type{types.T_int64.ToType(), types.T_bool.ToType()}, types.T_int64.ToType(), false), gTesMultiAggPrivate1)
		executor := MakeAgg(
			mg,
			2, false, []types.Type{types.T_int64.ToType(), types.T_bool.ToType()}...)
		require.NoError(t, executor.GroupGrow(1))
		v, err := executor.Flush()
		require.NoError(t, err)
		require.False(t, v.IsNull(0))
		require.Equal(t, int64(0), vector.MustFixedCol[int64](v)[0])
		v.Free(mg.Mp())
		executor.Free()
	}
	require.Equal(t, int64(0), mg.Mp().CurrNB())
}
