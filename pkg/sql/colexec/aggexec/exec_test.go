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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

var uniqueAggIdForTest int64 = 100
var uniqueAggIdForTestLock sync.RWMutex

func gUniqueAggIdForTest() int64 {
	uniqueAggIdForTestLock.Lock()
	uniqueAggIdForTest++
	ret := uniqueAggIdForTest
	uniqueAggIdForTestLock.Unlock()

	return ret
}

type testAggMemoryManager struct {
	mp *mpool.MPool
}

func (m *testAggMemoryManager) Mp() *mpool.MPool {
	return m.mp
}
func newTestAggMemoryManager() AggMemoryManager {
	return &testAggMemoryManager{mp: mpool.MustNewNoFixed("test_agg_exec")}
}

// the Following is a mock aggregation function that counts the number of non-null values.
// the aggregation function is a single column aggregation function that accepts int32 values and returns int64 values.
// it's just a simple test function.
func tSinglePrivate1Ret(_ []types.Type) types.Type {
	return types.T_int64.ToType()
}
func tSinglePrivate1InitResult(_ types.Type, _ ...types.Type) int64 {
	return 0
}
func fillSinglePrivate1(
	execContext AggGroupExecContext, commonContext AggCommonExecContext,
	value int32, isEmpty bool, getter AggGetter[int64], setter AggSetter[int64]) error {
	setter(getter() + 1)
	return nil
}
func fillsSinglePrivate1(
	execContext AggGroupExecContext, commonContext AggCommonExecContext,
	value int32, count int, isEmpty bool,
	getter AggGetter[int64], setter AggSetter[int64]) error {
	setter(getter() + int64(count))
	return nil
}
func mergeSinglePrivate1(
	ctx1, ctx2 AggGroupExecContext,
	commonContext AggCommonExecContext,
	aggIsEmpty1, aggIsEmpty2 bool,
	getter1 AggGetter[int64], getter2 AggGetter[int64],
	setter AggSetter[int64]) error {
	setter(getter1() + getter2())
	return nil
}

func registerTheTestingCount(id int64, emptyGroupToBeNull bool) {
	RegisterAggFromFixedRetFixed(
		MakeSingleColumnAggInformation(id, types.T_int32.ToType(), tSinglePrivate1Ret, emptyGroupToBeNull),
		nil, nil, tSinglePrivate1InitResult,
		fillSinglePrivate1, fillsSinglePrivate1, mergeSinglePrivate1, nil)
}

func registerTheTestingCountWithContext(id int64, commonCtx AggCommonContextInit, groupCtx AggGroupContextInit) {
	RegisterAggFromFixedRetFixed(
		MakeSingleColumnAggInformation(id, types.T_int32.ToType(), tSinglePrivate1Ret, true),
		commonCtx, groupCtx, tSinglePrivate1InitResult,
		fillSinglePrivate1, fillsSinglePrivate1, mergeSinglePrivate1, nil)
}

func TestSingleAggFuncExec1(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     gUniqueAggIdForTest(),
		distinct:  false,
		argType:   types.T_int32.ToType(),
		retType:   types.T_int64.ToType(),
		emptyNull: false,
	}
	registerTheTestingCount(info.aggID, info.emptyNull)
	executor := MakeAgg(
		mg,
		info.aggID, info.distinct, info.argType)

	// input first row of [3, null, 4, 5] - count 1
	// input second row of [3, null, 4, 5] - count 0
	// input [null * 2] - count 0
	// input [1 * 3] - count 3
	// input [1, 2, 3, 4] - count 4
	// and the total count is 1+1+0+3+4 = 9
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
			require.Equal(t, int64(8), vector.MustFixedColWithTypeCheck[int64](v)[0])
		}
		v.Free(mg.Mp())
	}
	{
		executor.Free()
		// memory check.
		for i := 1; i < len(inputs); i++ {
			inputs[i].Free(mg.Mp())
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

var mPrivate1Args = []types.Type{types.T_int64.ToType(), types.T_bool.ToType()}

func mPrivate1Ret(_ []types.Type) types.Type {
	return types.T_int64.ToType()
}
func fillWhich0(exec MultiAggRetFixed[int64], value int64) error {
	exec.(*testMultiAggPrivate1).firstIsNull = false
	return nil
}
func fillWhich1(exec MultiAggRetFixed[int64], value bool) error {
	return nil
}
func fillNullWhich0(exec MultiAggRetFixed[int64]) error {
	exec.(*testMultiAggPrivate1).firstIsNull = true
	return nil
}
func fillNullWhich1(exec MultiAggRetFixed[int64]) error {
	return nil
}
func validMPrivate1(exec MultiAggRetFixed[int64]) bool {
	return !exec.(*testMultiAggPrivate1).firstIsNull
}
func evalMPrivate1(exec MultiAggRetFixed[int64], getter AggGetter[int64], setter AggSetter[int64]) error {
	setter(getter() + 1)
	return nil
}
func mergeMPrivate1(exec1, exec2 MultiAggRetFixed[int64], getter1 AggGetter[int64], getter2 AggGetter[int64], setter AggSetter[int64]) error {
	setter(getter1() + getter2())
	return nil
}

var mPrivate1FillWhich = []any{fillWhich0, fillWhich1}
var mPrivate1FillNullWhich = []MultiAggFillNull1[int64]{fillNullWhich0, fillNullWhich1}

func (t *testMultiAggPrivate1) Marshal() []byte {
	return nil
}
func (t *testMultiAggPrivate1) Unmarshal([]byte) {}

func TestMultiAggFuncExec1(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := multiAggInfo{
		aggID:     gUniqueAggIdForTest(),
		distinct:  false,
		argTypes:  mPrivate1Args,
		retType:   types.T_int64.ToType(),
		emptyNull: false,
	}

	RegisterMultiAggRetFixed(
		MakeMultiAggRetFixedRegisteredInfo(
			MakeMultiColumnAggInformation(info.aggID, mPrivate1Args, mPrivate1Ret, info.emptyNull),
			gTesMultiAggPrivate1,
			nil,
			mPrivate1FillWhich,
			mPrivate1FillNullWhich,
			validMPrivate1,
			evalMPrivate1,
			mergeMPrivate1,
			nil,
		))

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
		{
			vec1 := vector.NewVec(inputType1)
			require.NoError(t, vector.AppendFixedList[int64](vec1, []int64{0, 1}, []bool{true, false}, mg.Mp()))
			vec2 := vector.NewVec(inputType2)
			require.NoError(t, vector.AppendFixedList[bool](vec2, []bool{false, true}, nil, mg.Mp()))
			inputs[0] = [2]*vector.Vector{vec1, vec2}
		}
		{
			vec1 := vector.NewVec(inputType1)
			require.NoError(t, vector.AppendFixedList[int64](vec1, []int64{0, 1}, []bool{true, false}, mg.Mp()))
			vec2 := vector.NewVec(inputType2)
			require.NoError(t, vector.AppendFixedList[bool](vec2, []bool{false, true}, nil, mg.Mp()))
			inputs[1] = [2]*vector.Vector{vec1, vec2}
		}

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
			require.Equal(t, int64(6), vector.MustFixedColWithTypeCheck[int64](v)[0])
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
	info := multiAggInfo{
		aggID:    gUniqueAggIdForTest(),
		distinct: false,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_char.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: false,
	}
	RegisterGroupConcatAgg(info.aggID, ",")

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
		// data merge
		executor2 := MakeAgg(mg, info.aggID, info.distinct, info.argTypes...)
		require.NoError(t, executor2.GroupGrow(1))
		require.NoError(t, executor2.Fill(0, 2, inputs))
		require.NoError(t, executor2.Fill(0, 3, inputs))
		require.NoError(t, executor.Merge(executor2, 0, 0))
		executor2.Free()
		// merge empty.
		executor3 := MakeAgg(mg, info.aggID, info.distinct, info.argTypes...)
		require.NoError(t, executor3.GroupGrow(1))
		require.NoError(t, executor.Merge(executor3, 0, 0))
		executor3.Free()
	}
	{
		// result check.
		v, err := executor.Flush()
		require.NoError(t, err)
		{
			require.NotNil(t, v)
			require.Equal(t, 1, v.Length())
			bs := vector.InefficientMustStrCol(v)
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
// if the emptyNull flag is true, empty groups will return NULL as the result.
func TestEmptyNullFlag(t *testing.T) {
	mg := newTestAggMemoryManager()
	{
		id := gUniqueAggIdForTest()
		registerTheTestingCount(id, true)
		executor := MakeAgg(
			mg,
			id, false, types.T_int32.ToType())
		require.NoError(t, executor.GroupGrow(1))
		v, err := executor.Flush()
		require.NoError(t, err)
		require.True(t, v.IsNull(0))
		v.Free(mg.Mp())
		executor.Free()
	}
	{
		id := gUniqueAggIdForTest()
		registerTheTestingCount(id, false)
		executor := MakeAgg(
			mg,
			id, false, types.T_int32.ToType())
		require.NoError(t, executor.GroupGrow(1))
		v, err := executor.Flush()
		require.NoError(t, err)
		require.False(t, v.IsNull(0))
		require.Equal(t, int64(0), vector.MustFixedColWithTypeCheck[int64](v)[0])
		v.Free(mg.Mp())
		executor.Free()
	}
	{
		id := gUniqueAggIdForTest()
		RegisterMultiAggRetFixed(
			MakeMultiAggRetFixedRegisteredInfo(
				MakeMultiColumnAggInformation(id, mPrivate1Args, mPrivate1Ret, true),
				gTesMultiAggPrivate1,
				nil,
				mPrivate1FillWhich,
				mPrivate1FillNullWhich,
				validMPrivate1,
				evalMPrivate1,
				mergeMPrivate1,
				nil,
			))
		executor := MakeAgg(
			mg,
			id, false, []types.Type{types.T_int64.ToType(), types.T_bool.ToType()}...)
		require.NoError(t, executor.GroupGrow(1))
		v, err := executor.Flush()
		require.NoError(t, err)
		require.True(t, v.IsNull(0))
		v.Free(mg.Mp())
		executor.Free()
	}
	{
		id := gUniqueAggIdForTest()
		RegisterMultiAggRetFixed(
			MakeMultiAggRetFixedRegisteredInfo(
				MakeMultiColumnAggInformation(id, mPrivate1Args, mPrivate1Ret, false),
				gTesMultiAggPrivate1,
				nil,
				mPrivate1FillWhich,
				mPrivate1FillNullWhich,
				validMPrivate1,
				evalMPrivate1,
				mergeMPrivate1,
				nil,
			))
		executor := MakeAgg(
			mg,
			id, false, []types.Type{types.T_int64.ToType(), types.T_bool.ToType()}...)
		require.NoError(t, executor.GroupGrow(1))
		v, err := executor.Flush()
		require.NoError(t, err)
		require.False(t, v.IsNull(0))
		require.Equal(t, int64(0), vector.MustFixedColWithTypeCheck[int64](v)[0])
		v.Free(mg.Mp())
		executor.Free()
	}
	require.Equal(t, int64(0), mg.Mp().CurrNB())
}
