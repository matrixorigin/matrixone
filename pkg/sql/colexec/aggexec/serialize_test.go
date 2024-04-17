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
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
	"testing"
)

func testAggExecSerialize(exec AggFuncExec, checkFn func(src, dst AggFuncExec) error) error {
	ds, marshalErr := MarshalAggFuncExec(exec)
	if marshalErr != nil {
		return marshalErr
	}
	newExec, unmarshalErr := UnmarshalAggFuncExec(nil, ds)
	if unmarshalErr != nil {
		return unmarshalErr
	}

	if checkFn == nil {
		return nil
	}
	return checkFn(exec, newExec)
}

func fillTestData(mg AggMemoryManager, groupNumber int, exec AggFuncExec, dataType types.Type) error {
	if err := exec.GroupGrow(groupNumber); err != nil {
		return err
	}

	vec := vector.NewVec(dataType)
	switch dataType.Oid {
	case types.T_int32:
		values := make([]int32, groupNumber)
		for i := 0; i < groupNumber; i++ {
			values[i] = int32(i)
		}
		if err := vector.AppendFixedList[int32](vec, values, nil, mg.Mp()); err != nil {
			return err
		}

	case types.T_varchar:
		values := make([][]byte, groupNumber)
		for i := 0; i < groupNumber; i++ {
			values[i] = []byte(fmt.Sprintf("%d", i))
		}
		if err := vector.AppendBytesList(vec, values, nil, mg.Mp()); err != nil {
			return err
		}

	case types.T_array_float32:
		values := make([][]float32, groupNumber)
		for i := 0; i < groupNumber; i++ {
			values[i] = []float32{float32(i), float32(i + 1), float32(i + 2)}
		}
		if err := vector.AppendArrayList[float32](vec, values, nil, mg.Mp()); err != nil {
			return err
		}

	default:
		return moerr.NewInternalErrorNoCtx("agg exec ut failed: unsupported data type")
	}

	inputs := []*vector.Vector{vec}
	for i := 0; i < groupNumber; i++ {
		if err := exec.BulkFill(i, inputs); err != nil {
			vec.Free(mg.Mp())
			return err
		}
	}
	vec.Free(mg.Mp())
	return nil
}

func TestSerial_SingleAggFuncExecSerial(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     gUniqueAggIdForTest(),
		distinct:  false,
		argType:   types.T_int32.ToType(),
		retType:   types.T_int64.ToType(),
		emptyNull: true,
	}
	RegisterSingleAggFromFixedToFixed(
		MakeSingleAgg1RegisteredInfo(
			MakeSingleColumnAggInformation(info.aggID, info.argType, tSinglePrivate1Ret, true, info.emptyNull),
			gTestSingleAggPrivateSer1,
			fillSinglePrivate1, fillNullSinglePrivate1, fillsSinglePrivate1, mergeSinglePrivate1, nil))

	// methods to check the correctness of the serialized AggFuncExec.
	checkFn := func(src, dst AggFuncExec) error {
		s1, ok1 := src.(*singleAggFuncExec1[int32, int64])
		s2, ok2 := dst.(*singleAggFuncExec1[int32, int64])
		if !ok1 || !ok2 {
			return moerr.NewInternalErrorNoCtx("type assertion failed")
		}

		if !s1.singleAggInfo.eq(s2.singleAggInfo) {
			return moerr.NewInternalErrorNoCtx("singleAggInfo not equal.")
		}

		if !s1.ret.eq(s2.ret) {
			return moerr.NewInternalErrorNoCtx("ret not equal.")
		}
		return nil
	}

	{
		executor := MakeAgg(
			mg,
			info.aggID, info.distinct, info.argType)
		require.NoError(t, fillTestData(mg, 10, executor, info.argType))
		require.NoError(t, testAggExecSerialize(executor, checkFn))
		executor.Free()
	}

	require.Equal(t, int64(0), mg.Mp().CurrNB())
}

func TestSerial_CountColumnAggFuncExec(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     gUniqueAggIdForTest(),
		distinct:  false,
		argType:   types.T_int32.ToType(),
		retType:   CountReturnType(nil),
		emptyNull: false,
	}
	RegisterCountColumnAgg(info.aggID)

	// methods to check the correctness of the serialized AggFuncExec.
	checkFn := func(src, dst AggFuncExec) error {
		s1, ok1 := src.(*countColumnExec)
		s2, ok2 := dst.(*countColumnExec)
		if !ok1 || !ok2 {
			return moerr.NewInternalErrorNoCtx("type assertion failed")
		}

		if !s1.singleAggInfo.eq(s2.singleAggInfo) {
			return moerr.NewInternalErrorNoCtx("singleAggInfo not equal.")
		}

		if !s1.ret.eq(s2.ret) {
			return moerr.NewInternalErrorNoCtx("ret not equal.")
		}
		return nil
	}

	{
		executor := MakeAgg(
			mg,
			info.aggID, info.distinct, info.argType)
		require.NoError(t, fillTestData(mg, 10, executor, info.argType))
		require.NoError(t, testAggExecSerialize(executor, checkFn))
		executor.Free()
	}

	require.Equal(t, int64(0), mg.Mp().CurrNB())
}

func TestSerial_CountStarAggFuncExec(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     gUniqueAggIdForTest(),
		distinct:  false,
		argType:   types.T_int32.ToType(),
		retType:   CountReturnType(nil),
		emptyNull: false,
	}
	RegisterCountStarAgg(info.aggID)

	// methods to check the correctness of the serialized AggFuncExec.
	checkFn := func(src, dst AggFuncExec) error {
		s1, ok1 := src.(*countStarExec)
		s2, ok2 := dst.(*countStarExec)
		if !ok1 || !ok2 {
			return moerr.NewInternalErrorNoCtx("type assertion failed")
		}

		if !s1.singleAggInfo.eq(s2.singleAggInfo) {
			return moerr.NewInternalErrorNoCtx("singleAggInfo not equal.")
		}

		if !s1.ret.eq(s2.ret) {
			return moerr.NewInternalErrorNoCtx("ret not equal.")
		}
		return nil
	}

	{
		executor := MakeAgg(
			mg,
			info.aggID, info.distinct, info.argType)
		require.NoError(t, fillTestData(mg, 10, executor, info.argType))
		require.NoError(t, testAggExecSerialize(executor, checkFn))
		executor.Free()
	}

	require.Equal(t, int64(0), mg.Mp().CurrNB())
}

func TestSerial_ApproxCountFixedAggFuncExec(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     gUniqueAggIdForTest(),
		distinct:  false,
		argType:   types.T_int32.ToType(),
		retType:   types.T_uint64.ToType(),
		emptyNull: false,
	}
	RegisterApproxCountAgg(info.aggID)

	// methods to check the correctness of the serialized AggFuncExec.
	checkFn := func(src, dst AggFuncExec) error {
		s1, ok1 := src.(*approxCountFixedExec[int32])
		s2, ok2 := dst.(*approxCountFixedExec[int32])
		if !ok1 || !ok2 {
			return moerr.NewInternalErrorNoCtx("type assertion failed")
		}

		if !s1.singleAggInfo.eq(s2.singleAggInfo) {
			return moerr.NewInternalErrorNoCtx("singleAggInfo not equal.")
		}

		if !s1.ret.eq(s2.ret) {
			return moerr.NewInternalErrorNoCtx("ret not equal.")
		}

		if len(s1.groups) == len(s2.groups) {
			for i := 0; i < len(s1.groups); i++ {
				if s1.groups[i] == nil {
					continue
				}

				if s2.groups[i] == nil {
					return moerr.NewInternalErrorNoCtx("groups not equal.")
				}
			}

			return nil
		}
		return moerr.NewInternalErrorNoCtx("groups not equal.")
	}

	{
		executor := MakeAgg(
			mg,
			info.aggID, info.distinct, info.argType)
		require.NoError(t, fillTestData(mg, 10, executor, info.argType))
		require.NoError(t, testAggExecSerialize(executor, checkFn))
		executor.Free()
	}

	require.Equal(t, int64(0), mg.Mp().CurrNB())
}

func TestSerial_ApproxCountVarAggFuncExec(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     gUniqueAggIdForTest(),
		distinct:  false,
		argType:   types.T_varchar.ToType(),
		retType:   types.T_uint64.ToType(),
		emptyNull: false,
	}
	RegisterApproxCountAgg(info.aggID)

	// methods to check the correctness of the serialized AggFuncExec.
	checkFn := func(src, dst AggFuncExec) error {
		s1, ok1 := src.(*approxCountVarExec)
		s2, ok2 := dst.(*approxCountVarExec)
		if !ok1 || !ok2 {
			return moerr.NewInternalErrorNoCtx("type assertion failed")
		}

		if !s1.singleAggInfo.eq(s2.singleAggInfo) {
			return moerr.NewInternalErrorNoCtx("singleAggInfo not equal.")
		}

		if !s1.ret.eq(s2.ret) {
			return moerr.NewInternalErrorNoCtx("ret not equal.")
		}

		if len(s1.groups) == len(s2.groups) {
			for i := 0; i < len(s1.groups); i++ {
				if s1.groups[i] == nil {
					continue
				}

				if s2.groups[i] == nil {
					return moerr.NewInternalErrorNoCtx("groups not equal.")
				}
			}

			return nil
		}
		return moerr.NewInternalErrorNoCtx("groups not equal.")
	}

	{
		executor := MakeAgg(
			mg,
			info.aggID, info.distinct, info.argType)
		require.NoError(t, fillTestData(mg, 10, executor, info.argType))
		require.NoError(t, testAggExecSerialize(executor, checkFn))
		executor.Free()
	}

	require.Equal(t, int64(0), mg.Mp().CurrNB())
}

func TestSerial_ClusterCentersExec(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     gUniqueAggIdForTest(),
		distinct:  false,
		argType:   types.T_array_float32.ToType(),
		retType:   ClusterCentersReturnType(nil),
		emptyNull: false,
	}
	RegisterClusterCenters(info.aggID)

	// methods to check the correctness of the serialized AggFuncExec.
	checkFn := func(src, dst AggFuncExec) error {
		s1, ok1 := src.(*clusterCentersExec)
		s2, ok2 := dst.(*clusterCentersExec)
		if !ok1 || !ok2 {
			return moerr.NewInternalErrorNoCtx("type assertion failed")
		}

		if !s1.singleAggInfo.eq(s2.singleAggInfo) {
			return moerr.NewInternalErrorNoCtx("singleAggInfo not equal.")
		}

		if !s1.ret.eq(s2.ret) {
			return moerr.NewInternalErrorNoCtx("ret not equal.")
		}

		{
			// kmeans parameters check.
			ne1 := s1.clusterCnt != s2.clusterCnt
			ne2 := s1.distType != s2.distType
			ne3 := s1.initType != s2.initType
			ne4 := s1.normalize != s2.normalize
			if ne1 || ne2 || ne3 || ne4 {
				return moerr.NewInternalErrorNoCtx("kmeans parameters not equal.")
			}
		}

		if len(s1.groupData) == len(s2.groupData) {
			for i := 0; i < len(s1.groupData); i++ {
				if s1.groupData[i] == nil || s1.groupData[i].Length() == 0 {
					continue
				}

				if l := s1.groupData[i].Length(); l != s2.groupData[i].Length() {
					return moerr.NewInternalErrorNoCtx("groupData length not equal.")
				}

				vs1 := vector.MustArrayCol[float32](s1.groupData[i])
				vs2 := vector.MustArrayCol[float32](s2.groupData[i])
				for n := 0; n < len(vs1); n++ {
					for m := 0; m < len(vs1[n]); m++ {
						if vs1[n][m] != vs2[n][m] {
							return moerr.NewInternalErrorNoCtx("groupData item not equal.")
						}
					}
				}
			}

			return nil
		}
		return moerr.NewInternalErrorNoCtx("groupData not equal.")
	}

	{
		executor := MakeAgg(
			mg,
			info.aggID, info.distinct, info.argType)
		require.NoError(t, fillTestData(mg, 10, executor, info.argType))
		require.NoError(t, testAggExecSerialize(executor, checkFn))
		executor.Free()
	}

	require.Equal(t, int64(0), mg.Mp().CurrNB())
}

func TestSerial_MedianColumnExec(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:    gUniqueAggIdForTest(),
		distinct: false,
		argType:  types.T_int32.ToType(),
	}
	info.retType = MedianReturnType([]types.Type{info.argType})

	RegisterMedian(info.aggID)

	// methods to check the correctness of the serialized AggFuncExec.
	checkFn := func(src, dst AggFuncExec) error {
		s1, ok1 := src.(*medianColumnNumericExec[int32])
		s2, ok2 := dst.(*medianColumnNumericExec[int32])
		if !ok1 || !ok2 {
			return moerr.NewInternalErrorNoCtx("type assertion failed")
		}

		if !s1.singleAggInfo.eq(s2.singleAggInfo) {
			return moerr.NewInternalErrorNoCtx("singleAggInfo not equal.")
		}

		if !s1.ret.eq(s2.ret) {
			return moerr.NewInternalErrorNoCtx("ret not equal.")
		}

		if len(s1.groups) == len(s2.groups) {
			for i := 0; i < len(s1.groups); i++ {
				if s1.groups[i] == nil || s1.groups[i].Length() == 0 {
					continue
				}

				if l := s1.groups[i].Length(); l != s2.groups[i].Length() {
					return moerr.NewInternalErrorNoCtx("groupData length not equal.")
				}

				vs1 := vector.MustFixedCol[int32](s1.groups[i])
				vs2 := vector.MustFixedCol[int32](s2.groups[i])
				for n := 0; n < len(vs1); n++ {
					if vs1[n] != vs2[n] {
						return moerr.NewInternalErrorNoCtx("groupData item not equal.")
					}
				}
			}

			return nil
		}

		return moerr.NewInternalErrorNoCtx("groupData not equal.")
	}

	{
		executor := MakeAgg(
			mg,
			info.aggID, info.distinct, info.argType)
		require.NoError(t, fillTestData(mg, 10, executor, info.argType))
		require.NoError(t, testAggExecSerialize(executor, checkFn))
		executor.Free()
	}

	require.Equal(t, int64(0), mg.Mp().CurrNB())
}

type testSingleAggPrivateSer1 struct {
	testSingleAggPrivate1
}

func gTestSingleAggPrivateSer1() SingleAggFromFixedRetFixed[int32, int64] {
	return &testSingleAggPrivateSer1{}
}

func (s *testSingleAggPrivateSer1) Marshal() []byte {
	return []byte("testSingleAggPrivateSer1")
}

func (s *testSingleAggPrivateSer1) Unmarshal(bs []byte) {
	if string(bs) != "testSingleAggPrivateSer1" {
		panic("unmarshal failed")
	}
}

// this test is to check if the agg framework can serialize and deserialize the private struct of the agg function.
func TestSerial_Agg1(t *testing.T) {
	mg := newTestAggMemoryManager()

	info := singleAggInfo{
		aggID:     gUniqueAggIdForTest(),
		distinct:  false,
		argType:   types.T_int32.ToType(),
		retType:   types.T_int64.ToType(),
		emptyNull: true,
	}
	RegisterSingleAggFromFixedToFixed(
		MakeSingleAgg1RegisteredInfo(
			MakeSingleColumnAggInformation(info.aggID, info.argType, tSinglePrivate1Ret, true, info.emptyNull),
			gTestSingleAggPrivateSer1,
			fillSinglePrivate1, fillNullSinglePrivate1, fillsSinglePrivate1, mergeSinglePrivate1, nil))

	{
		executor := MakeAgg(
			mg,
			info.aggID, info.distinct, info.argType)
		require.NoError(t, fillTestData(mg, 10, executor, info.argType))
		require.NoError(t, testAggExecSerialize(executor, nil))
		executor.Free()
	}

	require.Equal(t, int64(0), mg.Mp().CurrNB())
}
