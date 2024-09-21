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

package agg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"math"
)

func RegisterBitAnd2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), BitAndReturnType, true),
		nil, nil, aggBitAndInitResult,
		aggBitAndFill[uint64], aggBitAndFills[uint64], aggBitAndMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), BitAndReturnType, true),
		nil, nil, aggBitAndInitResult,
		aggBitAndFill[uint8], aggBitAndFills[uint8], aggBitAndMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), BitAndReturnType, true),
		nil, nil, aggBitAndInitResult,
		aggBitAndFill[uint16], aggBitAndFills[uint16], aggBitAndMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), BitAndReturnType, true),
		nil, nil, aggBitAndInitResult,
		aggBitAndFill[uint32], aggBitAndFills[uint32], aggBitAndMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), BitAndReturnType, true),
		nil, nil, aggBitAndInitResult,
		aggBitAndFill[uint64], aggBitAndFills[uint64], aggBitAndMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), BitAndReturnType, true),
		nil, nil, aggBitAndInitResult,
		aggBitAndFill[int8], aggBitAndFills[int8], aggBitAndMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), BitAndReturnType, true),
		nil, nil, aggBitAndInitResult,
		aggBitAndFill[int16], aggBitAndFills[int16], aggBitAndMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), BitAndReturnType, true),
		nil, nil, aggBitAndInitResult,
		aggBitAndFill[int32], aggBitAndFills[int32], aggBitAndMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), BitAndReturnType, true),
		nil, nil, aggBitAndInitResult,
		aggBitAndFill[int64], aggBitAndFills[int64], aggBitAndMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), BitAndReturnType, true),
		nil, nil, aggBitAndInitResult,
		aggBitAndFill[float32], aggBitAndFills[float32], aggBitAndMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), BitAndReturnType, true),
		nil, nil, aggBitAndInitResult,
		aggBitAndFill[float64], aggBitAndFills[float64], aggBitAndMerge, nil)

	aggexec.RegisterAggFromBytesRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_binary.ToType(), BitAndReturnType, true),
		nil, nil, nil,
		aggBitAndOfBinaryFill, aggBitAndOfBinaryFills, aggBitAndOfBinaryMerge, nil)

	aggexec.RegisterAggFromBytesRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_varbinary.ToType(), BitAndReturnType, true),
		nil, nil, nil,
		aggBitAndOfBinaryFill, aggBitAndOfBinaryFills, aggBitAndOfBinaryMerge, nil)
}

var BitAndSupportedParameters = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_binary, types.T_varbinary,
	types.T_bit,
}

func BitAndReturnType(typs []types.Type) types.Type {
	if typs[0].Oid == types.T_binary || typs[0].Oid == types.T_varbinary {
		return typs[0]
	}
	return types.T_uint64.ToType()
}

func aggBitAndInitResult(_ types.Type, _ ...types.Type) uint64 {
	return ^uint64(0)
}
func aggBitAndFill[from numeric](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, isEmpty bool,
	resultGetter aggexec.AggGetter[uint64], resultSetter aggexec.AggSetter[uint64]) error {
	vv := float64(value)
	if vv > math.MaxUint64 {
		resultSetter(math.MaxInt64 & resultGetter())
		return nil
	}
	if vv < 0 {
		resultSetter(uint64(int64(value)) & resultGetter())
		return nil
	}
	resultSetter(uint64(value) & resultGetter())
	return nil
}
func aggBitAndFills[from numeric](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[uint64], resultSetter aggexec.AggSetter[uint64]) error {
	return aggBitAndFill(nil, nil, value, isEmpty, resultGetter, resultSetter)
}
func aggBitAndMerge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[uint64],
	resultSetter aggexec.AggSetter[uint64]) error {
	resultSetter(resultGetter1() & resultGetter2())
	return nil
}

func aggBitAndOfBinaryFill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	if isEmpty {
		return resultSetter(value)
	}
	v := resultGetter()
	types.BitAnd(v, v, value)
	return nil
}
func aggBitAndOfBinaryFills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, count int, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	return aggBitAndOfBinaryFill(nil, nil, value, isEmpty, resultGetter, resultSetter)
}
func aggBitAndOfBinaryMerge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggBytesGetter,
	resultSetter aggexec.AggBytesSetter) error {
	if isEmpty2 {
		return nil
	}
	if isEmpty1 {
		return resultSetter(resultGetter2())
	}
	v1, v2 := resultGetter1(), resultGetter2()
	types.BitAnd(v1, v1, v2)
	return nil
}
