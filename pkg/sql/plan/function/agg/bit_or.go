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

func RegisterBitOr2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), BitAndReturnType, true),
		nil, nil, aggBitOrInitResult,
		aggBitOrFill[uint64], aggBitOrFills[uint64], aggBitOrMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), BitAndReturnType, true),
		nil, nil, aggBitOrInitResult,
		aggBitOrFill[uint8], aggBitOrFills[uint8], aggBitOrMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), BitAndReturnType, true),
		nil, nil, aggBitOrInitResult,
		aggBitOrFill[uint16], aggBitOrFills[uint16], aggBitOrMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), BitAndReturnType, true),
		nil, nil, aggBitOrInitResult,
		aggBitOrFill[uint32], aggBitOrFills[uint32], aggBitOrMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), BitAndReturnType, true),
		nil, nil, aggBitOrInitResult,
		aggBitOrFill[uint64], aggBitOrFills[uint64], aggBitOrMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), BitAndReturnType, true),
		nil, nil, aggBitOrInitResult,
		aggBitOrFill[int8], aggBitOrFills[int8], aggBitOrMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), BitAndReturnType, true),
		nil, nil, aggBitOrInitResult,
		aggBitOrFill[int16], aggBitOrFills[int16], aggBitOrMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), BitAndReturnType, true),
		nil, nil, aggBitOrInitResult,
		aggBitOrFill[int32], aggBitOrFills[int32], aggBitOrMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), BitAndReturnType, true),
		nil, nil, aggBitOrInitResult,
		aggBitOrFill[int64], aggBitOrFills[int64], aggBitOrMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), BitAndReturnType, true),
		nil, nil, aggBitOrInitResult,
		aggBitOrFill[float32], aggBitOrFills[float32], aggBitOrMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), BitAndReturnType, true),
		nil, nil, aggBitOrInitResult,
		aggBitOrFill[float64], aggBitOrFills[float64], aggBitOrMerge, nil)

	aggexec.RegisterAggFromBytesRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_binary.ToType(), BitAndReturnType, true),
		nil, nil, nil,
		aggBitOrOfBinaryFill, aggBitOrOfBinaryFills, aggBitOrOfBinaryMerge, nil)

	aggexec.RegisterAggFromBytesRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_varbinary.ToType(), BitAndReturnType, true),
		nil, nil, nil,
		aggBitOrOfBinaryFill, aggBitOrOfBinaryFills, aggBitOrOfBinaryMerge, nil)
}

func aggBitOrInitResult(_ types.Type, _ ...types.Type) uint64 {
	return 0
}
func aggBitOrFill[from numeric](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, isEmpty bool,
	resultGetter aggexec.AggGetter[uint64], resultSetter aggexec.AggSetter[uint64]) error {
	vv := float64(value)
	if vv > math.MaxUint64 {
		resultSetter(math.MaxInt64)
		return nil
	}
	if vv < 0 {
		resultSetter(uint64(int64(value)) | resultGetter())
		return nil
	}
	resultSetter(uint64(value) | resultGetter())
	return nil
}
func aggBitOrFills[from numeric](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[uint64], resultSetter aggexec.AggSetter[uint64]) error {
	return aggBitOrFill(nil, nil, value, isEmpty, resultGetter, resultSetter)
}
func aggBitOrMerge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[uint64],
	resultSetter aggexec.AggSetter[uint64]) error {
	resultSetter(resultGetter1() | resultGetter2())
	return nil
}

func aggBitOrOfBinaryFill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	if isEmpty {
		return resultSetter(value)
	}
	v := resultGetter()
	types.BitOr(v, v, value)
	return nil
}
func aggBitOrOfBinaryFills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, count int, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	return aggBitOrOfBinaryFill(nil, nil, value, isEmpty, resultGetter, resultSetter)
}
func aggBitOrOfBinaryMerge(
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
	types.BitOr(v1, v1, v2)
	return nil
}
