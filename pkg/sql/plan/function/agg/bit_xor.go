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

func RegisterBitXor2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), BitAndReturnType, true),
		nil, nil, aggBitXorInitResult,
		aggBitXorFill[uint64], aggBitXorFills[uint64], aggBitXorMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), BitAndReturnType, true),
		nil, nil, aggBitXorInitResult,
		aggBitXorFill[uint8], aggBitXorFills[uint8], aggBitXorMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), BitAndReturnType, true),
		nil, nil, aggBitXorInitResult,
		aggBitXorFill[uint16], aggBitXorFills[uint16], aggBitXorMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), BitAndReturnType, true),
		nil, nil, aggBitXorInitResult,
		aggBitXorFill[uint32], aggBitXorFills[uint32], aggBitXorMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), BitAndReturnType, true),
		nil, nil, aggBitXorInitResult,
		aggBitXorFill[uint64], aggBitXorFills[uint64], aggBitXorMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), BitAndReturnType, true),
		nil, nil, aggBitXorInitResult,
		aggBitXorFill[int8], aggBitXorFills[int8], aggBitXorMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), BitAndReturnType, true),
		nil, nil, aggBitXorInitResult,
		aggBitXorFill[int16], aggBitXorFills[int16], aggBitXorMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), BitAndReturnType, true),
		nil, nil, aggBitXorInitResult,
		aggBitXorFill[int32], aggBitXorFills[int32], aggBitXorMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), BitAndReturnType, true),
		nil, nil, aggBitXorInitResult,
		aggBitXorFill[int64], aggBitXorFills[int64], aggBitXorMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), BitAndReturnType, true),
		nil, nil, aggBitXorInitResult,
		aggBitXorFill[float32], aggBitXorFills[float32], aggBitXorMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), BitAndReturnType, true),
		nil, nil, aggBitXorInitResult,
		aggBitXorFill[float64], aggBitXorFills[float64], aggBitXorMerge, nil)

	aggexec.RegisterAggFromBytesRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_binary.ToType(), BitAndReturnType, true),
		nil, nil, nil,
		aggBitXorOfBinaryFill, aggBitXorOfBinaryFills, aggBitXorOfBinaryMerge, nil)

	aggexec.RegisterAggFromBytesRetBytes(
		aggexec.MakeSingleColumnAggInformation(id, types.T_varbinary.ToType(), BitAndReturnType, true),
		nil, nil, nil,
		aggBitXorOfBinaryFill, aggBitXorOfBinaryFills, aggBitXorOfBinaryMerge, nil)
}

func aggBitXorInitResult(_ types.Type, _ ...types.Type) uint64 {
	return 0
}
func aggBitXorFill[from numeric](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, isEmpty bool,
	resultGetter aggexec.AggGetter[uint64], resultSetter aggexec.AggSetter[uint64]) error {
	vv := float64(value)
	if vv > math.MaxUint64 {
		resultSetter(math.MaxInt64 ^ resultGetter())
		return nil
	}
	if vv < 0 {
		resultSetter(uint64(int64(value)) ^ resultGetter())
		return nil
	}
	resultSetter(uint64(value) ^ resultGetter())
	return nil
}
func aggBitXorFills[from numeric](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[uint64], resultSetter aggexec.AggSetter[uint64]) error {
	if count%2 == 1 {
		return aggBitXorFill(nil, nil, value, isEmpty, resultGetter, resultSetter)
	}
	return nil
}
func aggBitXorMerge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[uint64],
	resultSetter aggexec.AggSetter[uint64]) error {
	resultSetter(resultGetter1() ^ resultGetter2())
	return nil
}

func aggBitXorOfBinaryFill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	if isEmpty {
		return resultSetter(value)
	}
	v := resultGetter()
	types.BitXor(v, v, value)
	return nil
}
func aggBitXorOfBinaryFills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, count int, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	if count%2 == 1 {
		return aggBitXorOfBinaryFill(nil, nil, value, isEmpty, resultGetter, resultSetter)
	}
	if isEmpty {
		return resultSetter(make([]byte, len(value)))
	}
	return nil
}
func aggBitXorOfBinaryMerge(
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
	types.BitXor(v1, v1, v2)
	return nil
}
