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
)

func RegisterAnyValue2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[uint8], aggAnyValueFills[uint8], aggAnyValueMerge[uint8], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[uint16], aggAnyValueFills[uint16], aggAnyValueMerge[uint16], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[uint32], aggAnyValueFills[uint32], aggAnyValueMerge[uint32], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[uint64], aggAnyValueFills[uint64], aggAnyValueMerge[uint64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[int8], aggAnyValueFills[int8], aggAnyValueMerge[int8], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[int16], aggAnyValueFills[int16], aggAnyValueMerge[int16], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[int32], aggAnyValueFills[int32], aggAnyValueMerge[int32], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[int64], aggAnyValueFills[int64], aggAnyValueMerge[int64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[float32], aggAnyValueFills[float32], aggAnyValueMerge[float32], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[float64], aggAnyValueFills[float64], aggAnyValueMerge[float64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_date.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[types.Date], aggAnyValueFills[types.Date], aggAnyValueMerge[types.Date], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_datetime.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[types.Datetime], aggAnyValueFills[types.Datetime], aggAnyValueMerge[types.Datetime], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_timestamp.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[types.Timestamp], aggAnyValueFills[types.Timestamp], aggAnyValueMerge[types.Timestamp], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_time.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[types.Time], aggAnyValueFills[types.Time], aggAnyValueMerge[types.Time], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[types.Decimal64], aggAnyValueFills[types.Decimal64], aggAnyValueMerge[types.Decimal64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[types.Decimal128], aggAnyValueFills[types.Decimal128], aggAnyValueMerge[types.Decimal128], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bool.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[bool], aggAnyValueFills[bool], aggAnyValueMerge[bool], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[uint64], aggAnyValueFills[uint64], aggAnyValueMerge[uint64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uuid.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[types.Uuid], aggAnyValueFills[types.Uuid], aggAnyValueMerge[types.Uuid], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_Rowid.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[types.Rowid], aggAnyValueFills[types.Rowid], aggAnyValueMerge[types.Rowid], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_enum.ToType(), AnyValueReturnType, true),
		nil, nil, nil,
		aggAnyValueFill[types.Enum], aggAnyValueFills[types.Enum], aggAnyValueMerge[types.Enum], nil)

	varLenList := []types.T{types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_datalink, types.T_binary, types.T_varbinary}
	for _, t := range varLenList {
		aggexec.RegisterAggFromBytesRetBytes(
			aggexec.MakeSingleColumnAggInformation(id, t.ToType(), AnyValueReturnType, true),
			nil, nil, nil,
			aggAnyValueOfBytesFill, aggAnyValueOfBytesFills, aggAnyValueOfBytesMerge, nil)
	}
}

var AnyValueSupportedTypes = []types.T{
	types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
	types.T_int8, types.T_int16, types.T_int32, types.T_int64,
	types.T_float32, types.T_float64,
	types.T_date, types.T_datetime,
	types.T_timestamp, types.T_time,
	types.T_decimal64, types.T_decimal128,
	types.T_bool,
	types.T_bit,
	types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_datalink,
	types.T_uuid,
	types.T_binary, types.T_varbinary,
	types.T_Rowid,
}

func AnyValueReturnType(typs []types.Type) types.Type {
	return typs[0]
}

// any_value(not bytes).
func aggAnyValueFill[from types.FixedSizeTExceptStrType](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, isEmpty bool,
	resultGetter aggexec.AggGetter[from], resultSetter aggexec.AggSetter[from]) error {
	if isEmpty {
		resultSetter(value)
	}
	return nil
}
func aggAnyValueFills[from types.FixedSizeTExceptStrType](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[from], resultSetter aggexec.AggSetter[from]) error {
	return aggAnyValueFill(nil, nil, value, isEmpty, resultGetter, resultSetter)
}
func aggAnyValueMerge[from types.FixedSizeTExceptStrType](
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[from],
	resultSetter aggexec.AggSetter[from]) error {
	if isEmpty1 && !isEmpty2 {
		resultSetter(resultGetter2())
	}
	return nil
}

// any_value(bytes).
func aggAnyValueOfBytesFill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	if isEmpty {
		return resultSetter(value)
	}
	return nil
}
func aggAnyValueOfBytesFills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, count int, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	return aggAnyValueOfBytesFill(nil, nil, value, isEmpty, resultGetter, resultSetter)
}
func aggAnyValueOfBytesMerge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggBytesGetter,
	resultSetter aggexec.AggBytesSetter) error {
	if !isEmpty2 {
		return aggAnyValueOfBytesFill(nil, nil, resultGetter2(), isEmpty1, resultGetter1, resultSetter)
	}
	return nil
}
