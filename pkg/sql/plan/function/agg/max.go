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
	"bytes"
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

func RegisterMax2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[uint64],
		aggMaxFill[uint64], aggMaxFills[uint64], aggMaxMerge[uint64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[int8],
		aggMaxFill[int8], aggMaxFills[int8], aggMaxMerge[int8], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[int16],
		aggMaxFill[int16], aggMaxFills[int16], aggMaxMerge[int16], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[int32],
		aggMaxFill[int32], aggMaxFills[int32], aggMaxMerge[int32], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[int64],
		aggMaxFill[int64], aggMaxFills[int64], aggMaxMerge[int64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[uint8],
		aggMaxFill[uint8], aggMaxFills[uint8], aggMaxMerge[uint8], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[uint16],
		aggMaxFill[uint16], aggMaxFills[uint16], aggMaxMerge[uint16], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[uint32],
		aggMaxFill[uint32], aggMaxFills[uint32], aggMaxMerge[uint32], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[uint64],
		aggMaxFill[uint64], aggMaxFills[uint64], aggMaxMerge[uint64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[float32],
		aggMaxFill[float32], aggMaxFills[float32], aggMaxMerge[float32], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[float64],
		aggMaxFill[float64], aggMaxFills[float64], aggMaxMerge[float64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_date.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[types.Date],
		aggMaxFill[types.Date], aggMaxFills[types.Date], aggMaxMerge[types.Date], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_datetime.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[types.Datetime],
		aggMaxFill[types.Datetime], aggMaxFills[types.Datetime], aggMaxMerge[types.Datetime], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_timestamp.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[types.Timestamp],
		aggMaxFill[types.Timestamp], aggMaxFills[types.Timestamp], aggMaxMerge[types.Timestamp], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_time.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[types.Time],
		aggMaxFill[types.Time], aggMaxFills[types.Time], aggMaxMerge[types.Time], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bool.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[bool],
		aggMaxOfBoolFill, aggMaxOfBoolFills, aggMaxOfBoolMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_enum.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[types.Enum],
		aggMaxFill[types.Enum], aggMaxFills[types.Enum], aggMaxMerge[types.Enum], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uuid.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[types.Uuid],
		aggMaxOfUuidFill, aggMaxOfUuidFills, aggMaxOfUuidMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[types.Decimal64],
		aggMaxOfDecimal64Fill, aggMaxOfDecimal64Fills, aggMaxOfDecimal64Merge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), MaxReturnType, true),
		nil,
		nil,
		aggMaxInitResult[types.Decimal128],
		aggMaxOfDecimal128Fill, aggMaxOfDecimal128Fills, aggMaxOfDecimal128Merge, nil)

	varlenList := []types.T{types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_datalink, types.T_binary, types.T_varbinary}
	for _, t := range varlenList {
		aggexec.RegisterAggFromBytesRetBytes(
			aggexec.MakeSingleColumnAggInformation(id, t.ToType(), MaxReturnType, true),
			nil,
			nil,
			nil,
			aggMaxOfBytesFill, aggMaxOfBytesFills, aggMaxOfBytesMerge, nil)
	}
}

var MaxSupportedTypes = []types.T{
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
}

func MaxReturnType(typs []types.Type) types.Type {
	return typs[0]
}

var fromTypeIDtoMinValue = map[types.T]interface{}{
	types.T_bit:        uint64(0),
	types.T_uint8:      uint8(0),
	types.T_uint16:     uint16(0),
	types.T_uint32:     uint32(0),
	types.T_uint64:     uint64(0),
	types.T_int8:       int8(math.MinInt8),
	types.T_int16:      int16(math.MinInt16),
	types.T_int32:      int32(math.MinInt32),
	types.T_int64:      int64(math.MinInt64),
	types.T_float32:    float32(-math.MaxFloat32),
	types.T_float64:    -math.MaxFloat64,
	types.T_date:       types.Date(0),
	types.T_datetime:   types.Datetime(0),
	types.T_timestamp:  types.Timestamp(0),
	types.T_time:       types.Time(0),
	types.T_bool:       false,
	types.T_uuid:       types.Uuid([16]byte{}),
	types.T_decimal64:  types.Decimal64Min,
	types.T_decimal128: types.Decimal128Min,
}

func aggMaxInitResult[from canCompare | bool | types.Uuid | types.Decimal64 | types.Decimal128](
	_ types.Type, parameters ...types.Type) from {
	return fromTypeIDtoMinValue[parameters[0].Oid].(from)
}

// max(numeric)
func aggMaxFill[from canCompare](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, isEmpty bool,
	resultGetter aggexec.AggGetter[from], resultSetter aggexec.AggSetter[from]) error {
	if value > resultGetter() {
		resultSetter(value)
	}
	return nil
}
func aggMaxFills[from canCompare](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[from], resultSetter aggexec.AggSetter[from]) error {
	if value > resultGetter() {
		resultSetter(value)
	}
	return nil
}
func aggMaxMerge[from canCompare](
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[from],
	resultSetter aggexec.AggSetter[from]) error {
	if resultGetter2() > resultGetter1() {
		resultSetter(resultGetter2())
	}
	return nil
}

// max(bool)
func aggMaxOfBoolFill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value bool, isEmpty bool,
	resultGetter aggexec.AggGetter[bool], resultSetter aggexec.AggSetter[bool]) error {
	if value {
		resultSetter(value)
	}
	return nil
}
func aggMaxOfBoolFills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value bool, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[bool], resultSetter aggexec.AggSetter[bool]) error {
	if value {
		resultSetter(value)
	}
	return nil
}
func aggMaxOfBoolMerge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[bool],
	resultSetter aggexec.AggSetter[bool]) error {
	if resultGetter2() {
		resultSetter(true)
	}
	return nil
}

// max(uuid)
func aggMaxOfUuidFill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Uuid, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Uuid], resultSetter aggexec.AggSetter[types.Uuid]) error {
	if value.Compare(resultGetter()) > 0 {
		resultSetter(value)
	}
	return nil
}
func aggMaxOfUuidFills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Uuid, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Uuid], resultSetter aggexec.AggSetter[types.Uuid]) error {
	if value.Compare(resultGetter()) > 0 {
		resultSetter(value)
	}
	return nil
}
func aggMaxOfUuidMerge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Uuid],
	resultSetter aggexec.AggSetter[types.Uuid]) error {
	if resultGetter1().Compare(resultGetter2()) < 0 {
		resultSetter(resultGetter2())
	}
	return nil
}

// max(decimal64)
func aggMaxOfDecimal64Fill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Decimal64, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal64], resultSetter aggexec.AggSetter[types.Decimal64]) error {
	if value.Compare(resultGetter()) > 0 {
		resultSetter(value)
	}
	return nil
}
func aggMaxOfDecimal64Fills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Decimal64, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal64], resultSetter aggexec.AggSetter[types.Decimal64]) error {
	if value.Compare(resultGetter()) > 0 {
		resultSetter(value)
	}
	return nil
}
func aggMaxOfDecimal64Merge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Decimal64],
	resultSetter aggexec.AggSetter[types.Decimal64]) error {
	if r := resultGetter2(); r.Compare(resultGetter1()) > 0 {
		resultSetter(r)
	}
	return nil
}

// max(decimal128)
func aggMaxOfDecimal128Fill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Decimal128, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	if value.Compare(resultGetter()) > 0 {
		resultSetter(value)
	}
	return nil
}
func aggMaxOfDecimal128Fills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Decimal128, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	if value.Compare(resultGetter()) > 0 {
		resultSetter(value)
	}
	return nil
}
func aggMaxOfDecimal128Merge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Decimal128],
	resultSetter aggexec.AggSetter[types.Decimal128]) error {
	if r := resultGetter2(); r.Compare(resultGetter1()) > 0 {
		resultSetter(r)
	}
	return nil
}

// max(bytes)
func aggMaxOfBytesFill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	if isEmpty || bytes.Compare(value, resultGetter()) > 0 {
		return resultSetter(value)
	}
	return nil
}
func aggMaxOfBytesFills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, count int, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	return aggMaxOfBytesFill(nil, nil, value, isEmpty, resultGetter, resultSetter)
}
func aggMaxOfBytesMerge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggBytesGetter,
	resultSetter aggexec.AggBytesSetter) error {
	if !isEmpty2 {
		return aggMaxOfBytesFill(nil, nil, resultGetter2(), isEmpty1, resultGetter1, resultSetter)
	}
	return nil
}
