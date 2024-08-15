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

func RegisterMin2(id int64) {
	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[uint64],
		aggMinFill[uint64], aggMinFills[uint64], aggMinMerge[uint64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[int8],
		aggMinFill[int8], aggMinFills[int8], aggMinMerge[int8], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[int16],
		aggMinFill[int16], aggMinFills[int16], aggMinMerge[int16], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[int32],
		aggMinFill[int32], aggMinFills[int32], aggMinMerge[int32], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[int64],
		aggMinFill[int64], aggMinFills[int64], aggMinMerge[int64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[uint8],
		aggMinFill[uint8], aggMinFills[uint8], aggMinMerge[uint8], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[uint16],
		aggMinFill[uint16], aggMinFills[uint16], aggMinMerge[uint16], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[uint32],
		aggMinFill[uint32], aggMinFills[uint32], aggMinMerge[uint32], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[uint64],
		aggMinFill[uint64], aggMinFills[uint64], aggMinMerge[uint64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[float32],
		aggMinFill[float32], aggMinFills[float32], aggMinMerge[float32], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[float64],
		aggMinFill[float64], aggMinFills[float64], aggMinMerge[float64], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_date.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[types.Date],
		aggMinFill[types.Date], aggMinFills[types.Date], aggMinMerge[types.Date], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_datetime.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[types.Datetime],
		aggMinFill[types.Datetime], aggMinFills[types.Datetime], aggMinMerge[types.Datetime], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_timestamp.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[types.Timestamp],
		aggMinFill[types.Timestamp], aggMinFills[types.Timestamp], aggMinMerge[types.Timestamp], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_time.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[types.Time],
		aggMinFill[types.Time], aggMinFills[types.Time], aggMinMerge[types.Time], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_bool.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[bool],
		aggMinOfBoolFill, aggMinOfBoolFills, aggMinOfBoolMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_enum.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[types.Enum],
		aggMinFill[types.Enum], aggMinFills[types.Enum], aggMinMerge[types.Enum], nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_uuid.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[types.Uuid],
		aggMinOfUuidFill, aggMinOfUuidFills, aggMinOfUuidMerge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[types.Decimal64],
		aggMinOfDecimal64Fill, aggMinOfDecimal64Fills, aggMinOfDecimal64Merge, nil)

	aggexec.RegisterAggFromFixedRetFixed(
		aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), MinReturnType, true),
		nil,
		nil,
		aggMinInitResult[types.Decimal128],
		aggMinOfDecimal128Fill, aggMinOfDecimal128Fills, aggMinOfDecimal128Merge, nil)

	varlenList := []types.T{types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_datalink, types.T_binary, types.T_varbinary}
	for _, t := range varlenList {
		aggexec.RegisterAggFromBytesRetBytes(
			aggexec.MakeSingleColumnAggInformation(id, t.ToType(), MinReturnType, true),
			nil,
			nil,
			nil,
			aggMinOfBytesFill, aggMinOfBytesFills, aggMinOfBytesMerge, nil)
	}
}

var MinSupportedTypes = []types.T{
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

func MinReturnType(typs []types.Type) types.Type {
	return typs[0]
}

var fromTypeIDtoMaxValue = map[types.T]interface{}{
	types.T_bit:       uint64(math.MaxUint64),
	types.T_uint8:     uint8(math.MaxUint8),
	types.T_uint16:    uint16(math.MaxUint16),
	types.T_uint32:    uint32(math.MaxUint32),
	types.T_uint64:    uint64(math.MaxUint64),
	types.T_int8:      int8(math.MaxInt8),
	types.T_int16:     int16(math.MaxInt16),
	types.T_int32:     int32(math.MaxInt32),
	types.T_int64:     int64(math.MaxInt64),
	types.T_float32:   float32(math.MaxFloat32),
	types.T_float64:   math.MaxFloat64,
	types.T_date:      types.Date(math.MaxInt32),
	types.T_datetime:  types.Datetime(math.MaxInt64),
	types.T_timestamp: types.Timestamp(math.MaxInt64),
	types.T_time:      types.Time(math.MaxInt64),
	types.T_bool:      true,
	types.T_uuid: types.Uuid([16]byte{
		math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8,
		math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8, math.MaxUint8}),
	types.T_decimal64:  types.Decimal64Max,
	types.T_decimal128: types.Decimal128Max,
}

func aggMinInitResult[from canCompare | bool | types.Uuid | types.Decimal64 | types.Decimal128](
	_ types.Type, parameters ...types.Type) from {
	return fromTypeIDtoMaxValue[parameters[0].Oid].(from)
}

// min(numeric)
func aggMinFill[from canCompare](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, isEmpty bool,
	resultGetter aggexec.AggGetter[from], resultSetter aggexec.AggSetter[from]) error {
	if value < resultGetter() {
		resultSetter(value)
	}
	return nil
}
func aggMinFills[from canCompare](
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value from, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[from], resultSetter aggexec.AggSetter[from]) error {
	if value < resultGetter() {
		resultSetter(value)
	}
	return nil
}
func aggMinMerge[from canCompare](
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[from],
	resultSetter aggexec.AggSetter[from]) error {
	if resultGetter2() < resultGetter1() {
		resultSetter(resultGetter2())
	}
	return nil
}

// min(bool)
func aggMinOfBoolFill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value bool, isEmpty bool,
	resultGetter aggexec.AggGetter[bool], resultSetter aggexec.AggSetter[bool]) error {
	if !value {
		resultSetter(false)
	}
	return nil
}
func aggMinOfBoolFills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value bool, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[bool], resultSetter aggexec.AggSetter[bool]) error {
	return aggMinOfBoolFill(nil, nil, value, isEmpty, resultGetter, resultSetter)
}
func aggMinOfBoolMerge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[bool],
	resultSetter aggexec.AggSetter[bool]) error {
	if !resultGetter2() {
		resultSetter(false)
	}
	return nil
}

// min(uuid)
func aggMinOfUuidFill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Uuid, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Uuid], resultSetter aggexec.AggSetter[types.Uuid]) error {
	if value.Compare(resultGetter()) < 0 {
		resultSetter(value)
	}
	return nil
}
func aggMinOfUuidFills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Uuid, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Uuid], resultSetter aggexec.AggSetter[types.Uuid]) error {
	if value.Compare(resultGetter()) < 0 {
		resultSetter(value)
	}
	return nil
}
func aggMinOfUuidMerge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Uuid],
	resultSetter aggexec.AggSetter[types.Uuid]) error {
	if resultGetter1().Compare(resultGetter2()) > 0 {
		resultSetter(resultGetter2())
	}
	return nil
}

// min(decimal64)
func aggMinOfDecimal64Fill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Decimal64, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal64], resultSetter aggexec.AggSetter[types.Decimal64]) error {
	if value.Compare(resultGetter()) < 0 {
		resultSetter(value)
	}
	return nil
}
func aggMinOfDecimal64Fills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Decimal64, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal64], resultSetter aggexec.AggSetter[types.Decimal64]) error {
	if value.Compare(resultGetter()) < 0 {
		resultSetter(value)
	}
	return nil
}
func aggMinOfDecimal64Merge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Decimal64],
	resultSetter aggexec.AggSetter[types.Decimal64]) error {
	if r := resultGetter2(); r.Compare(resultGetter1()) < 0 {
		resultSetter(r)
	}
	return nil
}

// min(decimal128)
func aggMinOfDecimal128Fill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Decimal128, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	if value.Compare(resultGetter()) < 0 {
		resultSetter(value)
	}
	return nil
}
func aggMinOfDecimal128Fills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value types.Decimal128, count int, isEmpty bool,
	resultGetter aggexec.AggGetter[types.Decimal128], resultSetter aggexec.AggSetter[types.Decimal128]) error {
	if value.Compare(resultGetter()) < 0 {
		resultSetter(value)
	}
	return nil
}
func aggMinOfDecimal128Merge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggGetter[types.Decimal128],
	resultSetter aggexec.AggSetter[types.Decimal128]) error {
	if r := resultGetter2(); r.Compare(resultGetter1()) < 0 {
		resultSetter(r)
	}
	return nil
}

// min(bytes)
func aggMinOfBytesFill(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	if isEmpty || bytes.Compare(value, resultGetter()) < 0 {
		return resultSetter(value)
	}
	return nil
}
func aggMinOfBytesFills(
	_ aggexec.AggGroupExecContext, _ aggexec.AggCommonExecContext,
	value []byte, count int, isEmpty bool,
	resultGetter aggexec.AggBytesGetter, resultSetter aggexec.AggBytesSetter) error {
	return aggMinOfBytesFill(nil, nil, value, isEmpty, resultGetter, resultSetter)
}
func aggMinOfBytesMerge(
	_, _ aggexec.AggGroupExecContext,
	_ aggexec.AggCommonExecContext,
	isEmpty1, isEmpty2 bool,
	resultGetter1, resultGetter2 aggexec.AggBytesGetter,
	resultSetter aggexec.AggBytesSetter) error {
	if !isEmpty2 {
		return aggMinOfBytesFill(nil, nil, resultGetter2(), isEmpty1, resultGetter1, resultSetter)
	}
	return nil
}
