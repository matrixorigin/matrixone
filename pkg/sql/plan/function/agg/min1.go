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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

func RegisterMin1(id int64) {
	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), MinReturnType, false, true),
			newAggMin[uint8],
			FillAggMin1[uint8], nil, FillsAggMin1[uint8],
			MergeAggMin1[uint8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), MinReturnType, false, true),
			newAggMin[uint16],
			FillAggMin1[uint16], nil, FillsAggMin1[uint16],
			MergeAggMin1[uint16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), MinReturnType, false, true),
			newAggMin[uint32],
			FillAggMin1[uint32], nil, FillsAggMin1[uint32],
			MergeAggMin1[uint32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), MinReturnType, false, true),
			newAggMin[uint64],
			FillAggMin1[uint64], nil, FillsAggMin1[uint64],
			MergeAggMin1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), MinReturnType, false, true),
			newAggMin[int8],
			FillAggMin1[int8], nil, FillsAggMin1[int8],
			MergeAggMin1[int8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), MinReturnType, false, true),
			newAggMin[int16],
			FillAggMin1[int16], nil, FillsAggMin1[int16],
			MergeAggMin1[int16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), MinReturnType, false, true),
			newAggMin[int32],
			FillAggMin1[int32], nil, FillsAggMin1[int32],
			MergeAggMin1[int32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), MinReturnType, false, true),
			newAggMin[int64],
			FillAggMin1[int64], nil, FillsAggMin1[int64],
			MergeAggMin1[int64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), MinReturnType, false, true),
			newAggMin[float32],
			FillAggMin1[float32], nil, FillsAggMin1[float32],
			MergeAggMin1[float32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), MinReturnType, false, true),
			newAggMin[float64],
			FillAggMin1[float64], nil, FillsAggMin1[float64],
			MergeAggMin1[float64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_date.ToType(), MinReturnType, false, true),
			newAggMin[types.Date],
			FillAggMin1[types.Date], nil, FillsAggMin1[types.Date],
			MergeAggMin1[types.Date],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_datetime.ToType(), MinReturnType, false, true),
			newAggMin[types.Datetime],
			FillAggMin1[types.Datetime], nil, FillsAggMin1[types.Datetime],
			MergeAggMin1[types.Datetime],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_timestamp.ToType(), MinReturnType, false, true),
			newAggMin[types.Timestamp],
			FillAggMin1[types.Timestamp], nil, FillsAggMin1[types.Timestamp],
			MergeAggMin1[types.Timestamp],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_time.ToType(), MinReturnType, false, true),
			newAggMin[types.Time],
			FillAggMin1[types.Time], nil, FillsAggMin1[types.Time],
			MergeAggMin1[types.Time],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), MinReturnType, false, true),
			newAggMin[uint64],
			FillAggMin1[uint64], nil, FillsAggMin1[uint64],
			MergeAggMin1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bool.ToType(), MinReturnType, false, true),
			newAggMinBool,
			FillAggMinBool, nil, FillsAggMinBool,
			MergeAggMinBool,
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uuid.ToType(), MinReturnType, false, true),
			newAggUuidMin,
			FillAggMinUuid, nil, FillsAggMinUuid,
			MergeAggMinUuid,
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), MinReturnType, false, true),
			newAggMinDecimal64,
			FillAggMinDecimal64, nil, FillsAggMinDecimal64,
			MergeAggMinDecimal64,
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), MinReturnType, false, true),
			newAggMinDecimal128,
			FillAggMinDecimal128, nil, FillsAggMinDecimal128,
			MergeAggMinDecimal128,
			nil,
		))

	varlenList := []types.T{types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_binary, types.T_varbinary}
	for _, t := range varlenList {
		aggexec.RegisterSingleAggFromVarToVar(
			aggexec.MakeSingleAgg4RegisteredInfo(
				aggexec.MakeSingleColumnAggInformation(id, t.ToType(), MinReturnType, false, true),
				newAggBytesMin,
				FillAggMinBytes, nil, FillsAggMinBytes,
				MergeAggMinBytes,
				nil,
			))
	}
}

func FillAggMin1[from canCompare](
	exec aggexec.SingleAggFromFixedRetFixed[from, from],
	value from, getter aggexec.AggGetter[from], setter aggexec.AggSetter[from]) error {
	if value < getter() {
		setter(value)
	}
	return nil
}
func FillsAggMin1[from canCompare](
	exec aggexec.SingleAggFromFixedRetFixed[from, from],
	value from, isNull bool, count int, getter aggexec.AggGetter[from], setter aggexec.AggSetter[from]) error {
	if isNull {
		return nil
	}
	return FillAggMin1(exec, value, getter, setter)
}
func MergeAggMin1[from canCompare](
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[from, from],
	getter1, getter2 aggexec.AggGetter[from], setter1 aggexec.AggSetter[from]) error {
	if getter2() < getter1() {
		setter1(getter2())
	}
	return nil
}

func FillAggMinBool(
	exec aggexec.SingleAggFromFixedRetFixed[bool, bool],
	value bool, getter aggexec.AggGetter[bool], setter aggexec.AggSetter[bool]) error {
	if !value {
		setter(false)
	}
	return nil
}
func FillsAggMinBool(
	exec aggexec.SingleAggFromFixedRetFixed[bool, bool],
	value bool, isNull bool, count int, getter aggexec.AggGetter[bool], setter aggexec.AggSetter[bool]) error {
	if isNull {
		return nil
	}
	return FillAggMinBool(exec, value, getter, setter)
}
func MergeAggMinBool(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[bool, bool],
	getter1, getter2 aggexec.AggGetter[bool], setter1 aggexec.AggSetter[bool]) error {
	if !getter2() {
		setter1(false)
	}
	return nil
}

func FillAggMinUuid(
	exec aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid],
	value types.Uuid, getter aggexec.AggGetter[types.Uuid], setter aggexec.AggSetter[types.Uuid]) error {
	a := exec.(*aggUuidMin)
	if a.isEmpty {
		a.isEmpty = false
		setter(value)
	} else {
		if value.Compare(getter()) < 0 {
			setter(value)
		}
	}
	return nil
}
func FillsAggMinUuid(
	exec aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid],
	value types.Uuid, isNull bool, count int, getter aggexec.AggGetter[types.Uuid], setter aggexec.AggSetter[types.Uuid]) error {
	if isNull {
		return nil
	}
	return FillAggMinUuid(exec, value, getter, setter)
}
func MergeAggMinUuid(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid],
	getter1, getter2 aggexec.AggGetter[types.Uuid], setter1 aggexec.AggSetter[types.Uuid]) error {
	a := exec1.(*aggUuidMin)
	b := exec2.(*aggUuidMin)
	if a.isEmpty && !b.isEmpty {
		a.isEmpty = false
		setter1(getter2())
	} else if !a.isEmpty && !b.isEmpty {
		if getter1().Compare(getter2()) > 0 {
			setter1(getter2())
		}
	}
	return nil
}

func FillAggMinBytes(
	exec aggexec.SingleAggFromVarRetVar,
	value []byte, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a := exec.(*aggBytesMin)
	if a.isEmpty {
		a.isEmpty = false
		return setter(value)
	}
	if bytes.Compare(value, getter()) < 0 {
		return setter(value)
	}
	return nil
}
func FillsAggMinBytes(
	exec aggexec.SingleAggFromVarRetVar,
	value []byte, isNull bool, count int, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	if isNull {
		return nil
	}
	return FillAggMinBytes(exec, value, getter, setter)
}
func MergeAggMinBytes(
	exec1, exec2 aggexec.SingleAggFromVarRetVar,
	getter1, getter2 aggexec.AggBytesGetter, setter1 aggexec.AggBytesSetter) error {
	a := exec1.(*aggBytesMin)
	b := exec2.(*aggBytesMin)
	if a.isEmpty && !b.isEmpty {
		a.isEmpty = false
		return setter1(getter2())
	} else if !a.isEmpty && !b.isEmpty {
		if bytes.Compare(getter1(), getter2()) > 0 {
			return setter1(getter2())
		}
	}
	return nil
}

func FillAggMinDecimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64],
	value types.Decimal64, getter aggexec.AggGetter[types.Decimal64], setter aggexec.AggSetter[types.Decimal64]) error {
	if value.Compare(getter()) < 0 {
		setter(value)
	}
	return nil
}
func FillsAggMinDecimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64],
	value types.Decimal64, isNull bool, count int, getter aggexec.AggGetter[types.Decimal64], setter aggexec.AggSetter[types.Decimal64]) error {
	if isNull {
		return nil
	}
	return FillAggMinDecimal64(exec, value, getter, setter)
}
func MergeAggMinDecimal64(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64],
	getter1, getter2 aggexec.AggGetter[types.Decimal64], setter1 aggexec.AggSetter[types.Decimal64]) error {
	if getter2().Compare(getter1()) < 0 {
		setter1(getter2())
	}
	return nil
}

func FillAggMinDecimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	value types.Decimal128, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	if value.Compare(getter()) < 0 {
		setter(value)
	}
	return nil
}
func FillsAggMinDecimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	value types.Decimal128, isNull bool, count int, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	if isNull {
		return nil
	}
	return FillAggMinDecimal128(exec, value, getter, setter)
}
func MergeAggMinDecimal128(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	getter1, getter2 aggexec.AggGetter[types.Decimal128], setter1 aggexec.AggSetter[types.Decimal128]) error {
	if getter2().Compare(getter1()) < 0 {
		setter1(getter2())
	}
	return nil
}
