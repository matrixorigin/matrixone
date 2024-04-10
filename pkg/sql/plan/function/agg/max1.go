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

func RegisterMax1(id int64) {
	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), MaxReturnType, false, true),
			newAggMax[uint8],
			FillAggMax1[uint8], nil, FillsAggMax1[uint8],
			MergeAggMax1[uint8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), MaxReturnType, false, true),
			newAggMax[uint16],
			FillAggMax1[uint16], nil, FillsAggMax1[uint16],
			MergeAggMax1[uint16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), MaxReturnType, false, true),
			newAggMax[uint32],
			FillAggMax1[uint32], nil, FillsAggMax1[uint32],
			MergeAggMax1[uint32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), MaxReturnType, false, true),
			newAggMax[uint64],
			FillAggMax1[uint64], nil, FillsAggMax1[uint64],
			MergeAggMax1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), MaxReturnType, false, true),
			newAggMax[int8],
			FillAggMax1[int8], nil, FillsAggMax1[int8],
			MergeAggMax1[int8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), MaxReturnType, false, true),
			newAggMax[int16],
			FillAggMax1[int16], nil, FillsAggMax1[int16],
			MergeAggMax1[int16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), MaxReturnType, false, true),
			newAggMax[int32],
			FillAggMax1[int32], nil, FillsAggMax1[int32],
			MergeAggMax1[int32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), MaxReturnType, false, true),
			newAggMax[int64],
			FillAggMax1[int64], nil, FillsAggMax1[int64],
			MergeAggMax1[int64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), MaxReturnType, false, true),
			newAggMax[float32],
			FillAggMax1[float32], nil, FillsAggMax1[float32],
			MergeAggMax1[float32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), MaxReturnType, false, true),
			newAggMax[float64],
			FillAggMax1[float64], nil, FillsAggMax1[float64],
			MergeAggMax1[float64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_date.ToType(), MaxReturnType, false, true),
			newAggMax[types.Date],
			FillAggMax1[types.Date], nil, FillsAggMax1[types.Date],
			MergeAggMax1[types.Date],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_datetime.ToType(), MaxReturnType, false, true),
			newAggMax[types.Datetime],
			FillAggMax1[types.Datetime], nil, FillsAggMax1[types.Datetime],
			MergeAggMax1[types.Datetime],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_timestamp.ToType(), MaxReturnType, false, true),
			newAggMax[types.Timestamp],
			FillAggMax1[types.Timestamp], nil, FillsAggMax1[types.Timestamp],
			MergeAggMax1[types.Timestamp],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_time.ToType(), MaxReturnType, false, true),
			newAggMax[types.Time],
			FillAggMax1[types.Time], nil, FillsAggMax1[types.Time],
			MergeAggMax1[types.Time],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), MaxReturnType, false, true),
			newAggMax[uint64],
			FillAggMax1[uint64], nil, FillsAggMax1[uint64],
			MergeAggMax1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bool.ToType(), MaxReturnType, false, true),
			newAggMaxBool,
			FillAggMaxBool, nil, FillsAggMaxBool,
			MergeAggMaxBool,
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uuid.ToType(), MaxReturnType, false, true),
			newAggUuidMax,
			FillAggMaxUuid, nil, FillsAggMaxUuid,
			MergeAggMaxUuid,
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), MaxReturnType, false, true),
			newAggMaxDecimal64,
			FillAggMaxDecimal64, nil, FillsAggMaxDecimal64,
			MergeAggMaxDecimal64,
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), MaxReturnType, false, true),
			newAggMaxDecimal128,
			FillAggMaxDecimal128, nil, FillsAggMaxDecimal128,
			MergeAggMaxDecimal128,
			nil,
		))

	varlenList := []types.T{types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_binary, types.T_varbinary}
	for _, t := range varlenList {
		aggexec.RegisterSingleAggFromVarToVar(
			aggexec.MakeSingleAgg4RegisteredInfo(
				aggexec.MakeSingleColumnAggInformation(id, t.ToType(), MaxReturnType, false, true),
				newAggBytesMax,
				FillAggMaxBytes, nil, FillsAggMaxBytes,
				MergeAggMaxBytes,
				nil,
			))
	}
}

func FillAggMax1[from canCompare](
	exec aggexec.SingleAggFromFixedRetFixed[from, from],
	value from, getter aggexec.AggGetter[from], setter aggexec.AggSetter[from]) error {
	if value > getter() {
		setter(value)
	}
	return nil
}
func FillsAggMax1[from canCompare](
	exec aggexec.SingleAggFromFixedRetFixed[from, from],
	value from, isNull bool, count int, getter aggexec.AggGetter[from], setter aggexec.AggSetter[from]) error {
	if isNull {
		return nil
	}
	return FillAggMax1(exec, value, getter, setter)
}
func MergeAggMax1[from canCompare](
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[from, from],
	getter1, getter2 aggexec.AggGetter[from], setter1 aggexec.AggSetter[from]) error {
	if getter2() > getter1() {
		setter1(getter2())
	}
	return nil
}

func FillAggMaxBool(
	exec aggexec.SingleAggFromFixedRetFixed[bool, bool],
	value bool, getter aggexec.AggGetter[bool], setter aggexec.AggSetter[bool]) error {
	if value {
		setter(true)
	}
	return nil
}
func FillsAggMaxBool(
	exec aggexec.SingleAggFromFixedRetFixed[bool, bool],
	value bool, isNull bool, count int, getter aggexec.AggGetter[bool], setter aggexec.AggSetter[bool]) error {
	if isNull {
		return nil
	}
	return FillAggMaxBool(exec, value, getter, setter)
}
func MergeAggMaxBool(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[bool, bool],
	getter1, getter2 aggexec.AggGetter[bool], setter1 aggexec.AggSetter[bool]) error {
	if getter2() {
		setter1(true)
	}
	return nil
}

func FillAggMaxUuid(
	exec aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid],
	value types.Uuid, getter aggexec.AggGetter[types.Uuid], setter aggexec.AggSetter[types.Uuid]) error {
	a := exec.(*aggUuidMax)
	if a.isEmpty {
		a.isEmpty = false
		setter(value)
	} else {
		if value.Compare(getter()) > 0 {
			setter(value)
		}
	}
	return nil
}
func FillsAggMaxUuid(
	exec aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid],
	value types.Uuid, isNull bool, count int, getter aggexec.AggGetter[types.Uuid], setter aggexec.AggSetter[types.Uuid]) error {
	if isNull {
		return nil
	}
	return FillAggMaxUuid(exec, value, getter, setter)
}
func MergeAggMaxUuid(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Uuid, types.Uuid],
	getter1, getter2 aggexec.AggGetter[types.Uuid], setter1 aggexec.AggSetter[types.Uuid]) error {
	a := exec1.(*aggUuidMax)
	b := exec2.(*aggUuidMax)
	if a.isEmpty && !b.isEmpty {
		a.isEmpty = false
		setter1(getter2())
	} else if !a.isEmpty && !b.isEmpty {
		if getter1().Compare(getter2()) < 0 {
			setter1(getter2())
		}
	}
	return nil
}

func FillAggMaxBytes(
	exec aggexec.SingleAggFromVarRetVar,
	value []byte, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a := exec.(*aggBytesMax)
	if a.isEmpty {
		a.isEmpty = false
		return setter(value)
	}
	if bytes.Compare(value, getter()) > 0 {
		return setter(value)
	}
	return nil
}
func FillsAggMaxBytes(
	exec aggexec.SingleAggFromVarRetVar,
	value []byte, isNull bool, count int, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	if isNull {
		return nil
	}
	return FillAggMaxBytes(exec, value, getter, setter)
}
func MergeAggMaxBytes(
	exec1, exec2 aggexec.SingleAggFromVarRetVar,
	getter1, getter2 aggexec.AggBytesGetter, setter1 aggexec.AggBytesSetter) error {
	a := exec1.(*aggBytesMax)
	b := exec2.(*aggBytesMax)
	if a.isEmpty && !b.isEmpty {
		a.isEmpty = false
		return setter1(getter2())
	} else if !a.isEmpty && !b.isEmpty {
		if bytes.Compare(getter1(), getter2()) < 0 {
			return setter1(getter2())
		}
	}
	return nil
}

func FillAggMaxDecimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64],
	value types.Decimal64, getter aggexec.AggGetter[types.Decimal64], setter aggexec.AggSetter[types.Decimal64]) error {
	if value.Compare(getter()) > 0 {
		setter(value)
	}
	return nil
}
func FillsAggMaxDecimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64],
	value types.Decimal64, isNull bool, count int, getter aggexec.AggGetter[types.Decimal64], setter aggexec.AggSetter[types.Decimal64]) error {
	if isNull {
		return nil
	}
	return FillAggMaxDecimal64(exec, value, getter, setter)
}
func MergeAggMaxDecimal64(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64],
	getter1, getter2 aggexec.AggGetter[types.Decimal64], setter1 aggexec.AggSetter[types.Decimal64]) error {
	if getter2().Compare(getter1()) > 0 {
		setter1(getter2())
	}
	return nil
}

func FillAggMaxDecimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	value types.Decimal128, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	if value.Compare(getter()) > 0 {
		setter(value)
	}
	return nil
}
func FillsAggMaxDecimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	value types.Decimal128, isNull bool, count int, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	if isNull {
		return nil
	}
	return FillAggMaxDecimal128(exec, value, getter, setter)
}
func MergeAggMaxDecimal128(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	getter1, getter2 aggexec.AggGetter[types.Decimal128], setter1 aggexec.AggSetter[types.Decimal128]) error {
	if getter2().Compare(getter1()) > 0 {
		setter1(getter2())
	}
	return nil
}
