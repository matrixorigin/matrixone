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

func RegisterAnyValue1(id int64) {
	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[uint8],
			FillAnyValue1[uint8], nil, FillsAnyValue1[uint8],
			MergeAnyValue1[uint8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[uint16],
			FillAnyValue1[uint16], nil, FillsAnyValue1[uint16],
			MergeAnyValue1[uint16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[uint32],
			FillAnyValue1[uint32], nil, FillsAnyValue1[uint32],
			MergeAnyValue1[uint32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[uint64],
			FillAnyValue1[uint64], nil, FillsAnyValue1[uint64],
			MergeAnyValue1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[int8],
			FillAnyValue1[int8], nil, FillsAnyValue1[int8],
			MergeAnyValue1[int8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[int16],
			FillAnyValue1[int16], nil, FillsAnyValue1[int16],
			MergeAnyValue1[int16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[int32],
			FillAnyValue1[int32], nil, FillsAnyValue1[int32],
			MergeAnyValue1[int32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[int64],
			FillAnyValue1[int64], nil, FillsAnyValue1[int64],
			MergeAnyValue1[int64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[float32],
			FillAnyValue1[float32], nil, FillsAnyValue1[float32],
			MergeAnyValue1[float32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[float64],
			FillAnyValue1[float64], nil, FillsAnyValue1[float64],
			MergeAnyValue1[float64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_date.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[types.Date],
			FillAnyValue1[types.Date], nil, FillsAnyValue1[types.Date],
			MergeAnyValue1[types.Date],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_datetime.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[types.Datetime],
			FillAnyValue1[types.Datetime], nil, FillsAnyValue1[types.Datetime],
			MergeAnyValue1[types.Datetime],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_timestamp.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[types.Timestamp],
			FillAnyValue1[types.Timestamp], nil, FillsAnyValue1[types.Timestamp],
			MergeAnyValue1[types.Timestamp],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_time.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[types.Time],
			FillAnyValue1[types.Time], nil, FillsAnyValue1[types.Time],
			MergeAnyValue1[types.Time],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[types.Decimal64],
			FillAnyValue1[types.Decimal64], nil, FillsAnyValue1[types.Decimal64],
			MergeAnyValue1[types.Decimal64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[types.Decimal128],
			FillAnyValue1[types.Decimal128], nil, FillsAnyValue1[types.Decimal128],
			MergeAnyValue1[types.Decimal128],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bool.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[bool],
			FillAnyValue1[bool], nil, FillsAnyValue1[bool],
			MergeAnyValue1[bool],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[uint64],
			FillAnyValue1[uint64], nil, FillsAnyValue1[uint64],
			MergeAnyValue1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uuid.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[types.Uuid],
			FillAnyValue1[types.Uuid], nil, FillsAnyValue1[types.Uuid],
			MergeAnyValue1[types.Uuid],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_Rowid.ToType(), AnyValueReturnType, false, true),
			newAggAnyValue[types.Rowid],
			FillAnyValue1[types.Rowid], nil, FillsAnyValue1[types.Rowid],
			MergeAnyValue1[types.Rowid],
			nil,
		))

	varlenTs := []types.T{types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_binary, types.T_varbinary}
	for _, t := range varlenTs {
		aggexec.RegisterSingleAggFromVarToVar(
			aggexec.MakeSingleAgg4RegisteredInfo(
				aggexec.MakeSingleColumnAggInformation(id, t.ToType(), AnyValueReturnType, false, true),
				newAggAnyBytesValue,
				FillAnyValue2, nil, FillsAnyValue2,
				MergeAnyValue2,
				nil,
			))
	}
}

func FillAnyValue1[from types.FixedSizeTExceptStrType](
	exec aggexec.SingleAggFromFixedRetFixed[from, from], value from, getter aggexec.AggGetter[from], setter aggexec.AggSetter[from]) error {
	a := exec.(*aggAnyValue[from])
	if !a.has {
		a.has = true
		setter(value)
	}
	return nil
}
func FillsAnyValue1[from types.FixedSizeTExceptStrType](
	exec aggexec.SingleAggFromFixedRetFixed[from, from],
	value from, isNull bool, count int, getter aggexec.AggGetter[from], setter aggexec.AggSetter[from]) error {
	if !isNull {
		a := exec.(*aggAnyValue[from])
		if !a.has {
			a.has = true
			setter(value)
		}
	}
	return nil
}
func MergeAnyValue1[from types.FixedSizeTExceptStrType](
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[from, from], getter1, getter2 aggexec.AggGetter[from], setter aggexec.AggSetter[from]) error {
	a1 := exec1.(*aggAnyValue[from])
	a2 := exec2.(*aggAnyValue[from])
	if !a1.has && a2.has {
		a1.has = true
		setter(getter2())
	}
	return nil
}

func FillAnyValue2(
	exec aggexec.SingleAggFromVarRetVar, value []byte, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a := exec.(*aggAnyBytesValue)
	if !a.has {
		a.has = true
		return setter(value)
	}
	return nil
}
func FillsAnyValue2(
	exec aggexec.SingleAggFromVarRetVar,
	value []byte, isNull bool, count int, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	if !isNull {
		a := exec.(*aggAnyBytesValue)
		if !a.has {
			a.has = true
			return setter(value)
		}
	}
	return nil
}
func MergeAnyValue2(
	exec1, exec2 aggexec.SingleAggFromVarRetVar, getter1, getter2 aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a1 := exec1.(*aggAnyBytesValue)
	a2 := exec2.(*aggAnyBytesValue)
	if !a1.has && a2.has {
		a1.has = true
		return setter(getter2())
	}
	return nil
}
