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
	"math"
)

func RegisterMax1(id int64) {
	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), MaxReturnType, false, true),
			newAggMax[uint8],
			InitAggMax1[uint8],
			FillAggMax1[uint8], nil, FillsAggMax1[uint8],
			MergeAggMax1[uint8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), MaxReturnType, false, true),
			newAggMax[uint16],
			InitAggMax1[uint16],
			FillAggMax1[uint16], nil, FillsAggMax1[uint16],
			MergeAggMax1[uint16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), MaxReturnType, false, true),
			newAggMax[uint32],
			InitAggMax1[uint32],
			FillAggMax1[uint32], nil, FillsAggMax1[uint32],
			MergeAggMax1[uint32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), MaxReturnType, false, true),
			newAggMax[uint64],
			InitAggMax1[uint64],
			FillAggMax1[uint64], nil, FillsAggMax1[uint64],
			MergeAggMax1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), MaxReturnType, false, true),
			newAggMax[int8],
			InitAggMax1[int8],
			FillAggMax1[int8], nil, FillsAggMax1[int8],
			MergeAggMax1[int8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), MaxReturnType, false, true),
			newAggMax[int16],
			InitAggMax1[int16],
			FillAggMax1[int16], nil, FillsAggMax1[int16],
			MergeAggMax1[int16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), MaxReturnType, false, true),
			newAggMax[int32],
			InitAggMax1[int32],
			FillAggMax1[int32], nil, FillsAggMax1[int32],
			MergeAggMax1[int32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), MaxReturnType, false, true),
			newAggMax[int64],
			InitAggMax1[int64],
			FillAggMax1[int64], nil, FillsAggMax1[int64],
			MergeAggMax1[int64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), MaxReturnType, false, true),
			newAggMax[float32],
			InitAggMax1[float32],
			FillAggMax1[float32], nil, FillsAggMax1[float32],
			MergeAggMax1[float32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), MaxReturnType, false, true),
			newAggMax[float64],
			InitAggMax1[float64],
			FillAggMax1[float64], nil, FillsAggMax1[float64],
			MergeAggMax1[float64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_date.ToType(), MaxReturnType, false, true),
			newAggMax[types.Date],
			InitAggMax1[types.Date],
			FillAggMax1[types.Date], nil, FillsAggMax1[types.Date],
			MergeAggMax1[types.Date],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_datetime.ToType(), MaxReturnType, false, true),
			newAggMax[types.Datetime],
			InitAggMax1[types.Datetime],
			FillAggMax1[types.Datetime], nil, FillsAggMax1[types.Datetime],
			MergeAggMax1[types.Datetime],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_timestamp.ToType(), MaxReturnType, false, true),
			newAggMax[types.Timestamp],
			InitAggMax1[types.Timestamp],
			FillAggMax1[types.Timestamp], nil, FillsAggMax1[types.Timestamp],
			MergeAggMax1[types.Timestamp],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_time.ToType(), MaxReturnType, false, true),
			newAggMax[types.Time],
			InitAggMax1[types.Time],
			FillAggMax1[types.Time], nil, FillsAggMax1[types.Time],
			MergeAggMax1[types.Time],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), MaxReturnType, false, true),
			newAggMax[uint64],
			InitAggMax1[uint64],
			FillAggMax1[uint64], nil, FillsAggMax1[uint64],
			MergeAggMax1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bool.ToType(), MaxReturnType, false, true),
			newAggMaxBool,
			InitAggMaxBool,
			FillAggMaxBool, nil, FillsAggMaxBool,
			MergeAggMaxBool,
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uuid.ToType(), MaxReturnType, false, true),
			aggexec.GenerateFlagContextFromFixedToFixed[types.Uuid, types.Uuid],
			aggexec.InitFlagContextFromFixedToFixed[types.Uuid, types.Uuid],
			FillAggMaxUuid, nil, FillsAggMaxUuid,
			MergeAggMaxUuid,
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), MaxReturnType, false, true),
			newAggMaxDecimal64,
			InitAggMaxDecimal64,
			FillAggMaxDecimal64, nil, FillsAggMaxDecimal64,
			MergeAggMaxDecimal64,
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), MaxReturnType, false, true),
			newAggMaxDecimal128,
			InitAggMaxDecimal128,
			FillAggMaxDecimal128, nil, FillsAggMaxDecimal128,
			MergeAggMaxDecimal128,
			nil,
		))

	varlenList := []types.T{types.T_varchar, types.T_char, types.T_blob, types.T_text, types.T_binary, types.T_varbinary}
	for _, t := range varlenList {
		aggexec.RegisterSingleAggFromVarToVar(
			aggexec.MakeSingleAgg4RegisteredInfo(
				aggexec.MakeSingleColumnAggInformation(id, t.ToType(), MaxReturnType, false, true),
				aggexec.GenerateFlagContextFromVarToVar,
				FillAggMaxBytes, nil, FillsAggMaxBytes,
				MergeAggMaxBytes,
				nil,
			))
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
	types.T_varchar, types.T_char, types.T_blob, types.T_text,
	types.T_uuid,
	types.T_binary, types.T_varbinary,
}

func MaxReturnType(typs []types.Type) types.Type {
	return typs[0]
}

type aggMax[from canCompare] struct{}

func newAggMax[from canCompare]() aggexec.SingleAggFromFixedRetFixed[from, from] {
	return aggMax[from]{}
}
func (a aggMax[from]) Marshal() []byte  { return nil }
func (a aggMax[from]) Unmarshal([]byte) {}

var fromTypeIDtoMinValue = map[types.T]interface{}{
	types.T_bit:       uint64(0),
	types.T_uint8:     uint8(0),
	types.T_uint16:    uint16(0),
	types.T_uint32:    uint32(0),
	types.T_uint64:    uint64(0),
	types.T_int8:      int8(math.MinInt8),
	types.T_int16:     int16(math.MinInt16),
	types.T_int32:     int32(math.MinInt32),
	types.T_int64:     int64(math.MinInt64),
	types.T_float32:   float32(-math.MaxFloat32),
	types.T_float64:   -math.MaxFloat64,
	types.T_date:      types.Date(0),
	types.T_datetime:  types.Datetime(0),
	types.T_timestamp: types.Timestamp(0),
	types.T_time:      types.Time(0),
}

func InitAggMax1[from canCompare](
	exec aggexec.SingleAggFromFixedRetFixed[from, from], setter aggexec.AggSetter[from], arg, ret types.Type) error {
	setter(fromTypeIDtoMinValue[arg.Oid].(from))
	return nil
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

type aggMaxBool struct{}

func newAggMaxBool() aggexec.SingleAggFromFixedRetFixed[bool, bool] {
	return aggMaxBool{}
}

func (a aggMaxBool) Marshal() []byte     { return nil }
func (a aggMaxBool) Unmarshal(bs []byte) {}
func (a aggMaxBool) Init(setter aggexec.AggSetter[bool], arg types.Type, ret types.Type) error {
	setter(false)
	return nil
}

func InitAggMaxBool(exec aggexec.SingleAggFromFixedRetFixed[bool, bool], setter aggexec.AggSetter[bool], arg, ret types.Type) error {
	setter(false)
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
	a := exec.(*aggexec.ContextWithEmptyFlagOfSingleAggRetFixed[types.Uuid])
	if a.IsEmpty {
		a.IsEmpty = false
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
	a := exec1.(*aggexec.ContextWithEmptyFlagOfSingleAggRetFixed[types.Uuid])
	b := exec2.(*aggexec.ContextWithEmptyFlagOfSingleAggRetFixed[types.Uuid])
	if a.IsEmpty && !b.IsEmpty {
		a.IsEmpty = false
		setter1(getter2())
	} else if !a.IsEmpty && !b.IsEmpty {
		if getter1().Compare(getter2()) < 0 {
			setter1(getter2())
		}
	}
	return nil
}

func FillAggMaxBytes(
	exec aggexec.SingleAggFromVarRetVar,
	value []byte, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a := exec.(*aggexec.ContextWithEmptyFlagOfSingleAggRetBytes)
	if a.IsEmpty {
		a.IsEmpty = false
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
	a := exec1.(*aggexec.ContextWithEmptyFlagOfSingleAggRetBytes)
	b := exec2.(*aggexec.ContextWithEmptyFlagOfSingleAggRetBytes)
	if a.IsEmpty && !b.IsEmpty {
		a.IsEmpty = false
		return setter1(getter2())
	} else if !a.IsEmpty && !b.IsEmpty {
		if bytes.Compare(getter1(), getter2()) < 0 {
			return setter1(getter2())
		}
	}
	return nil
}

type aggMaxDecimal64 struct{}

func newAggMaxDecimal64() aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64] {
	return aggMaxDecimal64{}
}

func (a aggMaxDecimal64) Marshal() []byte  { return nil }
func (a aggMaxDecimal64) Unmarshal([]byte) {}

func InitAggMaxDecimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal64], setter aggexec.AggSetter[types.Decimal64], arg, ret types.Type) error {
	setter(types.Decimal64Min)
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

type aggMaxDecimal128 struct{}

func newAggMaxDecimal128() aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128] {
	return aggMaxDecimal128{}
}

func (a aggMaxDecimal128) Marshal() []byte  { return nil }
func (a aggMaxDecimal128) Unmarshal([]byte) {}

func InitAggMaxDecimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], setter aggexec.AggSetter[types.Decimal128], arg, ret types.Type) error {
	setter(types.Decimal128Min)
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
