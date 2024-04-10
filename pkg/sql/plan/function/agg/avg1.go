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

func RegisterAvg1(id int64) {
	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), AvgReturnType, false, true),
			newAggAvg[uint64],
			FillAggAvg1[uint64], nil, FillsAggAvg1[uint64],
			MergeAggAvg1[uint64],
			FlushAggAvg1[uint64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), AvgReturnType, false, true),
			newAggAvg[int8],
			FillAggAvg1[int8], nil, FillsAggAvg1[int8],
			MergeAggAvg1[int8],
			FlushAggAvg1[int8],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), AvgReturnType, false, true),
			newAggAvg[int16],
			FillAggAvg1[int16], nil, FillsAggAvg1[int16],
			MergeAggAvg1[int16],
			FlushAggAvg1[int16],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), AvgReturnType, false, true),
			newAggAvg[int32],
			FillAggAvg1[int32], nil, FillsAggAvg1[int32],
			MergeAggAvg1[int32],
			FlushAggAvg1[int32],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), AvgReturnType, false, true),
			newAggAvg[int64],
			FillAggAvg1[int64], nil, FillsAggAvg1[int64],
			MergeAggAvg1[int64],
			FlushAggAvg1[int64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), AvgReturnType, false, true),
			newAggAvg[uint8],
			FillAggAvg1[uint8], nil, FillsAggAvg1[uint8],
			MergeAggAvg1[uint8],
			FlushAggAvg1[uint8],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), AvgReturnType, false, true),
			newAggAvg[uint16],
			FillAggAvg1[uint16], nil, FillsAggAvg1[uint16],
			MergeAggAvg1[uint16],
			FlushAggAvg1[uint16],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), AvgReturnType, false, true),
			newAggAvg[uint32],
			FillAggAvg1[uint32], nil, FillsAggAvg1[uint32],
			MergeAggAvg1[uint32],
			FlushAggAvg1[uint32],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), AvgReturnType, false, true),
			newAggAvg[uint64],
			FillAggAvg1[uint64], nil, FillsAggAvg1[uint64],
			MergeAggAvg1[uint64],
			FlushAggAvg1[uint64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), AvgReturnType, false, true),
			newAggAvg[float32],
			FillAggAvg1[float32], nil, FillsAggAvg1[float32],
			MergeAggAvg1[float32],
			FlushAggAvg1[float32],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), AvgReturnType, false, true),
			newAggAvg[float64],
			FillAggAvg1[float64], nil, FillsAggAvg1[float64],
			MergeAggAvg1[float64],
			FlushAggAvg1[float64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), AvgReturnType, false, true),
			newAggAvgDecimal64,
			FillAggAvgDecimal64, nil, FillsAggAvgDecimal64,
			MergeAggAvgDecimal64,
			FlushAggAvgDecimal64,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), AvgReturnType, false, true),
			newAggAvgDecimal128,
			FillAggAvgDecimal128, nil, FillsAggAvgDecimal128,
			MergeAggAvgDecimal128,
			FlushAggAvgDecimal128,
		))
}

func FillAggAvg1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, float64], value from, getter aggexec.AggGetter[float64], setter aggexec.AggSetter[float64]) error {
	a := exec.(*aggAvg[from])
	a.count++
	setter(getter() + float64(value))
	return nil
}
func FillsAggAvg1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, float64], value from, isNull bool, count int, getter aggexec.AggGetter[float64], setter aggexec.AggSetter[float64]) error {
	if !isNull {
		a := exec.(*aggAvg[from])
		a.count += int64(count)
		setter(getter() + float64(value)*float64(count))
	}
	return nil
}
func MergeAggAvg1[from numeric](
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[from, float64], getter1, getter2 aggexec.AggGetter[float64], setter aggexec.AggSetter[float64]) error {
	a1 := exec1.(*aggAvg[from])
	a2 := exec2.(*aggAvg[from])
	a1.count += a2.count
	setter(getter1() + getter2())
	return nil
}
func FlushAggAvg1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, float64], getter aggexec.AggGetter[float64], setter aggexec.AggSetter[float64]) error {
	a := exec.(*aggAvg[from])
	if a.count == 0 {
		setter(0)
	} else {
		setter(getter() / float64(a.count))
	}
	return nil
}

func FillAggAvgDecimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128], value types.Decimal64, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	a := exec.(*aggAvgDecimal64)
	a.count++
	r, err := getter().Add64(value)
	if err == nil {
		setter(r)
	}
	return err
}
func FillsAggAvgDecimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128], value types.Decimal64, isNull bool, count int, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	if !isNull {
		a := exec.(*aggAvgDecimal64)
		v := types.Decimal128{B0_63: uint64(value), B64_127: 0}
		if value.Sign() {
			v.B64_127 = ^v.B64_127
		}
		r, _, err := v.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, a.argScale, 0)
		if err != nil {
			return err
		}
		if r, err = getter().Add128(r); err != nil {
			return err
		}
		setter(r)
		a.count += int64(count)
	}
	return nil
}
func MergeAggAvgDecimal64(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128], getter1, getter2 aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	a1 := exec1.(*aggAvgDecimal64)
	a2 := exec2.(*aggAvgDecimal64)
	r, err := getter1().Add128(getter2())
	if err == nil {
		setter(r)
	}
	a1.count += a2.count
	return err
}
func FlushAggAvgDecimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128], getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	a := exec.(*aggAvgDecimal64)
	if a.count == 0 {
		setter(types.Decimal128{B0_63: 0, B64_127: 0})
	} else {
		v, _, err := getter().Div(types.Decimal128{B0_63: uint64(a.count), B64_127: 0}, a.argScale, 0)
		if err == nil {
			setter(v)
		}
	}
	return nil
}

func FillAggAvgDecimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], value types.Decimal128, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	a := exec.(*aggAvgDecimal128)
	a.count++
	r, err := getter().Add128(value)
	if err == nil {
		setter(r)
	}
	return err
}
func FillsAggAvgDecimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], value types.Decimal128, isNull bool, count int, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	if !isNull {
		a := exec.(*aggAvgDecimal128)
		r, _, err := value.Mul(types.Decimal128{B0_63: uint64(count), B64_127: 0}, a.argScale, 0)
		if err != nil {
			return err
		}
		if r, err = getter().Add128(r); err != nil {
			return err
		}
		setter(r)
		a.count += int64(count)
	}
	return nil
}
func MergeAggAvgDecimal128(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], getter1, getter2 aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	a1 := exec1.(*aggAvgDecimal128)
	a2 := exec2.(*aggAvgDecimal128)
	r, err := getter1().Add128(getter2())
	if err == nil {
		setter(r)
	}
	a1.count += a2.count
	return err
}
func FlushAggAvgDecimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	a := exec.(*aggAvgDecimal128)
	if a.count == 0 {
		setter(types.Decimal128{B0_63: 0, B64_127: 0})
		return nil
	}
	v, _, err := getter().Div(types.Decimal128{B0_63: uint64(a.count), B64_127: 0}, a.argScale, 0)
	if err == nil {
		setter(v)
	}
	return err
}
