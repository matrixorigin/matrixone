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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"math"
)

func RegisterVarPop1(id int64) {
	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), VarPopReturnType, false, true),
			newAggVarPop[uint64],
			FillAggVarPop1[uint64], nil, FillsAggVarPop1[uint64],
			MergeAggVarPop1[uint64],
			FlushAggVarPop1[uint64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), VarPopReturnType, false, true),
			newAggVarPop[int8],
			FillAggVarPop1[int8], nil, FillsAggVarPop1[int8],
			MergeAggVarPop1[int8],
			FlushAggVarPop1[int8],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), VarPopReturnType, false, true),
			newAggVarPop[int16],
			FillAggVarPop1[int16], nil, FillsAggVarPop1[int16],
			MergeAggVarPop1[int16],
			FlushAggVarPop1[int16],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), VarPopReturnType, false, true),
			newAggVarPop[int32],
			FillAggVarPop1[int32], nil, FillsAggVarPop1[int32],
			MergeAggVarPop1[int32],
			FlushAggVarPop1[int32],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), VarPopReturnType, false, true),
			newAggVarPop[int64],
			FillAggVarPop1[int64], nil, FillsAggVarPop1[int64],
			MergeAggVarPop1[int64],
			FlushAggVarPop1[int64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), VarPopReturnType, false, true),
			newAggVarPop[uint8],
			FillAggVarPop1[uint8], nil, FillsAggVarPop1[uint8],
			MergeAggVarPop1[uint8],
			FlushAggVarPop1[uint8],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), VarPopReturnType, false, true),
			newAggVarPop[uint16],
			FillAggVarPop1[uint16], nil, FillsAggVarPop1[uint16],
			MergeAggVarPop1[uint16],
			FlushAggVarPop1[uint16],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), VarPopReturnType, false, true),
			newAggVarPop[uint32],
			FillAggVarPop1[uint32], nil, FillsAggVarPop1[uint32],
			MergeAggVarPop1[uint32],
			FlushAggVarPop1[uint32],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), VarPopReturnType, false, true),
			newAggVarPop[uint64],
			FillAggVarPop1[uint64], nil, FillsAggVarPop1[uint64],
			MergeAggVarPop1[uint64],
			FlushAggVarPop1[uint64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), VarPopReturnType, false, true),
			newAggVarPop[float32],
			FillAggVarPop1[float32], nil, FillsAggVarPop1[float32],
			MergeAggVarPop1[float32],
			FlushAggVarPop1[float32],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), VarPopReturnType, false, true),
			newAggVarPop[float64],
			FillAggVarPop1[float64], nil, FillsAggVarPop1[float64],
			MergeAggVarPop1[float64],
			FlushAggVarPop1[float64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), VarPopReturnType, false, true),
			newAggVarPopDecimal64,
			FillAggVarPop1Decimal64, nil, FillsAggVarPop1Decimal64,
			MergeAggVarPop1Decimal64,
			FlushAggVarPop1Decimal64,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), VarPopReturnType, false, true),
			newAggVarPopDecimal128,
			FillAggVarPop1Decimal128, nil, FillsAggVarPop1Decimal128,
			MergeAggVarPop1Decimal128,
			FlushAggVarPop1Decimal128,
		))
}

func FillAggVarPop1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, float64],
	value from, getter aggexec.AggGetter[float64], setter aggexec.AggSetter[float64]) error {
	a := exec.(*aggVarPop[from])
	a.sum += float64(value)
	a.count++
	setter(getter() + math.Pow(float64(value), 2))
	return nil
}
func FillsAggVarPop1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, float64],
	value from, isNull bool, count int, getter aggexec.AggGetter[float64], setter aggexec.AggSetter[float64]) error {
	if !isNull {
		a := exec.(*aggVarPop[from])
		a.sum += float64(value) * float64(count)
		a.count += int64(count)
		setter(getter() + math.Pow(float64(value), 2)*float64(count))
	}
	return nil
}
func MergeAggVarPop1[from numeric](
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[from, float64],
	getter1, getter2 aggexec.AggGetter[float64], setter1 aggexec.AggSetter[float64]) error {
	a1 := exec1.(*aggVarPop[from])
	a2 := exec2.(*aggVarPop[from])
	a1.sum += a2.sum
	a1.count += a2.count
	setter1(getter1() + getter2())
	return nil
}
func FlushAggVarPop1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, float64],
	getter aggexec.AggGetter[float64], setter aggexec.AggSetter[float64]) error {
	a := exec.(*aggVarPop[from])
	if a.count <= 1 {
		setter(0)
	} else {
		avg := a.sum / float64(a.count)
		setter(getter()/float64(a.count) - math.Pow(avg, 2))
	}
	return nil
}

func FillAggVarPop1Decimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	value types.Decimal128, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	a := exec.(*aggVarPopDecimal128)
	if !a.power2OutOfRange {
		a.count++
		newSum, newPow2, outOfRange := getNewValueSumAndNewPower2(a.sum, getter(), value, a.argScale, 1)
		if !outOfRange {
			a.sum = newSum
			setter(newPow2)
			return nil
		}
		a.power2OutOfRange = true
		return nil
	}
	return moerr.NewInternalErrorNoCtx("Decimal128 overflowed")
}
func FillsAggVarPop1Decimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	value types.Decimal128, isNull bool, count int, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	if !isNull {
		a := exec.(*aggVarPopDecimal128)
		if !a.power2OutOfRange {
			a.count += int64(count)

			newSum, newPow2, outOfRange := getNewValueSumAndNewPower2(a.sum, getter(), value, a.argScale, count)
			if !outOfRange {
				a.sum = newSum
				setter(newPow2)
				return nil
			}
			if a.count == 1 {
				a.power2OutOfRange = true
				return nil
			}
		}
		return moerr.NewInternalErrorNoCtx("Decimal128 overflowed")
	}
	return nil
}
func MergeAggVarPop1Decimal128(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	getter1, getter2 aggexec.AggGetter[types.Decimal128], setter1 aggexec.AggSetter[types.Decimal128]) error {
	a1 := exec1.(*aggVarPopDecimal128)
	a2 := exec2.(*aggVarPopDecimal128)
	if a2.count == 0 {
		return nil
	}
	if a1.count == 0 {
		a1.count = a2.count
		a1.sum = a2.sum
		a1.power2OutOfRange = a2.power2OutOfRange
		setter1(getter2())
		return nil
	}

	if a1.power2OutOfRange || a2.power2OutOfRange {
		return moerr.NewInternalErrorNoCtx("Decimal128 overflowed")
	}
	a1.count += a2.count
	var err error
	var newPow2 types.Decimal128
	a1.sum, err = a1.sum.Add128(a2.sum)
	if err == nil {
		newPow2, err = getter1().Add128(getter2())
	}
	if err != nil {
		if a1.count == 1 {
			a1.power2OutOfRange = true
			return nil
		}
		return moerr.NewInternalErrorNoCtx("Decimal128 overflowed")
	}

	setter1(newPow2)
	return nil
}
func FlushAggVarPop1Decimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	a := exec.(*aggVarPopDecimal128)
	r, err := getVarianceFromSumPowCount(a.sum, getter(), a.count, a.argScale)
	if err != nil {
		return err
	}
	setter(r)
	return nil
}

func FillAggVarPop1Decimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128],
	value types.Decimal64, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	a := exec.(*aggVarPopDecimal64)
	return FillAggVarPop1Decimal128(&a.aggVarPopDecimal128, aggexec.FromD64ToD128(value), getter, setter)
}
func FillsAggVarPop1Decimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128],
	value types.Decimal64, isNull bool, count int, getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	return FillsAggVarPop1Decimal128(&exec.(*aggVarPopDecimal64).aggVarPopDecimal128, aggexec.FromD64ToD128(value), isNull, count, getter, setter)
}
func MergeAggVarPop1Decimal64(
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128],
	getter1, getter2 aggexec.AggGetter[types.Decimal128], setter1 aggexec.AggSetter[types.Decimal128]) error {
	return MergeAggVarPop1Decimal128(&exec1.(*aggVarPopDecimal64).aggVarPopDecimal128, &exec2.(*aggVarPopDecimal64).aggVarPopDecimal128, getter1, getter2, setter1)
}
func FlushAggVarPop1Decimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128],
	getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	return FlushAggVarPop1Decimal128(&exec.(*aggVarPopDecimal64).aggVarPopDecimal128, getter, setter)
}
