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

package agg2

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"math"
)

func RegisterVarPop(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bit.ToType(), types.T_float64.ToType(), false, true), newAggVarPop[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_float64.ToType(), false, true), newAggVarPop[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_float64.ToType(), false, true), newAggAvg[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_float64.ToType(), false, true), newAggAvg[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_float64.ToType(), false, true), newAggAvg[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_float64.ToType(), false, true), newAggAvg[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_float64.ToType(), false, true), newAggAvg[float64])
	aggexec.RegisterFlexibleSingleAgg(
		aggexec.MakeFlexibleAggInfo(id, false, true),
		func(t []types.Type) types.Type {
			if t[0].IsDecimal() {
				if t[0].Scale > 12 {
					return types.New(types.T_decimal128, 38, t[0].Scale)
				}
				return types.New(types.T_decimal128, 38, 12)
			}
			panic("unexpected type for var_pop()")
		},
		func(args []types.Type, ret types.Type) any {
			switch args[0].Oid {
			case types.T_decimal64:
				return newAggVarPopDecimal64
			case types.T_decimal128:
				return newAggVarPopDecimal128
			default:
				panic("unexpected type for var_pop()")
			}
		})
}

// variance = E(X^2) - (E(X))^2
// and we use the result vector to store the sum of X^2.
type aggVarPop[T numeric] struct {
	sum   float64
	count int64
}

func newAggVarPop[T numeric](t types.Type) aggexec.SingleAggFromFixedRetFixed[T, float64] {
	return &aggVarPop[T]{}
}

func (a *aggVarPop[T]) Marshal() []byte {
	bs := types.EncodeFloat64(&a.sum)
	bs = append(bs, types.EncodeInt64(&a.count)...)
	return bs
}
func (a *aggVarPop[T]) Unmarshal(bs []byte) {
	a.sum = types.DecodeFloat64(bs[:8])
	a.count = types.DecodeInt64(bs[8:])
}
func (a *aggVarPop[T]) Init(set aggexec.AggSetter[float64], arg, ret types.Type) {
	a.sum = 0
	a.count = 0
	set(0)
}
func (a *aggVarPop[T]) Fill(value T, get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	a.sum += float64(value)
	a.count++
	set(get() + math.Pow(float64(value), 2))
}
func (a *aggVarPop[T]) FillNull(get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {}
func (a *aggVarPop[T]) Fills(value T, isNull bool, count int, get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	if !isNull {
		a.sum += float64(value) * float64(count)
		a.count += int64(count)
		set(get() + math.Pow(float64(value), 2)*float64(count))
	}
}
func (a *aggVarPop[T]) Merge(other aggexec.SingleAggFromFixedRetFixed[T, float64], get1, get2 aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	next := other.(*aggVarPop[T])
	a.sum += next.sum
	a.count += next.count
	set(get1() + get2())
}
func (a *aggVarPop[T]) Flush(get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	if a.count == 0 {
		set(0)
		return
	}
	avg := a.sum / float64(a.count)
	set(get()/float64(a.count) - math.Pow(avg, 2))
}

type aggVarPopDecimal128 struct {
	sum   types.Decimal128
	count int64
	scale int32
	// if true, any middle result is out of range
	power2OutOfRange bool
}

func newAggVarPopDecimal128() aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128] {
	return &aggVarPopDecimal128{}
}

func (a *aggVarPopDecimal128) Marshal() []byte {
	bs := types.EncodeInt64(&a.count)
	bs = append(bs, types.EncodeBool(&a.power2OutOfRange)...)
	bs = append(bs, types.EncodeInt32(&a.scale)...)
	bs = append(bs, types.EncodeDecimal128(&a.sum)...)
	return bs
}
func (a *aggVarPopDecimal128) Unmarshal(bs []byte) {
	a.count = types.DecodeInt64(bs[:8])
	a.power2OutOfRange = types.DecodeBool(bs[8:9])
	a.scale = types.DecodeInt32(bs[9:13])
	a.sum = types.DecodeDecimal128(bs[13:])
}
func (a *aggVarPopDecimal128) Init(set aggexec.AggSetter[types.Decimal128], arg, ret types.Type) {
	a.sum = types.Decimal128{B0_63: 0, B64_127: 0}
	a.count = 0
	a.power2OutOfRange = false
	a.scale = ret.Scale
	set(a.sum)
}
func (a *aggVarPopDecimal128) Fill(value types.Decimal128, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if !a.power2OutOfRange {
		a.count++
		newSum, newPow2, outOfRange := getNewValueSumAndNewPower2(a.sum, get(), value, 1)
		if !outOfRange {
			a.sum = newSum
			set(newPow2)
			return
		}
		a.power2OutOfRange = true
	}
	err := moerr.NewInternalErrorNoCtx("agg: out of range")
	panic(err)
}
func (a *aggVarPopDecimal128) FillNull(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
}
func (a *aggVarPopDecimal128) Fills(value types.Decimal128, isNull bool, count int, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	if !isNull {
		if !a.power2OutOfRange {
			a.count += int64(count)

			newSum, newPow2, outOfRange := getNewValueSumAndNewPower2(a.sum, get(), value, count)
			if !outOfRange {
				a.sum = newSum
				set(newPow2)
				return
			}
			if a.count == 1 {
				a.power2OutOfRange = true
				return
			}
		}
		err := moerr.NewInternalErrorNoCtx("agg: out of range")
		panic(err)
	}
}
func (a *aggVarPopDecimal128) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128], get1, get2 aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	next := other.(*aggVarPopDecimal128)
	if next.count == 0 {
		return
	}
	if a.count == 0 {
		a.count = next.count
		a.sum = next.sum
		a.power2OutOfRange = next.power2OutOfRange
		set(get2())
		return
	}

	if a.power2OutOfRange || next.power2OutOfRange {
		err := moerr.NewInternalErrorNoCtx("agg: out of range")
		panic(err)
	}
	a.count += next.count
	var err error
	var newPow2 types.Decimal128
	a.sum, err = a.sum.Add128(next.sum)
	if err == nil {
		newPow2, err = get1().Add128(get2())
	}
	if err != nil {
		if a.count == 1 {
			a.power2OutOfRange = true
			return
		}
		err = moerr.NewInternalErrorNoCtx("agg: out of range")
		panic(err)
	}

	set(newPow2)
}
func (a *aggVarPopDecimal128) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	r, err := getVarianceFromSumPowCount(a.sum, get(), a.count)
	if err != nil {
		panic(err)
	}
	set(r)
}

type aggVarPopDecimal64 struct {
	aggVarPopDecimal128
}

func newAggVarPopDecimal64() aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128] {
	return &aggVarPopDecimal64{}
}

func (a *aggVarPopDecimal64) Fill(value types.Decimal64, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	a.aggVarPopDecimal128.Fill(aggexec.FromD64ToD128(value), get, set)
}

func (a *aggVarPopDecimal64) Fills(value types.Decimal64, isNull bool, count int, get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	a.aggVarPopDecimal128.Fills(aggexec.FromD64ToD128(value), isNull, count, get, set)
}

func (a *aggVarPopDecimal64) Merge(other aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128], get1, get2 aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	next := other.(*aggVarPopDecimal64)
	a.aggVarPopDecimal128.Merge(&next.aggVarPopDecimal128, get1, get2, set)
}

func getNewValueSumAndNewPower2(
	oldSum types.Decimal128, oldPow2 types.Decimal128, value types.Decimal128, count int) (
	newSum types.Decimal128, newPow2 types.Decimal128, outOfRange bool) {
	var err error

	valueMulCount := value
	if count > 1 {
		count128 := types.Decimal128{B0_63: uint64(count), B64_127: 0}
		valueMulCount, err = value.Mul128(count128)
		if err != nil {
			return oldSum, oldPow2, true
		}
	}

	newSum, err = oldSum.Add128(valueMulCount)
	if err != nil {
		return oldSum, oldPow2, true
	}

	newPow2, err = oldPow2.Mul128(value)
	if err != nil {
		return oldSum, oldPow2, true
	}

	newPow2, err = newPow2.Add128(newPow2)
	return newSum, newPow2, err != nil
}

func getVarianceFromSumPowCount(
	sum types.Decimal128, pow types.Decimal128, count int64) (types.Decimal128, error) {
	if count <= 1 {
		return types.Decimal128{B0_63: 0, B64_127: 0}, nil
	}
	avg, err := sum.Div128(types.Decimal128{B0_63: uint64(count), B64_127: 0})
	if err != nil {
		return types.Decimal128{B0_63: 0, B64_127: 0}, err
	}
	part1, err := pow.Div128(types.Decimal128{B0_63: uint64(count), B64_127: 0})
	if err != nil {
		return types.Decimal128{B0_63: 0, B64_127: 0}, err
	}
	part2, err := avg.Mul128(avg)
	if err != nil {
		return types.Decimal128{B0_63: 0, B64_127: 0}, err
	}
	return part1.Sub128(part2)
}
