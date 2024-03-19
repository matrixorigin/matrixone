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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"math"
)

func RegisterStdVarPop(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bit.ToType(), types.T_float64.ToType(), false, true), newAggStdVarPop[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_float64.ToType(), false, true), newAggStdVarPop[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_float64.ToType(), false, true), newAggStdVarPop[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_float64.ToType(), false, true), newAggStdVarPop[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_float64.ToType(), false, true), newAggStdVarPop[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_float64.ToType(), false, true), newAggStdVarPop[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_float64.ToType(), false, true), newAggStdVarPop[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_float64.ToType(), false, true), newAggStdVarPop[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_float64.ToType(), false, true), newAggStdVarPop[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_float64.ToType(), false, true), newAggStdVarPop[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_float64.ToType(), false, true), newAggStdVarPop[float64])
	aggexec.RegisterFlexibleSingleAgg(
		aggexec.MakeFlexibleAggInfo(id, false, true),
		func(t []types.Type) types.Type {
			if t[0].IsDecimal() {
				if t[0].Scale > 12 {
					return types.New(types.T_decimal128, 38, t[0].Scale)
				}
				return types.New(types.T_decimal128, 38, 12)
			}
			panic("unexpected type for stddev_pop()")
		},
		func(args []types.Type, ret types.Type) any {
			switch args[0].Oid {
			case types.T_decimal64:
				return newAggStdVarPopDecimal64
			case types.T_decimal128:
				return newAggStdVarPopDecimal128
			default:
				panic("unexpected type for stddev_pop()")
			}
		})
}

type aggStdVarPop[T numeric] struct {
	aggVarPop[T]
}

func newAggStdVarPop[T numeric]() aggexec.SingleAggFromFixedRetFixed[T, float64] {
	return &aggStdVarPop[T]{}
}

func (a *aggStdVarPop[T]) Flush(get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	if a.count == 0 {
		set(0)
		return
	}
	avg := a.sum / float64(a.count)
	variance := get()/float64(a.count) - math.Pow(avg, 2)
	set(math.Sqrt(variance))
}

type aggStdVarPopDecimal64 struct {
	aggVarPopDecimal64
}

func newAggStdVarPopDecimal64() aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128] {
	return &aggStdVarPopDecimal64{}
}

func (a *aggStdVarPopDecimal64) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	r, err := getVarianceFromSumPowCount(a.sum, get(), a.count, a.argScale)
	if err != nil {
		panic(err)
	}
	if r.B0_63 == 0 && r.B64_127 == 0 {
		set(r)
		return
	}
	temp, err1 := types.Decimal128FromFloat64(
		math.Sqrt(types.Decimal128ToFloat64(get(), a.retScale)),
		38, a.retScale)
	if err1 != nil {
		panic(err1)
	}
	set(temp)
}

type aggStdVarPopDecimal128 struct {
	aggVarPopDecimal128
}

func newAggStdVarPopDecimal128() aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128] {
	return &aggStdVarPopDecimal128{}
}

func (a *aggStdVarPopDecimal128) Flush(get aggexec.AggGetter[types.Decimal128], set aggexec.AggSetter[types.Decimal128]) {
	r, err := getVarianceFromSumPowCount(a.sum, get(), a.count, a.argScale)
	if err != nil {
		panic(err)
	}
	if r.B0_63 == 0 && r.B64_127 == 0 {
		set(r)
		return
	}
	temp, err1 := types.Decimal128FromFloat64(
		math.Sqrt(types.Decimal128ToFloat64(get(), a.retScale)),
		38, a.retScale)
	if err1 != nil {
		panic(err1)
	}
	set(temp)
}
