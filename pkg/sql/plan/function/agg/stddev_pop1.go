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
	"math"
)

func RegisterStdDevPop1(id int64) {
	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), VarPopReturnType, false, true),
			newAggVarPop[uint64],
			FillAggVarPop1[uint64], nil, FillsAggVarPop1[uint64],
			MergeAggVarPop1[uint64],
			FlushAggStdDevPop1[uint64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), VarPopReturnType, false, true),
			newAggVarPop[int8],
			FillAggVarPop1[int8], nil, FillsAggVarPop1[int8],
			MergeAggVarPop1[int8],
			FlushAggStdDevPop1[int8],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), VarPopReturnType, false, true),
			newAggVarPop[int16],
			FillAggVarPop1[int16], nil, FillsAggVarPop1[int16],
			MergeAggVarPop1[int16],
			FlushAggStdDevPop1[int16],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), VarPopReturnType, false, true),
			newAggVarPop[int32],
			FillAggVarPop1[int32], nil, FillsAggVarPop1[int32],
			MergeAggVarPop1[int32],
			FlushAggStdDevPop1[int32],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), VarPopReturnType, false, true),
			newAggVarPop[int64],
			FillAggVarPop1[int64], nil, FillsAggVarPop1[int64],
			MergeAggVarPop1[int64],
			FlushAggStdDevPop1[int64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), VarPopReturnType, false, true),
			newAggVarPop[uint8],
			FillAggVarPop1[uint8], nil, FillsAggVarPop1[uint8],
			MergeAggVarPop1[uint8],
			FlushAggStdDevPop1[uint8],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), VarPopReturnType, false, true),
			newAggVarPop[uint16],
			FillAggVarPop1[uint16], nil, FillsAggVarPop1[uint16],
			MergeAggVarPop1[uint16],
			FlushAggStdDevPop1[uint16],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), VarPopReturnType, false, true),
			newAggVarPop[uint32],
			FillAggVarPop1[uint32], nil, FillsAggVarPop1[uint32],
			MergeAggVarPop1[uint32],
			FlushAggStdDevPop1[uint32],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), VarPopReturnType, false, true),
			newAggVarPop[uint64],
			FillAggVarPop1[uint64], nil, FillsAggVarPop1[uint64],
			MergeAggVarPop1[uint64],
			FlushAggStdDevPop1[uint64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), VarPopReturnType, false, true),
			newAggVarPop[float32],
			FillAggVarPop1[float32], nil, FillsAggVarPop1[float32],
			MergeAggVarPop1[float32],
			FlushAggStdDevPop1[float32],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), VarPopReturnType, false, true),
			newAggVarPop[float64],
			FillAggVarPop1[float64], nil, FillsAggVarPop1[float64],
			MergeAggVarPop1[float64],
			FlushAggStdDevPop1[float64],
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal64.ToType(), VarPopReturnType, false, true),
			newAggVarPopDecimal64,
			FillAggVarPop1Decimal64, nil, FillsAggVarPop1Decimal64,
			MergeAggVarPop1Decimal64,
			FlushAggStdDevPopDecimal64,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_decimal128.ToType(), VarPopReturnType, false, true),
			newAggVarPopDecimal128,
			FillAggVarPop1Decimal128, nil, FillsAggVarPop1Decimal128,
			MergeAggVarPop1Decimal128,
			FlushAggStdDevPopDecimal128,
		))
}

func FlushAggStdDevPop1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, float64],
	getter aggexec.AggGetter[float64], setter aggexec.AggSetter[float64]) error {
	a := exec.(*aggVarPop[from])
	if a.count <= 1 {
		setter(0)
		return nil
	}
	avg := a.sum / float64(a.count)
	variance := getter()/float64(a.count) - math.Pow(avg, 2)
	setter(math.Sqrt(variance))
	return nil
}

func FlushAggStdDevPopDecimal128(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal128, types.Decimal128],
	getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	a := exec.(*aggVarPopDecimal128)
	r, err := getVarianceFromSumPowCount(a.sum, getter(), a.count, a.argScale)
	if err != nil {
		return err
	}
	if r.B0_63 == 0 && r.B64_127 == 0 {
		setter(r)
		return nil
	}
	temp, err1 := types.Decimal128FromFloat64(
		math.Sqrt(types.Decimal128ToFloat64(r, a.retScale)),
		38, a.retScale)
	if err1 != nil {
		return err1
	}
	setter(temp)
	return nil
}

func FlushAggStdDevPopDecimal64(
	exec aggexec.SingleAggFromFixedRetFixed[types.Decimal64, types.Decimal128],
	getter aggexec.AggGetter[types.Decimal128], setter aggexec.AggSetter[types.Decimal128]) error {
	a := exec.(*aggVarPopDecimal64)
	r, err := getVarianceFromSumPowCount(a.sum, getter(), a.count, a.argScale)
	if err != nil {
		return err
	}
	if r.B0_63 == 0 && r.B64_127 == 0 {
		setter(r)
		return nil
	}
	temp, err1 := types.Decimal128FromFloat64(
		math.Sqrt(types.Decimal128ToFloat64(r, a.retScale)),
		38, a.retScale)
	if err1 != nil {
		return err1
	}
	setter(temp)
	return nil
}
