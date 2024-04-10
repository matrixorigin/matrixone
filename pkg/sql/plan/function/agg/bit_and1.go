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

func RegisterBitAnd1(id int64) {
	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), BitAndReturnType, false, true),
			newAggBitAnd[uint64],
			FillAggBitAnd1[uint64], nil, FillsAggBitAnd1[uint64],
			MergeAggBitAnd1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), BitAndReturnType, false, true),
			newAggBitAnd[uint8],
			FillAggBitAnd1[uint8], nil, FillsAggBitAnd1[uint8],
			MergeAggBitAnd1[uint8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), BitAndReturnType, false, true),
			newAggBitAnd[uint16],
			FillAggBitAnd1[uint16], nil, FillsAggBitAnd1[uint16],
			MergeAggBitAnd1[uint16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), BitAndReturnType, false, true),
			newAggBitAnd[uint32],
			FillAggBitAnd1[uint32], nil, FillsAggBitAnd1[uint32],
			MergeAggBitAnd1[uint32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), BitAndReturnType, false, true),
			newAggBitAnd[uint64],
			FillAggBitAnd1[uint64], nil, FillsAggBitAnd1[uint64],
			MergeAggBitAnd1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), BitAndReturnType, false, true),
			newAggBitAnd[int8],
			FillAggBitAnd1[int8], nil, FillsAggBitAnd1[int8],
			MergeAggBitAnd1[int8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), BitAndReturnType, false, true),
			newAggBitAnd[int16],
			FillAggBitAnd1[int16], nil, FillsAggBitAnd1[int16],
			MergeAggBitAnd1[int16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), BitAndReturnType, false, true),
			newAggBitAnd[int32],
			FillAggBitAnd1[int32], nil, FillsAggBitAnd1[int32],
			MergeAggBitAnd1[int32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), BitAndReturnType, false, true),
			newAggBitAnd[int64],
			FillAggBitAnd1[int64], nil, FillsAggBitAnd1[int64],
			MergeAggBitAnd1[int64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), BitAndReturnType, false, true),
			newAggBitAnd[float32],
			FillAggBitAnd1[float32], nil, FillsAggBitAnd1[float32],
			MergeAggBitAnd1[float32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), BitAndReturnType, false, true),
			newAggBitAnd[float64],
			FillAggBitAnd1[float64], nil, FillsAggBitAnd1[float64],
			MergeAggBitAnd1[float64],
			nil,
		))

	aggexec.RegisterSingleAggFromVarToVar(
		aggexec.MakeSingleAgg4RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_binary.ToType(), BitAndReturnType, false, true),
			newAggBitAndBinary,
			FillAggBitAndBinary, nil, FillsAggBitAndBinary,
			MergeAggBitAndBinary,
			nil,
		))

	aggexec.RegisterSingleAggFromVarToVar(
		aggexec.MakeSingleAgg4RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_varbinary.ToType(), BitAndReturnType, false, true),
			newAggBitAndBinary,
			FillAggBitAndBinary, nil, FillsAggBitAndBinary,
			MergeAggBitAndBinary,
			nil,
		))
}

func FillAggBitAnd1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, uint64], value from, getter aggexec.AggGetter[uint64], setter aggexec.AggSetter[uint64]) error {
	vv := float64(value)
	if vv > math.MaxUint64 {
		setter(math.MaxInt64 & getter())
		return nil
	}
	if vv < 0 {
		setter(uint64(int64(value)) & getter())
		return nil
	}
	setter(uint64(value) & getter())
	return nil
}
func FillsAggBitAnd1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, uint64],
	value from, isNull bool, count int, getter aggexec.AggGetter[uint64], setter aggexec.AggSetter[uint64]) error {
	if !isNull {
		return FillAggBitAnd1(exec, value, getter, setter)
	}
	return nil
}
func MergeAggBitAnd1[from numeric](
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[from, uint64],
	getter1, getter2 aggexec.AggGetter[uint64], setter aggexec.AggSetter[uint64]) error {
	setter(getter1() & getter2())
	return nil
}

func FillAggBitAndBinary(
	exec aggexec.SingleAggFromVarRetVar, value []byte, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a := exec.(*aggBitAndBinary)
	if a.isEmpty {
		a.isEmpty = false
		return setter(value)
	}
	v := getter()
	types.BitAnd(v, v, value)
	return nil
}
func FillsAggBitAndBinary(
	exec aggexec.SingleAggFromVarRetVar,
	value []byte, isNull bool, count int, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	if !isNull {
		return FillAggBitAndBinary(exec, value, getter, setter)
	}
	return nil
}
func MergeAggBitAndBinary(
	exec1, exec2 aggexec.SingleAggFromVarRetVar,
	getter1, getter2 aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a1 := exec1.(*aggBitAndBinary)
	a2 := exec2.(*aggBitAndBinary)
	if a2.isEmpty {
		return nil
	}
	if a1.isEmpty {
		a1.isEmpty = false
		return setter(getter2())
	}
	v1, v2 := getter1(), getter2()
	types.BitAnd(v1, v1, v2)
	return nil
}
