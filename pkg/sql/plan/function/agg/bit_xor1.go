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
// See the License for the specific language governing permissions Or
// limitations under the License.

package agg

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"math"
)

func RegisterBitXor1(id int64) {
	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), BitXorReturnType, false, true),
			newAggBitXor[uint64],
			FillAggBitXor1[uint64], nil, FillsAggBitXor1[uint64],
			MergeAggBitXor1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), BitXorReturnType, false, true),
			newAggBitXor[uint8],
			FillAggBitXor1[uint8], nil, FillsAggBitXor1[uint8],
			MergeAggBitXor1[uint8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), BitXorReturnType, false, true),
			newAggBitXor[uint16],
			FillAggBitXor1[uint16], nil, FillsAggBitXor1[uint16],
			MergeAggBitXor1[uint16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), BitXorReturnType, false, true),
			newAggBitXor[uint32],
			FillAggBitXor1[uint32], nil, FillsAggBitXor1[uint32],
			MergeAggBitXor1[uint32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), BitXorReturnType, false, true),
			newAggBitXor[uint64],
			FillAggBitXor1[uint64], nil, FillsAggBitXor1[uint64],
			MergeAggBitXor1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), BitXorReturnType, false, true),
			newAggBitXor[int8],
			FillAggBitXor1[int8], nil, FillsAggBitXor1[int8],
			MergeAggBitXor1[int8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), BitXorReturnType, false, true),
			newAggBitXor[int16],
			FillAggBitXor1[int16], nil, FillsAggBitXor1[int16],
			MergeAggBitXor1[int16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), BitXorReturnType, false, true),
			newAggBitXor[int32],
			FillAggBitXor1[int32], nil, FillsAggBitXor1[int32],
			MergeAggBitXor1[int32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), BitXorReturnType, false, true),
			newAggBitXor[int64],
			FillAggBitXor1[int64], nil, FillsAggBitXor1[int64],
			MergeAggBitXor1[int64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), BitXorReturnType, false, true),
			newAggBitXor[float32],
			FillAggBitXor1[float32], nil, FillsAggBitXor1[float32],
			MergeAggBitXor1[float32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), BitXorReturnType, false, true),
			newAggBitXor[float64],
			FillAggBitXor1[float64], nil, FillsAggBitXor1[float64],
			MergeAggBitXor1[float64],
			nil,
		))

	aggexec.RegisterSingleAggFromVarToVar(
		aggexec.MakeSingleAgg4RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_binary.ToType(), BitXorReturnType, false, true),
			newAggBitXorBinary,
			FillAggBitXorBinary, nil, FillsAggBitXorBinary,
			MergeAggBitXorBinary,
			nil,
		))

	aggexec.RegisterSingleAggFromVarToVar(
		aggexec.MakeSingleAgg4RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_varbinary.ToType(), BitXorReturnType, false, true),
			newAggBitXorBinary,
			FillAggBitXorBinary, nil, FillsAggBitXorBinary,
			MergeAggBitXorBinary,
			nil,
		))
}

var BitXorReturnType = BitAndReturnType

func FillAggBitXor1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, uint64], value from, getter aggexec.AggGetter[uint64], setter aggexec.AggSetter[uint64]) error {
	vv := float64(value)
	if vv > math.MaxUint64 {
		setter(math.MaxInt64 ^ getter())
		return nil
	}
	if vv < 0 {
		setter(uint64(int64(value)) ^ getter())
		return nil
	}
	setter(uint64(value) ^ getter())
	return nil
}
func FillsAggBitXor1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, uint64],
	value from, isNull bool, count int, getter aggexec.AggGetter[uint64], setter aggexec.AggSetter[uint64]) error {
	if !isNull {
		if count%2 == 1 {
			return FillAggBitXor1(exec, value, getter, setter)
		}
		setter(getter())
	}
	return nil
}
func MergeAggBitXor1[from numeric](
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[from, uint64],
	getter1, getter2 aggexec.AggGetter[uint64], setter aggexec.AggSetter[uint64]) error {
	setter(getter1() ^ getter2())
	return nil
}

func FillAggBitXorBinary(
	exec aggexec.SingleAggFromVarRetVar, value []byte, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a := exec.(*aggBitXorBinary)
	if a.isEmpty {
		a.isEmpty = false
		return setter(value)
	}
	v := getter()
	types.BitXor(v, v, value)
	return nil
}
func FillsAggBitXorBinary(
	exec aggexec.SingleAggFromVarRetVar,
	value []byte, isNull bool, count int, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	if !isNull {
		if count%2 == 1 {
			return FillAggBitXorBinary(exec, value, getter, setter)
		}
		a := exec.(*aggBitXorBinary)
		if a.isEmpty {
			a.isEmpty = false
			return setter(make([]byte, len(value)))
		}
	}
	return nil
}
func MergeAggBitXorBinary(
	exec1, exec2 aggexec.SingleAggFromVarRetVar,
	getter1, getter2 aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a1 := exec1.(*aggBitXorBinary)
	a2 := exec2.(*aggBitXorBinary)
	if a2.isEmpty {
		return nil
	}
	if a1.isEmpty {
		a1.isEmpty = false
		return setter(getter2())
	}
	v1, v2 := getter1(), getter2()
	types.BitXor(v1, v1, v2)
	return nil
}
