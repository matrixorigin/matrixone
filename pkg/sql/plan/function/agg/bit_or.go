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

func RegisterBitOr1(id int64) {
	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_bit.ToType(), BitOrReturnType, false, true),
			newAggBitOr[uint64],
			InitAggBitOr1[uint64],
			FillAggBitOr1[uint64], nil, FillsAggBitOr1[uint64],
			MergeAggBitOr1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint8.ToType(), BitOrReturnType, false, true),
			newAggBitOr[uint8],
			InitAggBitOr1[uint8],
			FillAggBitOr1[uint8], nil, FillsAggBitOr1[uint8],
			MergeAggBitOr1[uint8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint16.ToType(), BitOrReturnType, false, true),
			newAggBitOr[uint16],
			InitAggBitOr1[uint16],
			FillAggBitOr1[uint16], nil, FillsAggBitOr1[uint16],
			MergeAggBitOr1[uint16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint32.ToType(), BitOrReturnType, false, true),
			newAggBitOr[uint32],
			InitAggBitOr1[uint32],
			FillAggBitOr1[uint32], nil, FillsAggBitOr1[uint32],
			MergeAggBitOr1[uint32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_uint64.ToType(), BitOrReturnType, false, true),
			newAggBitOr[uint64],
			InitAggBitOr1[uint64],
			FillAggBitOr1[uint64], nil, FillsAggBitOr1[uint64],
			MergeAggBitOr1[uint64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int8.ToType(), BitOrReturnType, false, true),
			newAggBitOr[int8],
			InitAggBitOr1[int8],
			FillAggBitOr1[int8], nil, FillsAggBitOr1[int8],
			MergeAggBitOr1[int8],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int16.ToType(), BitOrReturnType, false, true),
			newAggBitOr[int16],
			InitAggBitOr1[int16],
			FillAggBitOr1[int16], nil, FillsAggBitOr1[int16],
			MergeAggBitOr1[int16],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int32.ToType(), BitOrReturnType, false, true),
			newAggBitOr[int32],
			InitAggBitOr1[int32],
			FillAggBitOr1[int32], nil, FillsAggBitOr1[int32],
			MergeAggBitOr1[int32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_int64.ToType(), BitOrReturnType, false, true),
			newAggBitOr[int64],
			InitAggBitOr1[int64],
			FillAggBitOr1[int64], nil, FillsAggBitOr1[int64],
			MergeAggBitOr1[int64],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float32.ToType(), BitOrReturnType, false, true),
			newAggBitOr[float32],
			InitAggBitOr1[float32],
			FillAggBitOr1[float32], nil, FillsAggBitOr1[float32],
			MergeAggBitOr1[float32],
			nil,
		))

	aggexec.RegisterSingleAggFromFixedToFixed(
		aggexec.MakeSingleAgg1RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_float64.ToType(), BitOrReturnType, false, true),
			newAggBitOr[float64],
			InitAggBitOr1[float64],
			FillAggBitOr1[float64], nil, FillsAggBitOr1[float64],
			MergeAggBitOr1[float64],
			nil,
		))

	aggexec.RegisterSingleAggFromVarToVar(
		aggexec.MakeSingleAgg4RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_binary.ToType(), BitOrReturnType, false, true),
			aggexec.GenerateFlagContextFromVarToVar,
			FillAggBitOrBinary, nil, FillsAggBitOrBinary,
			MergeAggBitOrBinary,
			nil,
		))

	aggexec.RegisterSingleAggFromVarToVar(
		aggexec.MakeSingleAgg4RegisteredInfo(
			aggexec.MakeSingleColumnAggInformation(id, types.T_varbinary.ToType(), BitOrReturnType, false, true),
			aggexec.GenerateFlagContextFromVarToVar,
			FillAggBitOrBinary, nil, FillsAggBitOrBinary,
			MergeAggBitOrBinary,
			nil,
		))
}

var BitOrReturnType = BitAndReturnType

type aggBitOr[T numeric] struct{}

func newAggBitOr[T numeric]() aggexec.SingleAggFromFixedRetFixed[T, uint64] {
	return aggBitOr[T]{}
}

func (a aggBitOr[T]) Marshal() []byte  { return nil }
func (a aggBitOr[T]) Unmarshal([]byte) {}

func InitAggBitOr1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, uint64], setter aggexec.AggSetter[uint64], arg, ret types.Type) error {
	setter(0)
	return nil
}
func FillAggBitOr1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, uint64], value from, getter aggexec.AggGetter[uint64], setter aggexec.AggSetter[uint64]) error {
	vv := float64(value)
	if vv > math.MaxUint64 {
		setter(math.MaxInt64)
		return nil
	}
	if vv < 0 {
		setter(uint64(int64(value)) | getter())
		return nil
	}
	setter(uint64(value) | getter())
	return nil
}
func FillsAggBitOr1[from numeric](
	exec aggexec.SingleAggFromFixedRetFixed[from, uint64],
	value from, isNull bool, count int, getter aggexec.AggGetter[uint64], setter aggexec.AggSetter[uint64]) error {
	if !isNull {
		return FillAggBitOr1(exec, value, getter, setter)
	}
	return nil
}
func MergeAggBitOr1[from numeric](
	exec1, exec2 aggexec.SingleAggFromFixedRetFixed[from, uint64],
	getter1, getter2 aggexec.AggGetter[uint64], setter aggexec.AggSetter[uint64]) error {
	setter(getter1() | getter2())
	return nil
}

func FillAggBitOrBinary(
	exec aggexec.SingleAggFromVarRetVar, value []byte, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a := exec.(*aggexec.ContextWithEmptyFlagOfSingleAggRetBytes)
	if a.IsEmpty {
		a.IsEmpty = false
		return setter(value)
	}
	v := getter()
	types.BitOr(v, v, value)
	return nil
}
func FillsAggBitOrBinary(
	exec aggexec.SingleAggFromVarRetVar,
	value []byte, isNull bool, count int, getter aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	if !isNull {
		return FillAggBitOrBinary(exec, value, getter, setter)
	}
	return nil
}
func MergeAggBitOrBinary(
	exec1, exec2 aggexec.SingleAggFromVarRetVar,
	getter1, getter2 aggexec.AggBytesGetter, setter aggexec.AggBytesSetter) error {
	a1 := exec1.(*aggexec.ContextWithEmptyFlagOfSingleAggRetBytes)
	a2 := exec2.(*aggexec.ContextWithEmptyFlagOfSingleAggRetBytes)
	if a2.IsEmpty {
		return nil
	}
	if a1.IsEmpty {
		a1.IsEmpty = false
		return setter(getter2())
	}
	v1, v2 := getter1(), getter2()
	types.BitOr(v1, v1, v2)
	return nil
}
