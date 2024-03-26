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

package agg2

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"math"
)

func RegisterBitOr(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bit.ToType(), types.T_uint64.ToType(), false, true), newAggBitOr[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_uint64.ToType(), false, true), newAggBitOr[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_uint64.ToType(), false, true), newAggBitOr[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_uint64.ToType(), false, true), newAggBitOr[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_uint64.ToType(), false, true), newAggBitOr[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_uint64.ToType(), false, true), newAggBitOr[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_uint64.ToType(), false, true), newAggBitOr[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_uint64.ToType(), false, true), newAggBitOr[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_uint64.ToType(), false, true), newAggBitOr[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_uint64.ToType(), false, true), newAggBitOr[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_uint64.ToType(), false, true), newAggBitOr[float64])
	aggexec.RegisterFlexibleSingleAgg(
		aggexec.MakeFlexibleAggInfo(id, false, true),
		BitAndReturnType,
		func(args []types.Type, ret types.Type) any {
			return newAggBitOrBinary
		})
}

type aggBitOr[T numeric] struct{}

func newAggBitOr[T numeric]() aggexec.SingleAggFromFixedRetFixed[T, uint64] {
	return aggBitOr[T]{}
}

func (a aggBitOr[T]) Marshal() []byte  { return nil }
func (a aggBitOr[T]) Unmarshal([]byte) {}
func (a aggBitOr[T]) Init(set aggexec.AggSetter[uint64], arg, ret types.Type) {
	set(0)
}
func (a aggBitOr[T]) Fill(value T, get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {
	vv := float64(value)
	if vv > math.MaxUint64 {
		set(math.MaxInt64)
		return
	}
	if vv < 0 {
		set(uint64(int64(value)) | get())
		return
	}
	set(uint64(value) | get())
}
func (a aggBitOr[T]) FillNull(get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {}
func (a aggBitOr[T]) Fills(value T, isNull bool, count int, get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {
	if !isNull {
		a.Fill(value, get, set)
	}
}
func (a aggBitOr[T]) Merge(other aggexec.SingleAggFromFixedRetFixed[T, uint64], get1, get2 aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {
	set(get1() | get2())
}
func (a aggBitOr[T]) Flush(get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {}

type aggBitOrBinary struct {
	aggBitBinary
}

func newAggBitOrBinary() aggexec.SingleAggFromVarRetVar {
	return &aggBitOrBinary{}
}

func (a *aggBitOrBinary) FillBytes(value []byte, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	if a.isEmpty {
		vs := make([]byte, len(value))
		for i := range vs {
			vs[i] = 0
		}
		_ = set(vs)
		a.isEmpty = false
	}
	v := get()
	types.BitOr(v, v, value)
}
func (a *aggBitOrBinary) FillNull(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
}
func (a *aggBitOrBinary) Fills(value []byte, isNull bool, count int, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	if !isNull {
		a.FillBytes(value, get, set)
	}
}
func (a *aggBitOrBinary) Merge(other aggexec.SingleAggFromVarRetVar, get1, get2 aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	next := other.(*aggBitOrBinary)
	if next.isEmpty {
		return
	}
	if a.isEmpty {
		_ = set(get2())
		a.isEmpty = false
		return
	}
	v1, v2 := get1(), get2()
	types.BitOr(v1, v1, v2)
}
func (a *aggBitOrBinary) Flush(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {}
