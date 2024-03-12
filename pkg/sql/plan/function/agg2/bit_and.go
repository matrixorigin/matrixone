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

func RegisterBitAnd(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bit.ToType(), types.T_uint64.ToType(), false, true), newAggBitAnd[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_uint64.ToType(), false, true), newAggBitAnd[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_uint64.ToType(), false, true), newAggBitAnd[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_uint64.ToType(), false, true), newAggBitAnd[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_uint64.ToType(), false, true), newAggBitAnd[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_uint64.ToType(), false, true), newAggBitAnd[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_uint64.ToType(), false, true), newAggBitAnd[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_uint64.ToType(), false, true), newAggBitAnd[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_uint64.ToType(), false, true), newAggBitAnd[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_uint64.ToType(), false, true), newAggBitAnd[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_uint64.ToType(), false, true), newAggBitAnd[float64])
	aggexec.RegisterFlexibleSingleAgg(
		aggexec.MakeFlexibleAggInfo(id, false, true),
		func(t []types.Type) types.Type {
			if t[0].Oid == types.T_binary || t[0].Oid == types.T_varbinary {
				return t[0]
			}
			panic("unexpect type for bit_or()")
		},
		func(args []types.Type, ret types.Type) any {
			return newAggBitAndBinary
		})
}

type aggBitAnd[T numeric] struct{}

func newAggBitAnd[T numeric]() aggexec.SingleAggFromFixedRetFixed[T, uint64] {
	return aggBitAnd[T]{}
}

func (a aggBitAnd[T]) Marshal() []byte  { return nil }
func (a aggBitAnd[T]) Unmarshal([]byte) {}
func (a aggBitAnd[T]) Init(set aggexec.AggSetter[uint64], arg, ret types.Type) {
	set(^uint64(0))
}
func (a aggBitAnd[T]) Fill(value T, get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {
	vv := float64(value)
	if vv > math.MaxUint64 {
		set(math.MaxInt64 & get())
		return
	}
	if vv < 0 {
		set(uint64(int64(value)) & get())
		return
	}
	set(uint64(value) & get())
}
func (a aggBitAnd[T]) FillNull(get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {}
func (a aggBitAnd[T]) Fills(value T, isNull bool, count int, get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {
	if !isNull {
		a.Fill(value, get, set)
	}
}
func (a aggBitAnd[T]) Merge(other aggexec.SingleAggFromFixedRetFixed[T, uint64], get1, get2 aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {
	set(get1() & get2())
}
func (a aggBitAnd[T]) Flush(get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {}

type aggBitAndBinary struct{}

func newAggBitAndBinary() aggexec.SingleAggFromVarRetVar {
	return aggBitAndBinary{}
}

func (a aggBitAndBinary) Marshal() []byte  { return nil }
func (a aggBitAndBinary) Unmarshal([]byte) {}
func (a aggBitAndBinary) Init(set aggexec.AggBytesSetter, arg types.Type, ret types.Type) {
	vs := make([]byte, ret.Width)
	for i := range vs {
		vs[i] = 1
	}
	_ = set(vs)
}
func (a aggBitAndBinary) FillBytes(value []byte, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	v := get()
	types.BitAnd(v, v, value)
}
func (a aggBitAndBinary) FillNull(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
}
func (a aggBitAndBinary) Fills(value []byte, isNull bool, count int, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	if !isNull {
		a.FillBytes(value, get, set)
	}
}
func (a aggBitAndBinary) Merge(other aggexec.SingleAggFromVarRetVar, get1, get2 aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	v1, v2 := get1(), get2()
	types.BitAnd(v1, v1, v2)
}
func (a aggBitAndBinary) Flush(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {}
