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

func RegisterBitXor(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_bit.ToType(), types.T_uint64.ToType(), false, true), newAggBitXor[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_uint64.ToType(), false, true), newAggBitXor[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_uint64.ToType(), false, true), newAggBitXor[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_uint64.ToType(), false, true), newAggBitXor[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_uint64.ToType(), false, true), newAggBitXor[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_uint64.ToType(), false, true), newAggBitXor[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_uint64.ToType(), false, true), newAggBitXor[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_uint64.ToType(), false, true), newAggBitXor[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_uint64.ToType(), false, true), newAggBitXor[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_uint64.ToType(), false, true), newAggBitXor[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_uint64.ToType(), false, true), newAggBitXor[float64])
	aggexec.RegisterFlexibleSingleAgg(
		aggexec.MakeFlexibleAggInfo(id, false, true),
		func(t []types.Type) types.Type {
			if t[0].Oid == types.T_binary || t[0].Oid == types.T_varbinary {
				return t[0]
			}
			panic("unexpect type for bit_or()")
		},
		func(args []types.Type, ret types.Type) any {
			return newAggBitXorBinary
		})
}

type aggBitXor[T numeric] struct{}

func newAggBitXor[T numeric]() aggexec.SingleAggFromFixedRetFixed[T, uint64] {
	return aggBitXor[T]{}
}

func (a aggBitXor[T]) Marshal() []byte  { return nil }
func (a aggBitXor[T]) Unmarshal([]byte) {}
func (a aggBitXor[T]) Init(set aggexec.AggSetter[uint64], arg, ret types.Type) {
	set(0)
}
func (a aggBitXor[T]) Fill(value T, get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {
	vv := float64(value)
	if vv > math.MaxUint64 {
		set(math.MaxInt64 ^ get())
		return
	}
	if vv < 0 {
		set(uint64(int64(value)) ^ get())
		return
	}
	set(uint64(value) ^ get())
}
func (a aggBitXor[T]) FillNull(get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {}
func (a aggBitXor[T]) Fills(value T, isNull bool, count int, get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {
	if !isNull {
		if count%2 == 1 {
			a.Fill(value, get, set)
			return
		}
		set(get())
	}
}
func (a aggBitXor[T]) Merge(other aggexec.SingleAggFromFixedRetFixed[T, uint64], get1, get2 aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {
	set(get1() ^ get2())
}
func (a aggBitXor[T]) Flush(get aggexec.AggGetter[uint64], set aggexec.AggSetter[uint64]) {}

type aggBitXorBinary struct{}

func newAggBitXorBinary() aggexec.SingleAggFromVarRetVar {
	return aggBitXorBinary{}
}

func (a aggBitXorBinary) Marshal() []byte  { return nil }
func (a aggBitXorBinary) Unmarshal([]byte) {}
func (a aggBitXorBinary) Init(set aggexec.AggBytesSetter, arg types.Type, ret types.Type) {
	vs := make([]byte, ret.Width)
	for i := range vs {
		vs[i] = 0
	}
	_ = set(vs)
}
func (a aggBitXorBinary) FillBytes(value []byte, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	v := get()
	types.BitXor(v, v, value)
}
func (a aggBitXorBinary) FillNull(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
}
func (a aggBitXorBinary) Fills(value []byte, isNull bool, count int, get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	if !isNull {
		if count%2 == 1 {
			a.FillBytes(value, get, set)
			return
		}
		_ = set(get())
	}
}
func (a aggBitXorBinary) Merge(other aggexec.SingleAggFromVarRetVar, get1, get2 aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {
	v1, v2 := get1(), get2()
	types.BitXor(v1, v1, v2)
}
func (a aggBitXorBinary) Flush(get aggexec.AggBytesGetter, set aggexec.AggBytesSetter) {}
