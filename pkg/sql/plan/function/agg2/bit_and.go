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
}

type aggBitAnd[T numeric] struct{}

func newAggBitAnd[T numeric]() aggexec.SingleAggFromFixedRetFixed[T, uint64] {
	return aggBitAnd[T]{}
}

func (a aggBitAnd[T]) Marshal() []byte  { return nil }
func (a aggBitAnd[T]) Unmarshal([]byte) {}
func (a aggBitAnd[T]) Init(set aggexec.AggSetter[uint64]) {
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

//type aggBitAndBinary struct{}
//
//func newAggBitAndBinary() aggexec.SingleAggFromVarRetVar {
//	return aggBitAndBinary{}
//}
//
//func (a aggBitAndBinary) Marshal() []byte  { return nil }
//func (a aggBitAndBinary) Unmarshal([]byte) {}
//func (a aggBitAndBinary) Init(set aggexec.AggBytesSetter) {
//	// todo: need type information here.
//}
