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
)

// todo: not done.
func RegisterAvg(id int64) {
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int8.ToType(), types.T_float64.ToType(), false, true), newAggAvg[int8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int16.ToType(), types.T_float64.ToType(), false, true), newAggAvg[int16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int32.ToType(), types.T_float64.ToType(), false, true), newAggAvg[int32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_int64.ToType(), types.T_float64.ToType(), false, true), newAggAvg[int64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint8.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint8])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint16.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint16])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint32.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_uint64.ToType(), types.T_float64.ToType(), false, true), newAggAvg[uint64])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float32.ToType(), types.T_float64.ToType(), false, true), newAggAvg[float32])
	aggexec.RegisterDeterminedSingleAgg(aggexec.MakeDeterminedSingleAggInfo(id, types.T_float64.ToType(), types.T_float64.ToType(), false, true), newAggAvg[float64])
}

type aggAvg[from numeric] struct {
	count int64
}

func newAggAvg[from numeric]() aggexec.SingleAggFromFixedRetFixed[from, float64] {
	return &aggAvg[from]{}
}

func (a *aggAvg[from]) Marshal() []byte       { return types.EncodeInt64(&a.count) }
func (a *aggAvg[from]) Unmarshal(data []byte) { a.count = types.DecodeInt64(data) }
func (a *aggAvg[from]) Init(set aggexec.AggSetter[float64]) {
	set(0)
	a.count = 0
}
func (a *aggAvg[from]) Fill(value from, get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	set(get() + float64(value))
	a.count++
}
func (a *aggAvg[from]) FillNull(get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {}
func (a *aggAvg[from]) Fills(value from, isNull bool, count int, get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	if isNull {
		return
	}
	set(get() + float64(value)*float64(count))
	a.count += int64(count)
}
func (a *aggAvg[from]) Merge(other aggexec.SingleAggFromFixedRetFixed[from, float64], get1, get2 aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	next := other.(*aggAvg[from])
	set(get1() + get2())
	a.count += next.count
}
func (a *aggAvg[from]) Flush(get aggexec.AggGetter[float64], set aggexec.AggSetter[float64]) {
	if a.count == 0 {
		set(0)
	} else {
		set(get() / float64(a.count))
	}
}
