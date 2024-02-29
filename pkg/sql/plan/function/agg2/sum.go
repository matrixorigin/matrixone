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

import "github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"

var _ aggexec.SingleAggFromFixedRetFixed[int32, int64] = &aggSum[int32, int64]{}

type aggSum[from numeric, to maxScaleNumeric] struct{}

func (a aggSum[from, to]) Marshal() []byte        { return nil }
func (a aggSum[from, to]) Unmarshal(bytes []byte) {}
func (a aggSum[from, to]) Init()                  {}
func (a aggSum[from, to]) Fill(from from, get aggexec.AggGetter[to], set aggexec.AggSetter[to]) {
	set(get() + to(from))
}
func (a aggSum[from, to]) FillNull(get aggexec.AggGetter[to], set aggexec.AggSetter[to]) {}
func (a aggSum[from, to]) Fills(value from, isNull bool, count int, get aggexec.AggGetter[to], set aggexec.AggSetter[to]) {
	if !isNull {
		set(get() + to(value)*to(count))
	}
}
func (a aggSum[from, to]) Merge(other aggexec.SingleAggFromFixedRetFixed[from, to], getter1, getter2 aggexec.AggGetter[to], set aggexec.AggSetter[to]) {
	set(getter1() + getter2())
}
func (a aggSum[from, to]) Flush(get aggexec.AggGetter[to], setter aggexec.AggSetter[to]) {}
