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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
)

type aggMax[from canCompare] struct{}

func newAggMax[from canCompare]() aggexec.SingleAggFromFixedRetFixed[from, from] {
	return aggMax[from]{}
}

func (a aggMax[from]) Marshal() []byte                  { return nil }
func (a aggMax[from]) Unmarshal([]byte)                 {}
func (a aggMax[from]) Init(set aggexec.AggSetter[from]) {}
func (a aggMax[from]) Fill(value from, get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {
	if value > get() {
		set(value)
	}
}
func (a aggMax[from]) FillNull(get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {}
func (a aggMax[from]) Fills(value from, isNull bool, count int, get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {
	if !isNull && value > get() {
		set(value)
	}
}
func (a aggMax[from]) Merge(other aggexec.SingleAggFromFixedRetFixed[from, from], get1, get2 aggexec.AggGetter[from], set aggexec.AggSetter[from]) {
	if get1() > get2() {
		set(get1())
	} else {
		set(get2())
	}
}
func (a aggMax[from]) Flush(get aggexec.AggGetter[from], set aggexec.AggSetter[from]) {}
