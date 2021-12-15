// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregator

import (
	"unsafe"
)

type Count struct{}

func (agg *Count) StateSize() uint8 {
	return 8
}

func (agg *Count) ResultSize() uint8 {
	return 8
}

func (agg *Count) NeedsInit() bool {
	return false
}

func (agg *Count) Init(state unsafe.Pointer) {}

func (agg *Count) AddBatch(states []unsafe.Pointer, _ unsafe.Pointer) {
	for _, s := range states {
		*(*uint64)(s)++
	}
}

func (agg *Count) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		*(*uint64)(s) += *(*uint64)(rstates[i])
	}
}

func (agg *Count) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint64)(results[i]) = *(*uint64)(s)
	}
}
