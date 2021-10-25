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
	"github.com/matrixorigin/matrixone/pkg/vectorize/add"
	"unsafe"
)

type Count struct{}

func (agg *Count) StateSize() uint8 {
	return 8
}

func (agg *Count) ResultSize() uint8 {
	return 8
}

func (agg *Count) Init(state, data []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 1
}

func (agg *Count) ArrayInit(array []byte) {
	*(*uint64)(unsafe.Pointer(&array[0])) = 0
	for i := int(agg.StateSize()); i < len(array); i *= 2 {
		copy(array[i:], array[:i])
	}
}

func (agg *Count) Aggregate(state, data []byte) {
	*(*uint64)(unsafe.Pointer(&state[0]))++
}

func (agg *Count) Merge(lstate, rstate []byte) {
	*(*uint64)(unsafe.Pointer(&lstate[0])) += *(*uint64)(unsafe.Pointer(&rstate[0]))
}

func (agg *Count) ArrayMerge(larray, rarray []byte) {
	lslice := unsafe.Slice((*uint64)(unsafe.Pointer(&larray[0])), len(larray)/8)
	rslice := unsafe.Slice((*uint64)(unsafe.Pointer(&rarray[0])), len(rarray)/8)
	add.Uint64Add(lslice, rslice, lslice)
}

func (agg *Count) Finalize(state, result []byte) {
	copy(result, state)
}
