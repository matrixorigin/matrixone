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
	"matrixone/pkg/vectorize/add"
	"reflect"
	"unsafe"
)

type Count struct{}

func (agg *Count) StateSize() uint8 {
	return 8
}

func (agg *Count) ResultSize() uint8 {
	return 8
}

func (agg *Count) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
}

func (agg *Count) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
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
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 8
	lheader.Cap /= 8
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 8
	rheader.Cap /= 8
	lslice := *(*[]uint64)(unsafe.Pointer(&lheader))
	rslice := *(*[]uint64)(unsafe.Pointer(&rheader))
	add.Uint64Add(lslice, rslice, lslice)
}

func (agg *Count) Finalize(state, result []byte) {
	copy(result, state)
}
