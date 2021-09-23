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

type Int8Sum struct{}
type Int16Sum struct{}
type Int32Sum struct{}
type Int64Sum struct{}
type Uint8Sum struct{}
type Uint16Sum struct{}
type Uint32Sum struct{}
type Uint64Sum struct{}
type Float32Sum struct{}
type Float64Sum struct{}

func (agg *Int8Sum) StateSize() uint8 {
	return 8
}

func (agg *Int8Sum) ResultSize() uint8 {
	return 8
}

func (agg *Int8Sum) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
}

func (agg *Int8Sum) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int8Sum) Aggregate(state, data []byte) {
	lhs := (*int64)(unsafe.Pointer(&state[0]))
	rhs := *(*int8)(unsafe.Pointer(&data[0]))
	*lhs += int64(rhs)
}

func (agg *Int8Sum) Merge(lstate, rstate []byte) {
	*(*int64)(unsafe.Pointer(&lstate[0])) += *(*int64)(unsafe.Pointer(&rstate[0]))
}

func (agg *Int8Sum) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 8
	lheader.Cap /= 8
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 8
	rheader.Cap /= 8
	lslice := *(*[]int64)(unsafe.Pointer(&lheader))
	rslice := *(*[]int64)(unsafe.Pointer(&rheader))
	add.Int64Add(lslice, rslice, lslice)
}

func (agg *Int8Sum) Finalize(state, result []byte) {
    copy(result, state)
}

func (agg *Int16Sum) StateSize() uint8 {
	return 8
}

func (agg *Int16Sum) ResultSize() uint8 {
	return 8
}

func (agg *Int16Sum) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
}

func (agg *Int16Sum) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int16Sum) Aggregate(state, data []byte) {
	lhs := (*int64)(unsafe.Pointer(&state[0]))
	rhs := *(*int16)(unsafe.Pointer(&data[0]))
	*lhs += int64(rhs)
}

func (agg *Int16Sum) Merge(lstate, rstate []byte) {
	*(*int64)(unsafe.Pointer(&lstate[0])) += *(*int64)(unsafe.Pointer(&rstate[0]))
}

func (agg *Int16Sum) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 8
	lheader.Cap /= 8
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 8
	rheader.Cap /= 8
	lslice := *(*[]int64)(unsafe.Pointer(&lheader))
	rslice := *(*[]int64)(unsafe.Pointer(&rheader))
	add.Int64Add(lslice, rslice, lslice)
}

func (agg *Int16Sum) Finalize(state, result []byte) {
    copy(result, state)
}

func (agg *Int32Sum) StateSize() uint8 {
	return 8
}

func (agg *Int32Sum) ResultSize() uint8 {
	return 8
}

func (agg *Int32Sum) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
}

func (agg *Int32Sum) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int32Sum) Aggregate(state, data []byte) {
	lhs := (*int64)(unsafe.Pointer(&state[0]))
	rhs := *(*int32)(unsafe.Pointer(&data[0]))
	*lhs += int64(rhs)
}

func (agg *Int32Sum) Merge(lstate, rstate []byte) {
	*(*int64)(unsafe.Pointer(&lstate[0])) += *(*int64)(unsafe.Pointer(&rstate[0]))
}

func (agg *Int32Sum) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 8
	lheader.Cap /= 8
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 8
	rheader.Cap /= 8
	lslice := *(*[]int64)(unsafe.Pointer(&lheader))
	rslice := *(*[]int64)(unsafe.Pointer(&rheader))
	add.Int64Add(lslice, rslice, lslice)
}

func (agg *Int32Sum) Finalize(state, result []byte) {
    copy(result, state)
}

func (agg *Int64Sum) StateSize() uint8 {
	return 8
}

func (agg *Int64Sum) ResultSize() uint8 {
	return 8
}

func (agg *Int64Sum) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
}

func (agg *Int64Sum) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int64Sum) Aggregate(state, data []byte) {
	lhs := (*int64)(unsafe.Pointer(&state[0]))
	rhs := *(*int64)(unsafe.Pointer(&data[0]))
	*lhs += int64(rhs)
}

func (agg *Int64Sum) Merge(lstate, rstate []byte) {
	*(*int64)(unsafe.Pointer(&lstate[0])) += *(*int64)(unsafe.Pointer(&rstate[0]))
}

func (agg *Int64Sum) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 8
	lheader.Cap /= 8
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 8
	rheader.Cap /= 8
	lslice := *(*[]int64)(unsafe.Pointer(&lheader))
	rslice := *(*[]int64)(unsafe.Pointer(&rheader))
	add.Int64Add(lslice, rslice, lslice)
}

func (agg *Int64Sum) Finalize(state, result []byte) {
    copy(result, state)
}

func (agg *Uint8Sum) StateSize() uint8 {
	return 8
}

func (agg *Uint8Sum) ResultSize() uint8 {
	return 8
}

func (agg *Uint8Sum) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
}

func (agg *Uint8Sum) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint8Sum) Aggregate(state, data []byte) {
	lhs := (*uint64)(unsafe.Pointer(&state[0]))
	rhs := *(*uint8)(unsafe.Pointer(&data[0]))
	*lhs += uint64(rhs)
}

func (agg *Uint8Sum) Merge(lstate, rstate []byte) {
	*(*uint64)(unsafe.Pointer(&lstate[0])) += *(*uint64)(unsafe.Pointer(&rstate[0]))
}

func (agg *Uint8Sum) ArrayMerge(larray, rarray []byte) {
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

func (agg *Uint8Sum) Finalize(state, result []byte) {
    copy(result, state)
}

func (agg *Uint16Sum) StateSize() uint8 {
	return 8
}

func (agg *Uint16Sum) ResultSize() uint8 {
	return 8
}

func (agg *Uint16Sum) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
}

func (agg *Uint16Sum) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint16Sum) Aggregate(state, data []byte) {
	lhs := (*uint64)(unsafe.Pointer(&state[0]))
	rhs := *(*uint16)(unsafe.Pointer(&data[0]))
	*lhs += uint64(rhs)
}

func (agg *Uint16Sum) Merge(lstate, rstate []byte) {
	*(*uint64)(unsafe.Pointer(&lstate[0])) += *(*uint64)(unsafe.Pointer(&rstate[0]))
}

func (agg *Uint16Sum) ArrayMerge(larray, rarray []byte) {
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

func (agg *Uint16Sum) Finalize(state, result []byte) {
    copy(result, state)
}

func (agg *Uint32Sum) StateSize() uint8 {
	return 8
}

func (agg *Uint32Sum) ResultSize() uint8 {
	return 8
}

func (agg *Uint32Sum) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
}

func (agg *Uint32Sum) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint32Sum) Aggregate(state, data []byte) {
	lhs := (*uint64)(unsafe.Pointer(&state[0]))
	rhs := *(*uint32)(unsafe.Pointer(&data[0]))
	*lhs += uint64(rhs)
}

func (agg *Uint32Sum) Merge(lstate, rstate []byte) {
	*(*uint64)(unsafe.Pointer(&lstate[0])) += *(*uint64)(unsafe.Pointer(&rstate[0]))
}

func (agg *Uint32Sum) ArrayMerge(larray, rarray []byte) {
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

func (agg *Uint32Sum) Finalize(state, result []byte) {
    copy(result, state)
}

func (agg *Uint64Sum) StateSize() uint8 {
	return 8
}

func (agg *Uint64Sum) ResultSize() uint8 {
	return 8
}

func (agg *Uint64Sum) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
}

func (agg *Uint64Sum) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint64Sum) Aggregate(state, data []byte) {
	lhs := (*uint64)(unsafe.Pointer(&state[0]))
	rhs := *(*uint64)(unsafe.Pointer(&data[0]))
	*lhs += uint64(rhs)
}

func (agg *Uint64Sum) Merge(lstate, rstate []byte) {
	*(*uint64)(unsafe.Pointer(&lstate[0])) += *(*uint64)(unsafe.Pointer(&rstate[0]))
}

func (agg *Uint64Sum) ArrayMerge(larray, rarray []byte) {
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

func (agg *Uint64Sum) Finalize(state, result []byte) {
    copy(result, state)
}

func (agg *Float32Sum) StateSize() uint8 {
	return 8
}

func (agg *Float32Sum) ResultSize() uint8 {
	return 8
}

func (agg *Float32Sum) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
}

func (agg *Float32Sum) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Float32Sum) Aggregate(state, data []byte) {
	lhs := (*float64)(unsafe.Pointer(&state[0]))
	rhs := *(*float32)(unsafe.Pointer(&data[0]))
	*lhs += float64(rhs)
}

func (agg *Float32Sum) Merge(lstate, rstate []byte) {
	*(*float64)(unsafe.Pointer(&lstate[0])) += *(*float64)(unsafe.Pointer(&rstate[0]))
}

func (agg *Float32Sum) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 8
	lheader.Cap /= 8
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 8
	rheader.Cap /= 8
	lslice := *(*[]float64)(unsafe.Pointer(&lheader))
	rslice := *(*[]float64)(unsafe.Pointer(&rheader))
	add.Float64Add(lslice, rslice, lslice)
}

func (agg *Float32Sum) Finalize(state, result []byte) {
    copy(result, state)
}

func (agg *Float64Sum) StateSize() uint8 {
	return 8
}

func (agg *Float64Sum) ResultSize() uint8 {
	return 8
}

func (agg *Float64Sum) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
}

func (agg *Float64Sum) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Float64Sum) Aggregate(state, data []byte) {
	lhs := (*float64)(unsafe.Pointer(&state[0]))
	rhs := *(*float64)(unsafe.Pointer(&data[0]))
	*lhs += float64(rhs)
}

func (agg *Float64Sum) Merge(lstate, rstate []byte) {
	*(*float64)(unsafe.Pointer(&lstate[0])) += *(*float64)(unsafe.Pointer(&rstate[0]))
}

func (agg *Float64Sum) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 8
	lheader.Cap /= 8
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 8
	rheader.Cap /= 8
	lslice := *(*[]float64)(unsafe.Pointer(&lheader))
	rslice := *(*[]float64)(unsafe.Pointer(&rheader))
	add.Float64Add(lslice, rslice, lslice)
}

func (agg *Float64Sum) Finalize(state, result []byte) {
    copy(result, state)
}
