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
	"math"
	"reflect"
	"unsafe"
)

type Int8Min struct{}
type Int16Min struct{}
type Int32Min struct{}
type Int64Min struct{}
type Uint8Min struct{}
type Uint16Min struct{}
type Uint32Min struct{}
type Uint64Min struct{}
type Float32Min struct{}
type Float64Min struct{}

func (agg *Int8Min) StateSize() uint8 {
	return 1
}

func (agg *Int8Min) ResultSize() uint8 {
	return 1
}

func (agg *Int8Min) Init(state []byte) {
	*(*int8)(unsafe.Pointer(&state[0])) = math.MaxInt8
}

func (agg *Int8Min) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int8Min) Aggregate(state, data []byte) {
	lhs := (*int8)(unsafe.Pointer(&state[0]))
	rhs := *(*int8)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = rhs + (mask & mask >> 7)
}

func (agg *Int8Min) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Int8Min) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 1
	lheader.Cap /= 1
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 1
	rheader.Cap /= 1
	lslice := *(*[]int8)(unsafe.Pointer(&lheader))
	rslice := *(*[]int8)(unsafe.Pointer(&rheader))
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = v + (mask & mask >> 7)
	}
}

func (agg *Int8Min) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Int16Min) StateSize() uint8 {
	return 2
}

func (agg *Int16Min) ResultSize() uint8 {
	return 2
}

func (agg *Int16Min) Init(state []byte) {
	*(*int16)(unsafe.Pointer(&state[0])) = math.MaxInt16
}

func (agg *Int16Min) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int16Min) Aggregate(state, data []byte) {
	lhs := (*int16)(unsafe.Pointer(&state[0]))
	rhs := *(*int16)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = rhs + (mask & mask >> 15)
}

func (agg *Int16Min) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Int16Min) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 2
	lheader.Cap /= 2
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 2
	rheader.Cap /= 2
	lslice := *(*[]int16)(unsafe.Pointer(&lheader))
	rslice := *(*[]int16)(unsafe.Pointer(&rheader))
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = v + (mask & mask >> 15)
	}
}

func (agg *Int16Min) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Int32Min) StateSize() uint8 {
	return 4
}

func (agg *Int32Min) ResultSize() uint8 {
	return 4
}

func (agg *Int32Min) Init(state []byte) {
	*(*int32)(unsafe.Pointer(&state[0])) = math.MaxInt32
}

func (agg *Int32Min) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int32Min) Aggregate(state, data []byte) {
	lhs := (*int32)(unsafe.Pointer(&state[0]))
	rhs := *(*int32)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = rhs + (mask & mask >> 31)
}

func (agg *Int32Min) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Int32Min) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 4
	lheader.Cap /= 4
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 4
	rheader.Cap /= 4
	lslice := *(*[]int32)(unsafe.Pointer(&lheader))
	rslice := *(*[]int32)(unsafe.Pointer(&rheader))
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = v + (mask & mask >> 31)
	}
}

func (agg *Int32Min) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Int64Min) StateSize() uint8 {
	return 8
}

func (agg *Int64Min) ResultSize() uint8 {
	return 8
}

func (agg *Int64Min) Init(state []byte) {
	*(*int64)(unsafe.Pointer(&state[0])) = math.MaxInt64
}

func (agg *Int64Min) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int64Min) Aggregate(state, data []byte) {
	lhs := (*int64)(unsafe.Pointer(&state[0]))
	rhs := *(*int64)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = rhs + (mask & mask >> 63)
}

func (agg *Int64Min) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Int64Min) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 8
	lheader.Cap /= 8
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 8
	rheader.Cap /= 8
	lslice := *(*[]int64)(unsafe.Pointer(&lheader))
	rslice := *(*[]int64)(unsafe.Pointer(&rheader))
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = v + (mask & mask >> 63)
	}
}

func (agg *Int64Min) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Uint8Min) StateSize() uint8 {
	return 1
}

func (agg *Uint8Min) ResultSize() uint8 {
	return 1
}

func (agg *Uint8Min) Init(state []byte) {
	*(*uint8)(unsafe.Pointer(&state[0])) = math.MaxUint8
}

func (agg *Uint8Min) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint8Min) Aggregate(state, data []byte) {
	lhs := (*uint8)(unsafe.Pointer(&state[0]))
	rhs := *(*uint8)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = rhs + (mask & uint8(int8(mask)>>7))
}

func (agg *Uint8Min) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Uint8Min) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 1
	lheader.Cap /= 1
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 1
	rheader.Cap /= 1
	lslice := *(*[]uint8)(unsafe.Pointer(&lheader))
	rslice := *(*[]uint8)(unsafe.Pointer(&rheader))
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = v + (mask & uint8(int8(mask)>>7))
	}
}

func (agg *Uint8Min) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Uint16Min) StateSize() uint8 {
	return 2
}

func (agg *Uint16Min) ResultSize() uint8 {
	return 2
}

func (agg *Uint16Min) Init(state []byte) {
	*(*uint16)(unsafe.Pointer(&state[0])) = math.MaxUint16
}

func (agg *Uint16Min) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint16Min) Aggregate(state, data []byte) {
	lhs := (*uint16)(unsafe.Pointer(&state[0]))
	rhs := *(*uint16)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = rhs + (mask & uint16(int16(mask)>>15))
}

func (agg *Uint16Min) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Uint16Min) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 2
	lheader.Cap /= 2
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 2
	rheader.Cap /= 2
	lslice := *(*[]uint16)(unsafe.Pointer(&lheader))
	rslice := *(*[]uint16)(unsafe.Pointer(&rheader))
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = v + (mask & uint16(int16(mask)>>15))
	}
}

func (agg *Uint16Min) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Uint32Min) StateSize() uint8 {
	return 4
}

func (agg *Uint32Min) ResultSize() uint8 {
	return 4
}

func (agg *Uint32Min) Init(state []byte) {
	*(*uint32)(unsafe.Pointer(&state[0])) = math.MaxUint32
}

func (agg *Uint32Min) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint32Min) Aggregate(state, data []byte) {
	lhs := (*uint32)(unsafe.Pointer(&state[0]))
	rhs := *(*uint32)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = rhs + (mask & uint32(int32(mask)>>31))
}

func (agg *Uint32Min) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Uint32Min) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 4
	lheader.Cap /= 4
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 4
	rheader.Cap /= 4
	lslice := *(*[]uint32)(unsafe.Pointer(&lheader))
	rslice := *(*[]uint32)(unsafe.Pointer(&rheader))
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = v + (mask & uint32(int32(mask)>>31))
	}
}

func (agg *Uint32Min) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Uint64Min) StateSize() uint8 {
	return 8
}

func (agg *Uint64Min) ResultSize() uint8 {
	return 8
}

func (agg *Uint64Min) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = math.MaxUint64
}

func (agg *Uint64Min) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint64Min) Aggregate(state, data []byte) {
	lhs := (*uint64)(unsafe.Pointer(&state[0]))
	rhs := *(*uint64)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = rhs + (mask & uint64(int64(mask)>>63))
}

func (agg *Uint64Min) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Uint64Min) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 8
	lheader.Cap /= 8
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 8
	rheader.Cap /= 8
	lslice := *(*[]uint64)(unsafe.Pointer(&lheader))
	rslice := *(*[]uint64)(unsafe.Pointer(&rheader))
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = v + (mask & uint64(int64(mask)>>63))
	}
}

func (agg *Uint64Min) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Float32Min) StateSize() uint8 {
	return 4
}

func (agg *Float32Min) ResultSize() uint8 {
	return 4
}

func (agg *Float32Min) Init(state []byte) {
	*(*float32)(unsafe.Pointer(&state[0])) = float32(math.Inf(1))
}

func (agg *Float32Min) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Float32Min) Aggregate(state, data []byte) {
	lhs := (*float32)(unsafe.Pointer(&state[0]))
	rhs := *(*float32)(unsafe.Pointer(&data[0]))
	if *lhs > rhs {
		*lhs = rhs
	}
}

func (agg *Float32Min) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Float32Min) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 4
	lheader.Cap /= 4
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 4
	rheader.Cap /= 4
	lslice := *(*[]float32)(unsafe.Pointer(&lheader))
	rslice := *(*[]float32)(unsafe.Pointer(&rheader))
	for i, v := range rslice {
		if lslice[i] > v {
			lslice[i] = v
		}
	}
}

func (agg *Float32Min) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Float64Min) StateSize() uint8 {
	return 8
}

func (agg *Float64Min) ResultSize() uint8 {
	return 8
}

func (agg *Float64Min) Init(state []byte) {
	*(*float64)(unsafe.Pointer(&state[0])) = math.Inf(1)
}

func (agg *Float64Min) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Float64Min) Aggregate(state, data []byte) {
	lhs := (*float64)(unsafe.Pointer(&state[0]))
	rhs := *(*float64)(unsafe.Pointer(&data[0]))
	*lhs = math.Min(*lhs, rhs)
}

func (agg *Float64Min) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Float64Min) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 8
	lheader.Cap /= 8
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 8
	rheader.Cap /= 8
	lslice := *(*[]float64)(unsafe.Pointer(&lheader))
	rslice := *(*[]float64)(unsafe.Pointer(&rheader))
	for i, v := range rslice {
		lslice[i] = math.Min(lslice[i], v)
	}
}

func (agg *Float64Min) Finalize(state, result []byte) {
	copy(result, state)
}
