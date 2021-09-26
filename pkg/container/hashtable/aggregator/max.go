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
	"unsafe"
)

type Int8Max struct{}
type Int16Max struct{}
type Int32Max struct{}
type Int64Max struct{}
type Uint8Max struct{}
type Uint16Max struct{}
type Uint32Max struct{}
type Uint64Max struct{}
type Float32Max struct{}
type Float64Max struct{}

func (agg *Int8Max) StateSize() uint8 {
	return 1
}

func (agg *Int8Max) ResultSize() uint8 {
	return 1
}

func (agg *Int8Max) Init(state, data []byte) {
	copy(state, data)
}

func (agg *Int8Max) ArrayInit(array []byte) {
	*(*int8)(unsafe.Pointer(&array[0])) = math.MinInt8
	for i := 1; i < len(array); i *= 2 {
		copy(array[i:], array[:i])
	}
}

func (agg *Int8Max) Aggregate(state, data []byte) {
	lhs := (*int8)(unsafe.Pointer(&state[0]))
	rhs := *(*int8)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = *lhs - (mask & (mask >> 7))
}

func (agg *Int8Max) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Int8Max) ArrayMerge(larray, rarray []byte) {
	lslice := unsafe.Slice((*int8)(unsafe.Pointer(&larray[0])), len(larray)/1)
	rslice := unsafe.Slice((*int8)(unsafe.Pointer(&rarray[0])), len(rarray)/1)
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = lslice[i] - (mask & (mask >> 7))
	}
}

func (agg *Int8Max) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Int16Max) StateSize() uint8 {
	return 2
}

func (agg *Int16Max) ResultSize() uint8 {
	return 2
}

func (agg *Int16Max) Init(state, data []byte) {
	copy(state, data)
}

func (agg *Int16Max) ArrayInit(array []byte) {
	*(*int16)(unsafe.Pointer(&array[0])) = math.MinInt16
	for i := 2; i < len(array); i *= 2 {
		copy(array[i:], array[:i])
	}
}

func (agg *Int16Max) Aggregate(state, data []byte) {
	lhs := (*int16)(unsafe.Pointer(&state[0]))
	rhs := *(*int16)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = *lhs - (mask & (mask >> 15))
}

func (agg *Int16Max) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Int16Max) ArrayMerge(larray, rarray []byte) {
	lslice := unsafe.Slice((*int16)(unsafe.Pointer(&larray[0])), len(larray)/2)
	rslice := unsafe.Slice((*int16)(unsafe.Pointer(&rarray[0])), len(rarray)/2)
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = lslice[i] - (mask & (mask >> 15))
	}
}

func (agg *Int16Max) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Int32Max) StateSize() uint8 {
	return 4
}

func (agg *Int32Max) ResultSize() uint8 {
	return 4
}

func (agg *Int32Max) Init(state, data []byte) {
	copy(state, data)
}

func (agg *Int32Max) ArrayInit(array []byte) {
	*(*int32)(unsafe.Pointer(&array[0])) = math.MinInt32
	for i := 4; i < len(array); i *= 2 {
		copy(array[i:], array[:i])
	}
}

func (agg *Int32Max) Aggregate(state, data []byte) {
	lhs := (*int32)(unsafe.Pointer(&state[0]))
	rhs := *(*int32)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = *lhs - (mask & (mask >> 31))
}

func (agg *Int32Max) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Int32Max) ArrayMerge(larray, rarray []byte) {
	lslice := unsafe.Slice((*int32)(unsafe.Pointer(&larray[0])), len(larray)/4)
	rslice := unsafe.Slice((*int32)(unsafe.Pointer(&rarray[0])), len(rarray)/4)
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = lslice[i] - (mask & (mask >> 31))
	}
}

func (agg *Int32Max) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Int64Max) StateSize() uint8 {
	return 8
}

func (agg *Int64Max) ResultSize() uint8 {
	return 8
}

func (agg *Int64Max) Init(state, data []byte) {
	copy(state, data)
}

func (agg *Int64Max) ArrayInit(array []byte) {
	*(*int64)(unsafe.Pointer(&array[0])) = math.MinInt64
	for i := 8; i < len(array); i *= 2 {
		copy(array[i:], array[:i])
	}
}

func (agg *Int64Max) Aggregate(state, data []byte) {
	lhs := (*int64)(unsafe.Pointer(&state[0]))
	rhs := *(*int64)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = *lhs - (mask & (mask >> 63))
}

func (agg *Int64Max) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Int64Max) ArrayMerge(larray, rarray []byte) {
	lslice := unsafe.Slice((*int64)(unsafe.Pointer(&larray[0])), len(larray)/8)
	rslice := unsafe.Slice((*int64)(unsafe.Pointer(&rarray[0])), len(rarray)/8)
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = lslice[i] - (mask & (mask >> 63))
	}
}

func (agg *Int64Max) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Uint8Max) StateSize() uint8 {
	return 1
}

func (agg *Uint8Max) ResultSize() uint8 {
	return 1
}

func (agg *Uint8Max) Init(state, data []byte) {
	copy(state, data)
}

func (agg *Uint8Max) ArrayInit(array []byte) {
	*(*uint8)(unsafe.Pointer(&array[0])) = 0
	for i := 1; i < len(array); i *= 2 {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint8Max) Aggregate(state, data []byte) {
	lhs := (*uint8)(unsafe.Pointer(&state[0]))
	rhs := *(*uint8)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = *lhs - (mask & uint8(int8(mask)>>7))
}

func (agg *Uint8Max) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Uint8Max) ArrayMerge(larray, rarray []byte) {
	lslice := unsafe.Slice((*uint8)(unsafe.Pointer(&larray[0])), len(larray)/1)
	rslice := unsafe.Slice((*uint8)(unsafe.Pointer(&rarray[0])), len(rarray)/1)
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = lslice[i] - (mask & uint8(int8(mask)>>7))
	}
}

func (agg *Uint8Max) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Uint16Max) StateSize() uint8 {
	return 2
}

func (agg *Uint16Max) ResultSize() uint8 {
	return 2
}

func (agg *Uint16Max) Init(state, data []byte) {
	copy(state, data)
}

func (agg *Uint16Max) ArrayInit(array []byte) {
	*(*uint16)(unsafe.Pointer(&array[0])) = 0
	for i := 2; i < len(array); i *= 2 {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint16Max) Aggregate(state, data []byte) {
	lhs := (*uint16)(unsafe.Pointer(&state[0]))
	rhs := *(*uint16)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = *lhs - (mask & uint16(int16(mask)>>15))
}

func (agg *Uint16Max) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Uint16Max) ArrayMerge(larray, rarray []byte) {
	lslice := unsafe.Slice((*uint16)(unsafe.Pointer(&larray[0])), len(larray)/2)
	rslice := unsafe.Slice((*uint16)(unsafe.Pointer(&rarray[0])), len(rarray)/2)
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = lslice[i] - (mask & uint16(int16(mask)>>15))
	}
}

func (agg *Uint16Max) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Uint32Max) StateSize() uint8 {
	return 4
}

func (agg *Uint32Max) ResultSize() uint8 {
	return 4
}

func (agg *Uint32Max) Init(state, data []byte) {
	copy(state, data)
}

func (agg *Uint32Max) ArrayInit(array []byte) {
	*(*uint32)(unsafe.Pointer(&array[0])) = 0
	for i := 4; i < len(array); i *= 2 {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint32Max) Aggregate(state, data []byte) {
	lhs := (*uint32)(unsafe.Pointer(&state[0]))
	rhs := *(*uint32)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = *lhs - (mask & uint32(int32(mask)>>31))
}

func (agg *Uint32Max) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Uint32Max) ArrayMerge(larray, rarray []byte) {
	lslice := unsafe.Slice((*uint32)(unsafe.Pointer(&larray[0])), len(larray)/4)
	rslice := unsafe.Slice((*uint32)(unsafe.Pointer(&rarray[0])), len(rarray)/4)
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = lslice[i] - (mask & uint32(int32(mask)>>31))
	}
}

func (agg *Uint32Max) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Uint64Max) StateSize() uint8 {
	return 8
}

func (agg *Uint64Max) ResultSize() uint8 {
	return 8
}

func (agg *Uint64Max) Init(state, data []byte) {
	copy(state, data)
}

func (agg *Uint64Max) ArrayInit(array []byte) {
	*(*uint64)(unsafe.Pointer(&array[0])) = 0
	for i := 8; i < len(array); i *= 2 {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint64Max) Aggregate(state, data []byte) {
	lhs := (*uint64)(unsafe.Pointer(&state[0]))
	rhs := *(*uint64)(unsafe.Pointer(&data[0]))
	mask := *lhs - rhs
	*lhs = *lhs - (mask & uint64(int64(mask)>>63))
}

func (agg *Uint64Max) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Uint64Max) ArrayMerge(larray, rarray []byte) {
	lslice := unsafe.Slice((*uint64)(unsafe.Pointer(&larray[0])), len(larray)/8)
	rslice := unsafe.Slice((*uint64)(unsafe.Pointer(&rarray[0])), len(rarray)/8)
	for i, v := range rslice {
		mask := lslice[i] - v
		lslice[i] = lslice[i] - (mask & uint64(int64(mask)>>63))
	}
}

func (agg *Uint64Max) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Float32Max) StateSize() uint8 {
	return 4
}

func (agg *Float32Max) ResultSize() uint8 {
	return 4
}

func (agg *Float32Max) Init(state, data []byte) {
	copy(state, data)
}

func (agg *Float32Max) ArrayInit(array []byte) {
	*(*float32)(unsafe.Pointer(&array[0])) = float32(math.Inf(-1))
	for i := 4; i < len(array); i *= 2 {
		copy(array[i:], array[:i])
	}
}

func (agg *Float32Max) Aggregate(state, data []byte) {
	lhs := (*float32)(unsafe.Pointer(&state[0]))
	rhs := *(*float32)(unsafe.Pointer(&data[0]))
	if *lhs < rhs {
		*lhs = rhs
	}
}

func (agg *Float32Max) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Float32Max) ArrayMerge(larray, rarray []byte) {
	lslice := unsafe.Slice((*float32)(unsafe.Pointer(&larray[0])), len(larray)/4)
	rslice := unsafe.Slice((*float32)(unsafe.Pointer(&rarray[0])), len(rarray)/4)
	for i, v := range rslice {
		if lslice[i] < v {
			lslice[i] = v
		}
	}
}

func (agg *Float32Max) Finalize(state, result []byte) {
	copy(result, state)
}

func (agg *Float64Max) StateSize() uint8 {
	return 8
}

func (agg *Float64Max) ResultSize() uint8 {
	return 8
}

func (agg *Float64Max) Init(state, data []byte) {
	copy(state, data)
}

func (agg *Float64Max) ArrayInit(array []byte) {
	*(*float64)(unsafe.Pointer(&array[0])) = math.Inf(-1)
	for i := 8; i < len(array); i *= 2 {
		copy(array[i:], array[:i])
	}
}

func (agg *Float64Max) Aggregate(state, data []byte) {
	lhs := (*float64)(unsafe.Pointer(&state[0]))
	rhs := *(*float64)(unsafe.Pointer(&data[0]))
	if *lhs < rhs {
		*lhs = rhs
	}
}

func (agg *Float64Max) Merge(lstate, rstate []byte) {
	agg.Aggregate(lstate, rstate)
}

func (agg *Float64Max) ArrayMerge(larray, rarray []byte) {
	lslice := unsafe.Slice((*float64)(unsafe.Pointer(&larray[0])), len(larray)/8)
	rslice := unsafe.Slice((*float64)(unsafe.Pointer(&rarray[0])), len(rarray)/8)
	for i, v := range rslice {
		if lslice[i] < v {
			lslice[i] = v
		}
	}
}

func (agg *Float64Max) Finalize(state, result []byte) {
	copy(result, state)
}
