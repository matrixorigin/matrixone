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
	"reflect"
	"unsafe"
)

type Int8Avg struct{}
type Int16Avg struct{}
type Int32Avg struct{}
type Int64Avg struct{}
type Uint8Avg struct{}
type Uint16Avg struct{}
type Uint32Avg struct{}
type Uint64Avg struct{}
type Float32Avg struct{}
type Float64Avg struct{}

type int8AvgState struct {
	sum int64
	cnt uint64
}

func (agg *Int8Avg) StateSize() uint8 {
	return 16
}

func (agg *Int8Avg) ResultSize() uint8 {
	return 8
}

func (agg *Int8Avg) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
	*(*uint64)(unsafe.Pointer(&state[8])) = 0
}

func (agg *Int8Avg) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int8Avg) Aggregate(state, data []byte) {
	lhs := (*int64)(unsafe.Pointer(&state[0]))
	rhs := *(*int8)(unsafe.Pointer(&data[0]))
	*lhs += int64(rhs)
	*(*uint64)(unsafe.Pointer(&state[8]))++
}

func (agg *Int8Avg) Merge(lstate, rstate []byte) {
	*(*int64)(unsafe.Pointer(&lstate[0])) += *(*int64)(unsafe.Pointer(&rstate[0]))
	*(*uint64)(unsafe.Pointer(&lstate[8])) += *(*uint64)(unsafe.Pointer(&rstate[8]))
}

func (agg *Int8Avg) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 16
	lheader.Cap /= 16
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 16
	rheader.Cap /= 16
	lslice := *(*[]int8AvgState)(unsafe.Pointer(&lheader))
	rslice := *(*[]int8AvgState)(unsafe.Pointer(&rheader))
	for i := range lslice {
		lslice[i].sum += rslice[i].sum
		lslice[i].cnt += rslice[i].cnt
	}
}

func (agg *Int8Avg) Finalize(state, result []byte) {
	sum := *(*int64)(unsafe.Pointer(&state[0]))
	cnt := *(*uint64)(unsafe.Pointer(&state[8]))
	*(*float64)(unsafe.Pointer(&result[0])) = float64(sum) / float64(cnt)
}

type int16AvgState struct {
	sum int64
	cnt uint64
}

func (agg *Int16Avg) StateSize() uint8 {
	return 16
}

func (agg *Int16Avg) ResultSize() uint8 {
	return 8
}

func (agg *Int16Avg) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
	*(*uint64)(unsafe.Pointer(&state[8])) = 0
}

func (agg *Int16Avg) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int16Avg) Aggregate(state, data []byte) {
	lhs := (*int64)(unsafe.Pointer(&state[0]))
	rhs := *(*int16)(unsafe.Pointer(&data[0]))
	*lhs += int64(rhs)
	*(*uint64)(unsafe.Pointer(&state[8]))++
}

func (agg *Int16Avg) Merge(lstate, rstate []byte) {
	*(*int64)(unsafe.Pointer(&lstate[0])) += *(*int64)(unsafe.Pointer(&rstate[0]))
	*(*uint64)(unsafe.Pointer(&lstate[8])) += *(*uint64)(unsafe.Pointer(&rstate[8]))
}

func (agg *Int16Avg) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 16
	lheader.Cap /= 16
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 16
	rheader.Cap /= 16
	lslice := *(*[]int16AvgState)(unsafe.Pointer(&lheader))
	rslice := *(*[]int16AvgState)(unsafe.Pointer(&rheader))
	for i := range lslice {
		lslice[i].sum += rslice[i].sum
		lslice[i].cnt += rslice[i].cnt
	}
}

func (agg *Int16Avg) Finalize(state, result []byte) {
	sum := *(*int64)(unsafe.Pointer(&state[0]))
	cnt := *(*uint64)(unsafe.Pointer(&state[8]))
	*(*float64)(unsafe.Pointer(&result[0])) = float64(sum) / float64(cnt)
}

type int32AvgState struct {
	sum int64
	cnt uint64
}

func (agg *Int32Avg) StateSize() uint8 {
	return 16
}

func (agg *Int32Avg) ResultSize() uint8 {
	return 8
}

func (agg *Int32Avg) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
	*(*uint64)(unsafe.Pointer(&state[8])) = 0
}

func (agg *Int32Avg) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int32Avg) Aggregate(state, data []byte) {
	lhs := (*int64)(unsafe.Pointer(&state[0]))
	rhs := *(*int32)(unsafe.Pointer(&data[0]))
	*lhs += int64(rhs)
	*(*uint64)(unsafe.Pointer(&state[8]))++
}

func (agg *Int32Avg) Merge(lstate, rstate []byte) {
	*(*int64)(unsafe.Pointer(&lstate[0])) += *(*int64)(unsafe.Pointer(&rstate[0]))
	*(*uint64)(unsafe.Pointer(&lstate[8])) += *(*uint64)(unsafe.Pointer(&rstate[8]))
}

func (agg *Int32Avg) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 16
	lheader.Cap /= 16
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 16
	rheader.Cap /= 16
	lslice := *(*[]int32AvgState)(unsafe.Pointer(&lheader))
	rslice := *(*[]int32AvgState)(unsafe.Pointer(&rheader))
	for i := range lslice {
		lslice[i].sum += rslice[i].sum
		lslice[i].cnt += rslice[i].cnt
	}
}

func (agg *Int32Avg) Finalize(state, result []byte) {
	sum := *(*int64)(unsafe.Pointer(&state[0]))
	cnt := *(*uint64)(unsafe.Pointer(&state[8]))
	*(*float64)(unsafe.Pointer(&result[0])) = float64(sum) / float64(cnt)
}

type int64AvgState struct {
	sum int64
	cnt uint64
}

func (agg *Int64Avg) StateSize() uint8 {
	return 16
}

func (agg *Int64Avg) ResultSize() uint8 {
	return 8
}

func (agg *Int64Avg) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
	*(*uint64)(unsafe.Pointer(&state[8])) = 0
}

func (agg *Int64Avg) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Int64Avg) Aggregate(state, data []byte) {
	lhs := (*int64)(unsafe.Pointer(&state[0]))
	rhs := *(*int64)(unsafe.Pointer(&data[0]))
	*lhs += int64(rhs)
	*(*uint64)(unsafe.Pointer(&state[8]))++
}

func (agg *Int64Avg) Merge(lstate, rstate []byte) {
	*(*int64)(unsafe.Pointer(&lstate[0])) += *(*int64)(unsafe.Pointer(&rstate[0]))
	*(*uint64)(unsafe.Pointer(&lstate[8])) += *(*uint64)(unsafe.Pointer(&rstate[8]))
}

func (agg *Int64Avg) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 16
	lheader.Cap /= 16
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 16
	rheader.Cap /= 16
	lslice := *(*[]int64AvgState)(unsafe.Pointer(&lheader))
	rslice := *(*[]int64AvgState)(unsafe.Pointer(&rheader))
	for i := range lslice {
		lslice[i].sum += rslice[i].sum
		lslice[i].cnt += rslice[i].cnt
	}
}

func (agg *Int64Avg) Finalize(state, result []byte) {
	sum := *(*int64)(unsafe.Pointer(&state[0]))
	cnt := *(*uint64)(unsafe.Pointer(&state[8]))
	*(*float64)(unsafe.Pointer(&result[0])) = float64(sum) / float64(cnt)
}

type uint8AvgState struct {
	sum uint64
	cnt uint64
}

func (agg *Uint8Avg) StateSize() uint8 {
	return 16
}

func (agg *Uint8Avg) ResultSize() uint8 {
	return 8
}

func (agg *Uint8Avg) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
	*(*uint64)(unsafe.Pointer(&state[8])) = 0
}

func (agg *Uint8Avg) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint8Avg) Aggregate(state, data []byte) {
	lhs := (*uint64)(unsafe.Pointer(&state[0]))
	rhs := *(*uint8)(unsafe.Pointer(&data[0]))
	*lhs += uint64(rhs)
	*(*uint64)(unsafe.Pointer(&state[8]))++
}

func (agg *Uint8Avg) Merge(lstate, rstate []byte) {
	*(*uint64)(unsafe.Pointer(&lstate[0])) += *(*uint64)(unsafe.Pointer(&rstate[0]))
	*(*uint64)(unsafe.Pointer(&lstate[8])) += *(*uint64)(unsafe.Pointer(&rstate[8]))
}

func (agg *Uint8Avg) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 16
	lheader.Cap /= 16
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 16
	rheader.Cap /= 16
	lslice := *(*[]uint8AvgState)(unsafe.Pointer(&lheader))
	rslice := *(*[]uint8AvgState)(unsafe.Pointer(&rheader))
	for i := range lslice {
		lslice[i].sum += rslice[i].sum
		lslice[i].cnt += rslice[i].cnt
	}
}

func (agg *Uint8Avg) Finalize(state, result []byte) {
	sum := *(*uint64)(unsafe.Pointer(&state[0]))
	cnt := *(*uint64)(unsafe.Pointer(&state[8]))
	*(*float64)(unsafe.Pointer(&result[0])) = float64(sum) / float64(cnt)
}

type uint16AvgState struct {
	sum uint64
	cnt uint64
}

func (agg *Uint16Avg) StateSize() uint8 {
	return 16
}

func (agg *Uint16Avg) ResultSize() uint8 {
	return 8
}

func (agg *Uint16Avg) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
	*(*uint64)(unsafe.Pointer(&state[8])) = 0
}

func (agg *Uint16Avg) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint16Avg) Aggregate(state, data []byte) {
	lhs := (*uint64)(unsafe.Pointer(&state[0]))
	rhs := *(*uint16)(unsafe.Pointer(&data[0]))
	*lhs += uint64(rhs)
	*(*uint64)(unsafe.Pointer(&state[8]))++
}

func (agg *Uint16Avg) Merge(lstate, rstate []byte) {
	*(*uint64)(unsafe.Pointer(&lstate[0])) += *(*uint64)(unsafe.Pointer(&rstate[0]))
	*(*uint64)(unsafe.Pointer(&lstate[8])) += *(*uint64)(unsafe.Pointer(&rstate[8]))
}

func (agg *Uint16Avg) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 16
	lheader.Cap /= 16
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 16
	rheader.Cap /= 16
	lslice := *(*[]uint16AvgState)(unsafe.Pointer(&lheader))
	rslice := *(*[]uint16AvgState)(unsafe.Pointer(&rheader))
	for i := range lslice {
		lslice[i].sum += rslice[i].sum
		lslice[i].cnt += rslice[i].cnt
	}
}

func (agg *Uint16Avg) Finalize(state, result []byte) {
	sum := *(*uint64)(unsafe.Pointer(&state[0]))
	cnt := *(*uint64)(unsafe.Pointer(&state[8]))
	*(*float64)(unsafe.Pointer(&result[0])) = float64(sum) / float64(cnt)
}

type uint32AvgState struct {
	sum uint64
	cnt uint64
}

func (agg *Uint32Avg) StateSize() uint8 {
	return 16
}

func (agg *Uint32Avg) ResultSize() uint8 {
	return 8
}

func (agg *Uint32Avg) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
	*(*uint64)(unsafe.Pointer(&state[8])) = 0
}

func (agg *Uint32Avg) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint32Avg) Aggregate(state, data []byte) {
	lhs := (*uint64)(unsafe.Pointer(&state[0]))
	rhs := *(*uint32)(unsafe.Pointer(&data[0]))
	*lhs += uint64(rhs)
	*(*uint64)(unsafe.Pointer(&state[8]))++
}

func (agg *Uint32Avg) Merge(lstate, rstate []byte) {
	*(*uint64)(unsafe.Pointer(&lstate[0])) += *(*uint64)(unsafe.Pointer(&rstate[0]))
	*(*uint64)(unsafe.Pointer(&lstate[8])) += *(*uint64)(unsafe.Pointer(&rstate[8]))
}

func (agg *Uint32Avg) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 16
	lheader.Cap /= 16
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 16
	rheader.Cap /= 16
	lslice := *(*[]uint32AvgState)(unsafe.Pointer(&lheader))
	rslice := *(*[]uint32AvgState)(unsafe.Pointer(&rheader))
	for i := range lslice {
		lslice[i].sum += rslice[i].sum
		lslice[i].cnt += rslice[i].cnt
	}
}

func (agg *Uint32Avg) Finalize(state, result []byte) {
	sum := *(*uint64)(unsafe.Pointer(&state[0]))
	cnt := *(*uint64)(unsafe.Pointer(&state[8]))
	*(*float64)(unsafe.Pointer(&result[0])) = float64(sum) / float64(cnt)
}

type uint64AvgState struct {
	sum uint64
	cnt uint64
}

func (agg *Uint64Avg) StateSize() uint8 {
	return 16
}

func (agg *Uint64Avg) ResultSize() uint8 {
	return 8
}

func (agg *Uint64Avg) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
	*(*uint64)(unsafe.Pointer(&state[8])) = 0
}

func (agg *Uint64Avg) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Uint64Avg) Aggregate(state, data []byte) {
	lhs := (*uint64)(unsafe.Pointer(&state[0]))
	rhs := *(*uint64)(unsafe.Pointer(&data[0]))
	*lhs += uint64(rhs)
	*(*uint64)(unsafe.Pointer(&state[8]))++
}

func (agg *Uint64Avg) Merge(lstate, rstate []byte) {
	*(*uint64)(unsafe.Pointer(&lstate[0])) += *(*uint64)(unsafe.Pointer(&rstate[0]))
	*(*uint64)(unsafe.Pointer(&lstate[8])) += *(*uint64)(unsafe.Pointer(&rstate[8]))
}

func (agg *Uint64Avg) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 16
	lheader.Cap /= 16
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 16
	rheader.Cap /= 16
	lslice := *(*[]uint64AvgState)(unsafe.Pointer(&lheader))
	rslice := *(*[]uint64AvgState)(unsafe.Pointer(&rheader))
	for i := range lslice {
		lslice[i].sum += rslice[i].sum
		lslice[i].cnt += rslice[i].cnt
	}
}

func (agg *Uint64Avg) Finalize(state, result []byte) {
	sum := *(*uint64)(unsafe.Pointer(&state[0]))
	cnt := *(*uint64)(unsafe.Pointer(&state[8]))
	*(*float64)(unsafe.Pointer(&result[0])) = float64(sum) / float64(cnt)
}

type float32AvgState struct {
	sum float64
	cnt uint64
}

func (agg *Float32Avg) StateSize() uint8 {
	return 16
}

func (agg *Float32Avg) ResultSize() uint8 {
	return 8
}

func (agg *Float32Avg) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
	*(*uint64)(unsafe.Pointer(&state[8])) = 0
}

func (agg *Float32Avg) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Float32Avg) Aggregate(state, data []byte) {
	lhs := (*float64)(unsafe.Pointer(&state[0]))
	rhs := *(*float32)(unsafe.Pointer(&data[0]))
	*lhs += float64(rhs)
	*(*uint64)(unsafe.Pointer(&state[8]))++
}

func (agg *Float32Avg) Merge(lstate, rstate []byte) {
	*(*float64)(unsafe.Pointer(&lstate[0])) += *(*float64)(unsafe.Pointer(&rstate[0]))
	*(*uint64)(unsafe.Pointer(&lstate[8])) += *(*uint64)(unsafe.Pointer(&rstate[8]))
}

func (agg *Float32Avg) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 16
	lheader.Cap /= 16
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 16
	rheader.Cap /= 16
	lslice := *(*[]float32AvgState)(unsafe.Pointer(&lheader))
	rslice := *(*[]float32AvgState)(unsafe.Pointer(&rheader))
	for i := range lslice {
		lslice[i].sum += rslice[i].sum
		lslice[i].cnt += rslice[i].cnt
	}
}

func (agg *Float32Avg) Finalize(state, result []byte) {
	sum := *(*float64)(unsafe.Pointer(&state[0]))
	cnt := *(*uint64)(unsafe.Pointer(&state[8]))
	*(*float64)(unsafe.Pointer(&result[0])) = float64(sum) / float64(cnt)
}

type float64AvgState struct {
	sum float64
	cnt uint64
}

func (agg *Float64Avg) StateSize() uint8 {
	return 16
}

func (agg *Float64Avg) ResultSize() uint8 {
	return 8
}

func (agg *Float64Avg) Init(state []byte) {
	*(*uint64)(unsafe.Pointer(&state[0])) = 0
	*(*uint64)(unsafe.Pointer(&state[8])) = 0
}

func (agg *Float64Avg) ArrayInit(array []byte) {
	agg.Init(array)
	for i := int(agg.StateSize()); i < len(array); i++ {
		copy(array[i:], array[:i])
	}
}

func (agg *Float64Avg) Aggregate(state, data []byte) {
	lhs := (*float64)(unsafe.Pointer(&state[0]))
	rhs := *(*float64)(unsafe.Pointer(&data[0]))
	*lhs += float64(rhs)
	*(*uint64)(unsafe.Pointer(&state[8]))++
}

func (agg *Float64Avg) Merge(lstate, rstate []byte) {
	*(*float64)(unsafe.Pointer(&lstate[0])) += *(*float64)(unsafe.Pointer(&rstate[0]))
	*(*uint64)(unsafe.Pointer(&lstate[8])) += *(*uint64)(unsafe.Pointer(&rstate[8]))
}

func (agg *Float64Avg) ArrayMerge(larray, rarray []byte) {
	lheader := *(*reflect.SliceHeader)(unsafe.Pointer(&larray))
	lheader.Len /= 16
	lheader.Cap /= 16
	rheader := *(*reflect.SliceHeader)(unsafe.Pointer(&rarray))
	rheader.Len /= 16
	rheader.Cap /= 16
	lslice := *(*[]float64AvgState)(unsafe.Pointer(&lheader))
	rslice := *(*[]float64AvgState)(unsafe.Pointer(&rheader))
	for i := range lslice {
		lslice[i].sum += rslice[i].sum
		lslice[i].cnt += rslice[i].cnt
	}
}

func (agg *Float64Avg) Finalize(state, result []byte) {
	sum := *(*float64)(unsafe.Pointer(&state[0]))
	cnt := *(*uint64)(unsafe.Pointer(&state[8]))
	*(*float64)(unsafe.Pointer(&result[0])) = float64(sum) / float64(cnt)
}
