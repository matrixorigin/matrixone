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

func (agg *Int8Max) NeedsInit() bool {
	return true
}

func (agg *Int8Max) Init(state unsafe.Pointer) {
	*(*int8)(state) = math.MinInt8
}

func (agg *Int8Max) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int8)(s)
		vImpl := *(*int8)(values)
		mask := *sImpl - vImpl
		*sImpl = *sImpl - (mask & int8(int8(mask)>>7))
		values = unsafe.Add(values, 1)
	}
}

func (agg *Int8Max) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int8)(s)
		rsImpl := *(*int8)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = *sImpl - (mask & int8(int8(mask)>>7))
	}
}

func (agg *Int8Max) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int8)(results[i]) += *(*int8)(s)
	}
}

func (agg *Int16Max) StateSize() uint8 {
	return 2
}

func (agg *Int16Max) ResultSize() uint8 {
	return 2
}

func (agg *Int16Max) NeedsInit() bool {
	return true
}

func (agg *Int16Max) Init(state unsafe.Pointer) {
	*(*int16)(state) = math.MinInt16
}

func (agg *Int16Max) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int16)(s)
		vImpl := *(*int16)(values)
		mask := *sImpl - vImpl
		*sImpl = *sImpl - (mask & int16(int16(mask)>>15))
		values = unsafe.Add(values, 2)
	}
}

func (agg *Int16Max) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int16)(s)
		rsImpl := *(*int16)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = *sImpl - (mask & int16(int16(mask)>>15))
	}
}

func (agg *Int16Max) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int16)(results[i]) += *(*int16)(s)
	}
}

func (agg *Int32Max) StateSize() uint8 {
	return 4
}

func (agg *Int32Max) ResultSize() uint8 {
	return 4
}

func (agg *Int32Max) NeedsInit() bool {
	return true
}

func (agg *Int32Max) Init(state unsafe.Pointer) {
	*(*int32)(state) = math.MinInt32
}

func (agg *Int32Max) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int32)(s)
		vImpl := *(*int32)(values)
		mask := *sImpl - vImpl
		*sImpl = *sImpl - (mask & int32(int32(mask)>>31))
		values = unsafe.Add(values, 4)
	}
}

func (agg *Int32Max) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int32)(s)
		rsImpl := *(*int32)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = *sImpl - (mask & int32(int32(mask)>>31))
	}
}

func (agg *Int32Max) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int32)(results[i]) += *(*int32)(s)
	}
}

func (agg *Int64Max) StateSize() uint8 {
	return 8
}

func (agg *Int64Max) ResultSize() uint8 {
	return 8
}

func (agg *Int64Max) NeedsInit() bool {
	return true
}

func (agg *Int64Max) Init(state unsafe.Pointer) {
	*(*int64)(state) = math.MinInt64
}

func (agg *Int64Max) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int64)(s)
		vImpl := *(*int64)(values)
		mask := *sImpl - vImpl
		*sImpl = *sImpl - (mask & int64(int64(mask)>>63))
		values = unsafe.Add(values, 8)
	}
}

func (agg *Int64Max) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int64)(s)
		rsImpl := *(*int64)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = *sImpl - (mask & int64(int64(mask)>>63))
	}
}

func (agg *Int64Max) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int64)(results[i]) += *(*int64)(s)
	}
}

func (agg *Uint8Max) StateSize() uint8 {
	return 1
}

func (agg *Uint8Max) ResultSize() uint8 {
	return 1
}

func (agg *Uint8Max) NeedsInit() bool {
	return false
}

func (agg *Uint8Max) Init(state unsafe.Pointer) {}

func (agg *Uint8Max) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint8)(s)
		vImpl := *(*uint8)(values)
		mask := *sImpl - vImpl
		*sImpl = *sImpl - (mask & uint8(int8(mask)>>7))
		values = unsafe.Add(values, 1)
	}
}

func (agg *Uint8Max) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint8)(s)
		rsImpl := *(*uint8)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = *sImpl - (mask & uint8(int8(mask)>>7))
	}
}

func (agg *Uint8Max) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint8)(results[i]) += *(*uint8)(s)
	}
}

func (agg *Uint16Max) StateSize() uint8 {
	return 2
}

func (agg *Uint16Max) ResultSize() uint8 {
	return 2
}

func (agg *Uint16Max) NeedsInit() bool {
	return false
}

func (agg *Uint16Max) Init(state unsafe.Pointer) {}

func (agg *Uint16Max) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint16)(s)
		vImpl := *(*uint16)(values)
		mask := *sImpl - vImpl
		*sImpl = *sImpl - (mask & uint16(int16(mask)>>15))
		values = unsafe.Add(values, 2)
	}
}

func (agg *Uint16Max) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint16)(s)
		rsImpl := *(*uint16)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = *sImpl - (mask & uint16(int16(mask)>>15))
	}
}

func (agg *Uint16Max) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint16)(results[i]) += *(*uint16)(s)
	}
}

func (agg *Uint32Max) StateSize() uint8 {
	return 4
}

func (agg *Uint32Max) ResultSize() uint8 {
	return 4
}

func (agg *Uint32Max) NeedsInit() bool {
	return false
}

func (agg *Uint32Max) Init(state unsafe.Pointer) {}

func (agg *Uint32Max) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint32)(s)
		vImpl := *(*uint32)(values)
		mask := *sImpl - vImpl
		*sImpl = *sImpl - (mask & uint32(int32(mask)>>31))
		values = unsafe.Add(values, 4)
	}
}

func (agg *Uint32Max) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint32)(s)
		rsImpl := *(*uint32)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = *sImpl - (mask & uint32(int32(mask)>>31))
	}
}

func (agg *Uint32Max) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint32)(results[i]) += *(*uint32)(s)
	}
}

func (agg *Uint64Max) StateSize() uint8 {
	return 8
}

func (agg *Uint64Max) ResultSize() uint8 {
	return 8
}

func (agg *Uint64Max) NeedsInit() bool {
	return false
}

func (agg *Uint64Max) Init(state unsafe.Pointer) {}

func (agg *Uint64Max) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint64)(s)
		vImpl := *(*uint64)(values)
		mask := *sImpl - vImpl
		*sImpl = *sImpl - (mask & uint64(int64(mask)>>63))
		values = unsafe.Add(values, 8)
	}
}

func (agg *Uint64Max) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint64)(s)
		rsImpl := *(*uint64)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = *sImpl - (mask & uint64(int64(mask)>>63))
	}
}

func (agg *Uint64Max) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint64)(results[i]) += *(*uint64)(s)
	}
}

func (agg *Float32Max) StateSize() uint8 {
	return 4
}

func (agg *Float32Max) ResultSize() uint8 {
	return 4
}

func (agg *Float32Max) NeedsInit() bool {
	return true
}

func (agg *Float32Max) Init(state unsafe.Pointer) {
	*(*float32)(state) = -math.MaxFloat32
}

func (agg *Float32Max) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*float32)(s)
		vImpl := *(*float32)(values)
		if vImpl > *sImpl {
			*sImpl = vImpl
		}
		values = unsafe.Add(values, 4)
	}
}

func (agg *Float32Max) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*float32)(s)
		rsImpl := *(*float32)(rstates[i])
		if rsImpl > *sImpl {
			*sImpl = rsImpl
		}
	}
}

func (agg *Float32Max) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*float32)(results[i]) += *(*float32)(s)
	}
}

func (agg *Float64Max) StateSize() uint8 {
	return 8
}

func (agg *Float64Max) ResultSize() uint8 {
	return 8
}

func (agg *Float64Max) NeedsInit() bool {
	return true
}

func (agg *Float64Max) Init(state unsafe.Pointer) {
	*(*float64)(state) = -math.MaxFloat64
}

func (agg *Float64Max) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*float64)(s)
		vImpl := *(*float64)(values)
		if vImpl > *sImpl {
			*sImpl = vImpl
		}
		values = unsafe.Add(values, 8)
	}
}

func (agg *Float64Max) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*float64)(s)
		rsImpl := *(*float64)(rstates[i])
		if rsImpl > *sImpl {
			*sImpl = rsImpl
		}
	}
}

func (agg *Float64Max) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*float64)(results[i]) += *(*float64)(s)
	}
}
