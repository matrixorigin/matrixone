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

func (agg *Int8Min) NeedsInit() bool {
	return true
}

func (agg *Int8Min) Init(state unsafe.Pointer) {
	*(*int8)(state) = math.MaxInt8
}

func (agg *Int8Min) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int8)(s)
		vImpl := *(*int8)(values)
		mask := *sImpl - vImpl
		*sImpl = vImpl + (mask & int8(int8(mask)>>7))
		values = unsafe.Add(values, 1)
	}
}

func (agg *Int8Min) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int8)(s)
		rsImpl := *(*int8)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = rsImpl + (mask & int8(int8(mask)>>7))
	}
}

func (agg *Int8Min) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int8)(results[i]) += *(*int8)(s)
	}
}

func (agg *Int16Min) StateSize() uint8 {
	return 2
}

func (agg *Int16Min) ResultSize() uint8 {
	return 2
}

func (agg *Int16Min) NeedsInit() bool {
	return true
}

func (agg *Int16Min) Init(state unsafe.Pointer) {
	*(*int16)(state) = math.MaxInt16
}

func (agg *Int16Min) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int16)(s)
		vImpl := *(*int16)(values)
		mask := *sImpl - vImpl
		*sImpl = vImpl + (mask & int16(int16(mask)>>15))
		values = unsafe.Add(values, 2)
	}
}

func (agg *Int16Min) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int16)(s)
		rsImpl := *(*int16)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = rsImpl + (mask & int16(int16(mask)>>15))
	}
}

func (agg *Int16Min) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int16)(results[i]) += *(*int16)(s)
	}
}

func (agg *Int32Min) StateSize() uint8 {
	return 4
}

func (agg *Int32Min) ResultSize() uint8 {
	return 4
}

func (agg *Int32Min) NeedsInit() bool {
	return true
}

func (agg *Int32Min) Init(state unsafe.Pointer) {
	*(*int32)(state) = math.MaxInt32
}

func (agg *Int32Min) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int32)(s)
		vImpl := *(*int32)(values)
		mask := *sImpl - vImpl
		*sImpl = vImpl + (mask & int32(int32(mask)>>31))
		values = unsafe.Add(values, 4)
	}
}

func (agg *Int32Min) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int32)(s)
		rsImpl := *(*int32)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = rsImpl + (mask & int32(int32(mask)>>31))
	}
}

func (agg *Int32Min) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int32)(results[i]) += *(*int32)(s)
	}
}

func (agg *Int64Min) StateSize() uint8 {
	return 8
}

func (agg *Int64Min) ResultSize() uint8 {
	return 8
}

func (agg *Int64Min) NeedsInit() bool {
	return true
}

func (agg *Int64Min) Init(state unsafe.Pointer) {
	*(*int64)(state) = math.MaxInt64
}

func (agg *Int64Min) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int64)(s)
		vImpl := *(*int64)(values)
		mask := *sImpl - vImpl
		*sImpl = vImpl + (mask & int64(int64(mask)>>63))
		values = unsafe.Add(values, 8)
	}
}

func (agg *Int64Min) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int64)(s)
		rsImpl := *(*int64)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = rsImpl + (mask & int64(int64(mask)>>63))
	}
}

func (agg *Int64Min) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int64)(results[i]) += *(*int64)(s)
	}
}

func (agg *Uint8Min) StateSize() uint8 {
	return 1
}

func (agg *Uint8Min) ResultSize() uint8 {
	return 1
}

func (agg *Uint8Min) NeedsInit() bool {
	return true
}

func (agg *Uint8Min) Init(state unsafe.Pointer) {
	*(*uint8)(state) = math.MaxUint8
}

func (agg *Uint8Min) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint8)(s)
		vImpl := *(*uint8)(values)
		mask := *sImpl - vImpl
		*sImpl = vImpl + (mask & uint8(int8(mask)>>7))
		values = unsafe.Add(values, 1)
	}
}

func (agg *Uint8Min) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint8)(s)
		rsImpl := *(*uint8)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = rsImpl + (mask & uint8(int8(mask)>>7))
	}
}

func (agg *Uint8Min) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint8)(results[i]) += *(*uint8)(s)
	}
}

func (agg *Uint16Min) StateSize() uint8 {
	return 2
}

func (agg *Uint16Min) ResultSize() uint8 {
	return 2
}

func (agg *Uint16Min) NeedsInit() bool {
	return true
}

func (agg *Uint16Min) Init(state unsafe.Pointer) {
	*(*uint16)(state) = math.MaxUint16
}

func (agg *Uint16Min) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint16)(s)
		vImpl := *(*uint16)(values)
		mask := *sImpl - vImpl
		*sImpl = vImpl + (mask & uint16(int16(mask)>>15))
		values = unsafe.Add(values, 2)
	}
}

func (agg *Uint16Min) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint16)(s)
		rsImpl := *(*uint16)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = rsImpl + (mask & uint16(int16(mask)>>15))
	}
}

func (agg *Uint16Min) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint16)(results[i]) += *(*uint16)(s)
	}
}

func (agg *Uint32Min) StateSize() uint8 {
	return 4
}

func (agg *Uint32Min) ResultSize() uint8 {
	return 4
}

func (agg *Uint32Min) NeedsInit() bool {
	return true
}

func (agg *Uint32Min) Init(state unsafe.Pointer) {
	*(*uint32)(state) = math.MaxUint32
}

func (agg *Uint32Min) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint32)(s)
		vImpl := *(*uint32)(values)
		mask := *sImpl - vImpl
		*sImpl = vImpl + (mask & uint32(int32(mask)>>31))
		values = unsafe.Add(values, 4)
	}
}

func (agg *Uint32Min) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint32)(s)
		rsImpl := *(*uint32)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = rsImpl + (mask & uint32(int32(mask)>>31))
	}
}

func (agg *Uint32Min) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint32)(results[i]) += *(*uint32)(s)
	}
}

func (agg *Uint64Min) StateSize() uint8 {
	return 8
}

func (agg *Uint64Min) ResultSize() uint8 {
	return 8
}

func (agg *Uint64Min) NeedsInit() bool {
	return true
}

func (agg *Uint64Min) Init(state unsafe.Pointer) {
	*(*uint64)(state) = math.MaxUint64
}

func (agg *Uint64Min) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint64)(s)
		vImpl := *(*uint64)(values)
		mask := *sImpl - vImpl
		*sImpl = vImpl + (mask & uint64(int64(mask)>>63))
		values = unsafe.Add(values, 8)
	}
}

func (agg *Uint64Min) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint64)(s)
		rsImpl := *(*uint64)(rstates[i])
		mask := *sImpl - rsImpl
		*sImpl = rsImpl + (mask & uint64(int64(mask)>>63))
	}
}

func (agg *Uint64Min) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint64)(results[i]) += *(*uint64)(s)
	}
}

func (agg *Float32Min) StateSize() uint8 {
	return 4
}

func (agg *Float32Min) ResultSize() uint8 {
	return 4
}

func (agg *Float32Min) NeedsInit() bool {
	return true
}

func (agg *Float32Min) Init(state unsafe.Pointer) {
	*(*float32)(state) = math.MaxFloat32
}

func (agg *Float32Min) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*float32)(s)
		vImpl := *(*float32)(values)
		if vImpl < *sImpl {
			*sImpl = vImpl
		}
		values = unsafe.Add(values, 4)
	}
}

func (agg *Float32Min) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*float32)(s)
		rsImpl := *(*float32)(rstates[i])
		if rsImpl < *sImpl {
			*sImpl = rsImpl
		}
	}
}

func (agg *Float32Min) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*float32)(results[i]) += *(*float32)(s)
	}
}

func (agg *Float64Min) StateSize() uint8 {
	return 8
}

func (agg *Float64Min) ResultSize() uint8 {
	return 8
}

func (agg *Float64Min) NeedsInit() bool {
	return true
}

func (agg *Float64Min) Init(state unsafe.Pointer) {
	*(*float64)(state) = math.MaxFloat64
}

func (agg *Float64Min) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*float64)(s)
		vImpl := *(*float64)(values)
		if vImpl < *sImpl {
			*sImpl = vImpl
		}
		values = unsafe.Add(values, 8)
	}
}

func (agg *Float64Min) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*float64)(s)
		rsImpl := *(*float64)(rstates[i])
		if rsImpl < *sImpl {
			*sImpl = rsImpl
		}
	}
}

func (agg *Float64Min) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*float64)(results[i]) += *(*float64)(s)
	}
}
