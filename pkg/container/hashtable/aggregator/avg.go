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

func (agg *Int8Avg) NeedsInit() bool {
	return false
}

func (agg *Int8Avg) Init(state unsafe.Pointer) {}

func (agg *Int8Avg) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int8AvgState)(s)
		sImpl.sum += int64(*(*int8)(values))
		sImpl.cnt++
		values = unsafe.Add(values, 1)
	}
}

func (agg *Int8Avg) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int8AvgState)(s)
		rsImpl := (*int8AvgState)(rstates[i])
		sImpl.sum += rsImpl.sum
		sImpl.cnt += rsImpl.cnt
	}
}

func (agg *Int8Avg) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		sImpl := (*int8AvgState)(s)
		*(*float64)(results[i]) = float64(sImpl.sum) / float64(sImpl.cnt)
	}
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

func (agg *Int16Avg) NeedsInit() bool {
	return false
}

func (agg *Int16Avg) Init(state unsafe.Pointer) {}

func (agg *Int16Avg) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int16AvgState)(s)
		sImpl.sum += int64(*(*int16)(values))
		sImpl.cnt++
		values = unsafe.Add(values, 2)
	}
}

func (agg *Int16Avg) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int16AvgState)(s)
		rsImpl := (*int16AvgState)(rstates[i])
		sImpl.sum += rsImpl.sum
		sImpl.cnt += rsImpl.cnt
	}
}

func (agg *Int16Avg) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		sImpl := (*int16AvgState)(s)
		*(*float64)(results[i]) = float64(sImpl.sum) / float64(sImpl.cnt)
	}
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

func (agg *Int32Avg) NeedsInit() bool {
	return false
}

func (agg *Int32Avg) Init(state unsafe.Pointer) {}

func (agg *Int32Avg) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int32AvgState)(s)
		sImpl.sum += int64(*(*int32)(values))
		sImpl.cnt++
		values = unsafe.Add(values, 4)
	}
}

func (agg *Int32Avg) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int32AvgState)(s)
		rsImpl := (*int32AvgState)(rstates[i])
		sImpl.sum += rsImpl.sum
		sImpl.cnt += rsImpl.cnt
	}
}

func (agg *Int32Avg) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		sImpl := (*int32AvgState)(s)
		*(*float64)(results[i]) = float64(sImpl.sum) / float64(sImpl.cnt)
	}
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

func (agg *Int64Avg) NeedsInit() bool {
	return false
}

func (agg *Int64Avg) Init(state unsafe.Pointer) {}

func (agg *Int64Avg) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*int64AvgState)(s)
		sImpl.sum += int64(*(*int64)(values))
		sImpl.cnt++
		values = unsafe.Add(values, 8)
	}
}

func (agg *Int64Avg) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*int64AvgState)(s)
		rsImpl := (*int64AvgState)(rstates[i])
		sImpl.sum += rsImpl.sum
		sImpl.cnt += rsImpl.cnt
	}
}

func (agg *Int64Avg) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		sImpl := (*int64AvgState)(s)
		*(*float64)(results[i]) = float64(sImpl.sum) / float64(sImpl.cnt)
	}
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

func (agg *Uint8Avg) NeedsInit() bool {
	return false
}

func (agg *Uint8Avg) Init(state unsafe.Pointer) {}

func (agg *Uint8Avg) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint8AvgState)(s)
		sImpl.sum += uint64(*(*uint8)(values))
		sImpl.cnt++
		values = unsafe.Add(values, 1)
	}
}

func (agg *Uint8Avg) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint8AvgState)(s)
		rsImpl := (*uint8AvgState)(rstates[i])
		sImpl.sum += rsImpl.sum
		sImpl.cnt += rsImpl.cnt
	}
}

func (agg *Uint8Avg) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		sImpl := (*uint8AvgState)(s)
		*(*float64)(results[i]) = float64(sImpl.sum) / float64(sImpl.cnt)
	}
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

func (agg *Uint16Avg) NeedsInit() bool {
	return false
}

func (agg *Uint16Avg) Init(state unsafe.Pointer) {}

func (agg *Uint16Avg) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint16AvgState)(s)
		sImpl.sum += uint64(*(*uint16)(values))
		sImpl.cnt++
		values = unsafe.Add(values, 2)
	}
}

func (agg *Uint16Avg) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint16AvgState)(s)
		rsImpl := (*uint16AvgState)(rstates[i])
		sImpl.sum += rsImpl.sum
		sImpl.cnt += rsImpl.cnt
	}
}

func (agg *Uint16Avg) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		sImpl := (*uint16AvgState)(s)
		*(*float64)(results[i]) = float64(sImpl.sum) / float64(sImpl.cnt)
	}
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

func (agg *Uint32Avg) NeedsInit() bool {
	return false
}

func (agg *Uint32Avg) Init(state unsafe.Pointer) {}

func (agg *Uint32Avg) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint32AvgState)(s)
		sImpl.sum += uint64(*(*uint32)(values))
		sImpl.cnt++
		values = unsafe.Add(values, 4)
	}
}

func (agg *Uint32Avg) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint32AvgState)(s)
		rsImpl := (*uint32AvgState)(rstates[i])
		sImpl.sum += rsImpl.sum
		sImpl.cnt += rsImpl.cnt
	}
}

func (agg *Uint32Avg) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		sImpl := (*uint32AvgState)(s)
		*(*float64)(results[i]) = float64(sImpl.sum) / float64(sImpl.cnt)
	}
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

func (agg *Uint64Avg) NeedsInit() bool {
	return false
}

func (agg *Uint64Avg) Init(state unsafe.Pointer) {}

func (agg *Uint64Avg) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*uint64AvgState)(s)
		sImpl.sum += uint64(*(*uint64)(values))
		sImpl.cnt++
		values = unsafe.Add(values, 8)
	}
}

func (agg *Uint64Avg) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*uint64AvgState)(s)
		rsImpl := (*uint64AvgState)(rstates[i])
		sImpl.sum += rsImpl.sum
		sImpl.cnt += rsImpl.cnt
	}
}

func (agg *Uint64Avg) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		sImpl := (*uint64AvgState)(s)
		*(*float64)(results[i]) = float64(sImpl.sum) / float64(sImpl.cnt)
	}
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

func (agg *Float32Avg) NeedsInit() bool {
	return false
}

func (agg *Float32Avg) Init(state unsafe.Pointer) {}

func (agg *Float32Avg) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*float32AvgState)(s)
		sImpl.sum += float64(*(*float32)(values))
		sImpl.cnt++
		values = unsafe.Add(values, 4)
	}
}

func (agg *Float32Avg) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*float32AvgState)(s)
		rsImpl := (*float32AvgState)(rstates[i])
		sImpl.sum += rsImpl.sum
		sImpl.cnt += rsImpl.cnt
	}
}

func (agg *Float32Avg) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		sImpl := (*float32AvgState)(s)
		*(*float64)(results[i]) = float64(sImpl.sum) / float64(sImpl.cnt)
	}
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

func (agg *Float64Avg) NeedsInit() bool {
	return false
}

func (agg *Float64Avg) Init(state unsafe.Pointer) {}

func (agg *Float64Avg) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		sImpl := (*float64AvgState)(s)
		sImpl.sum += float64(*(*float64)(values))
		sImpl.cnt++
		values = unsafe.Add(values, 8)
	}
}

func (agg *Float64Avg) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		sImpl := (*float64AvgState)(s)
		rsImpl := (*float64AvgState)(rstates[i])
		sImpl.sum += rsImpl.sum
		sImpl.cnt += rsImpl.cnt
	}
}

func (agg *Float64Avg) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		sImpl := (*float64AvgState)(s)
		*(*float64)(results[i]) = float64(sImpl.sum) / float64(sImpl.cnt)
	}
}
