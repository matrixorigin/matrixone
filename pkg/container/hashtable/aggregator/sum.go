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

func (agg *Int8Sum) NeedsInit() bool {
	return false
}

func (agg *Int8Sum) Init(state unsafe.Pointer) {}

func (agg *Int8Sum) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		*(*int64)(s) += *(*int64)(values)
		values = unsafe.Add(values, 1)
	}
}

func (agg *Int8Sum) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		*(*int64)(s) += *(*int64)(rstates[i])
	}
}

func (agg *Int8Sum) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int64)(results[i]) += *(*int64)(s)
	}
}

func (agg *Int16Sum) StateSize() uint8 {
	return 8
}

func (agg *Int16Sum) ResultSize() uint8 {
	return 8
}

func (agg *Int16Sum) NeedsInit() bool {
	return false
}

func (agg *Int16Sum) Init(state unsafe.Pointer) {}

func (agg *Int16Sum) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		*(*int64)(s) += *(*int64)(values)
		values = unsafe.Add(values, 2)
	}
}

func (agg *Int16Sum) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		*(*int64)(s) += *(*int64)(rstates[i])
	}
}

func (agg *Int16Sum) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int64)(results[i]) += *(*int64)(s)
	}
}

func (agg *Int32Sum) StateSize() uint8 {
	return 8
}

func (agg *Int32Sum) ResultSize() uint8 {
	return 8
}

func (agg *Int32Sum) NeedsInit() bool {
	return false
}

func (agg *Int32Sum) Init(state unsafe.Pointer) {}

func (agg *Int32Sum) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		*(*int64)(s) += *(*int64)(values)
		values = unsafe.Add(values, 4)
	}
}

func (agg *Int32Sum) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		*(*int64)(s) += *(*int64)(rstates[i])
	}
}

func (agg *Int32Sum) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int64)(results[i]) += *(*int64)(s)
	}
}

func (agg *Int64Sum) StateSize() uint8 {
	return 8
}

func (agg *Int64Sum) ResultSize() uint8 {
	return 8
}

func (agg *Int64Sum) NeedsInit() bool {
	return false
}

func (agg *Int64Sum) Init(state unsafe.Pointer) {}

func (agg *Int64Sum) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		*(*int64)(s) += *(*int64)(values)
		values = unsafe.Add(values, 8)
	}
}

func (agg *Int64Sum) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		*(*int64)(s) += *(*int64)(rstates[i])
	}
}

func (agg *Int64Sum) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*int64)(results[i]) += *(*int64)(s)
	}
}

func (agg *Uint8Sum) StateSize() uint8 {
	return 8
}

func (agg *Uint8Sum) ResultSize() uint8 {
	return 8
}

func (agg *Uint8Sum) NeedsInit() bool {
	return false
}

func (agg *Uint8Sum) Init(state unsafe.Pointer) {}

func (agg *Uint8Sum) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		*(*uint64)(s) += *(*uint64)(values)
		values = unsafe.Add(values, 1)
	}
}

func (agg *Uint8Sum) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		*(*uint64)(s) += *(*uint64)(rstates[i])
	}
}

func (agg *Uint8Sum) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint64)(results[i]) += *(*uint64)(s)
	}
}

func (agg *Uint16Sum) StateSize() uint8 {
	return 8
}

func (agg *Uint16Sum) ResultSize() uint8 {
	return 8
}

func (agg *Uint16Sum) NeedsInit() bool {
	return false
}

func (agg *Uint16Sum) Init(state unsafe.Pointer) {}

func (agg *Uint16Sum) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		*(*uint64)(s) += *(*uint64)(values)
		values = unsafe.Add(values, 2)
	}
}

func (agg *Uint16Sum) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		*(*uint64)(s) += *(*uint64)(rstates[i])
	}
}

func (agg *Uint16Sum) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint64)(results[i]) += *(*uint64)(s)
	}
}

func (agg *Uint32Sum) StateSize() uint8 {
	return 8
}

func (agg *Uint32Sum) ResultSize() uint8 {
	return 8
}

func (agg *Uint32Sum) NeedsInit() bool {
	return false
}

func (agg *Uint32Sum) Init(state unsafe.Pointer) {}

func (agg *Uint32Sum) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		*(*uint64)(s) += *(*uint64)(values)
		values = unsafe.Add(values, 4)
	}
}

func (agg *Uint32Sum) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		*(*uint64)(s) += *(*uint64)(rstates[i])
	}
}

func (agg *Uint32Sum) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint64)(results[i]) += *(*uint64)(s)
	}
}

func (agg *Uint64Sum) StateSize() uint8 {
	return 8
}

func (agg *Uint64Sum) ResultSize() uint8 {
	return 8
}

func (agg *Uint64Sum) NeedsInit() bool {
	return false
}

func (agg *Uint64Sum) Init(state unsafe.Pointer) {}

func (agg *Uint64Sum) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		*(*uint64)(s) += *(*uint64)(values)
		values = unsafe.Add(values, 8)
	}
}

func (agg *Uint64Sum) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		*(*uint64)(s) += *(*uint64)(rstates[i])
	}
}

func (agg *Uint64Sum) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*uint64)(results[i]) += *(*uint64)(s)
	}
}

func (agg *Float32Sum) StateSize() uint8 {
	return 8
}

func (agg *Float32Sum) ResultSize() uint8 {
	return 8
}

func (agg *Float32Sum) NeedsInit() bool {
	return false
}

func (agg *Float32Sum) Init(state unsafe.Pointer) {}

func (agg *Float32Sum) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		*(*float64)(s) += *(*float64)(values)
		values = unsafe.Add(values, 4)
	}
}

func (agg *Float32Sum) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		*(*float64)(s) += *(*float64)(rstates[i])
	}
}

func (agg *Float32Sum) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*float64)(results[i]) += *(*float64)(s)
	}
}

func (agg *Float64Sum) StateSize() uint8 {
	return 8
}

func (agg *Float64Sum) ResultSize() uint8 {
	return 8
}

func (agg *Float64Sum) NeedsInit() bool {
	return false
}

func (agg *Float64Sum) Init(state unsafe.Pointer) {}

func (agg *Float64Sum) AddBatch(states []unsafe.Pointer, values unsafe.Pointer) {
	for _, s := range states {
		*(*float64)(s) += *(*float64)(values)
		values = unsafe.Add(values, 8)
	}
}

func (agg *Float64Sum) MergeBatch(lstates, rstates []unsafe.Pointer) {
	for i, s := range lstates {
		*(*float64)(s) += *(*float64)(rstates[i])
	}
}

func (agg *Float64Sum) Finalize(states, results []unsafe.Pointer) {
	for i, s := range states {
		*(*float64)(results[i]) += *(*float64)(s)
	}
}
