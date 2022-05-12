// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import "github.com/matrixorigin/matrixone/pkg/container/types"

type RowGen interface {
	HasNext() bool
	Next() uint32
}

func InplaceDeleteRows(orig interface{}, rowGen RowGen) interface{} {
	if !rowGen.HasNext() {
		return orig
	}
	prevRow := -1
	currPos := 0

	switch arr := orig.(type) {
	case []int8:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []int16:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []int32:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []int64:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []uint8:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []uint16:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []uint32:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []uint64:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []types.Decimal64:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []types.Decimal128:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []float32:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []float64:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []types.Date:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	case []types.Datetime:
		for rowGen.HasNext() {
			currRow := int(rowGen.Next())
			copy(arr[currPos:], arr[prevRow+1:currRow])
			currPos += currRow - prevRow - 1
			prevRow = currRow
		}
		left := len(arr[prevRow+1:])
		copy(arr[currPos:], arr[prevRow+1:])
		currPos += left
		return arr[:currPos]
	}
	panic("not support")
}
