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

package compute

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

func InplaceDeleteRowsFromSlice[T types.FixedSizeT](v any, rowGen common.RowGen) any {
	if !rowGen.HasNext() {
		return v
	}
	slice := v.([]T)
	prevRow := -1
	currPos := 0
	for rowGen.HasNext() {
		currRow := int(rowGen.Next())
		copy(slice[currPos:], slice[prevRow+1:currRow])
		currPos += currRow - prevRow - 1
		prevRow = currRow
	}
	left := len(slice[prevRow+1:])
	copy(slice[currPos:], slice[prevRow+1:])
	currPos += left
	return slice[:currPos]
}

func InplaceDeleteRows(orig any, rowGen common.RowGen) any {
	if !rowGen.HasNext() {
		return orig
	}

	switch arr := orig.(type) {
	case []bool:
		return InplaceDeleteRowsFromSlice[bool](arr, rowGen)
	case []int8:
		return InplaceDeleteRowsFromSlice[int8](arr, rowGen)
	case []int16:
		return InplaceDeleteRowsFromSlice[int16](arr, rowGen)
	case []int32:
		return InplaceDeleteRowsFromSlice[int32](arr, rowGen)
	case []int64:
		return InplaceDeleteRowsFromSlice[int64](arr, rowGen)
	case []uint8:
		return InplaceDeleteRowsFromSlice[uint8](arr, rowGen)
	case []uint16:
		return InplaceDeleteRowsFromSlice[uint16](arr, rowGen)
	case []uint32:
		return InplaceDeleteRowsFromSlice[uint32](arr, rowGen)
	case []uint64:
		return InplaceDeleteRowsFromSlice[uint64](arr, rowGen)
	case []types.Timestamp:
		return InplaceDeleteRowsFromSlice[types.Timestamp](arr, rowGen)
	case []types.Decimal64:
		return InplaceDeleteRowsFromSlice[types.Decimal64](arr, rowGen)
	case []types.Decimal128:
		return InplaceDeleteRowsFromSlice[types.Decimal128](arr, rowGen)
	case []float32:
		return InplaceDeleteRowsFromSlice[float32](arr, rowGen)
	case []float64:
		return InplaceDeleteRowsFromSlice[float64](arr, rowGen)
	case []types.Date:
		return InplaceDeleteRowsFromSlice[types.Date](arr, rowGen)
	case []types.Datetime:
		return InplaceDeleteRowsFromSlice[types.Datetime](arr, rowGen)
	}
	panic("not support")
}
