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
	case []types.Decimal:
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
