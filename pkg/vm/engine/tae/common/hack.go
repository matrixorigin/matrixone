package common

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
	}
	panic("not support")
}
