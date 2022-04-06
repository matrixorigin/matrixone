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

package substring

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
)

/*
Substring function rule description
*/

var (
	substringFromLeftConstOffsetUnbounded  func(*types.Bytes, *types.Bytes, int64) *types.Bytes
	substringFromRightConstOffsetUnbounded func(*types.Bytes, *types.Bytes, int64) *types.Bytes
	substringFromZeroConstOffsetUnbounded  func(*types.Bytes, *types.Bytes) *types.Bytes
	substringFromZeroConstOffsetBounded    func(*types.Bytes, *types.Bytes) *types.Bytes
	substringDynamicOffsetUnbounded        func(*types.Bytes, *types.Bytes, interface{}, types.T) *types.Bytes
	substringFromLeftConstOffsetBounded    func(*types.Bytes, *types.Bytes, int64, int64) *types.Bytes
	substringFromRightConstOffsetBounded   func(*types.Bytes, *types.Bytes, int64, int64) *types.Bytes
	substringDynamicOffsetBounded          func(*types.Bytes, *types.Bytes, interface{}, types.T, interface{}, types.T, []bool) *types.Bytes
)

func init() {
	substringFromLeftConstOffsetUnbounded = SliceFromLeftConstantOffsetUnbounded
	substringFromRightConstOffsetUnbounded = SliceFromRightConstantOffsetUnbounded
	substringFromZeroConstOffsetUnbounded = SliceFromZeroConstantOffsetUnbounded
	substringDynamicOffsetUnbounded = SliceDynamicOffsetUnbounded
	substringFromZeroConstOffsetBounded = SliceFromZeroConstantOffsetBounded
	substringFromLeftConstOffsetBounded = SliceFromLeftConstantOffsetBounded
	substringFromRightConstOffsetBounded = SliceFromRightConstantOffsetBounded
	substringDynamicOffsetBounded = SliceDynamicOffsetBounded
}

//Slice from left to right, starting from 0
func getSliceFromLeft(bytes []byte, offset int64) ([]byte, int64) {
	elemsize := int64(len(bytes))
	if offset > elemsize {
		return []byte{}, 0
	}
	return bytes[offset:], elemsize - offset
}

// Cut the slice with length from left to right, starting from 0
func getSliceFromLeftWithLength(bytes []byte, offset int64, length int64) ([]byte, int64) {
	elemsize := int64(len(bytes))

	if length < 0 {
		length += elemsize - offset
	}

	if offset >= elemsize || length < 0 {
		return []byte{}, 0
	}
	return bytes[offset : offset+min(length, elemsize-offset)], min(length, elemsize-offset)
}

// Cut slices from right to left, starting from 1
func getSliceFromRight(bytes []byte, offset int64) ([]byte, int64) {
	elemsize := int64(len(bytes))
	if offset > elemsize {
		return bytes[:], elemsize
	}
	return bytes[elemsize-offset:], offset
}

// From right to left, cut the slice with length from 1
func getSliceFromRightWithLength(bytes []byte, offset int64, length int64) ([]byte, int64) {
	elemsize := int64(len(bytes))
	if length < 0 {
		length += elemsize - offset
	}
	if length < 0 {
		return []byte{}, 0
	}

	if offset > elemsize {
		if length+elemsize > offset {
			return bytes[:min(elemsize, length+elemsize-offset)], min(elemsize, length+elemsize-offset)
		} else {
			return []byte{}, 0
		}
	}
	return bytes[elemsize-offset : elemsize-offset+min(length, offset)], min(length, offset)
}

//The length parameter is not bound. Cut the string from the left
func SliceFromLeftConstantOffsetUnbounded(src *types.Bytes, res *types.Bytes, start int64) *types.Bytes {
	var retCursor uint32 = 0
	for idx, offset := range src.Offsets {
		cursor := offset
		curLen := src.Lengths[idx]

		bytes := src.Data[cursor : cursor+curLen]

		slice, size := getSliceFromLeft(bytes, start)
		for _, b := range slice {
			res.Data[retCursor] = b
			retCursor++
		}
		if idx != 0 {
			res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
		} else {
			res.Offsets[idx] = uint32(0)
		}
		res.Lengths[idx] = uint32(size)
	}
	return res
}

//The length parameter is not bound. Cut the string from the right
func SliceFromRightConstantOffsetUnbounded(src *types.Bytes, res *types.Bytes, start int64) *types.Bytes {
	var retCursor uint32 = 0
	for idx, offset := range src.Offsets {
		cursor := offset
		curLen := src.Lengths[idx]

		bytes := src.Data[cursor : cursor+curLen]
		slice, size := getSliceFromRight(bytes, start)
		for _, b := range slice {
			res.Data[retCursor] = b
			retCursor++
		}
		if idx != 0 {
			res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
		} else {
			res.Offsets[idx] = uint32(0)
		}
		res.Lengths[idx] = uint32(size)
	}
	return res
}

//The length parameter is not bound. Cut the string from 0
func SliceFromZeroConstantOffsetUnbounded(src *types.Bytes, res *types.Bytes) *types.Bytes {
	for idx := range src.Offsets {
		if idx != 0 {
			res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
		} else {
			res.Offsets[idx] = uint32(0)
		}
		res.Lengths[idx] = uint32(0)
	}
	return res
}

//bound length parameter. Cut the string from 0
func SliceFromZeroConstantOffsetBounded(src *types.Bytes, res *types.Bytes) *types.Bytes {
	for idx := range src.Offsets {
		if idx != 0 {
			res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
		} else {
			res.Offsets[idx] = uint32(0)
		}
		res.Lengths[idx] = uint32(0)
	}
	return res
}

//Without binding the length parameter, dynamically cut the string
func SliceDynamicOffsetUnbounded(src *types.Bytes, res *types.Bytes, start_column interface{}, start_column_type types.T) *types.Bytes {
	var retCursor uint32
	for idx, offset := range src.Offsets {
		cursor := offset
		curLen := src.Lengths[idx]
		//get substring str parameter value of bytes
		bytes := src.Data[cursor : cursor+curLen]
		//get substring pos parameter value
		var start_value int64

		switch start_column_type {
		case types.T_uint8:
			start_value = int64(start_column.([]uint8)[idx])
		case types.T_uint16:
			start_value = int64(start_column.([]uint16)[idx])
		case types.T_uint32:
			start_value = int64(start_column.([]uint32)[idx])
		case types.T_uint64:
			start_value = int64(start_column.([]uint64)[idx])
		case types.T_int8:
			start_value = int64(start_column.([]int8)[idx])
		case types.T_int16:
			start_value = int64(start_column.([]int16)[idx])
		case types.T_int32:
			start_value = int64(start_column.([]int32)[idx])
		case types.T_int64:
			start_value = start_column.([]int64)[idx]
		default:
			start_value = int64(1)
		}

		if start_value > 0 {
			slice, size := getSliceFromLeft(bytes, start_value-1)
			for _, b := range slice {
				res.Data[retCursor] = b
				retCursor++
			}
			if idx != 0 {
				res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
			} else {
				res.Offsets[idx] = uint32(0)
			}
			res.Lengths[idx] = uint32(size)
		} else if start_value < 0 {
			slice, size := getSliceFromRight(bytes, -start_value)
			for _, b := range slice {
				res.Data[retCursor] = b
				retCursor++
			}
			if idx != 0 {
				res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
			} else {
				res.Offsets[idx] = uint32(0)
			}
			res.Lengths[idx] = uint32(size)
		} else {
			if idx != 0 {
				res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
			} else {
				res.Offsets[idx] = uint32(0)
			}
			res.Lengths[idx] = uint32(0)
		}
	}
	return res
}

//bound length parameter. Cut the string from left
func SliceFromLeftConstantOffsetBounded(src *types.Bytes, res *types.Bytes, start int64, length int64) *types.Bytes {
	var retCursor uint32 = 0
	for idx, offset := range src.Offsets {
		cursor := offset
		curLen := src.Lengths[idx]

		//获取当前行源字符串对应的byte切片
		bytes := src.Data[cursor : cursor+curLen]
		slice, size := getSliceFromLeftWithLength(bytes, start, length)
		for _, b := range slice {
			res.Data[retCursor] = b
			retCursor++
		}
		if idx != 0 {
			res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
		} else {
			res.Offsets[idx] = uint32(0)
		}
		res.Lengths[idx] = uint32(size)
	}
	return res
}

//bound length parameter. Cut the string from right
func SliceFromRightConstantOffsetBounded(src *types.Bytes, res *types.Bytes, start int64, length int64) *types.Bytes {
	var retCursor uint32 = 0
	for idx, offset := range src.Offsets {
		cursor := offset
		curLen := src.Lengths[idx]

		bytes := src.Data[cursor : cursor+curLen]
		slice, size := getSliceFromRightWithLength(bytes, start, length)
		for _, b := range slice {
			res.Data[retCursor] = b
			retCursor++
		}
		if idx != 0 {
			res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
		} else {
			res.Offsets[idx] = uint32(0)
		}
		res.Lengths[idx] = uint32(size)
	}
	return res
}

// bound the length parameter, dynamically cut the string
func SliceDynamicOffsetBounded(src *types.Bytes, res *types.Bytes, start_column interface{}, start_column_type types.T,
	length_column interface{}, length_column_type types.T, cs []bool) *types.Bytes {
	var retCursor uint32
	for idx, offset := range src.Offsets {
		cursor := offset
		curLen := src.Lengths[idx]
		//get substring str parameter value of bytes
		bytes := src.Data[cursor : cursor+curLen]

		//get substring pos parameter value
		start_value := getColumnValue(start_column, start_column_type, idx, cs[1])
		//get substring len parameter value
		length_value := getColumnValue(length_column, length_column_type, idx, cs[2])

		if length_value < 0 {
			if start_value > 0 {
				length_value += int64(curLen) - (start_value - 1)
			} else {
				length_value += -start_value
			}
		}

		if start_value != 0 && length_value > 0 {
			var slice []byte
			var size int64
			if start_value > 0 {
				slice, size = getSliceFromLeftWithLength(bytes, start_value-1, length_value)
			} else {
				slice, size = getSliceFromRightWithLength(bytes, -start_value, length_value)
			}
			for _, b := range slice {
				res.Data[retCursor] = b
				retCursor++
			}
			if idx != 0 {
				res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
			} else {
				res.Offsets[idx] = uint32(0)
			}
			res.Lengths[idx] = uint32(size)
		} else {
			if idx != 0 {
				res.Offsets[idx] = res.Offsets[idx-1] + res.Lengths[idx-1]
			} else {
				res.Offsets[idx] = uint32(0)
			}
			res.Lengths[idx] = uint32(0)
		}
	}
	return res
}

// get the min value of two int64 numbers
func min(x, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

// get value of column by index
func getColumnValue(src_column interface{}, column_type types.T, idx int, isConsttant bool) int64 {
	var dstValue int64
	if isConsttant {
		idx = 0
	}
	switch column_type {
	case types.T_uint8:
		dstValue = int64(src_column.([]uint8)[idx])
	case types.T_uint16:
		dstValue = int64(src_column.([]uint16)[idx])
	case types.T_uint32:
		dstValue = int64(src_column.([]uint32)[idx])
	case types.T_uint64:
		dstValue = int64(src_column.([]uint64)[idx])
	case types.T_int8:
		dstValue = int64(src_column.([]int8)[idx])
	case types.T_int16:
		dstValue = int64(src_column.([]int16)[idx])
	case types.T_int32:
		dstValue = int64(src_column.([]int32)[idx])
	case types.T_int64:
		dstValue = src_column.([]int64)[idx]
	default:
		dstValue = int64(1)
	}
	return dstValue
}
