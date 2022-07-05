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
	SubstringFromLeftConstOffsetUnbounded  func(*types.Bytes, *types.Bytes, int64) *types.Bytes
	SubstringFromRightConstOffsetUnbounded func(*types.Bytes, *types.Bytes, int64) *types.Bytes
	SubstringFromZeroConstOffsetUnbounded  func(*types.Bytes, *types.Bytes) *types.Bytes
	SubstringFromZeroConstOffsetBounded    func(*types.Bytes, *types.Bytes) *types.Bytes
	SubstringDynamicOffsetUnbounded        func(*types.Bytes, *types.Bytes, interface{}, types.T) *types.Bytes
	SubstringFromLeftConstOffsetBounded    func(*types.Bytes, *types.Bytes, int64, int64) *types.Bytes
	SubstringFromRightConstOffsetBounded   func(*types.Bytes, *types.Bytes, int64, int64) *types.Bytes
	SubstringDynamicOffsetBounded          func(*types.Bytes, *types.Bytes, interface{}, types.T, interface{}, types.T, []bool) *types.Bytes
)

func init() {
	SubstringFromLeftConstOffsetUnbounded = substringFromLeftConstOffsetUnbounded
	SubstringFromRightConstOffsetUnbounded = substringFromRightConstOffsetUnbounded
	SubstringFromZeroConstOffsetUnbounded = substringFromZeroConstOffsetUnbounded
	SubstringDynamicOffsetUnbounded = substringDynamicOffsetUnbounded
	SubstringFromZeroConstOffsetBounded = substringFromZeroConstOffsetBounded
	SubstringFromLeftConstOffsetBounded = substringFromLeftConstOffsetBounded
	SubstringFromRightConstOffsetBounded = substringFromRightConstOffsetBounded
	SubstringDynamicOffsetBounded = substringDynamicOffsetBounded
}

//Slice from left to right, starting from 0
func getSliceFromLeft(bytes []byte, offset int64) ([]byte, int64) {
	sourceRune := []rune(string(bytes))
	elemsize := int64(len(sourceRune))
	if offset > elemsize {
		return []byte{}, 0
	}
	substrRune := sourceRune[offset:]
	substrSlice := []byte(string(substrRune))
	substrSliceLen := int64(len(substrSlice))
	return substrSlice, substrSliceLen
}

// Cut the slice with length from left to right, starting from 0
func getSliceFromLeftWithLength(bytes []byte, offset int64, length int64) ([]byte, int64) {
	sourceRune := []rune(string(bytes))
	elemsize := int64(len(sourceRune))
	if length < 0 {
		length = 0
	}
	if offset >= elemsize || length < 0 {
		return []byte{}, 0
	}
	substrRune := sourceRune[offset : offset+min(length, elemsize-offset)]
	substrSlice := []byte(string(substrRune))
	substrSliceLen := int64(len(substrSlice))
	return substrSlice, substrSliceLen
}

// Cut slices from right to left, starting from 1
func getSliceFromRight(bytes []byte, offset int64) ([]byte, int64) {
	sourceRune := []rune(string(bytes))
	elemsize := int64(len(sourceRune))
	if offset > elemsize {
		return []byte{}, 0
	}
	substrRune := sourceRune[elemsize-offset:]
	substrSlice := []byte(string(substrRune))
	substrSliceLen := int64(len(substrSlice))
	return substrSlice, substrSliceLen
}

// From right to left, cut the slice with length from 1
func getSliceFromRightWithLength(bytes []byte, offset int64, length int64) ([]byte, int64) {
	sourceRune := []rune(string(bytes))
	elemsize := int64(len(sourceRune))
	if length < 0 {
		length = 0
	}
	if length < 0 || offset < 0 {
		return []byte{}, 0
	}
	if offset > elemsize {
		return []byte{}, 0
	}
	substrRune := sourceRune[elemsize-offset : elemsize-offset+min(length, offset)]
	substrSlice := []byte(string(substrRune))
	substrSliceLen := int64(len(substrSlice))
	return substrSlice, substrSliceLen
}

//The length parameter is not bound. Cut the string from the left
func substringFromLeftConstOffsetUnbounded(src *types.Bytes, res *types.Bytes, start int64) *types.Bytes {
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
func substringFromRightConstOffsetUnbounded(src *types.Bytes, res *types.Bytes, start int64) *types.Bytes {
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
func substringFromZeroConstOffsetUnbounded(src *types.Bytes, res *types.Bytes) *types.Bytes {
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
func substringFromZeroConstOffsetBounded(src *types.Bytes, res *types.Bytes) *types.Bytes {
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
func substringDynamicOffsetUnbounded(src *types.Bytes, res *types.Bytes, startColumn interface{}, startColumnType types.T) *types.Bytes {
	var retCursor uint32
	for idx, offset := range src.Offsets {
		cursor := offset
		curLen := src.Lengths[idx]
		//get substring str parameter value of bytes
		bytes := src.Data[cursor : cursor+curLen]
		//get substring pos parameter value
		var startValue int64

		switch startColumnType {
		case types.T_uint8:
			startValue = int64(startColumn.([]uint8)[idx])
		case types.T_uint16:
			startValue = int64(startColumn.([]uint16)[idx])
		case types.T_uint32:
			startValue = int64(startColumn.([]uint32)[idx])
		case types.T_uint64:
			startValue = int64(startColumn.([]uint64)[idx])
		case types.T_int8:
			startValue = int64(startColumn.([]int8)[idx])
		case types.T_int16:
			startValue = int64(startColumn.([]int16)[idx])
		case types.T_int32:
			startValue = int64(startColumn.([]int32)[idx])
		case types.T_int64:
			startValue = startColumn.([]int64)[idx]
		default:
			startValue = int64(1)
		}

		if startValue > 0 {
			slice, size := getSliceFromLeft(bytes, startValue-1)
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
		} else if startValue < 0 {
			slice, size := getSliceFromRight(bytes, -startValue)
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
func substringFromLeftConstOffsetBounded(src *types.Bytes, res *types.Bytes, start int64, length int64) *types.Bytes {
	var retCursor uint32 = 0
	for idx, offset := range src.Offsets {
		cursor := offset
		curLen := src.Lengths[idx]

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
func substringFromRightConstOffsetBounded(src *types.Bytes, res *types.Bytes, start int64, length int64) *types.Bytes {
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
func substringDynamicOffsetBounded(src *types.Bytes, res *types.Bytes, startColumn interface{}, startColumnType types.T,
	lengthColumn interface{}, lengthColumnType types.T, cs []bool) *types.Bytes {
	var retCursor uint32
	for idx := range res.Offsets {
		var cursor uint32
		var curLen uint32
		var bytes []byte
		if cs[0] {
			cursor = src.Offsets[0]
			curLen = src.Lengths[0]
			//get substring str parameter value of bytes
			bytes = src.Data[cursor : cursor+curLen]
		} else {
			cursor = src.Offsets[0]
			curLen = src.Lengths[idx]
			//get substring str parameter value of bytes
			bytes = src.Data[cursor : cursor+curLen]
		}

		//get substring pos parameter value
		startValue := getColumnValue(startColumn, startColumnType, idx, cs[1])
		//get substring len parameter value
		lengthValue := getColumnValue(lengthColumn, lengthColumnType, idx, cs[2])

		if lengthValue < 0 {
			if startValue > 0 {
				lengthValue += int64(curLen) - (startValue - 1)
			} else {
				lengthValue += -startValue
			}
		}

		if startValue != 0 && lengthValue > 0 {
			var slice []byte
			var size int64
			if startValue > 0 {
				slice, size = getSliceFromLeftWithLength(bytes, startValue-1, lengthValue)
			} else {
				slice, size = getSliceFromRightWithLength(bytes, -startValue, lengthValue)
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

// get src string value of column by index
func getStrColumnValue(src *types.Bytes, cursor uint32, curLen uint32, isConstant bool) []byte {
	var dstValue []byte
	if isConstant {
		cursor0 := src.Offsets[0]
		curLen0 := src.Lengths[0]
		dstValue = src.Data[cursor0 : cursor0+curLen0]
	} else {
		dstValue = src.Data[cursor : cursor+curLen]
	}
	return dstValue
}

// get value of column by index
func getColumnValue(srcColumn interface{}, columnType types.T, idx int, isConsttant bool) int64 {
	var dstValue int64
	if isConsttant {
		idx = 0
	}
	switch columnType {
	case types.T_uint8:
		dstValue = int64(srcColumn.([]uint8)[idx])
	case types.T_uint16:
		dstValue = int64(srcColumn.([]uint16)[idx])
	case types.T_uint32:
		dstValue = int64(srcColumn.([]uint32)[idx])
	case types.T_uint64:
		dstValue = int64(srcColumn.([]uint64)[idx])
	case types.T_int8:
		dstValue = int64(srcColumn.([]int8)[idx])
	case types.T_int16:
		dstValue = int64(srcColumn.([]int16)[idx])
	case types.T_int32:
		dstValue = int64(srcColumn.([]int32)[idx])
	case types.T_int64:
		dstValue = srcColumn.([]int64)[idx]
	default:
		dstValue = int64(1)
	}
	return dstValue
}
