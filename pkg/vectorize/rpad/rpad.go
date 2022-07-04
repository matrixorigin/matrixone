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

package rpad

import (
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorize/typecast"
)

var (
	Rpad   func(int, *types.Bytes, interface{}, interface{}, []bool, []*nulls.Nulls) (*types.Bytes, *nulls.Nulls, error)
	MaxPad int64
)

const UINT16_MAX = ^uint16(0)

func init() {
	Rpad = rpad
	MaxPad = int64(16 * 1024 * 1024)
}

// rpad returns a *types.Bytes containing the padded strings and a corresponding bitmap *nulls.Nulls.
// rpad is multibyte-safe
func rpad(rowCount int, strs *types.Bytes, sizes interface{}, pads interface{}, isConst []bool, oriNsp []*nulls.Nulls) (*types.Bytes, *nulls.Nulls, error) {
	// typecast
	var padstrs = &types.Bytes{}
	var err error
	switch pd := pads.(type) {
	case *types.Bytes:
		padstrs = pd
	case []int64:
		_, err = typecast.Int64ToBytes(pd, padstrs)
	case []int32:
		_, err = typecast.Int32ToBytes(pd, padstrs)
	case []int16:
		_, err = typecast.Int16ToBytes(pd, padstrs)
	case []int8:
		_, err = typecast.Int8ToBytes(pd, padstrs)
	case []uint64:
		_, err = typecast.Uint64ToBytes(pd, padstrs)
	case []uint32:
		_, err = typecast.Uint32ToBytes(pd, padstrs)
	case []uint16:
		_, err = typecast.Uint16ToBytes(pd, padstrs)
	case []uint8:
		_, err = typecast.Uint8ToBytes(pd, padstrs)
	case []float32:
		_, err = typecast.Float32ToBytes(pd, padstrs)
	case []float64:
		_, err = typecast.Float64ToBytes(pd, padstrs)
	default:
		// empty string
		padstrs = &types.Bytes{
			Lengths: make([]uint32, 1),
			Offsets: make([]uint32, 1),
		}
		isConst[2] = true
	}
	if err != nil {
		return nil, nil, err
	}

	// do rpad
	var result *types.Bytes
	var nsp *nulls.Nulls
	var err2 error
	switch sz := sizes.(type) {
	case []int64:
		result, nsp = rpadInt64(rowCount, strs, sz, padstrs, isConst, oriNsp)
	case []int32:
		sizesInt64 := make([]int64, len(sz))
		sizesInt64, err2 = typecast.Int32ToInt64(sz, sizesInt64)
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp)
	case []int16:
		sizesInt64 := make([]int64, len(sz))
		sizesInt64, err2 = typecast.Int16ToInt64(sz, sizesInt64)
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp)
	case []int8:
		sizesInt64 := make([]int64, len(sz))
		sizesInt64, err2 = typecast.Int8ToInt64(sz, sizesInt64)
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp)
	case []float64:
		sizesInt64 := make([]int64, len(sz))
		isEmptyStringOrNull := make([]int, len(sz))
		sizesInt64, err2 = typecast.Float64ToInt64(sz, sizesInt64, isEmptyStringOrNull)
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp, isEmptyStringOrNull)
	case []float32:
		sizesInt64 := make([]int64, len(sz))
		sizesInt64, err2 = typecast.Float32ToInt64(sz, sizesInt64)
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp)
	case []uint64:
		result, nsp = rpadUint64(rowCount, strs, sz, padstrs, isConst, oriNsp)
	case []uint32:
		sizesUint64 := make([]uint64, len(sz))
		sizesUint64, err2 = typecast.Uint32ToUint64(sz, sizesUint64)
		result, nsp = rpadUint64(rowCount, strs, sizesUint64, padstrs, isConst, oriNsp)
	case []uint16:
		sizesUint64 := make([]uint64, len(sz))
		sizesUint64, err2 = typecast.Uint16ToUint64(sz, sizesUint64)
		result, nsp = rpadUint64(rowCount, strs, sizesUint64, padstrs, isConst, oriNsp)
	case []uint8:
		sizesUint64 := make([]uint64, len(sz))
		sizesUint64, err2 = typecast.Uint8ToUint64(sz, sizesUint64)
		result, nsp = rpadUint64(rowCount, strs, sizesUint64, padstrs, isConst, oriNsp)
	case *types.Bytes:
		sizesFloat64 := make([]float64, len(sz.Lengths))
		isEmptyStringOrNull := make([]int, len(sz.Lengths))
		sizesFloat64, err2 = typecast.BytesToFloat(sz, sizesFloat64, isEmptyStringOrNull)
		sizesInt64 := make([]int64, len(sz.Lengths))
		for i, val := range sizesFloat64 { //for func rpad,like '1.8', is 1, not 2.
			sizesInt64[i] = int64(math.Floor(val))
		}
		result, nsp = rpadInt64(rowCount, strs, sizesInt64, padstrs, isConst, oriNsp, isEmptyStringOrNull)
	default:
		// return empty strings if sizes is a non-numerical type slice
		nsp = new(nulls.Nulls)
		nulls.Set(nsp, oriNsp[0])
		result = &types.Bytes{
			Offsets: make([]uint32, len(strs.Lengths)),
			Lengths: make([]uint32, len(strs.Lengths)),
		}
	}
	if err2 != nil {
		return nil, nil, err2
	}
	return result, nsp, nil
}

//note that: for flag:
//0: nothing todo
//1: is an overflow flag
//2: is an parse_error flag
func rpadInt64(rowCount int, strs *types.Bytes, sizes []int64, padstrs *types.Bytes, isConst []bool, oriNsp []*nulls.Nulls, isEmptyStringOrNull ...[]int) (*types.Bytes, *nulls.Nulls) {
	results := &types.Bytes{}
	resultNsp := new(nulls.Nulls)
	usedEmptyStringOrNull := len(isEmptyStringOrNull) > 0
	for i := 0; i < rowCount; i++ {
		var newSize int64
		var EmptyStringOrNull int //we use flag1 to see if we need to give "" but not NULL
		if isConst[1] {
			if usedEmptyStringOrNull {
				EmptyStringOrNull = isEmptyStringOrNull[0][0]
			}
			// accepts a constant literal
			newSize = sizes[0]
		} else {
			if usedEmptyStringOrNull {
				EmptyStringOrNull = isEmptyStringOrNull[0][i]
			}
			// accepts an attribute name
			newSize = sizes[i]
		}
		if EmptyStringOrNull == 2 {
			results.Offsets = append(results.Offsets, uint32(len(results.Data)))
			results.Lengths = append(results.Lengths, 0)
			continue
		}
		// gets NULL if any arg is NULL or the newSize < 0
		if row := uint64(i); nulls.Contains(oriNsp[0], row) || nulls.Contains(oriNsp[1], row) || nulls.Contains(oriNsp[2], row) || newSize < 0 || newSize > int64(UINT16_MAX) || newSize > MaxPad {
			nulls.Add(resultNsp, row)
			results.Offsets = append(results.Offsets, uint32(len(results.Data)))
			results.Lengths = append(results.Lengths, 0)
			continue
		}

		var padRunes []rune
		if isConst[2] {
			padRunes = []rune(string(padstrs.Get(int64(0))))
		} else {
			padRunes = []rune(string(padstrs.Get(int64(i))))
		}
		var oriRunes []rune
		if isConst[0] {
			oriRunes = []rune(string(strs.Get(int64(0))))
		} else {
			oriRunes = []rune(string(strs.Get(int64(i))))
		}
		// gets the padded string
		if int(newSize) <= len(oriRunes) {
			// truncates the original string
			tmp := string(oriRunes[:newSize])
			results.Offsets = append(results.Offsets, uint32(len(results.Data)))
			results.Data = append(results.Data, tmp...)
			results.Lengths = append(results.Lengths, uint32(len(tmp)))
		} else {
			var tmp []byte
			if len(padRunes) == 0 {
				// gets an empty string if the padRunes is also an empty string and newSize > len(oriRunes)
				// E.x. in mysql 8.0
				// select rpad("test",5,"");
				// +-----------------+
				// |rpad("test",5,"")|
				// +-----------------+
				// |                 |
				// +-----------------+
				results.Offsets = append(results.Offsets, uint32(len(results.Data)))
				results.Lengths = append(results.Lengths, 0)
			} else {
				padding := int(newSize) - len(oriRunes)
				// builds a padded string
				tmp = make([]byte, 0, padding)
				if isConst[0] {
					tmp = append(tmp, strs.Get(int64(0))...)
				} else {
					tmp = append(tmp, strs.Get(int64(i))...)
				}
				// adds some pads
				for j := 0; j < padding/len(padRunes); j++ {
					tmp = append(tmp, string(padRunes)...)
				}
				// adds the remaining part
				tmp = append(tmp, string(padRunes[:padding%len(padRunes)])...)

				results.Offsets = append(results.Offsets, uint32(len(results.Data)))
				results.Data = append(results.Data, tmp...)
				results.Lengths = append(results.Lengths, uint32(len(tmp)))
			}
		}
	}
	return results, resultNsp
}

func rpadUint64(rowCount int, strs *types.Bytes, sizes []uint64, padstrs *types.Bytes, isConst []bool, oriNsp []*nulls.Nulls) (*types.Bytes, *nulls.Nulls) {
	results := &types.Bytes{}
	resultNsp := new(nulls.Nulls)
	for i := 0; i < rowCount; i++ {
		var newSize uint64
		if isConst[1] {
			// accepts a constant literal
			newSize = sizes[0]
		} else {
			// accepts an attribute name
			newSize = sizes[i]
		}
		// gets NULL if any arg is NULL or the newSize < 0
		if row := uint64(i); nulls.Contains(oriNsp[0], row) || nulls.Contains(oriNsp[1], row) || nulls.Contains(oriNsp[2], row) || newSize > uint64(UINT16_MAX) || newSize > uint64(MaxPad) {
			nulls.Add(resultNsp, row)
			results.Offsets = append(results.Offsets, uint32(len(results.Data)))
			results.Lengths = append(results.Lengths, 0)
			continue
		}

		var padRunes []rune
		if isConst[2] {
			padRunes = []rune(string(padstrs.Get(int64(0))))
		} else {
			padRunes = []rune(string(padstrs.Get(int64(i))))
		}
		var oriRunes []rune
		if isConst[0] {
			oriRunes = []rune(string(strs.Get(int64(0))))
		} else {
			oriRunes = []rune(string(strs.Get(int64(i))))
		}
		// gets the padded string
		if int(newSize) <= len(oriRunes) {
			// truncates the original string
			tmp := string(oriRunes[:newSize])
			results.Offsets = append(results.Offsets, uint32(len(results.Data)))
			results.Data = append(results.Data, tmp...)
			results.Lengths = append(results.Lengths, uint32(len(tmp)))
		} else {
			var tmp []byte
			if len(padRunes) == 0 {
				// gets an empty string if the padRunes is also an empty string and newSize > len(oriRunes)
				// E.x. in mysql 8.0
				// select rpad("test",5,"");
				// +-----------------+
				// |rpad("test",5,"")|
				// +-----------------+
				// |                 |
				// +-----------------+
				results.Offsets = append(results.Offsets, uint32(len(results.Data)))
				results.Lengths = append(results.Lengths, 0)
			} else {
				padding := int(newSize) - len(oriRunes)
				// builds a padded string
				tmp = make([]byte, 0, padding)
				if isConst[0] {
					tmp = append(tmp, strs.Get(int64(0))...)
				} else {
					tmp = append(tmp, strs.Get(int64(i))...)
				}
				// adds some pads
				for j := 0; j < padding/len(padRunes); j++ {
					tmp = append(tmp, string(padRunes)...)
				}
				// adds the remaining part
				tmp = append(tmp, string(padRunes[:padding%len(padRunes)])...)

				results.Offsets = append(results.Offsets, uint32(len(results.Data)))
				results.Data = append(results.Data, tmp...)
				results.Lengths = append(results.Lengths, uint32(len(tmp)))
			}
		}
	}
	return results, resultNsp
}
