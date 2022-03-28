// Copyright 2022 Matrix Origin
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

package space

import (
	"bytes"
	"math"
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorize/sum"
)

func CountSpacesForUnsignedInt(originalVecCol interface{}) int64 {
	switch originalVecCol.(type) {
	case []uint8:
		return int64(sum.Uint8Sum(originalVecCol.([]uint8)))
	case []uint16:
		return int64(sum.Uint16Sum(originalVecCol.([]uint16)))
	case []uint32:
		return int64(sum.Uint32Sum(originalVecCol.([]uint32)))
	case []uint64:
		return int64(sum.Uint64Sum(originalVecCol.([]uint64)))
	default:
		return 0
	}
}

func CountSpacesForSignedInt(originalVecCol interface{}) int64 {
	var result int64

	switch originalVecCol.(type) {
	case []int8:
		result = sum.Int8Sum(originalVecCol.([]int8))
	case []int16:
		result = sum.Int16Sum(originalVecCol.([]int16))
	case []int32:
		result = sum.Int32Sum(originalVecCol.([]int32))
	case []int64:
		result = sum.Int64Sum(originalVecCol.([]int64))
	}

	if result < 0 {
		return 0
	} else {
		return result
	}
}

func CountSpacesForFloat(originalVecCol interface{}) int64 {
	var result int64

	switch originalVecCol.(type) {
	case []float32:
		for _, i := range originalVecCol.([]float32) {
			if i < 0 {
				continue
			}

			result += int64(math.Round(float64(i)))
		}
	case []float64:
		for _, i := range originalVecCol.([]float64) {
			if i < 0 {
				continue
			}

			result += int64(math.Round(i))
		}
	}

	return result
}

func parseStringAsInt64(s string) int64 {
	var result int64

	if len(s) == 0 {
		return 0
	}

	for _, i := range s {
		if i == ' ' || i == '\t' {
			continue
		}

		if !unicode.IsDigit(i) {
			break
		}

		result *= 10
		result += int64(i - '0')
	}

	return result
}

func CountSpacesForCharVarChar(originalVecCol *types.Bytes) int64 {
	var result int64

	for i, offset := range originalVecCol.Offsets {
		result += parseStringAsInt64(string(originalVecCol.Data[offset : offset+originalVecCol.Lengths[i]]))
	}

	return result
}

func FillSpacesUint8(originalVecCol []uint8, result *types.Bytes) *types.Bytes {
	var buf bytes.Buffer

	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset

		var j uint8
		for j = 0; j < length; j++ {
			buf.WriteRune(' ')
		}
		offset += uint32(length)
	}
	result.Data = buf.Bytes()

	return result
}

func FillSpacesUint16(originalVecCol []uint16, result *types.Bytes) *types.Bytes {
	var buf bytes.Buffer

	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset

		var j uint16
		for j = 0; j < length; j++ {
			buf.WriteRune(' ')
		}
		offset += uint32(length)
	}
	result.Data = buf.Bytes()

	return result
}

func FillSpacesUint32(originalVecCol []uint32, result *types.Bytes) *types.Bytes {
	var buf bytes.Buffer

	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset

		var j uint32
		for j = 0; j < length; j++ {
			buf.WriteRune(' ')
		}
		offset += uint32(length)
	}
	result.Data = buf.Bytes()

	return result
}

func FillSpacesUint64(originalVecCol []uint64, result *types.Bytes) *types.Bytes {
	var buf bytes.Buffer

	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset

		var j uint64
		for j = 0; j < length; j++ {
			buf.WriteRune(' ')
		}
		offset += uint32(length)
	}
	result.Data = buf.Bytes()

	return result
}

func FillSpacesInt8(originalVecCol []int8, result *types.Bytes) *types.Bytes {
	var buf bytes.Buffer

	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset

		var j int8
		for j = 0; j < length; j++ {
			buf.WriteRune(' ')
		}
		offset += uint32(length)
	}
	result.Data = buf.Bytes()

	return result
}

func FillSpacesInt16(originalVecCol []int16, result *types.Bytes) *types.Bytes {
	var buf bytes.Buffer

	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset

		var j int16
		for j = 0; j < length; j++ {
			buf.WriteRune(' ')
		}
		offset += uint32(length)
	}
	result.Data = buf.Bytes()

	return result
}

func FillSpacesInt32(originalVecCol []int32, result *types.Bytes) *types.Bytes {
	var buf bytes.Buffer

	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset

		var j int32
		for j = 0; j < length; j++ {
			buf.WriteRune(' ')
		}
		offset += uint32(length)
	}
	result.Data = buf.Bytes()

	return result
}

func FillSpacesInt64(originalVecCol []int64, result *types.Bytes) *types.Bytes {
	var buf bytes.Buffer

	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset

		var j int64
		for j = 0; j < length; j++ {
			buf.WriteRune(' ')
		}
		offset += uint32(length)
	}
	result.Data = buf.Bytes()

	return result
}

func FillSpacesFloat32(originalVecCol []float32, result *types.Bytes) *types.Bytes {
	var buf bytes.Buffer

	var offset uint32 = 0
	for i, length := range originalVecCol {
		roundLen := math.Round(float64(length))

		result.Lengths[i] = uint32(roundLen)
		result.Offsets[i] = offset

		var j int32
		for j = 0; j < int32(roundLen); j++ {
			buf.WriteRune(' ')
		}
		offset += uint32(roundLen)
	}
	result.Data = buf.Bytes()

	return result
}

func FillSpacesFloat64(originalVecCol []float64, result *types.Bytes) *types.Bytes {
	var buf bytes.Buffer

	var offset uint32 = 0
	for i, length := range originalVecCol {
		roundLen := math.Round(length)

		result.Lengths[i] = uint32(roundLen)
		result.Offsets[i] = offset

		var j int32
		for j = 0; j < int32(roundLen); j++ {
			buf.WriteRune(' ')
		}
		offset += uint32(roundLen)
	}
	result.Data = buf.Bytes()

	return result
}

func FillSpacesCharVarChar(originalVecCol, result *types.Bytes) *types.Bytes {
	var buf bytes.Buffer

	var bytesWriten uint32 = 0
	for i, offset := range originalVecCol.Offsets {
		length := parseStringAsInt64(string(originalVecCol.Data[offset : offset+originalVecCol.Lengths[i]]))

		result.Lengths[i] = uint32(length)
		result.Offsets[i] = uint32(bytesWriten)

		var j int64
		for j = 0; j < length; j++ {
			buf.WriteRune(' ')
		}
		bytesWriten += uint32(length)
	}
	result.Data = buf.Bytes()

	return result
}
