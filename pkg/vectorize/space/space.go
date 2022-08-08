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

// for input value greater than 8000, function space will output NULL,
// for input value less than 0, function space will output empty string ""
// for positive float inputs, function space will round(away from zero)

package space

import (
	"bytes"
	"fmt"
	"math"
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"golang.org/x/exp/constraints"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorize/sum"
)

var MaxAllowedValue = int64(8000)

// maximum 512mb
var TotalMaximumSpaceCount = int64(512 * 1024 * 1024)

var (
	errorSpaceCountExceedsThreshold = fmt.Errorf("total space count exceeds %d MB", TotalMaximumSpaceCount/1024/1024)
	errorSpaceCountExceedsMaxAllow  = fmt.Errorf("the space count exceeds maxallowedCount %d", MaxAllowedValue)
)

func CountSpacesForUnsignedInt(originalVecCol interface{}) int64 {
	switch col := originalVecCol.(type) {
	case []uint8:
		return int64(sum.Uint8Sum(col))
	case []uint16:
		return int64(sum.Uint16Sum(col))
	case []uint32:
		return int64(sum.Uint32Sum(col))
	case []uint64:
		return int64(sum.Uint64Sum(col))
	default:
		return 0
	}
}

func CountSpacesForSignedInt(originalVecCol interface{}) int64 {
	var result int64

	switch col := originalVecCol.(type) {
	case []int8:
		result = sum.Int8Sum(col)
	case []int16:
		result = sum.Int16Sum(col)
	case []int32:
		result = sum.Int32Sum(col)
	case []int64:
		result = sum.Int64Sum(col)
	}

	if result < 0 {
		return 0
	} else {
		return result
	}
}

type Result struct {
	Result *types.Bytes
	Nsp    *nulls.Nulls
}

func CountSpacesSigned[T constraints.Signed](columnValues []T) (int64, error) {
	result := int64(0)
	for _, columnValue := range columnValues {
		if columnValue <= 0 {
			continue
		} else if int64(columnValue) > MaxAllowedValue {
			return 0, errorSpaceCountExceedsMaxAllow
		} else {
			result += int64(columnValue)
			//t will not exceed TotalMaximumSpaceCount
			if result > TotalMaximumSpaceCount {
				return -1, errorSpaceCountExceedsThreshold
			}
		}
	}
	if result < 0 {
		return 0, errorSpaceCountExceedsThreshold
	} else {
		return result, nil
	}
}

func CountSpacesUnsigned[T constraints.Unsigned](columnValues []T) (int64, error) {
	result := uint64(0)
	for _, columnValue := range columnValues {
		if uint64(columnValue) > uint64(MaxAllowedValue) {
			return 0, errorSpaceCountExceedsMaxAllow
		} else {
			result += uint64(columnValue)
			//t will not exceed TotalMaximumSpaceCount
			if result > uint64(TotalMaximumSpaceCount) {
				return -1, errorSpaceCountExceedsThreshold
			}
		}
	}
	return int64(result), nil
}

func CountSpacesFloat[T constraints.Float](columnValues []T) (int64, error) {
	var result int64

	for _, columnValue := range columnValues {
		if columnValue < 0 {
			continue
		} else if int64(columnValue) > MaxAllowedValue {
			return 0, errorSpaceCountExceedsMaxAllow
		}
		result += int64(math.Round(float64(columnValue)))
		//t will not exceed TotalMaximumSpaceCount
		if result > TotalMaximumSpaceCount {
			return -1, errorSpaceCountExceedsThreshold
		}
	}
	if result < 0 {
		return 0, errorSpaceCountExceedsThreshold
	} else {
		return result, nil
	}
}

func FillSpacesUnsigned[T constraints.Unsigned](originalVecCol []T, resultBytes *types.Bytes) Result {
	result := Result{Result: resultBytes, Nsp: new(nulls.Nulls)}
	var offset uint32 = 0
	for i, length := range originalVecCol {
		if int64(length) > MaxAllowedValue {
			resultBytes.Lengths[i] = 0
			resultBytes.Offsets[i] = offset
			nulls.Add(result.Nsp, uint64(i))
		} else {
			resultBytes.Lengths[i] = uint32(length)
			resultBytes.Offsets[i] = offset
			offset += uint32(length)
		}
	}
	for i := range resultBytes.Data {
		resultBytes.Data[i] = ' '
	}
	return result
}

func FillSpacesSigned[T constraints.Signed](originalVecCol []T, resultBytes *types.Bytes) Result {
	result := Result{Result: resultBytes, Nsp: new(nulls.Nulls)}
	var offset uint32 = 0
	for i, length := range originalVecCol {
		if length <= 0 {
			resultBytes.Lengths[i] = 0
			resultBytes.Offsets[i] = offset
		} else if int64(length) > MaxAllowedValue {
			resultBytes.Lengths[i] = 0
			resultBytes.Offsets[i] = offset
			nulls.Add(result.Nsp, uint64(i))
		} else {
			resultBytes.Lengths[i] = uint32(length) // this cast is guaranteed safe because length > 0
			resultBytes.Offsets[i] = offset
			offset += uint32(length)
		}
	}
	for i := range resultBytes.Data {
		resultBytes.Data[i] = ' '
	}
	return result
}

func FillSpacesFloat[T constraints.Float](originalVecCol []T, resultBytes *types.Bytes) Result {
	result := Result{Result: resultBytes, Nsp: new(nulls.Nulls)}
	var offset uint32 = 0
	for i, length := range originalVecCol {
		roundLen := math.Round(float64(length))
		if roundLen <= 0 {
			resultBytes.Lengths[i] = 0
			resultBytes.Offsets[i] = offset
		} else if int64(length) > MaxAllowedValue {
			resultBytes.Lengths[i] = 0
			resultBytes.Offsets[i] = offset
			nulls.Add(result.Nsp, uint64(i))
		} else {
			resultBytes.Lengths[i] = uint32(roundLen)
			resultBytes.Offsets[i] = offset
			offset += uint32(roundLen)
		}
	}
	for i := range resultBytes.Data {
		resultBytes.Data[i] = ' '
	}
	return result
}

func CountSpacesForUint64(columnValues []uint64) int64 {
	return int64(sum.Uint64Sum(columnValues))
}
func CountSpacesForFloat[T constraints.Float](columnValues []T) int64 {
	var result int64

	for _, i := range columnValues {
		if i < 0 {
			continue
		}

		result += int64(math.Round(float64(i)))
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

func encodeStringSliceToTypeBytes(ss []string) *types.Bytes {
	var (
		offset uint32 = 0
		result        = &types.Bytes{
			Lengths: make([]uint32, len(ss)),
			Offsets: make([]uint32, len(ss)),
		}
		buf bytes.Buffer
	)

	for i, s := range ss {
		buf.WriteString(s)
		result.Lengths[i] = uint32(len(s))
		result.Offsets[i] = offset

		offset += uint32(len(s))
	}
	result.Data = buf.Bytes()

	return result
}

func FillSpacesUint8(originalVecCol []uint8, result *types.Bytes) *types.Bytes {
	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset
		offset += uint32(length)
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}

func FillSpacesUint16(originalVecCol []uint16, result *types.Bytes) *types.Bytes {
	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset
		offset += uint32(length)
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}

func FillSpacesUint32(originalVecCol []uint32, result *types.Bytes) *types.Bytes {
	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = length
		result.Offsets[i] = offset
		offset += length
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}

func FillSpacesUint64(originalVecCol []uint64, result *types.Bytes) *types.Bytes {
	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset
		offset += uint32(length)
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}

func FillSpacesInt8(originalVecCol []int8, result *types.Bytes) *types.Bytes {
	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset
		offset += uint32(length)
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}

func FillSpacesInt16(originalVecCol []int16, result *types.Bytes) *types.Bytes {
	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset
		offset += uint32(length)
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}

func FillSpacesInt32(originalVecCol []int32, result *types.Bytes) *types.Bytes {
	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset
		offset += uint32(length)
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}

func FillSpacesInt64(originalVecCol []int64, result *types.Bytes) *types.Bytes {
	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset
		offset += uint32(length)
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}

/*
func FillSpacesSigned[T constraints.Integer](originalVecCol []T, result *types.Bytes) *types.Bytes {
	var offset uint32 = 0
	for i, length := range originalVecCol {
		result.Lengths[i] = uint32(length)
		result.Offsets[i] = offset
		offset += uint32(length)
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}

*/

func FillSpacesFloat32(originalVecCol []float32, result *types.Bytes) *types.Bytes {
	var offset uint32 = 0
	for i, length := range originalVecCol {
		roundLen := math.Round(float64(length))

		result.Lengths[i] = uint32(roundLen)
		result.Offsets[i] = offset
		offset += uint32(roundLen)
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}

func FillSpacesFloat64(originalVecCol []float64, result *types.Bytes) *types.Bytes {
	var offset uint32 = 0
	for i, length := range originalVecCol {
		roundLen := math.Round(length)

		result.Lengths[i] = uint32(roundLen)
		result.Offsets[i] = offset
		offset += uint32(roundLen)
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}

func FillSpacesCharVarChar(originalVecCol, result *types.Bytes) *types.Bytes {
	var bytesWriten uint32 = 0
	for i, offset := range originalVecCol.Offsets {
		length := parseStringAsInt64(string(originalVecCol.Data[offset : offset+originalVecCol.Lengths[i]]))

		result.Lengths[i] = uint32(length)
		result.Offsets[i] = bytesWriten
		bytesWriten += uint32(length)
	}

	for i := range result.Data {
		result.Data[i] = ' '
	}

	return result
}
