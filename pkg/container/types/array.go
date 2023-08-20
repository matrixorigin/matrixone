// Copyright 2023 Matrix Origin
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

package types

import (
	"bytes"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"io"
	"strconv"
	"strings"
	"unsafe"
)

// NOTE: vecf32 and vecf64 in SQL is internally represented using T_array_float32 and T_array_float64.
// array is used to avoid potential conflicts with the already existing vector class from vectorized execution engine.

const (
	MaxArrayDimension = 65536
)

func BytesToArray[T RealNumbers](input []byte) (res []T) {
	return DecodeSlice[T](input)
}

func ArrayToBytes[T RealNumbers](input []T) []byte {
	return EncodeSlice[T](input)
}

func ArrayToString[T RealNumbers](input []T) string {
	var buffer bytes.Buffer
	_, _ = io.WriteString(&buffer, "[")
	for i, value := range input {
		if i > 0 {
			_, _ = io.WriteString(&buffer, ", ")
		}
		_, _ = io.WriteString(&buffer, fmt.Sprintf("%v", value))
	}
	_, _ = io.WriteString(&buffer, "]")
	return buffer.String()
}

func ArraysToString[T RealNumbers](input [][]T) string {
	strValues := make([]string, len(input))
	for i, row := range input {
		strValues[i] = ArrayToString[T](row)
	}
	return strings.Join(strValues, " ")
}

func StringToArray[T RealNumbers](str string) ([]T, error) {
	input := strings.TrimSpace(str)

	if !(strings.HasPrefix(input, "[") && strings.HasSuffix(input, "]")) {
		return nil, moerr.NewInternalErrorNoCtx("malformed vector input: %s", str)
	}

	input = strings.ReplaceAll(input, "[", "")
	input = strings.ReplaceAll(input, "]", "")

	if input == "" {
		return nil, moerr.NewInternalErrorNoCtx("vector must not be of zero size.")
	}

	numStrs := strings.Split(input, ",")
	if len(numStrs) > MaxArrayDimension {
		return nil, moerr.NewInternalErrorNoCtx("typeLen is over the MaxVectorLen: %v", MaxArrayDimension)
	}
	result := make([]T, len(numStrs))

	var t T
	for i, numStr := range numStrs {
		switch any(t).(type) {
		case float32:
			num, err := strconv.ParseFloat(numStr, 32)
			if err != nil {
				return nil, moerr.NewInternalErrorNoCtx("error while casting %s to %s", numStr, T_float32.String())
			}
			// FIX: https://stackoverflow.com/a/36391858/1609570
			numf32 := float32(num)
			result[i] = *(*T)(unsafe.Pointer(&numf32))
		case float64:
			num, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return nil, moerr.NewInternalErrorNoCtx("error while casting %s to %s", numStr, T_float64.String())
			}
			result[i] = *(*T)(unsafe.Pointer(&num))
		default:
			panic(moerr.NewInternalErrorNoCtx("not implemented"))
		}

	}

	return result, nil
}

// StringToArrayToBytes convert "[1,2,3]" --> []float32{1.0,2.0,3.0} --> []bytes{11,33...}
func StringToArrayToBytes[T RealNumbers](input string) ([]byte, error) {
	// Convert "[1,2,3]" --> []float32{1.0, 2.0, 3.0}
	a, err := StringToArray[T](input)
	if err != nil {
		return nil, err
	}
	// Convert []float32{1.0, 2.0, 3.0} --> []byte{11, 33, 45, 56,.....}
	return ArrayToBytes[T](a), nil
}
