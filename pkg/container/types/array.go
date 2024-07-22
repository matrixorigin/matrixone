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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"io"
	"strconv"
	"strings"
)

// NOTE: vecf32 and vecf64 in SQL is internally represented using T_array_float32 and T_array_float64.
// array is used to avoid potential conflicts with the already existing vector class from vectorized execution engine.

const (
	MaxArrayDimension        = MaxVarcharLen
	DefaultArraysToStringSep = " "
)

// BytesToArray bytes should be of little-endian format
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

		// following the similar logic of float32 and float64 from
		// - output.go #extractRowFromVector()
		// - mysql_protocol.go #makeResultSetTextRow() MYSQL_TYPE_FLOAT  & MYSQL_TYPE_DOUBLE
		// NOTE: vector does not handle NaN and Inf.
		switch value := any(value).(type) {
		case float32:
			_, _ = io.WriteString(&buffer, strconv.FormatFloat(float64(value), 'f', -1, 32))
		case float64:
			_, _ = io.WriteString(&buffer, strconv.FormatFloat(value, 'f', -1, 64))
		}
	}
	_, _ = io.WriteString(&buffer, "]")
	return buffer.String()
}

func ArraysToString[T RealNumbers](input [][]T, sep string) string {
	strValues := make([]string, len(input))
	for i, row := range input {
		strValues[i] = ArrayToString[T](row)
	}
	return strings.Join(strValues, sep)
}

func StringToArray[T RealNumbers](str string) ([]T, error) {
	input := strings.ReplaceAll(str, " ", "")

	if !(strings.HasPrefix(input, "[") && strings.HasSuffix(input, "]")) {
		return nil, moerr.NewInternalErrorNoCtx("malformed vector input: %s", str)
	}

	if len(input) == 2 {
		// We don't handle empty vector like "[]"
		return nil, moerr.NewInternalErrorNoCtx("malformed vector input: %s", str)
	}

	// remove "[" and "]"
	input = input[1 : len(input)-1]

	numStrs := strings.Split(input, ",")
	if len(numStrs) > MaxArrayDimension {
		return nil, moerr.NewInternalErrorNoCtx("typeLen is over the MaxVectorLen: %v", MaxArrayDimension)
	}
	result := make([]T, len(numStrs))

	var t T
	var err error
	for i, numStr := range numStrs {
		t, err = stringToT[T](numStr)
		if err != nil {
			return nil, err
		}
		result[i] = t
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

func BytesToArrayToString[T RealNumbers](input []byte) string {
	// Convert []byte{11, 33, 45, 56,.....} --> []float32{1.0, 2.0, 3.0}
	a := BytesToArray[T](input)

	// Convert []float32{1.0, 2.0, 3.0} --> "[1,2,3]"
	return ArrayToString[T](a)
}
