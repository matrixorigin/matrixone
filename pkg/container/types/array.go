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
	"unicode"
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

// StringToArrayV2 this implementation uses only one forloop instead of split(",")
func StringToArrayV2[T RealNumbers](str string) ([]T, error) {
	// Convert the string to a rune slice for efficient index based looping
	// TODO: Should we use unsafe here?
	runes := []rune(str)

	strLen := len(runes)
	strIdx := 0

	// Trim Space from beginning
	for strIdx < strLen && unicode.IsSpace(runes[strIdx]) {
		strIdx++
	}

	// If beginning is not [ it is malformed.
	if runes[strIdx] != '[' {
		return nil, moerr.NewInternalErrorNoCtx("malformed vector input: %s", str)
	}

	// skip [
	strIdx++

	// we take the subarray from x based on final array dimension.
	x := make([]T, MaxArrayDimension)
	dim := 0
	var err error

	// find the next "," from the strIdx. More like IndexOf(",", startIdx) from java.
	commaPos := indexFrom(str, ",", strIdx)

	for strIdx < strLen && runes[strIdx] != ']' {
		if dim == MaxArrayDimension {
			return nil, moerr.NewInternalErrorNoCtx("typeLen is over the MaxVectorLen: %v", MaxArrayDimension)
		}
		// skip space in the numStr, Eg:- "[  1, 2]"
		for strIdx < strLen && unicode.IsSpace(runes[strIdx]) {
			strIdx++
		}

		// if there is no "," then break and check for the "]" condition
		if commaPos == -1 {
			break
		}

		// this means that numStr is ""
		if strIdx == commaPos {
			return nil, moerr.NewInternalErrorNoCtx("malformed vector input: %s", str)
		}

		// convert numStr to T
		x[dim], err = stringToT[T](str[strIdx:commaPos])
		if err != nil {
			return nil, err
		}
		dim++

		// move strIdx to pos after commaPos
		strIdx = commaPos + 1

		// find the next comma pos from strIdx.
		// If there is none, the break condition kicks in and rest of the code checks for
		// "]" condition.
		commaPos = indexFrom(str, ",", strIdx)
	}

	// skip space for case:- [   ]
	for strIdx < strLen && unicode.IsSpace(runes[strIdx]) {
		strIdx++
	}

	// find next occurrence of "]" from strIdx.
	endBracketPos := indexFrom(str, "]", strIdx)
	if endBracketPos == -1 {
		return nil, moerr.NewInternalErrorNoCtx("malformed vector input: %s", str)
	}

	// if numStr is ""
	if strIdx == endBracketPos {
		return nil, moerr.NewInternalErrorNoCtx("malformed vector input: %s", str)
	}

	// convert numStr to T and incr dim
	x[dim], err = stringToT[T](str[strIdx:endBracketPos])
	if err != nil {
		return nil, err
	}
	dim++

	// set strIdx after endBracket
	strIdx = endBracketPos + 1

	// Trim of the trailing spaces
	for strIdx < strLen && unicode.IsSpace(runes[strIdx]) {
		strIdx++
	}

	// If after TrimEnd, there are still data in the str, then it is malformed.
	// Eg:- [1,2,3] 1
	if strIdx != strLen {
		return nil, moerr.NewInternalErrorNoCtx("malformed vector input: %s", str)
	}

	// return subarray of x based on the final dimension.
	return x[:dim], nil
}

// Index Find the next occurrence of substr from start
func indexFrom(str, substr string, start int) int {
	pos := strings.Index(str[start:], substr)
	if pos != -1 {
		pos += start
	}
	return pos
}

func stringToT[T RealNumbers](str string) (t T, err error) {
	switch any(t).(type) {
	case float32:
		num, err := strconv.ParseFloat(str, 32)
		if err != nil {
			return t, moerr.NewInternalErrorNoCtx("error while casting %s to %s", str, T_float32.String())
		}
		// FIX: https://stackoverflow.com/a/36391858/1609570
		numf32 := float32(num)
		return *(*T)(unsafe.Pointer(&numf32)), nil
	case float64:
		num, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return t, moerr.NewInternalErrorNoCtx("error while casting %s to %s", str, T_float64.String())
		}
		return *(*T)(unsafe.Pointer(&num)), nil
	default:
		panic(moerr.NewInternalErrorNoCtx("not implemented"))
	}
	return t, nil
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
