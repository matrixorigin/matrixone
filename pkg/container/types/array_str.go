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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"strconv"
	"strings"
	"unicode"
	"unsafe"
)

// This class is only for benchmark analysis for StringToArray and ArrayToString.
// Once the strategy is finalized, we will remove it.

// StringToArrayV2 this implementation uses only one forloop instead of split(",")
func StringToArrayV2[T RealNumbers](str string) ([]T, error) {
	// Convert the string to a rune slice for efficient index based looping
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

	// we tried making x as `array` with size=MaxArrayDimension and returning the subarray as result.
	// Then we made x as `slice` and did the same operation. We did not observe any significant change.
	// May be should we add pooling on `x`?
	var x []T
	dim := 0
	var val T
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
		val, err = stringToT[T](str[strIdx:commaPos])
		x = append(x, val)
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
	val, err = stringToT[T](str[strIdx:endBracketPos])
	x = append(x, val)
	if err != nil {
		return nil, err
	}
	//dim++

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
	return x, nil
}

// StringToArrayV3 this implementation uses only one forloop without casting string to rune array (via unsafe)
func StringToArrayV3[T RealNumbers](str string) ([]T, error) {
	strLen := len(str)
	strIdx := 0

	// Trim Space from beginning
	for strIdx < strLen && unicode.IsSpace(unsafeStringAt(str, strIdx)) {
		strIdx++
	}

	// If beginning is not [ it is malformed.
	if unsafeStringAt(str, strIdx) != '[' {
		return nil, moerr.NewInternalErrorNoCtx("malformed vector input: %s", str)
	}

	// skip [
	strIdx++

	// we tried making x as `array` with size=MaxArrayDimension and returning the subarray as result.
	// Then we made x as `slice` and did the same operation. We did not observe any significant change.
	// May be should we add pooling on `x`?
	var x []T
	dim := 0
	var val T
	var err error

	// find the next "," from the strIdx. More like IndexOf(",", startIdx) from java.
	commaPos := indexFrom(str, ",", strIdx)

	for strIdx < strLen && unsafeStringAt(str, strIdx) != ']' {
		if dim == MaxArrayDimension {
			return nil, moerr.NewInternalErrorNoCtx("typeLen is over the MaxVectorLen: %v", MaxArrayDimension)
		}
		// skip space in the numStr, Eg:- "[  1, 2]"
		for strIdx < strLen && unicode.IsSpace(unsafeStringAt(str, strIdx)) {
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
		val, err = stringToT[T](str[strIdx:commaPos])
		x = append(x, val)
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
	for strIdx < strLen && unicode.IsSpace(unsafeStringAt(str, strIdx)) {
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
	val, err = stringToT[T](str[strIdx:endBracketPos])
	x = append(x, val)
	if err != nil {
		return nil, err
	}
	//dim++

	// set strIdx after endBracket
	strIdx = endBracketPos + 1

	// Trim of the trailing spaces
	for strIdx < strLen && unicode.IsSpace(unsafeStringAt(str, strIdx)) {
		strIdx++
	}

	// If after TrimEnd, there are still data in the str, then it is malformed.
	// Eg:- [1,2,3] 1
	if strIdx != strLen {
		return nil, moerr.NewInternalErrorNoCtx("malformed vector input: %s", str)
	}

	// return subarray of x based on the final dimension.
	return x, nil
}

// unsafeStringAt used when we want str[idx] without casting str to []rune
func unsafeStringAt(str string, idx int) rune {
	// version 1.20 and older:
	// SCA fix from here: https://github.com/go101/go101/blob/7487c205ec72cd2658b614f9e289a77a5b42a99a/pages/fundamentals/unsafe.html#L1160
	// version 1.21 and further:
	pbyte := (*byte)(unsafe.Add(unsafe.Pointer(unsafe.StringData(str)), idx))
	return rune(*pbyte)
}

// indexFrom Find the next occurrence of substr after start
func indexFrom(str, substr string, start int) int {
	pos := strings.Index(str[start:], substr)
	if pos != -1 {
		pos += start
	}
	return pos
}

// stringToT convert str to T
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
}
