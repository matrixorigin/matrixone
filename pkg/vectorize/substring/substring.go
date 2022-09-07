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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

/*
Substring function rule description
*/

// Slice from left to right, starting from 0
func getSliceFromLeft(s string, offset int64) string {
	sourceRune := []rune(s)
	elemsize := int64(len(sourceRune))
	if offset > elemsize {
		return ""
	}
	substrRune := sourceRune[offset:]
	return string(substrRune)
}

func getSliceOffsetLen(s string, offset int64, length int64) string {
	sourceRune := []rune(s)
	elemsize := int64(len(sourceRune))
	if offset < 0 {
		offset += elemsize
		if offset < 0 {
			return ""
		}
	}
	if offset >= elemsize {
		return ""
	}

	if length <= 0 {
		return ""
	} else {
		end := offset + length
		if end > elemsize {
			end = elemsize
		}
		substrRune := sourceRune[offset:end]
		return string(substrRune)
	}
}

// Cut the slice with length from left to right, starting from 0
func getSliceFromLeftWithLength(s string, offset int64, length int64) string {
	if offset < 0 {
		return ""
	}
	return getSliceOffsetLen(s, offset, length)
}

// From right to left, cut the slice with length from 1
func getSliceFromRightWithLength(s string, offset int64, length int64) string {
	return getSliceOffsetLen(s, -offset, length)
}

// Cut slices from right to left, starting from 1
func getSliceFromRight(s string, offset int64) string {
	sourceRune := []rune(s)
	elemsize := int64(len(sourceRune))
	if offset > elemsize {
		return ""
	}
	substrRune := sourceRune[elemsize-offset:]
	return string(substrRune)
}

// The length parameter is not bound. Cut the string from the left
func SubstringFromLeftConstOffsetUnbounded(src []string, res []string, start int64) []string {
	for idx, s := range src {
		res[idx] = getSliceFromLeft(s, start)
	}
	return res
}

// The length parameter is not bound. Cut the string from the right
func SubstringFromRightConstOffsetUnbounded(src []string, res []string, start int64) []string {
	for idx, s := range src {
		res[idx] = getSliceFromRight(s, start)
	}
	return res
}

// Per MySQL substring doc, if pos is 0, return empty strings.
func SubstringFromZeroConstOffsetUnbounded(src []string, res []string) []string {
	for idx := range src {
		res[idx] = ""
	}
	return res
}

// Per MySQL substring doc, if pos is 0, return empty strings.
func SubstringFromZeroConstOffsetBounded(src []string, res []string) []string {
	for idx := range src {
		res[idx] = ""
	}
	return res
}

// Without binding the length parameter, dynamically cut the string
func SubstringDynamicOffsetUnbounded[T types.BuiltinNumber](src []string, res []string, startColumn []T, cs []bool) []string {
	for idx := range src {
		var s string
		if cs[0] {
			s = src[0]
		} else {
			s = src[idx]
		}

		var startValue int64
		if cs[1] {
			startValue = int64(startColumn[0])
		} else {
			startValue = int64(startColumn[idx])
		}

		if startValue > math.MaxInt32 || startValue < math.MinInt32 {
			// XXX better error handling
			panic("substring index out of range")
		}

		if startValue > 0 {
			res[idx] = getSliceFromLeft(s, startValue-1)
		} else if startValue < 0 {
			res[idx] = getSliceFromRight(s, -startValue)
		} else {
			// MySQL: pos 0 return empty string
			res[idx] = ""
		}
	}
	return res
}

// bound length parameter. Cut the string from left
func SubstringFromLeftConstOffsetBounded(src []string, res []string, start int64, length int64) []string {
	for idx, s := range src {
		res[idx] = getSliceFromLeftWithLength(s, start, length)
	}
	return res
}

// bound length parameter. Cut the string from right
func SubstringFromRightConstOffsetBounded(src []string, res []string, start, length int64) []string {
	for idx, s := range src {
		res[idx] = getSliceFromRightWithLength(s, start, length)
	}
	return res
}

// bound the length parameter, dynamically cut the string
func SubstringDynamicOffsetBounded[T1, T2 types.BuiltinNumber](src []string, res []string, startColumn []T1, lengthColumn []T2, cs []bool) []string {
	for idx, s := range src {
		//get substring pos parameter value
		var startValue, lengthValue int64

		if cs[1] {
			startValue = int64(startColumn[0])
		} else {
			startValue = int64(startColumn[idx])
		}
		if startValue > math.MaxInt32 || startValue < math.MinInt32 {
			panic("substring start value out of bound")
		}

		if cs[2] {
			lengthValue = int64(lengthColumn[0])
		} else {
			lengthValue = int64(lengthColumn[idx])
		}
		if lengthValue > math.MaxInt32 || lengthValue < math.MinInt32 {
			panic("substring length value out of bound")
		}

		if startValue > 0 {
			res[idx] = getSliceFromLeftWithLength(s, startValue-1, lengthValue)
		} else if startValue < 0 {
			res[idx] = getSliceFromRightWithLength(s, -startValue, lengthValue)
		} else {
			res[idx] = ""
		}
	}
	return res
}
