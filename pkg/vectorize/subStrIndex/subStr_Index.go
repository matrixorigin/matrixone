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

package substrindex

import (
	"strings"
)

// function subStrIndex is used to return a substring from a string
//before a specified number of occurrences of the delimiter.
//-------------------------------------------------------------------------
// param1 str: The original string from which we want to create a substring.
// param2 delim: Is a string that acts as a delimiter.
// param3 count: It identifies the number of times to search for the delimiter. It can be both a positive or negative number.
// If it is a positive number, this function returns all to the left of the delimiter.
// If it is a negative number, this function returns all to the right of the delimiter.

func subStrIndex(str, delim string, count int64) (string, error) {
	// if the length of delim is 0, return empty string
	if len(delim) == 0 {
		return "", nil
	}
	// if the count is 0, return empty string
	if count == 0 {
		return "", nil
	}

	partions := strings.Split(str, delim)
	start, end := int64(0), int64(len(partions))

	if count > 0 {
		//is count is positive, reset the end position
		if count < end {
			end = count
		}
	} else {
		count = -count

		// -count overflows max int64, return the whole string.
		if count < 0 {
			return str, nil
		}

		//if count is negative, reset the start postion
		if count < end {
			start = end - count
		}
	}
	subPartions := partions[start:end]
	return strings.Join(subPartions, delim), nil
}

func SubStrIndex(strs, delims []string, counts []int64, rowCount int, constVectors []bool, results []string) {
	if constVectors[0] {
		str := strs[0]
		if constVectors[1] {
			delim := delims[0]
			if constVectors[2] {
				//scalar - scalar - scalar
				results[0], _ = subStrIndex(str, delim, counts[0])
			} else {
				//scalar - scalar - vector
				for i := 0; i < rowCount; i++ {
					results[i], _ = subStrIndex(str, delim, counts[i])
				}
			}
		} else {
			if constVectors[2] {
				count := counts[0]
				//scalar - vector - scalar
				for i := 0; i < rowCount; i++ {
					results[i], _ = subStrIndex(str, delims[i], count)
				}
			} else {
				//scalar - vector - vector
				for i := 0; i < rowCount; i++ {
					results[i], _ = subStrIndex(str, delims[i], counts[i])
				}
			}
		}
	} else {
		if constVectors[1] {
			delim := delims[0]
			if constVectors[2] {
				count := counts[0]
				//vector - scalar - scalar
				for i := 0; i < rowCount; i++ {
					results[i], _ = subStrIndex(strs[i], delim, count)
				}
			} else {
				//vaetor - scalar - vector
				for i := 0; i < rowCount; i++ {
					results[i], _ = subStrIndex(strs[i], delim, counts[i])
				}
			}
		} else {
			if constVectors[2] {
				count := counts[0]
				//vector - vector - scalar
				for i := 0; i < rowCount; i++ {
					results[i], _ = subStrIndex(strs[i], delims[i], count)
				}
			} else {
				//vector - vector - vector
				for i := 0; i < rowCount; i++ {
					results[i], _ = subStrIndex(strs[i], delims[i], counts[i])
				}
			}
		}
	}
}
