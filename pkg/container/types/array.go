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

const (
	//MaxArrayDimension Comment: https://github.com/arjunsk/matrixone/pull/35#discussion_r1275713689
	MaxArrayDimension = 65536
)

func BytesToArray[T BuiltinNumber](input []byte) (res []T) {
	return DecodeSlice[T](input)
}

func ArrayToBytes[T BuiltinNumber](input []T) []byte {
	return EncodeSlice[T](input)
}

func ArrayToString[T BuiltinNumber](input []T) string {
	var buffer bytes.Buffer
	for i, value := range input {
		if i > 0 {
			_, _ = io.WriteString(&buffer, ", ")
		}
		_, _ = io.WriteString(&buffer, fmt.Sprintf("%v", value))
	}
	return "[" + buffer.String() + "]"
}

func ArraysToString[T BuiltinNumber](input [][]T) string {
	var strValues []string
	for _, row := range input {
		strValues = append(strValues, ArrayToString[T](row))
	}
	return strings.Join(strValues, " ")
}

func StringToArray[T BuiltinNumber](input string) ([]T, error) {
	input = strings.ReplaceAll(input, "[", "")
	input = strings.ReplaceAll(input, "]", "")
	input = strings.ReplaceAll(input, " ", "")

	numStrs := strings.Split(input, ",")
	result := make([]T, len(numStrs))

	var t T
	for i, numStr := range numStrs {
		switch any(t).(type) {
		case float32:
			num, err := strconv.ParseFloat(numStr, 32)
			if err != nil {
				return nil, moerr.NewInternalErrorNoCtx("Error while parsing array : %v", err)
			}
			// FIX: https://stackoverflow.com/a/36391858/1609570
			numf32 := float32(num)
			result[i] = *(*T)(unsafe.Pointer(&numf32))
		case float64:
			num, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return nil, moerr.NewInternalErrorNoCtx("Error while parsing array : %v", err)
			}
			result[i] = *(*T)(unsafe.Pointer(&num))
		default:
			panic(moerr.NewInternalErrorNoCtx("not implemented"))
		}

	}

	return result, nil
}

func CompareArray[T BuiltinNumber](left, right []T) int64 {

	if len(left) != len(right) {
		//TODO: check this with Min.
		panic(moerr.NewInternalErrorNoCtx("Dimensions should be same"))
	}

	for i := 0; i < len(left); i++ {
		if left[i] == right[i] {
			continue
		} else if left[i] > right[i] {
			return +1
		} else {
			return -1
		}
	}

	return 0
}
