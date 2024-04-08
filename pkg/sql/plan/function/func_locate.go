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

package function

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"strings"
	"unicode/utf8"
)

// LOCATE(substr, str)
func buildInLocate2Args(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[int64](result)
	substrVs := vector.GenerateFunctionStrParameter(parameters[0])
	strVs := vector.GenerateFunctionStrParameter(parameters[1])

	for i := uint64(0); i < uint64(length); i++ {
		substr, null1 := substrVs.GetStrValue(i)
		str, null2 := strVs.GetStrValue(i)

		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			pos := Locate2Args(functionUtil.QuickBytesToStr(bytes.ToUpper(str)), functionUtil.QuickBytesToStr(bytes.ToUpper(substr)))
			rs.AppendMustValue(pos)
		}
	}
	return nil
}

// LOCATE(substr, str, [position])
func buildInLocate3Args(parameters []*vector.Vector, result vector.FunctionResultWrapper, proc *process.Process, length int) error {
	rs := vector.MustFunctionResult[int64](result)
	substrVs := vector.GenerateFunctionStrParameter(parameters[0])
	strVs := vector.GenerateFunctionStrParameter(parameters[1])
	posVs := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])

	for i := uint64(0); i < uint64(length); i++ {
		substr, null1 := substrVs.GetStrValue(i)
		str, null2 := strVs.GetStrValue(i)
		position, null3 := posVs.GetValue(i)

		if null1 || null2 || null3 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
		} else {
			pos := Locate3Args(functionUtil.QuickBytesToStr(bytes.ToUpper(str)), functionUtil.QuickBytesToStr(bytes.ToUpper(substr)), position)
			rs.AppendMustValue(pos)
		}
	}
	return nil
}

// evalate LOCATE(substr, str)
func Locate2Args(str string, subStr string) int64 {
	subStrLen := len(subStr)
	if subStrLen == 0 {
		return 1
	}
	ret, idx := 0, strings.Index(str, subStr)
	if idx >= 0 {
		prefix := str[:idx]
		prefixLen := utf8.RuneCountInString(prefix)
		ret = prefixLen + 1
	}
	//if idx != -1 {
	//	ret = idx + 1
	//}

	return int64(ret)
}

// evalate LOCATE(substr, str, pos)
func Locate3Args(str string, subStr string, pos int64) int64 {
	// Transfer the argument which starts from 1 to real index which starts from 0.
	pos--
	subStrLen := len(subStr)
	if pos < 0 || pos > int64(len(str)-subStrLen) {
		return 0
	} else if subStrLen == 0 {
		return pos + 1
	}

	slice := getSubstring(str, int(pos))

	idx := strings.Index(slice, subStr)
	if idx >= 0 {
		prefix := slice[:idx]
		prefixLen := utf8.RuneCountInString(prefix)
		return pos + int64(prefixLen) + 1
	}

	return 0
}

// getSubstring Used to obtain the starting position of a substring in characters.
// takes two parameters: str is the target string, and start is the starting position (in characters) of the substring to be obtained.
func getSubstring(str string, start int) string {
	if start >= utf8.RuneCountInString(str) {
		return ""
	}

	byteOffset := 0
	for i := 0; i < start; i++ {
		_, size := utf8.DecodeRuneInString(str[byteOffset:])
		byteOffset += size
	}

	return str[byteOffset:]
}
