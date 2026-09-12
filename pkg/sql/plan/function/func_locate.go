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
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// LOCATE(substr, str)
func buildInLocate2Args(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)
	substrVs := vector.GenerateFunctionStrParameter(parameters[0])
	strVs := vector.GenerateFunctionStrParameter(parameters[1])
	uniformBinary, perRow := stringDomainMode(parameters[1])
	caseInsensitive := parameters[1].GetType().Charset == types.CharsetUTF8

	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selectList, row) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		substr, null1 := substrVs.GetStrValue(row)
		str, null2 := strVs.GetStrValue(row)
		if null1 || null2 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		binary := binaryStringAt(parameters[1], int(row), uniformBinary, perRow)
		rs.AppendMustValue(locateString(substr, str, 1, binary, caseInsensitive))
	}
	return nil
}

// LOCATE(substr, str, position)
func buildInLocate3Args(parameters []*vector.Vector, result vector.FunctionResultWrapper, _ *process.Process, length int, selectList *FunctionSelectList) error {
	rs := vector.MustFunctionResult[int64](result)
	substrVs := vector.GenerateFunctionStrParameter(parameters[0])
	strVs := vector.GenerateFunctionStrParameter(parameters[1])
	posVs := vector.GenerateFunctionFixedTypeParameter[int64](parameters[2])
	uniformBinary, perRow := stringDomainMode(parameters[1])
	caseInsensitive := parameters[1].GetType().Charset == types.CharsetUTF8

	for row := uint64(0); row < uint64(length); row++ {
		if functionRowSkipped(selectList, row) {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		substr, null1 := substrVs.GetStrValue(row)
		str, null2 := strVs.GetStrValue(row)
		position, null3 := posVs.GetValue(row)
		if null1 || null2 || null3 {
			if err := rs.Append(0, true); err != nil {
				return err
			}
			continue
		}
		binary := binaryStringAt(parameters[1], int(row), uniformBinary, perRow)
		rs.AppendMustValue(locateString(substr, str, position, binary, caseInsensitive))
	}
	return nil
}

func locateString(needle, haystack []byte, position int64, binary, caseInsensitive bool) int64 {
	if position < 1 {
		return 0
	}
	if binary {
		start := position - 1
		if start > int64(len(haystack)) {
			return 0
		}
		if len(needle) == 0 {
			return position
		}
		idx := bytes.Index(haystack[int(start):], needle)
		if idx < 0 {
			return 0
		}
		return position + int64(idx)
	}

	str, substr := string(haystack), string(needle)
	if caseInsensitive {
		str, substr = strings.ToUpper(str), strings.ToUpper(substr)
	}
	start, ok := runeByteOffset(str, position-1)
	if !ok {
		return 0
	}
	if len(substr) == 0 {
		return position
	}
	idx := strings.Index(str[start:], substr)
	if idx < 0 {
		return 0
	}
	return position + int64(utf8.RuneCountInString(str[start:start+idx]))
}

func runeByteOffset(value string, runeIndex int64) (int, bool) {
	if runeIndex < 0 {
		return 0, false
	}
	offset := 0
	for index := int64(0); index < runeIndex; index++ {
		if offset >= len(value) {
			return 0, false
		}
		_, size := utf8.DecodeRuneInString(value[offset:])
		offset += size
	}
	return offset, true
}

// Locate2Args is retained for direct callers and follows the existing text
// general-ci approximation.
func Locate2Args(str string, subStr string) int64 {
	return locateString([]byte(subStr), []byte(str), 1, false, false)
}

// Locate3Args is retained for direct callers and uses character positions.
func Locate3Args(str string, subStr string, pos int64) int64 {
	return locateString([]byte(subStr), []byte(str), pos, false, false)
}

// getSubstring returns a suffix whose start is measured in characters.
func getSubstring(str string, start int) string {
	offset, ok := runeByteOffset(str, int64(start))
	if !ok {
		return ""
	}
	return str[offset:]
}
