// Copyright 2026 Matrix Origin
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
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/functionUtil"
)

// regexpStringParameter decodes each operand independently when a binary
// operand meets a text operand. In particular, a binary pattern never changes
// the subject's character positions. Homogeneous binary pairs keep the byte
// matcher and its zero-copy ASCII path.
//
// This is a REGEXP consumer policy, not a change to the parameter vector: BIT,
// JSON, numeric and ordinary string consumers must see the original bytes.
type regexpStringParameter struct {
	vector.FunctionParameterWrapper[types.Varlena]
	parameters []*vector.Vector
	converter  regexpReplacementDomainConverter
}

func newRegexpStringParameter(parameters []*vector.Vector, position int) *regexpStringParameter {
	return &regexpStringParameter{
		FunctionParameterWrapper: vector.GenerateFunctionStrParameter(parameters[position]),
		parameters:               parameters,
		converter:                newRegexpReplacementDomainConverter(parameters[position]),
	}
}

func (p *regexpStringParameter) GetStrValue(row uint64) ([]byte, bool) {
	value, isNull := p.FunctionParameterWrapper.GetStrValue(row)
	if isNull {
		return value, true
	}
	text := functionUtil.QuickBytesToStr(value)
	matchingBinary := regexpMatchUsesBinary(p.parameters, int(row))
	if p.converter.parameter.GetIsBinaryStringAt(int(row)) {
		text = p.converter.forMatchDomain(text, int(row), matchingBinary)
	} else {
		text = regexpValidTextPrefix(text)
		if matchingBinary {
			text = regexpTextToBinaryBytes(text)
		}
	}
	return functionUtil.QuickStrToBytes(text), false
}

// MySQL passes the successfully decoded prefix to the regexp library when a
// text operand contains an invalid UTF-8 sequence. StringSource identifies an
// owner, not UTF-8 validity: VARCHAR columns and expressions can also contain
// arbitrary bytes when sql_mode permits them.
func regexpValidTextPrefix(value string) string {
	for i := 0; i < len(value); {
		if value[i] < utf8.RuneSelf {
			i++
			continue
		}
		_, width := utf8.DecodeRuneInString(value[i:])
		if width == 1 {
			return value[:i]
		}
		i += width
	}
	return value
}

// Result encoding is a separate decision from matching. A bare user variable
// contributes its current binary charset, whereas SQL/COM_STMT markers retain
// their text result charset even when EXECUTE supplies BLOB bytes.
func regexpResultUsesBinary(parameters []*vector.Vector, row int) bool {
	for _, parameter := range parameters[:min(RegexpMatchStringOperandCount, len(parameters))] {
		switch parameter.GetStringSourceAt(row) {
		case types.StringSourceSQLPrepare, types.StringSourceCOMStmt:
			continue
		}
		if parameter.GetIsBinaryStringAt(row) {
			return true
		}
	}
	return false
}

func regexpEncodeResult(value string, matchingBinary, resultBinary bool) string {
	if matchingBinary == resultBinary {
		return value
	}
	if matchingBinary {
		return regexpBinaryBytesToText(value)
	}
	return regexpTextToBinaryBytes(value)
}

// This is the inverse of the regexp library's Windows-1252 input conversion.
// A character outside the result charset is represented by '?' by MySQL.
func regexpTextToBinaryBytes(value string) string {
	first := -1
	for i := 0; i < len(value); i++ {
		if value[i] >= utf8.RuneSelf {
			first = i
			break
		}
	}
	if first < 0 {
		return value
	}
	var output strings.Builder
	output.Grow(len(value))
	output.WriteString(value[:first])
	for _, r := range value[first:] {
		if r < 0x80 || r >= 0xa0 && r <= 0xff {
			output.WriteByte(byte(r))
			continue
		}
		encoded := byte('?')
		for b := 0x80; b < 0xa0; b++ {
			if regexpWindows1252Rune(byte(b)) == r {
				encoded = byte(b)
				break
			}
		}
		output.WriteByte(encoded)
	}
	return output.String()
}
