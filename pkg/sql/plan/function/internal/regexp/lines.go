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

package regexp

// LineMode changes multiline assertions only. Consuming instructions, text
// anchors, word boundaries and scoped flags keep the pinned Go semantics.
type LineMode uint8

const (
	UnixLines LineMode = iota
	WideTextLines
	WideBinaryLines
	WideBinaryFoldLines
)

func (mode LineMode) isWideTerminator(r rune) bool {
	if r == '\r' {
		return true
	}
	switch mode {
	case WideTextLines:
		return r == 0x85 || r == 0x2028 || r == 0x2029
	case WideBinaryLines:
		return r == 0xE085 // byte 0x85 in the SQL binary matching alphabet
	case WideBinaryFoldLines:
		return r == 0x2026 // byte 0x85 in the Windows-1252 folding alphabet
	}
	return false
}

// CompileWithLineMode compiles before publishing the immutable line policy.
func CompileWithLineMode(expr string, mode LineMode) (*Regexp, error) {
	re, err := Compile(expr)
	if err != nil {
		return nil, err
	}
	re.lineMode = mode
	return re, nil
}

// FindStringIndexAt searches from a byte offset without slicing away context.
func (re *Regexp) FindStringIndexAt(s string, start int) []int {
	if start < 0 || start > len(s) {
		return nil
	}
	return re.find(nil, nil, s, start, 2, nil)
}

// FindStringSubmatchIndexAt retains both original context and capture numbering.
func (re *Regexp) FindStringSubmatchIndexAt(s string, start int) []int {
	if start < 0 || start > len(s) {
		return nil
	}
	return re.pad(re.find(nil, nil, s, start, re.prog.NumCap, nil))
}
