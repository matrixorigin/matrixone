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

import "github.com/matrixorigin/matrixone/pkg/sql/plan/function/internal/regexpclass"

// LineMode changes multiline assertions only. Consuming instructions, text
// anchors, word boundaries and scoped flags keep the pinned Go semantics.
type LineMode uint8

const (
	UnixLines LineMode = iota
	WideTextLines
	WideBinaryLines
	// WideBinaryFoldLines is retained for source compatibility only. Both
	// binary alphabets share one policy, {CR, VT, FF} plus native LF: byte
	// 0x85 arrives as U+E085 (or folds to U+2026), which ICU never treats
	// as a line terminator, while the ASCII bytes pass through unchanged.
	WideBinaryFoldLines
)

// isWideTerminator reports the line terminators beyond the native LF, which
// stays special-cased in lazyFlag.match so CRLF can stay one atomic break.
func (mode LineMode) isWideTerminator(r rune) bool {
	if r == '\r' || r == '\v' || r == '\f' {
		return true
	}
	if mode == WideTextLines {
		return r == 0x85 || r == 0x2028 || r == 0x2029
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
	if mode != UnixLines {
		re.lineMask = regexpclass.Classify(expr, regexpclass.DialectRE2).SuppressCRLFInterior()
	}
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
