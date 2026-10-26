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

/*
Package regexp2 line-policy extensions owned by MatrixOne.

These additions implement the MySQL/ICU multiline line-terminator contract
for issue #30210 without changing the vendored engine's default behavior:
UnixLines keeps the upstream tables exactly.
*/

package regexp2

// LineMode changes multiline assertions only. Consuming instructions, text
// anchors, word boundaries and scoped flags keep the engine's semantics.
type LineMode uint8

const (
	UnixLines LineMode = iota
	WideTextLines
	WideBinaryLines
)

// isWideTerminator reports the line terminators beyond the native LF, which
// stays special-cased in the assertion tables so CRLF can stay one atomic
// break. VT and FF terminate lines in every wide mode; NEL, LS and PS join
// only in the text mode because the binary matching alphabet never treats
// them as line breaks.
func (mode LineMode) isWideTerminator(r rune) bool {
	if r == '\r' || r == '\v' || r == '\f' {
		return true
	}
	if mode == WideTextLines {
		return r == 0x85 || r == 0x2028 || r == 0x2029
	}
	return false
}

// SetLinePolicy pins the immutable multiline assertion policy. It must be
// called before the Regexp is published to concurrent use. mask records the
// shared classifier's candidate rule: a masked line-anchored search visits
// line starts only, so no match can start at a CRLF interior. The mask is
// left-to-right only; right-to-left searches keep the default enumeration.
func (re *Regexp) SetLinePolicy(mode LineMode, mask bool) {
	re.lineMode = mode
	re.lineMask = mask
}

// wideBolHolds evaluates the multiline caret at the current position.
func (r *runner) wideBolHolds() bool {
	if r.leftchars() == 0 {
		return true
	}
	left := r.charAt(r.textPos() - 1)
	if left == '\n' {
		return true
	}
	// Otherwise the caret needs a following character: after a trailing
	// wide terminator other than LF no new line begins.
	return r.rightchars() > 0 && r.re.lineMode.isWideTerminator(left)
}

// wideEolHolds evaluates the multiline dollar at the current position. CR
// and LF form one atomic break, so the dollar does not hold between them.
func (r *runner) wideEolHolds() bool {
	if r.rightchars() == 0 {
		return true
	}
	right := r.charAt(r.textPos())
	if r.re.lineMode.isWideTerminator(right) {
		return true
	}
	return right == '\n' && !(r.leftchars() > 0 && r.charAt(r.textPos()-1) == '\r')
}

// lineCandidateMasked reports whether this search enumerates line starts
// only: wide line modes with the classifier's candidate mask pinned, in
// left-to-right direction.
func (r *runner) lineCandidateMasked() bool {
	return r.re.lineMode != UnixLines && r.re.lineMask && !r.re.RightToLeft()
}

// lineStartAtCurrent reports whether runtextpos begins a line: beginning of
// the original text, or the position right after a line terminator. CRLF is
// one atomic terminator, so its interior is never a line start.
func (r *runner) lineStartAtCurrent() bool {
	if r.runtextpos == 0 {
		return true
	}
	prev := r.runtext[r.runtextpos-1]
	if prev == '\n' {
		return true
	}
	if !r.re.lineMode.isWideTerminator(prev) {
		return false
	}
	return prev != '\r' || r.runtextpos >= r.runtextend || r.runtext[r.runtextpos] != '\n'
}
