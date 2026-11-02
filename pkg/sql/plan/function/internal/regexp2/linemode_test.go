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
MatrixOne-owned tests for the line-policy additions in linemode.go (issue
#30210). Upstream behavior with the zero policy is pinned by the existing
package tests; these rows pin only the wide line modes and the candidate
mask.
*/

package regexp2

import "testing"

func matchWithPolicy(t *testing.T, pattern string, mode LineMode, mask bool, subject string) bool {
	t.Helper()
	re, err := Compile(pattern, RE2)
	if err != nil {
		t.Fatalf("compile %q: %v", pattern, err)
	}
	re.SetLinePolicy(mode, mask)
	m, err := re.MatchString(subject)
	if err != nil {
		t.Fatalf("match %q: %v", subject, err)
	}
	return m
}

func TestWideLineAssertionTables(t *testing.T) {
	for _, tc := range []struct {
		name, pattern, subject string
		mode                   LineMode
		want                   bool
	}{
		// The wide caret recognizes every wide terminator; CRLF stays
		// atomic but its interior holds the caret after CR (ICU).
		{"caret_after_cr", `(?m)^b`, "a\rb", WideTextLines, true},
		{"caret_after_vt", `(?m)^b`, "a\vb", WideTextLines, true},
		{"caret_after_ff", `(?m)^b`, "a\fb", WideTextLines, true},
		{"caret_after_nel", `(?m)^b`, "a\u0085b", WideTextLines, true},
		{"caret_after_ls", `(?m)^b`, "a\u2028b", WideTextLines, true},
		{"caret_after_ps", `(?m)^b`, "a\u2029b", WideTextLines, true},
		{"caret_after_lf", `(?m)^b`, "a\nb", WideTextLines, true},
		{"caret_unix_cr", `(?m)^b`, "a\rb", UnixLines, false},
		{"caret_unix_vt", `(?m)^b`, "a\vb", UnixLines, false},
		// The binary alphabet terminates only on the ASCII controls.
		{"caret_binary_cr", `(?m)^b`, "a\rb", WideBinaryLines, true},
		{"caret_binary_vt", `(?m)^b`, "a\vb", WideBinaryLines, true},
		{"caret_binary_ff", `(?m)^b`, "a\fb", WideBinaryLines, true},
		{"caret_binary_nel", `(?m)^b`, "a\u0085b", WideBinaryLines, false},
		{"caret_binary_ls", `(?m)^b`, "a\u2028b", WideBinaryLines, false},
		// The wide dollar mirrors every terminator before the following
		// text, and never holds inside an atomic CRLF pair.
		{"dollar_before_cr", `(?m)b$`, "b\rx", WideTextLines, true},
		{"dollar_before_vt", `(?m)b$`, "b\vx", WideTextLines, true},
		{"dollar_before_ff", `(?m)b$`, "b\fx", WideTextLines, true},
		{"dollar_before_nel", `(?m)b$`, "b\u0085x", WideTextLines, true},
		{"dollar_crlf_interior", `(?m)\r$`, "b\r\n", WideTextLines, false},
		{"dollar_trailing_cr", `(?m)\r$`, "b\r", WideTextLines, true},
		{"dollar_unix_cr", `(?m)b$`, "b\rx", UnixLines, false},
		// The caret needs a following character after a non-LF terminator.
		{"caret_trailing_cr", `(?m)^$`, "a\r", WideTextLines, false},
		{"caret_trailing_vt", `(?m)^$`, "a\v", WideTextLines, false},
		{"caret_trailing_lf", `(?m)^$`, "a\n", WideTextLines, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchWithPolicy(t, tc.pattern, tc.mode, false, tc.subject); got != tc.want {
				t.Errorf("pattern %q subject %q mode %d: got %v, want %v", tc.pattern, tc.subject, tc.mode, got, tc.want)
			}
		})
	}
}

func TestLineMaskCandidateEnumeration(t *testing.T) {
	// Without the mask the interior assertion holds after CR and the
	// interior match succeeds; with the mask the search visits line
	// starts only and the interior is never offered.
	if got := matchWithPolicy(t, `(?m)^\n$\nx`, WideTextLines, false, "a\r\n\nxb"); !got {
		t.Error("unmasked interior search must admit the CRLF-interior match")
	}
	if got := matchWithPolicy(t, `(?m)^\n$\nx`, WideTextLines, true, "a\r\n\nxb"); got {
		t.Error("masked line-anchored search must skip the CRLF interior")
	}
	// The mask keeps every real line start reachable.
	if got := matchWithPolicy(t, `(?m)^b`, WideTextLines, true, "a\r\nb"); !got {
		t.Error("masked search must still reach the line start after CRLF")
	}
	if got := matchWithPolicy(t, `(?m)^b`, WideTextLines, true, "a\vb"); !got {
		t.Error("masked search must still reach the line start after VT")
	}
	// The mask never engages for the Unix line mode; the outcome matches
	// the unmasked Unix search (ICU's `mu` verdict for this family).
	if got := matchWithPolicy(t, `(?m)^\n$\nx`, UnixLines, false, "a\r\n\nxb"); got {
		t.Error("Unix unmasked search: the interior has no line break under LF-only tables")
	}
	if got := matchWithPolicy(t, `(?m)^\n$\nx`, UnixLines, true, "a\r\n\nxb"); got {
		t.Error("Unix masked search must equal the unmasked Unix search")
	}
}
