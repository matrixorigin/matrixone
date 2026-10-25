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

package regexpclass

import (
	"testing"
)

// TestClassifyBinaryDialect pins the binary route's source semantics
// (review round 2, finding 4): the SQL translator leaves \Uxxxxxxxx
// verbatim for the fork over a binary subject, so it scans as the literal
// char 'U'; \Q...\E still expands; everything else matches DialectRegexp2.
func TestClassifyBinaryDialect(t *testing.T) {
	eventRows := []struct {
		pattern string
		want    []string
	}{
		{`\U0000000A`, []string{`literal U+0055`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0041`, "end"}},
		{`[\U0000000A]`, []string{"class-open", "class-close card=2", "end"}},
		{`\Q\n\E`, []string{`literal U+005C`, `literal U+006E`, "end"}},
		{`(?=<)\<`, []string{"group-open lookahead", `literal U+003C`, "group-close lookahead", `literal U+003C`, "end"}},
		{`\x{41}\u0042`, []string{`literal U+0041`, `literal U+0042`, "end"}},
		{`(?>a)\k<x>`, []string{"group-open atomic", `literal U+0061`, "group-close atomic", "backref", "end"}},
	}
	for _, r := range eventRows {
		events := DebugEvents(r.pattern, DialectRegexp2Binary)
		if len(events) != len(r.want) {
			t.Errorf("pattern %q: got %d events, want %d: %v", r.pattern, len(events), len(r.want), events)
			continue
		}
		for i, w := range r.want {
			if got := shortEvent(events[i]); got != w {
				t.Errorf("pattern %q event %d: got %q want %q (all: %v)", r.pattern, i, got, w, events)
				break
			}
		}
	}

	descRows := []struct {
		pattern string
		line    bool
		exempt  bool
		note    string
	}{
		{`^\U0000000A(?=)`, true, true, "binary route: literal 'U' plus seven characters is exempt"},
		{`^\nx`, true, true, "binary route keeps text escape semantics"},
	}
	for _, r := range descRows {
		pattern := "(?m)" + r.pattern
		got := Classify(pattern, DialectRegexp2Binary)
		if got.LineAnchoredStart != r.line || got.StringExempt != r.exempt {
			t.Errorf("pattern %q: got {line=%v exempt=%v}, want {line=%v exempt=%v} (%s)",
				pattern, got.LineAnchoredStart, got.StringExempt, r.line, r.exempt, r.note)
		}
	}
}
