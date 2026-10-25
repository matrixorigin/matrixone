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
	"fmt"
	"strings"
	"testing"
)

// TestScanAngleRefBoundsR2 pins the fork-mirrored angle/quote reference
// recognition with complete bounds checks (review round 2, finding 1):
// trailing escaped '<' and apostrophe are literal characters in the fork, an
// unterminated or empty body without \k falls back to the literal open
// delimiter, and \k forms are engine errors that classify conservatively.
func TestScanAngleRefBoundsR2(t *testing.T) {
	rows := []struct {
		pattern string
		want    []string
	}{
		{`(?=<)\<`, []string{"group-open lookahead", `literal U+003C`, "group-close lookahead", `literal U+003C`, "end"}},
		{`(?=')\'`, []string{"group-open lookahead", `literal U+0027`, "group-close lookahead", `literal U+0027`, "end"}},
		{`\k`, []string{"opaque", "end"}},
		{`\k<`, []string{"opaque", "end"}},
		{`\k<>`, []string{"opaque", "end"}},
		{`\k<nope`, []string{"opaque", "end"}},
		{`\k'nope`, []string{"opaque", "end"}},
		{`\<nope`, []string{`literal U+003C`, `literal U+006E`, `literal U+006F`, `literal U+0070`, `literal U+0065`, "end"}},
		{`\<>`, []string{`literal U+003C`, `literal U+003E`, "end"}},
		{`\<13>`, []string{"backref", "end"}},
		{`\<a b>`, []string{`literal U+003C`, `literal U+0061`, `literal U+0020`, `literal U+0062`, `literal U+003E`, "end"}},
		{`\k<nope>`, []string{"backref", "end"}},
		{`\'name'`, []string{"backref", "end"}},
		{`\'`, []string{`literal U+0027`, "end"}},
	}
	for _, r := range rows {
		events := DebugEvents(r.pattern, DialectRegexp2)
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
}

// TestClassifyLookaroundFramePop pins that closing a skipped lookaround
// restores the enclosing frame stack (review round 2, finding 2): the
// alternation after a lookaround belongs to the enclosing scope, so carets
// recorded in the earlier arm are discarded as bare.
func TestClassifyLookaroundFramePop(t *testing.T) {
	rows := []struct {
		pattern string
		line    bool
		exempt  bool
		note    string
	}{
		{`^a(?=b)|\n`, false, false, "review example: caret discarded in earlier arm"},
		{`x|(?=a)^\n`, false, false, "post-branch caret is bare"},
		{`(^a(?=b)|\n)`, false, false, "capture scope"},
		{`(?:^a(?=b)|\n)+`, false, false, "quantified alternation"},
		{`((?=b)^a|\nx)`, false, false, "nested scope"},
		{`(?:(?=b)^a|\nx){1,2}`, false, false, "interval quantifier"},
		{`(?=(?!b))^a|x`, false, false, "nested lookarounds then branch"},
		{`(?=b)^\n`, true, false, "preserved control: lookaround keeps atStart"},
		{`(?=(?!b))^a`, true, false, "nested lookarounds preserve atStart"},
	}
	for _, r := range rows {
		pattern := "(?m)" + r.pattern
		got := Classify(pattern, DialectRegexp2)
		if got.LineAnchoredStart != r.line || got.StringExempt != r.exempt {
			t.Errorf("pattern %q: got {line=%v exempt=%v}, want {line=%v exempt=%v} (%s)",
				pattern, got.LineAnchoredStart, got.StringExempt, r.line, r.exempt, r.note)
		}
	}
}

// TestClassifyClassIntervalUnion pins insertion-order-independent interval
// merging (review round 2, finding 3): overlapping, adjacent, duplicate
// and containing members must never be dropped by the accumulator.
func TestClassifyClassIntervalUnion(t *testing.T) {
	eventRows := []struct {
		pattern string
		want    []string
	}{
		{`[\n-\r\n]`, []string{"class-open", "class-close card=2", "end"}},
		{`[\n\v]`, []string{"class-open", "class-close card=2", "end"}},
		{`[\n\n]`, []string{"class-open", `class-close card=1 U+000A`, "end"}},
		{`[\n-\r\r]`, []string{"class-open", "class-close card=2", "end"}},
		{`[^\n-\r\n]`, []string{"class-open", "class-close card=2", "end"}},
		{`[\n]`, []string{"class-open", `class-close card=1 U+000A`, "end"}},
	}
	for _, r := range eventRows {
		events := DebugEvents(r.pattern, DialectRegexp2)
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
		{`^[\n-\r\n]x`, true, false, "review example: multi-member class is not exempt"},
		{`^[\n\v]x`, true, false, "adjacent members stay a set"},
		{`^[\n]x`, true, true, "preserved control: singleton joins the run"},
	}
	for _, r := range descRows {
		pattern := "(?m)" + r.pattern
		got := Classify(pattern, DialectRE2)
		if got.LineAnchoredStart != r.line || got.StringExempt != r.exempt {
			t.Errorf("pattern %q: got {line=%v exempt=%v}, want {line=%v exempt=%v} (%s)",
				pattern, got.LineAnchoredStart, got.StringExempt, r.line, r.exempt, r.note)
		}
	}
}

// TestAccumulatorUnion exercises the accumulator arithmetic directly:
// order independence, adjacency coalescing, containment, duplicates and
// the subtraction sweep.
func TestAccumulatorUnion(t *testing.T) {
	norm := func(in [][2]rune) *classAccumulator {
		a := &classAccumulator{}
		for _, p := range in {
			a.addInterval(p[0], p[1])
		}
		return a
	}
	same := func(a, b *classAccumulator) bool {
		a.normalize()
		b.normalize()
		if len(a.intervals) != len(b.intervals) {
			return false
		}
		for i := range a.intervals {
			if a.intervals[i] != b.intervals[i] {
				return false
			}
		}
		return true
	}

	ab := norm([][2]rune{{'\n', '\r'}, {'\n', '\n'}})
	ba := norm([][2]rune{{'\n', '\n'}, {'\n', '\r'}})
	if !same(ab, ba) {
		t.Fatalf("insertion order changed the union: %v vs %v", ab.intervals, ba.intervals)
	}
	if ab.count() != 4 {
		t.Fatalf("union [\\n-\\r\\n] has %d members, want 4", ab.count())
	}
	adjacent := norm([][2]rune{{1, 2}, {3, 4}})
	adjacent.normalize()
	if len(adjacent.intervals) != 1 || adjacent.intervals[0] != (interval{1, 4}) {
		t.Fatalf("adjacent intervals did not coalesce: %v", adjacent.intervals)
	}
	gapped := norm([][2]rune{{1, 2}, {4, 5}})
	gapped.normalize()
	if len(gapped.intervals) != 2 {
		t.Fatalf("gapped intervals must stay separate: %v", gapped.intervals)
	}
	dup := norm([][2]rune{{7, 7}, {7, 7}})
	if dup.count() != 1 {
		t.Fatalf("duplicate member counted twice: %v", dup.intervals)
	}
	if r, ok := dup.singleton(); !ok || r != 7 {
		t.Fatalf("duplicate singleton resolves to %v,%v", r, ok)
	}
	containing := norm([][2]rune{{5, 10}, {6, 8}, {1, 6}})
	if containing.count() != 10 || !containing.member(1) || !containing.member(10) || containing.member(11) {
		t.Fatalf("containing union lost members: %v", containing.intervals)
	}

	// Subtraction sweeps sorted disjoincts without rescanning.
	base := norm([][2]rune{{2, 10}})
	sub := norm([][2]rune{{4, 5}, {8, 20}})
	base.subtract(sub)
	if len(base.intervals) != 2 || base.intervals[0] != (interval{2, 3}) || base.intervals[1] != (interval{6, 7}) {
		t.Fatalf("subtraction intervals = %v, want [{2,3} {6,7}]", base.intervals)
	}
	whole := norm([][2]rune{{5, 9}})
	whole.subtract(norm([][2]rune{{0, 20}}))
	if whole.count() != 0 {
		t.Fatalf("covering subtraction left %v", whole.intervals)
	}
}

// TestClassifyQuoteAndUnicodeEscapes pins the SQL source semantics of \Q..\E
// and \Uxxxxxxxx on the regexp2 route (review round 2, finding 4): both are
// rewritten by the SQL translator before the engine sees them, so the
// classifier must mirror the source, not the fork's default escape rule.
// The binary route leaves \U to the engine (literal 'U').
func TestClassifyQuoteAndUnicodeEscapes(t *testing.T) {
	eventRows := []struct {
		pattern string
		dialect Dialect
		want    []string
	}{
		{`\Q\n\E`, DialectRegexp2, []string{`literal U+005C`, `literal U+006E`, "end"}},
		{`\Qab`, DialectRegexp2, []string{`literal U+0061`, `literal U+0062`, "end"}},
		{`a\Q\Ez`, DialectRegexp2, []string{`literal U+0061`, `literal U+007A`, "end"}},
		{`\U0000000A`, DialectRegexp2, []string{`literal U+000A`, "end"}},
		{`a\U0001F600b`, DialectRegexp2, []string{`literal U+0061`, "literal U+1F600", `literal U+0062`, "end"}},
		{`\U000000`, DialectRegexp2, []string{`literal U+0055`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, "end"}},
		{`\U0000000G`, DialectRegexp2, []string{`literal U+0055`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0047`, "end"}},
		{`\U0000000A`, DialectRE2, []string{"opaque", `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0030`, `literal U+0041`, "end"}},
		{`[\U0000000A]`, DialectRegexp2, []string{"class-open", `class-close card=1 U+000A`, "end"}},
		{`[\U0000000A]`, DialectRE2, []string{"class-open", "class-close card=2", "end"}},
		{`[\Q]`, DialectRegexp2, []string{"class-open", `class-close card=1 U+0051`, "end"}},
	}
	for _, r := range eventRows {
		events := DebugEvents(r.pattern, r.dialect)
		if len(events) != len(r.want) {
			t.Errorf("pattern %q (%v): got %d events, want %d: %v", r.pattern, r.dialect, len(events), len(r.want), events)
			continue
		}
		for i, w := range r.want {
			if got := shortEvent(events[i]); got != w {
				t.Errorf("pattern %q (%v) event %d: got %q want %q (all: %v)", r.pattern, r.dialect, i, got, w, events)
				break
			}
		}
	}

	descRows := []struct {
		pattern string
		dialect Dialect
		line    bool
		exempt  bool
		note    string
	}{
		{`^\U0000000A(?=)`, DialectRegexp2, true, false, "single LF head masks CRLF interior"},
		{"^\\Q\n\\E(?=)", DialectRegexp2, true, false, "quoted single LF masks CRLF interior"},
	}
	for _, r := range descRows {
		pattern := "(?m)" + r.pattern
		got := Classify(pattern, r.dialect)
		if got.LineAnchoredStart != r.line || got.StringExempt != r.exempt {
			t.Errorf("pattern %q (%v): got {line=%v exempt=%v}, want {line=%v exempt=%v} (%s)",
				pattern, r.dialect, got.LineAnchoredStart, got.StringExempt, r.line, r.exempt, r.note)
		}
	}
}

// TestClassifyLargeSparseClass pins cardinality over a large sparse class
// (review round 2, finding 5): membership arithmetic must stay exact and
// the classification must complete without per-insertion rescans.
func TestClassifyLargeSparseClass(t *testing.T) {
	var body strings.Builder
	body.WriteString("[")
	for i := 0; i < 2048; i++ {
		body.WriteRune(rune(0x10000 + 2*i))
	}
	body.WriteString("]")
	events := DebugEvents(body.String(), DialectRegexp2)
	if len(events) != 3 || shortEvent(events[1]) != "class-close card=2" {
		t.Fatalf("sparse class events = %v, want class-open/card=2/end", events)
	}

	acc := &classAccumulator{}
	for i := 0; i < 2048; i++ {
		acc.addRune(rune(0x10000 + 2*i))
	}
	if got := acc.count(); got != 2048 {
		t.Fatalf("sparse class count = %d, want 2048", got)
	}
	if !acc.member(0x10000) || acc.member(0x10001) || !acc.member(0x10000+2*2047) {
		t.Fatalf("sparse class membership lost members: %v", acc.intervals)
	}
}

// BenchmarkClassifySparseClass measures uncached classification over
// increasing sparse-class sizes (review round 2, finding 5). The union
// must scale close to linearly with the member count.
func BenchmarkClassifySparseClass(b *testing.B) {
	for _, n := range []int{32, 128, 512, 2048, 8192} {
		var body strings.Builder
		body.WriteString("(?m)^[")
		for i := 0; i < n; i++ {
			body.WriteRune(rune(0x10000 + 2*i))
		}
		body.WriteString("](?=x)")
		pattern := body.String()
		b.Run(fmt.Sprintf("members=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = Classify(pattern, DialectRegexp2)
			}
		})
	}
}

// TestClassifyStreamingBatchEquivalence pins that the streaming public
// entry computes exactly the batch fold of the recorded event stream,
// including numeric backreference resolution (review round 2, finding 6
// bounded representation).
func TestClassifyStreamingBatchEquivalence(t *testing.T) {
	corpus := []struct {
		pattern string
		dialect Dialect
	}{
		{`^\nx`, DialectRE2},
		{`(?=())^\n\1x`, DialectRegexp2},
		{`(a)\1b`, DialectRegexp2},
		{`\1(a)`, DialectRegexp2},
		{`\8(a)`, DialectRegexp2},
		{`\12(1)(2)(3)(4)(5)(6)(7)(8)(9)(a)(b)(c)`, DialectRegexp2},
		{`(?m)^a(?=b)|\n`, DialectRegexp2},
		{`(?m)^[\n-\r\n]x`, DialectRE2},
		{`(?m)^[\n-\r\n]x(?=b)`, DialectRegexp2},
		{`\Q\n\E`, DialectRegexp2},
		{"^\\Q\n\\E(?=)", DialectRegexp2},
		{`^\U0000000A(?=)`, DialectRegexp2},
		{`(?i)^\nx{2,3}?((a)|(b))+|\d`, DialectRegexp2},
		{`(?x-m:^a) # c` + "\n" + `(?m)b`, DialectRegexp2},
		{`(?<n>\d)\k<n>(?=)` + `\<n\>`, DialectRegexp2},
	}
	for _, tc := range corpus {
		want := fold(scan(tc.pattern, tc.dialect))
		got := Classify(tc.pattern, tc.dialect)
		if got != want {
			t.Errorf("pattern %q (%v): streaming Classify = %+v, batch fold = %+v", tc.pattern, tc.dialect, got, want)
		}
	}
}
