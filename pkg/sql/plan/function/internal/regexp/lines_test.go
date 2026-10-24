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

import (
	"reflect"
	std "regexp"
	"strings"
	"sync"
	"testing"
)

func TestDefaultMatchesPinnedStandardLibrary(t *testing.T) {
	patterns := []string{``, `a`, `a+`, `a*`, `a?`, `(a|ab)+`, `(?P<x>a)(b)?`, `(?m)^a$`, `\Aa.*\z`, `(?s:a.*b)`, `\b(a)\B`, `[^a]+`, `(?i:α+)`, `a.*?b`, `(?:$)+`, `(a?)*`, `\Q[a]`, `(?m:a$\r^b)`, `\A(?:[ab]*)(c?)\z`}
	subjects := []string{"", "a", "b", "ab", "aaab", "a\rb", "a\r\nb", "a\nb\n", "a\u0085b", "a\u2028b", "a\u2029b", "Αα", "\xffa", strings.Repeat("ab", 20000)}
	for _, pattern := range patterns {
		want := std.MustCompile(pattern)
		got := MustCompile(pattern)
		for _, subject := range subjects {
			if got.MatchString(subject) != want.MatchString(subject) ||
				!reflect.DeepEqual(got.FindStringSubmatchIndex(subject), want.FindStringSubmatchIndex(subject)) ||
				!reflect.DeepEqual(got.FindAllStringSubmatchIndex(subject, 4), want.FindAllStringSubmatchIndex(subject, 4)) ||
				got.ReplaceAllString(subject, `${x}:$1`) != want.ReplaceAllString(subject, `${x}:$1`) {
				t.Fatalf("default mode diverged: pattern=%q subject bytes=%d", pattern, len(subject))
			}
		}
	}
	for _, pattern := range []string{`[`, `\`, `a{1001}`, `(?q)a`} {
		_, want := std.Compile(pattern)
		_, got := Compile(pattern)
		if got == nil || want == nil || got.Error() != want.Error() {
			t.Fatalf("compile error differs for %q: %v / %v", pattern, got, want)
		}
	}
}

func TestWideAssertionsAllExecutors(t *testing.T) {
	for _, tc := range []struct {
		name, pattern, subject string
		mode                   LineMode
		want                   []int
	}{
		{"bitstate", `(?m)^b`, "a\rb", WideTextLines, []int{2, 3}},
		{"nfa", `(?m)^b`, strings.Repeat("a", 1<<18) + "\rb", WideTextLines, []int{1<<18 + 1, 1<<18 + 2}},
		{"onepass", `\A(?m:a$\r^b)\z`, "a\rb", WideTextLines, []int{0, 3}},
		{"crlf_atomic", `\A(?m:a\r$\n^b)\z`, "a\r\nb", WideTextLines, nil},
		{"binary", `(?m)^b`, "a\uE085b", WideBinaryLines, []int{4, 5}},
		{"binary_fold", `(?m)^b`, "a\u2026b", WideBinaryFoldLines, []int{4, 5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			re, err := CompileWithLineMode(tc.pattern, tc.mode)
			if err != nil {
				t.Fatal(err)
			}
			switch tc.name {
			case "onepass":
				if re.onepass == nil {
					t.Fatal("did not select one-pass executor")
				}
			case "bitstate":
				if re.onepass != nil || len(tc.subject) >= re.maxBitStateLen {
					t.Fatal("did not select bit-state executor")
				}
			case "nfa":
				if re.onepass != nil || len(tc.subject) < re.maxBitStateLen {
					t.Fatal("did not select NFA executor")
				}
			}
			if got := re.FindStringIndex(tc.subject); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestWideBoundaryTruthTable(t *testing.T) {
	for _, separator := range []string{"\n", "\r", "\r\n", "\u0085", "\u2028", "\u2029"} {
		subject := "a" + separator + "b"
		for _, tc := range []struct {
			pattern string
			want    []int
		}{
			{`(?m)a$`, []int{0, 1}}, {`(?m)^b`, []int{1 + len(separator), 2 + len(separator)}},
			{`\Ab`, nil}, {`a\z`, nil}, {`(?m)(?-m:^b)|^z`, nil},
		} {
			re, err := CompileWithLineMode(tc.pattern, WideTextLines)
			if err != nil {
				t.Fatal(err)
			}
			if got := re.FindStringIndex(subject); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("separator=%q pattern=%q got=%v want=%v", separator, tc.pattern, got, tc.want)
			}
		}
		// The accepted LF compatibility exception differs from a lone wide terminator.
		re, _ := CompileWithLineMode(`(?m)^$`, WideTextLines)
		if got := re.MatchString("a" + separator); got != strings.HasSuffix(separator, "\n") {
			t.Fatalf("terminal caret for %q: %v", separator, got)
		}
		re, _ = CompileWithLineMode(`(?m)($)+`, WideTextLines)
		if got := re.FindStringIndexAt("a"+separator, len("a"+separator)); !reflect.DeepEqual(got, []int{len("a" + separator), len("a" + separator)}) {
			t.Fatalf("missing terminal dollar for %q: %v", separator, got)
		}
	}
}

func TestOffsetContextAndConcurrentReuse(t *testing.T) {
	re, err := CompileWithLineMode(`(?m)(?P<letter>^b|\Ab)|($)+`, WideTextLines)
	if err != nil {
		t.Fatal(err)
	}
	subject := "a\rb"
	want := []int{2, 3, 2, 3, -1, -1}
	if got := re.FindStringSubmatchIndexAt(subject, 2); !reflect.DeepEqual(got, want) {
		t.Fatalf("offset captures got %v, want %v", got, want)
	}
	if re.FindStringIndexAt(subject, -1) != nil || re.FindStringIndexAt(subject, 4) != nil {
		t.Fatal("invalid offset accepted")
	}
	var wg sync.WaitGroup
	bad := make(chan []int, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 32 {
				if got := re.FindStringSubmatchIndexAt(subject, 2); !reflect.DeepEqual(got, want) {
					bad <- got
					return
				}
			}
		}()
	}
	wg.Wait()
	close(bad)
	for got := range bad {
		t.Fatalf("concurrent capture result %v", got)
	}
}
