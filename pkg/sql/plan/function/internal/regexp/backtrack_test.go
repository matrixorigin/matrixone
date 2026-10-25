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
	"fmt"
	"reflect"
	std "regexp"
	"regexp/syntax"
	"strings"
	"testing"
)

func TestTerminalRepeatCompilation(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		tagged  int
	}{
		{`.*$`, 1}, {`(?s)(a|ab).*\z`, 1}, {`a.*b|c.*\z`, 1},
		{`a.*$|b.+$`, 2}, {`a.*?$`, 0}, {`a(.*)$`, 0},
		{`(?m)a.*$`, 0}, {`a.*\b$`, 0}, {`a.*b`, 0},
		{`\Aa.*\z`, 0},                          // The existing one-pass executor owns this pattern.
		{strings.Repeat(`(a)`, 180) + `.*$`, 0}, // Too large for bitstate.
	} {
		t.Run(tc.pattern, func(t *testing.T) {
			re := MustCompile(tc.pattern)
			ast, err := syntax.Parse(tc.pattern, syntax.Perl)
			if err != nil {
				t.Fatal(err)
			}
			original, err := syntax.Compile(ast.Simplify())
			if err != nil {
				t.Fatal(err)
			}
			if re.prog.Start != original.Start || re.prog.NumCap != original.NumCap || len(re.prog.Inst) != len(original.Inst) {
				t.Fatal("graph/capture metadata changed")
			}
			n := 0
			for pc, inst := range re.prog.Inst {
				if inst.Op == syntax.InstAltMatch {
					n++
					inst.Op = syntax.InstAlt
				}
				if !reflect.DeepEqual(inst, original.Inst[pc]) {
					t.Fatalf("instruction %d changed beyond its optimization tag", pc)
				}
			}
			if n != tc.tagged {
				t.Fatalf("tagged=%d, want %d", n, tc.tagged)
			}
		})
	}
}

func TestTerminalRepeatMatchesStandardLibrary(t *testing.T) {
	patterns := []string{
		`.*$`, `.+\z`, `(?s).*$`, `(a|ab).*\z`, `(a)?(?:b|a).*\z`,
		`(?P<x>a*).*\z`, `a*.*$|(?s:b.*$)`, `(?:a+|b).*\z`,
		`^https?://(?:www\.)?([^/]+)/.*$`,
		// Near misses must retain captures, lazy priority and line assertions.
		`a.*?$`, `a(.*)$`, `a(?:.)*$`, `a.*\b$`, `a.*b$`, `(?m)a.*$`,
		`a.*$|(?s:.*)`, `(?s:a.*)$`, `a.*(?:\z|b)`,
	}
	subjects := []string{
		"", "ab", "aaa\nb", "a\nb\na", "a\r\nb", "a\ra", "a\u0085b",
		"a\u2028b", "a\u2029b", "a\x00b", "a\xffb", "a雪😀", "\na", "a\n",
		"https://www.example.test/a/b", "http://example.test/", "http://a/b\nc",
		strings.Repeat("a", 2048) + "\n" + strings.Repeat("b", 2048),
	}
	// Exhaust short inputs to challenge alternative/capture priorities and LF
	// failures followed by later start positions, without a large fixture.
	for n := 0; n < 256; n++ {
		v := n
		var s [4]byte
		for i := range s {
			s[i] = "ab\n\xff"[v%4]
			v /= 4
		}
		subjects = append(subjects, string(s[:]))
	}
	for _, pattern := range patterns {
		for _, longest := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/longest=%t", pattern, longest), func(t *testing.T) {
				got, want := MustCompile(pattern), std.MustCompile(pattern)
				if longest {
					got.Longest()
					want.Longest()
				}
				for _, s := range subjects {
					if got.MatchString(s) != want.MatchString(s) ||
						!reflect.DeepEqual(got.FindStringSubmatchIndex(s), want.FindStringSubmatchIndex(s)) ||
						!reflect.DeepEqual(got.FindSubmatchIndex([]byte(s)), want.FindSubmatchIndex([]byte(s))) ||
						!reflect.DeepEqual(got.FindAllStringSubmatchIndex(s, -1), want.FindAllStringSubmatchIndex(s, -1)) ||
						got.ReplaceAllString(s, `${x}:$1`) != want.ReplaceAllString(s, `${x}:$1`) {
						t.Fatalf("match/capture/replacement differs for %q", s)
					}
				}
			})
		}
	}
}

func TestTerminalRepeatOffsetsAndReuse(t *testing.T) {
	for _, mode := range []LineMode{UnixLines, WideTextLines, WideBinaryLines, WideBinaryFoldLines} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			// Parallel calls share the immutable compiled program while alternating
			// failed/successful inputs exercises input-local pooled state reset.
			re, err := CompileWithLineMode(`(?m)(a|ab).*\z`, mode)
			if err != nil {
				t.Fatal(err)
			}
			if re.onepass != nil {
				t.Fatal("expected bitstate execution")
			}
			nfa := *re
			nfa.maxBitStateLen = 0
			for worker := 0; worker < 8; worker++ {
				t.Run(fmt.Sprint(worker), func(t *testing.T) {
					t.Parallel()
					for range 8 {
						for _, s := range []string{"a\nb\n", "a\nb\na", "ab\r雪", "a\u0085b", "a\uE085b", "a\u2028b", "a\xffb", "a", ""} {
							for start := 0; start <= len(s); start++ {
								got := re.FindStringSubmatchIndexAt(s, start)
								want := nfa.FindStringSubmatchIndexAt(s, start)
								if !reflect.DeepEqual(got, want) {
									t.Fatalf("subject=%q offset=%d: got %v want %v", s, start, got, want)
								}
							}
						}
					}
				})
			}
		})
	}
}

func TestTerminalRepeatExecutorBoundary(t *testing.T) {
	for _, pattern := range []string{`a.*$`, `(?s)a.*$`, `(a|aa).*\z`} {
		re, want := MustCompile(pattern), std.MustCompile(pattern)
		if re.onepass != nil || re.maxBitStateLen == 0 {
			t.Fatal("expected a bitstate/NFA boundary")
		}
		for _, size := range []int{re.maxBitStateLen - 1, re.maxBitStateLen} {
			for _, middle := range []string{"a", "\n"} {
				s := "a" + strings.Repeat("b", size/2) + middle + strings.Repeat("b", size-size/2-2)
				expected := want.FindStringSubmatchIndex(s)
				if !reflect.DeepEqual(re.FindStringSubmatchIndex(s), expected) ||
					!reflect.DeepEqual(re.FindReaderSubmatchIndex(strings.NewReader(s)), expected) {
					t.Fatalf("executor boundary differs: pattern=%q size=%d middle=%q", pattern, size, middle)
				}
			}
		}
	}
	// POSIX selects longest matching independently of the Perl-style compiler.
	s := "ab\naab"
	if got, want := MustCompilePOSIX(`(a|aa).*$`).FindStringSubmatchIndex(s), std.MustCompilePOSIX(`(a|aa).*$`).FindStringSubmatchIndex(s); !reflect.DeepEqual(got, want) {
		t.Fatalf("POSIX captures got %v want %v", got, want)
	}
}

func BenchmarkTerminalRepeat(b *testing.B) {
	for _, tc := range []struct{ name, pattern, subject string }{
		{"tail", `(a|ab).*\z`, "ab" + strings.Repeat("xyz", 100)},
		{"dotall", `(?s)(a|ab).*\z`, "ab\n" + strings.Repeat("xyz", 100)},
		{"short", `(a|ab).*\z`, "ab"},
		{"prefix_failure", `a.*\z`, strings.Repeat("b", 300)},
		{"early_lf", `a.*\z`, "a\n" + strings.Repeat("b", 4096)},
		{"captured_tail", `(a.*)\z`, "a" + strings.Repeat("xyz", 100)},
		{"lazy_tail", `a.*?\z`, "a" + strings.Repeat("xyz", 100)},
		{"multiline", `(?m)a.*$`, "a" + strings.Repeat("xyz", 100)},
		{"nonterminal", `a.*b`, "a" + strings.Repeat("xyz", 100) + "b"},
		{"mixed_program", `a.*b|c.*\z`, "a" + strings.Repeat("xyz", 100) + "b"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			re := MustCompile(tc.pattern)
			b.ReportAllocs()
			for b.Loop() {
				re.FindStringSubmatchIndex(tc.subject)
			}
		})
	}
	for _, size := range []int{256, 1024, 4096} {
		b.Run(fmt.Sprintf("failed_starts/%d", size), func(b *testing.B) {
			re := MustCompile(`a.*\z`)
			s := strings.Repeat("a", size) + "\n"
			b.ReportAllocs()
			for b.Loop() {
				re.FindStringSubmatchIndex(s)
			}
		})
	}
}
