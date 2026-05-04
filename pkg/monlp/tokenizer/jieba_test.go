// Copyright 2024 Matrix Origin
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

package tokenizer

import (
	"testing"
)

func collectJieba(tknz *JiebaTokenizer, input string) []Token {
	var out []Token
	for tk := range tknz.Tokenize([]byte(input)) {
		out = append(out, tk)
	}
	return out
}

func tokenString(tk Token) string {
	return string(tk.TokenBytes[1 : tk.TokenBytes[0]+1])
}

func checkJieba(t *testing.T, useHmm bool, input string, want []string) {
	t.Helper()
	tknz := NewJiebaTokenizer(useHmm)
	defer tknz.Free()

	got := collectJieba(tknz, input)
	if len(got) != len(want) {
		gotStrs := make([]string, len(got))
		for i, tk := range got {
			gotStrs[i] = tokenString(tk)
		}
		t.Fatalf("Tokenize(%q, hmm=%v) length = %d (%v), want %d (%v)",
			input, useHmm, len(got), gotStrs, len(want), want)
	}
	for i, tk := range got {
		if tokenString(tk) != want[i] {
			t.Errorf("Tokenize(%q, hmm=%v)[%d] = %q, want %q",
				input, useHmm, i, tokenString(tk), want[i])
		}
		if tk.TokenPos != int32(i) {
			t.Errorf("Tokenize(%q, hmm=%v)[%d] TokenPos = %d, want %d",
				input, useHmm, i, tk.TokenPos, i)
		}
	}
}

func TestJiebaTokenizerCJK(t *testing.T) {
	checkJieba(t, true, "我来到北京清华大学",
		[]string{"我", "来到", "北京", "清华大学"})

	checkJieba(t, true, "小明硕士毕业于中国科学院计算所",
		[]string{"小明", "硕士", "毕业", "于", "中国科学院", "计算所"})

	checkJieba(t, false, "小明硕士毕业于中国科学院计算所",
		[]string{"小", "明", "硕士", "毕业", "于", "中国科学院", "计算所"})
}

func TestJiebaTokenizerHMMDifference(t *testing.T) {
	// "杭研" is not in the default dictionary; HMM new-word discovery should
	// glue it together while pure dictionary mode keeps the chars separate.
	checkJieba(t, true, "他来到了网易杭研大厦",
		[]string{"他", "来到", "了", "网易", "杭研", "大厦"})
	checkJieba(t, false, "他来到了网易杭研大厦",
		[]string{"他", "来到", "了", "网易", "杭", "研", "大厦"})
}

func TestJiebaTokenizerLatinAndPunct(t *testing.T) {
	// ASCII punctuation and whitespace are dropped; Latin tokens are lowered.
	checkJieba(t, true, "hello, world!", []string{"hello", "world"})
	checkJieba(t, true, "I love 中国", []string{"i", "love", "中国"})
}

func TestJiebaTokenizerEmpty(t *testing.T) {
	tknz := NewJiebaTokenizer(true)
	defer tknz.Free()

	if got := collectJieba(tknz, ""); len(got) != 0 {
		t.Errorf("empty input produced %d tokens, want 0", len(got))
	}
}

func TestJiebaTokenizerBytePos(t *testing.T) {
	// BytePos must point at the start of the token within the original input.
	input := "我来到北京清华大学"
	tknz := NewJiebaTokenizer(true)
	defer tknz.Free()

	got := collectJieba(tknz, input)
	wantStarts := []int32{0, 3, 9, 15}
	if len(got) != len(wantStarts) {
		t.Fatalf("got %d tokens, want %d", len(got), len(wantStarts))
	}
	for i, tk := range got {
		if tk.BytePos != wantStarts[i] {
			t.Errorf("token[%d] BytePos = %d, want %d", i, tk.BytePos, wantStarts[i])
		}
		// Token text should be a prefix of input[BytePos:] (modulo lowercase).
		s := tokenString(tk)
		if int(tk.BytePos)+len(s) > len(input) {
			t.Errorf("token[%d] BytePos+len out of range", i)
			continue
		}
		if input[tk.BytePos:int(tk.BytePos)+len(s)] != s {
			t.Errorf("token[%d] %q does not match input slice %q",
				i, s, input[tk.BytePos:int(tk.BytePos)+len(s)])
		}
	}
}

func TestJiebaTokenizerImplementsInterface(t *testing.T) {
	var _ Tokenizer = (*JiebaTokenizer)(nil)
}

func TestJiebaTokenizerFreeIdempotent(t *testing.T) {
	tknz := NewJiebaTokenizer(true)
	tknz.Free()
	// Second Free must be a no-op.
	tknz.Free()
	// Tokenize after Free returns no tokens rather than panicking.
	if got := collectJieba(tknz, "我来到北京"); len(got) != 0 {
		t.Errorf("tokenize after Free produced %d tokens, want 0", len(got))
	}
}

func TestJiebaTokenizerLongLatinTruncated(t *testing.T) {
	// A 30-char latin word exceeds MAX_TOKEN_SIZE (23). The output token bytes
	// must be capped at MAX_TOKEN_SIZE.
	input := "abcdefghijklmnopqrstuvwxyzabcd"
	tknz := NewJiebaTokenizer(true)
	defer tknz.Free()

	got := collectJieba(tknz, input)
	if len(got) == 0 {
		t.Fatalf("expected at least one token for %q", input)
	}
	for i, tk := range got {
		if int(tk.TokenBytes[0]) > MAX_TOKEN_SIZE {
			t.Errorf("token[%d] length %d exceeds MAX_TOKEN_SIZE %d",
				i, tk.TokenBytes[0], MAX_TOKEN_SIZE)
		}
	}
}
