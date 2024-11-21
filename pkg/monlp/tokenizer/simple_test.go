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
	"strings"
	"testing"
)

func tokenize(input []byte) []Token {
	tknz, _ := NewSimpleTokenizer(input)
	var tokens []Token
	for t := range tknz.Tokenize() {
		tokens = append(tokens, t)
	}
	return tokens
}

func checkTokenize(t *testing.T, input string, checkTokens []Token) {
	tks := tokenize([]byte(input))
	if len(tks) != len(checkTokens) {
		t.Errorf("tokenize(%s) = %v, want %v", input, tks, checkTokens)
		return
	}
	for i, tk := range tks {
		if tk.TokenBytes != checkTokens[i].TokenBytes ||
			tk.TokenPos != checkTokens[i].TokenPos {
			t.Errorf("tokenize(%s) = %v, want %v", input, tks, checkTokens)
			return
		}
	}
}

func makeToken(token string, pos int32) Token {
	var tk Token
	tk.TokenBytes[0] = byte(len(token))
	copy(tk.TokenBytes[1:], []byte(token))
	tk.TokenPos = pos
	return tk
}

func TestLatin(t *testing.T) {
	checkTokenize(t, "hello, world", []Token{
		makeToken("hello", 0),
		makeToken("world", 2),
	})
	checkTokenize(t, " hello, world!   From Me.", []Token{
		makeToken("hello", 0),
		makeToken("world", 2),
		makeToken("from", 4),
		makeToken("me", 5),
	})
	checkTokenize(t, "  H1N1 Covid19 a b@b\nc3", []Token{
		makeToken("h1n1", 1),
		makeToken("covid19", 2),
		makeToken("a", 3),
		makeToken("b", 4),
		makeToken("b", 5),
		makeToken("c3", 6),
	})
	checkTokenize(t, "À bon chat, bon rat", []Token{
		makeToken(strings.ToLower("À"), 0),
		makeToken("bon", 1),
		makeToken("chat", 2),
		makeToken("bon", 4),
		makeToken("rat", 5),
	})
	checkTokenize(t, "Mieux vaut prévenir que guérir", []Token{
		makeToken("mieux", 0),
		makeToken("vaut", 1),
		makeToken("prévenir", 2),
		makeToken("que", 3),
		makeToken("guérir", 4),
	})
	checkTokenize(t, "abcdefgHiJklMnOpqRstUvwxyz is a 26 letters long word!", []Token{
		makeToken("abcdefghijklmnopqrstuvw", 0),
		makeToken("is", 1),
		makeToken("a", 2),
		makeToken("26", 3),
		makeToken("letters", 4),
		makeToken("long", 5),
		makeToken("word", 6),
	})
}

func TestCJK(t *testing.T) {
	checkTokenize(t, "相见时难别亦难", []Token{
		makeToken("相见时", 0),
		makeToken("见时难", 1),
		makeToken("时难别", 2),
		makeToken("难别亦", 3),
		makeToken("别亦难", 4),
		makeToken("亦难", 5),
		makeToken("难", 6),
	})
	checkTokenize(t, "I come, I see, I征服", []Token{
		makeToken("i", 0),
		makeToken("come", 1),
		makeToken("i", 3),
		makeToken("see", 4),
		makeToken("i", 6),
		makeToken("征服", 7),
		makeToken("服", 8),
	})
	checkTokenize(t, "中华铅笔2B的好用, 6B的太软了", []Token{
		makeToken("中华铅", 0),
		makeToken("华铅笔", 1),
		makeToken("铅笔", 2),
		makeToken("笔", 3),
		makeToken("2b", 4),
		makeToken("的好用", 5),
		makeToken("好用", 6),
		makeToken("用", 7),
		makeToken("6b", 9),
		makeToken("的太软", 10),
		makeToken("太软了", 11),
		makeToken("软了", 12),
		makeToken("了", 13),
	})
}

func TestBreakerRunes(t *testing.T) {
	for _, r := range " ,.!?;:()[]'{}<>\t\n\r-@#$%^&*_=+|\\\"`~" {
		if !isBreakerRune(r) {
			t.Errorf("Latin isBreakerRune(%c) = false, want true", r)
		}
	}
	for _, r := range "，。《》！-+" {
		if !isBreakerRune(r) {
			t.Errorf("CJK isBreakerRune(%c) = false, want true", r)
		}
	}
	for _, r := range "abcABC123" {
		if isBreakerRune(r) {
			t.Errorf("Latine isBreakerRune(%c) = true, want false", r)
		}
	}
	for _, r := range "相见时难别亦难" {
		if isBreakerRune(r) {
			t.Errorf("CJK isBreakerRune(%c) = true, want false", r)
		}
	}
}
