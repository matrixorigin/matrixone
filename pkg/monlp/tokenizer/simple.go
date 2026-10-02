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
	"iter"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	MAX_TOKEN_SIZE = 23
)

type Token struct {
	TokenBytes [1 + MAX_TOKEN_SIZE]byte
	TokenPos   int32
	BytePos    int32
	// OrigLen is the token's span in the ORIGINAL input bytes [BytePos, BytePos+OrigLen).
	// It can differ from len(TokenBytes) when case folding changes the byte length (e.g.
	// U+023A folds to a 3-byte rune from a 2-byte one), so overlap detection must use this,
	// not the folded length.
	OrigLen int32
	// Latin reports the ORIGINAL rune class of the run (outputLatin vs outputCJK), independent
	// of the folded spelling's class. A Latin rune can fold to a >=0x7FF rune, so classification
	// must use this, not []rune of the folded token.
	Latin bool
}

// Tokenizer yields a sequence of (Token, error) pairs. Implementations may
// report errors before any token is emitted (e.g. input validation), or
// mid-stream for tokenizers that can fail partway through the input. Callers
// must check err on each iteration and stop on the first non-nil err.
type Tokenizer interface {
	Tokenize(input []byte) iter.Seq2[Token, error]
}

// SimpleTokenizer holds no per-call state; concurrent Tokenize calls on the
// same instance are safe.
type SimpleTokenizer struct{}

const simpleMaxInputSize = 1024 * 1024 * 1024

func NewSimpleTokenizer() *SimpleTokenizer {
	return &SimpleTokenizer{}
}

// simpleState carries the per-call tokenization state. Allocated once per
// Tokenize invocation and threaded through the handler chain.
type simpleState struct {
	input        []byte
	begin        int
	currTokenPos int32
	done         bool
}

func isBreakerRune(rune rune) bool {
	if rune < 128 {
		if rune >= '0' && rune <= '9' {
			return false
		} else if rune >= 'A' && rune <= 'Z' {
			return false
		} else if rune >= 'a' && rune <= 'z' {
			return false
		}
		return true
	}
	return unicode.IsPunct(rune) || unicode.IsSpace(rune)
}

// Assume we already tested isBreakerRune.  Test if rune is 1 or 2 byte UTF-8
func isLatin(rune rune) bool {
	return rune < 0x7FF
}

type handler func(st *simpleState, pos int, rune rune, yield func(Token, error) bool) handler

func beginToken(st *simpleState, pos int, rune rune, yield func(Token, error) bool) handler {
	if isBreakerRune(rune) {
		st.begin = pos
		return breakerToken
	} else if isLatin(rune) {
		st.begin = pos
		return latinToken
	} else {
		st.begin = pos
		return cjkToken
	}
}

func breakerToken(st *simpleState, pos int, rune rune, yield func(Token, error) bool) handler {
	if isBreakerRune(rune) {
		return breakerToken
	} else {
		// if the breaker is not a single byte we increase token count.
		if pos > st.begin+1 {
			st.currTokenPos += 1
		}
		if isLatin(rune) {
			st.begin = pos
			return latinToken
		} else {
			st.begin = pos
			return cjkToken
		}
	}
}

func latinToken(st *simpleState, pos int, rune rune, yield func(Token, error) bool) handler {
	if isBreakerRune(rune) {
		outputLatin(st, pos, yield)
		st.begin = pos
		return breakerToken
	} else if isLatin(rune) {
		// noop
		return latinToken
	} else {
		outputLatin(st, pos, yield)
		st.begin = pos
		return cjkToken
	}
}

func cjkToken(st *simpleState, pos int, rune rune, yield func(Token, error) bool) handler {
	if isBreakerRune(rune) {
		outputCJK(st, pos, yield)
		st.begin = pos
		return breakerToken
	} else if isLatin(rune) {
		outputCJK(st, pos, yield)
		st.begin = pos
		return latinToken
	} else {
		return cjkToken
	}
}

// TruncateLatinToken caps a Latin run at MAX_TOKEN_SIZE bytes using the exact byte-boundary rule
// SimpleTokenizer applies when it stores a token: if the last kept byte is ASCII, keep MAX_TOKEN_SIZE
// bytes; otherwise walk back to the leading byte of the last multi-byte char and drop it. This is NOT
// a clean UTF-8-boundary truncation (truncateUTF8) — when the cap lands one byte past a complete
// multi-byte char it still drops that char, so e.g. `a`+12×`я` (25B) stores `a`+10×`я` (21B), not 23B.
// A query token built outside the tokenizer (fulltext2's ngramPhraseSlots) must reproduce this rule
// byte-for-byte or NL/BM25/quoted-boolean look up a token the index never stored (#29276).
func TruncateLatinToken(bs []byte) []byte {
	if len(bs) <= MAX_TOKEN_SIZE {
		return bs
	}
	if bs[MAX_TOKEN_SIZE-1] <= 127 {
		// last character is ascii
		return bs[:MAX_TOKEN_SIZE]
	}
	// find the leading byte
	n := 1
	for i := range 4 {
		// leading byte must have value at least 192 (binary 11000000)
		if bs[MAX_TOKEN_SIZE-i-1] >= 192 {
			break
		}
		n++
	}
	return bs[:MAX_TOKEN_SIZE-n]
}

// NormalizeLatinToken reproduces, for a raw Latin run, the exact token bytes
// SimpleTokenizer.outputLatin stores: cap the run at MAX_TOKEN_SIZE on the byte-boundary
// rule, lowercase it, then RE-CAP the folded bytes. Case folding can EXPAND a Latin run --
// U+023A is 2 bytes, its lowercase U+2C65 is 3 -- so the lowered form can exceed
// MAX_TOKEN_SIZE even when the original bytes fit; without the re-cap a quoted BOOLEAN
// phrase of 8x U+023A folded to 24 bytes and panicked when a reader sliced the 24-byte
// value from the fixed buffer (#29271 P2). A query token built OUTSIDE the tokenizer
// (fulltext2's ngramPhraseSlots -> NL / BM25 / quoted-boolean) MUST call this, or it looks
// up a token the index never stored (#29276).
func NormalizeLatinToken(raw []byte) []byte {
	bs := TruncateLatinToken(raw)
	return TruncateLatinToken([]byte(strings.ToLower(string(bs))))
}

func outputLatin(st *simpleState, pos int, yield func(Token, error) bool) {
	ls := NormalizeLatinToken(st.input[st.begin:pos])
	token := Token{}
	token.TokenBytes[0] = byte(len(ls))
	copy(token.TokenBytes[1:], ls)
	token.TokenPos = st.currTokenPos
	token.BytePos = int32(st.begin)
	token.OrigLen = int32(pos - st.begin)
	token.Latin = true
	if !yield(token, nil) {
		st.done = true
		return
	}
	st.currTokenPos += 1
}

// outputCJK outputs the CJK token from st.begin to pos
// if token contains latin letter, we do not normalize like outputLatin
func outputCJK(st *simpleState, pos int, yield func(Token, error) bool) {
	ibuf := st.input[st.begin:pos]
	ia := 0
	_, ib := utf8.DecodeRune(ibuf)
	_, sz := utf8.DecodeRune(ibuf[ib:])
	ic := ib + sz
	_, sz = utf8.DecodeRune(ibuf[ic:])
	id := ic + sz

	for ia < id {
		token := Token{}
		token.TokenBytes[0] = byte(id - ia)
		copy(token.TokenBytes[1:], ibuf[ia:id])
		token.TokenPos = st.currTokenPos
		token.BytePos = int32(st.begin + ia)
		token.OrigLen = int32(id - ia)
		if !yield(token, nil) {
			st.done = true
			return
		}
		st.currTokenPos += 1
		ia = ib
		ib = ic
		ic = id
		_, sz = utf8.DecodeRune(ibuf[id:])
		id += sz
	}
}

func (SimpleTokenizer) Tokenize(input []byte) iter.Seq2[Token, error] {
	return func(yield func(Token, error) bool) {
		if len(input) > simpleMaxInputSize {
			yield(Token{}, moerr.NewInternalErrorNoCtx("input too large"))
			return
		}
		if len(input) == 0 {
			return
		}

		st := &simpleState{input: input}
		h := handler(beginToken)

		for pos, rune := range string(input) {
			if st.done {
				return
			}

			h = h(st, pos, rune, yield)
			if h == nil {
				break
			}
		}

		// send a space to output last token
		if !st.done {
			h(st, len(input), ' ', yield)
		}
	}
}
