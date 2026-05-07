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

func outputLatin(st *simpleState, pos int, yield func(Token, error) bool) {
	var bs []byte
	if pos <= st.begin+MAX_TOKEN_SIZE {
		bs = st.input[st.begin:pos]
	} else {
		if st.input[st.begin+MAX_TOKEN_SIZE-1] <= 127 {
			// last character is ascii
			bs = st.input[st.begin : st.begin+MAX_TOKEN_SIZE]
		} else {
			// find the leading byte
			n := 1
			for i := range 4 {
				// leading byte must have value at least 192 (binary 11000000)
				if st.input[st.begin+MAX_TOKEN_SIZE-i-1] >= 192 {
					break
				}
				n++
			}
			bs = st.input[st.begin : st.begin+MAX_TOKEN_SIZE-n]
		}
	}

	ls := strings.ToLower(string(bs))
	token := Token{}
	token.TokenBytes[0] = byte(len(ls))
	copy(token.TokenBytes[1:], []byte(ls))
	token.TokenPos = st.currTokenPos
	token.BytePos = int32(st.begin)
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
