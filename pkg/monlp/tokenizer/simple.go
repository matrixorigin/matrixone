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
	"bytes"
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

type SimpleTokenizer struct {
	// input and output
	input []byte

	// the buffer to store the _token
	begin        int
	currTokenPos int
	latinBuf     bytes.Buffer

	Done bool
	Err  error
}

func NewSimpleTokenizer(input []byte) (*SimpleTokenizer, error) {
	if len(input) > 1024*1024*1024 {
		return nil, moerr.NewInternalErrorNoCtx("input too large")
	}
	return &SimpleTokenizer{input: input}, nil
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

type handler func(t *SimpleTokenizer, pos int, rune rune, yield func(Token) bool) handler

func beginToken(t *SimpleTokenizer, pos int, rune rune, yield func(Token) bool) handler {
	if isBreakerRune(rune) {
		t.begin = pos
		return breakerToken
	} else if isLatin(rune) {
		t.begin = pos
		return latinToken
	} else {
		t.begin = pos
		return cjkToken
	}
}

func breakerToken(t *SimpleTokenizer, pos int, rune rune, yield func(Token) bool) handler {
	if isBreakerRune(rune) {
		return breakerToken
	} else {
		// if the breaker is not a single byte we increase token count.
		if pos > t.begin+1 {
			t.currTokenPos += 1
		}
		if isLatin(rune) {
			t.begin = pos
			return latinToken
		} else {
			t.begin = pos
			return cjkToken
		}
	}
}

func latinToken(t *SimpleTokenizer, pos int, rune rune, yield func(Token) bool) handler {
	if isBreakerRune(rune) {
		t.outputLatin(pos, yield)
		t.begin = pos
		return breakerToken
	} else if isLatin(rune) {
		// noop
		return latinToken
	} else {
		t.outputLatin(pos, yield)
		t.begin = pos
		return cjkToken
	}
}

func cjkToken(t *SimpleTokenizer, pos int, rune rune, yield func(Token) bool) handler {
	if isBreakerRune(rune) {
		t.outputCJK(pos, yield)
		t.begin = pos
		return breakerToken
	} else if isLatin(rune) {
		t.outputCJK(pos, yield)
		t.begin = pos
		return latinToken
	} else {
		return cjkToken
	}
}

func (t *SimpleTokenizer) outputLatin(pos int, yield func(Token) bool) {
	t.latinBuf.Reset()
	var bs []byte
	if pos <= t.begin+MAX_TOKEN_SIZE {
		bs = t.input[t.begin:pos]
	} else {
		if t.input[t.begin+MAX_TOKEN_SIZE-1] <= 127 {
			bs = t.input[t.begin : t.begin+MAX_TOKEN_SIZE]
		} else {
			bs = t.input[t.begin : pos+MAX_TOKEN_SIZE-1]
		}
	}

	ls := strings.ToLower(string(bs))
	token := Token{}
	token.TokenBytes[0] = byte(len(ls))
	copy(token.TokenBytes[1:], []byte(ls))
	token.TokenPos = int32(t.currTokenPos)
	token.BytePos = int32(t.begin)
	if !yield(token) {
		t.Done = true
		return
	}
	t.currTokenPos += 1
}

// outputCJK outputs the CJK token from t.begin to pos
// if token contains latin letter, we do not normalize like outputLatin
func (t *SimpleTokenizer) outputCJK(pos int, yield func(Token) bool) {
	ibuf := t.input[t.begin:pos]
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
		token.TokenPos = int32(t.currTokenPos)
		token.BytePos = int32(t.begin + ia)
		if !yield(token) {
			t.Done = true
			return
		}
		t.currTokenPos += 1
		ia = ib
		ib = ic
		ic = id
		_, sz = utf8.DecodeRune(ibuf[id:])
		id += sz
	}
}

func (t *SimpleTokenizer) Tokenize() iter.Seq[Token] {
	return func(yield func(Token) bool) {
		if len(t.input) == 0 {
			return
		}

		h := beginToken

		for pos, rune := range string(t.input) {
			if t.Done {
				return
			}

			h = h(t, pos, rune, yield)
			if h == nil {
				break
			}
		}

		// send a space to output last token
		h(t, len(t.input), ' ', yield)
	}
}
