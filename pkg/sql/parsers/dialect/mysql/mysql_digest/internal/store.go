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
//
// Portions derived from github.com/rashiq/mysql-digest; see ../LICENSE.

package internal

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

type storedToken struct {
	tokType int
	text    string
}

type tokenStore struct {
	tokens      []storedToken
	tokenArray  []byte
	tokenConfig *TokenConfig
	maxLength   int
	full        bool
}

// NewTokenStore creates a token store with an optional byte limit.
func NewTokenStore(maxLength *int) *tokenStore {
	limit := -1
	if maxLength != nil {
		limit = *maxLength
	}
	return &tokenStore{
		tokens:      make([]storedToken, 0, 256),
		tokenArray:  make([]byte, 0, 1024),
		tokenConfig: GetTokenConfig(),
		maxLength:   limit,
	}
}

func (s *tokenStore) push(tokType int) {
	if s.full {
		return
	}
	if s.maxLength >= 0 && len(s.tokenArray)+2 > s.maxLength {
		s.full = true
		return
	}
	s.tokens = append(s.tokens, storedToken{tokType: tokType})
	binTok := tokType
	s.tokenArray = append(s.tokenArray,
		byte(binTok&0xff),
		byte((binTok>>8)&0xff))
}

// Binary format for identifiers: 2 bytes (token) + 2 bytes (length) + N bytes (text).
func (s *tokenStore) pushIdent(text string) {
	if s.full {
		return
	}
	if s.maxLength >= 0 && len(s.tokenArray)+4+len(text) > s.maxLength {
		s.full = true
		return
	}
	s.tokens = append(s.tokens, storedToken{tokType: TOK_IDENT, text: text})
	binTok := TOK_IDENT
	s.tokenArray = append(s.tokenArray,
		byte(binTok&0xff),
		byte((binTok>>8)&0xff),
		byte(len(text)&0xff),
		byte((len(text)>>8)&0xff))
	s.tokenArray = append(s.tokenArray, text...)
}

func (s *tokenStore) pop(n int) {
	if n <= 0 || n > len(s.tokens) {
		return
	}
	s.tokens = s.tokens[:len(s.tokens)-n]
	bytesToRemove := n * 2
	if bytesToRemove > len(s.tokenArray) {
		bytesToRemove = len(s.tokenArray)
	}
	s.tokenArray = s.tokenArray[:len(s.tokenArray)-bytesToRemove]
}

// peek2 returns the last two token types (second-to-last, last).
// Returns TOK_UNUSED for missing positions.
func (s *tokenStore) peek2() (int, int) {
	n := len(s.tokens)
	t1, t0 := TOK_UNUSED, TOK_UNUSED
	if n >= 1 {
		t0 = s.tokens[n-1].tokType
	}
	if n >= 2 {
		t1 = s.tokens[n-2].tokType
	}
	return t1, t0
}

// peek3 returns the last three token types (third-to-last, second-to-last, last).
// Returns TOK_UNUSED for missing positions.
func (s *tokenStore) peek3() (int, int, int) {
	n := len(s.tokens)
	t2, t1, t0 := TOK_UNUSED, TOK_UNUSED, TOK_UNUSED
	if n >= 1 {
		t0 = s.tokens[n-1].tokType
	}
	if n >= 2 {
		t1 = s.tokens[n-2].tokType
	}
	if n >= 3 {
		t2 = s.tokens[n-3].tokType
	}
	return t2, t1, t0
}

func (s *tokenStore) last() int {
	if len(s.tokens) == 0 {
		return TOK_UNUSED
	}
	return s.tokens[len(s.tokens)-1].tokType
}

func (s *tokenStore) len() int {
	return len(s.tokens)
}

// ComputeHash returns the digest hash.
func (s *tokenStore) ComputeHash() string {
	hash := sha256.Sum256(s.tokenArray)
	return hex.EncodeToString(hash[:])
}

// BuildText returns the normalized query text.
func (s *tokenStore) BuildText() string {
	var b strings.Builder
	addSpace := false

	for _, tok := range s.tokens {
		text := s.tokenToText(tok)
		if text == "" {
			continue
		}
		if addSpace {
			b.WriteByte(' ')
		}
		b.WriteString(text)
		addSpace = TokenAppendSpace(tok.tokType)
	}

	return b.String()
}

func (s *tokenStore) tokenToText(tok storedToken) string {
	if tok.tokType == TOK_IDENT {
		return "`" + escapeBackticks(tok.text) + "`"
	}
	text := s.tokenConfig.GetString(tok.tokType)
	if text == "(unknown)" {
		return ""
	}
	return text
}
