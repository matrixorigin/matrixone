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
	"fmt"
	"strings"
)

type LexError struct {
	Position int
	Message  string
	Input    string
}

func (e *LexError) Error() string {
	if e.Input != "" {
		return fmt.Sprintf("lex error at position %d: %s (near %q)", e.Position, e.Message, e.Input)
	}
	return fmt.Sprintf("lex error at position %d: %s", e.Position, e.Message)
}

const (
	DefaultMySQLVersionID = 80400

	ErrUnterminatedString   = "unterminated string literal"
	ErrUnterminatedComment  = "unterminated block comment"
	ErrUnterminatedIdent    = "unterminated quoted identifier"
	ErrUnterminatedHint     = "unterminated optimizer hint"
	ErrInvalidHexLiteral    = "invalid hex literal"
	ErrInvalidBinaryLiteral = "invalid binary literal"
	ErrInvalidTokenBounds   = "invalid token bounds"
	ErrUnterminatedDollar   = "unterminated dollar-quoted string"
	ErrParameterMarker      = "parameter markers are not supported"
)

func NewLexError(position int, message string, input string) *LexError {
	return &LexError{Position: position, Message: message, Input: input}
}

type SQLMode uint64

const (
	// MODE_NO_BACKSLASH_ESCAPES disables backslash as escape character in strings
	MODE_NO_BACKSLASH_ESCAPES SQLMode = 1 << 0
	// MODE_ANSI_QUOTES treats " as identifier delimiter instead of string delimiter
	MODE_ANSI_QUOTES SQLMode = 1 << 1
	// MODE_PIPES_AS_CONCAT makes || use MySQL's concatenation token.
	MODE_PIPES_AS_CONCAT SQLMode = 1 << 2
	// MODE_HIGH_NOT_PRECEDENCE makes NOT use the high-precedence token.
	MODE_HIGH_NOT_PRECEDENCE SQLMode = 1 << 3
	// MODE_IGNORE_SPACE allows whitespace between function names and '('.
	MODE_IGNORE_SPACE SQLMode = 1 << 4
)

type Token struct {
	Type  int
	Start int
	End   int
	Err   error
}

func (t Token) IsError() bool {
	return t.Type == ABORT_SYM || t.Err != nil
}

type Lexer struct {
	input            string
	pos              int
	tokStart         int
	nextState        LexState
	sqlMode          SQLMode
	stmtPrepareMode  bool
	inHintComment    bool
	inVersionComment bool
	hintAllowed      bool
	tokenConfig      *TokenConfig
	mysqlVersionID   int
	sawComment       bool
	sawNonComment    bool
}

func (l *Lexer) SawComment() bool {
	return l.sawComment
}

func (l *Lexer) SawNonComment() bool {
	return l.sawNonComment
}

func NewLexer(input string) *Lexer {
	return &Lexer{
		input:          input,
		nextState:      MY_LEX_START,
		tokenConfig:    GetTokenConfig(),
		mysqlVersionID: DefaultMySQLVersionID,
	}
}

func (l *Lexer) SetSQLMode(mode SQLMode) {
	l.sqlMode = mode
}

func (l *Lexer) mysqlVersionInt() int {
	return l.mysqlVersionID
}

func (l *Lexer) SetMySQLVersionID(versionID int) {
	if versionID <= 0 {
		versionID = DefaultMySQLVersionID
	}
	l.mysqlVersionID = versionID
}

func (l *Lexer) SetPrepareMode(enabled bool) {
	l.stmtPrepareMode = enabled
}

func (l *Lexer) TokenText(t Token) (string, error) {
	if t.Start < 0 || t.End > len(l.input) || t.Start > t.End {
		return "", NewLexError(t.Start, ErrInvalidTokenBounds, "")
	}
	return l.input[t.Start:t.End], nil
}

func (l *Lexer) followedBySingleQuote(pos int) bool {
	for pos < len(l.input) {
		if !strings.ContainsRune(" \t\r\n", rune(l.input[pos])) {
			return l.input[pos] == '\''
		}
		pos++
	}
	return false
}

func (l *Lexer) peek() byte {
	if l.pos >= len(l.input) {
		return 0
	}
	return l.input[l.pos]
}

func (l *Lexer) peekN(n int) byte {
	if l.pos+n >= len(l.input) {
		return 0
	}
	return l.input[l.pos+n]
}

func (l *Lexer) advance() byte {
	if l.pos >= len(l.input) {
		return 0
	}
	c := l.input[l.pos]
	l.pos++
	return c
}

func (l *Lexer) skip() {
	if l.pos < len(l.input) {
		l.pos++
	}
}

func (l *Lexer) skipN(n int) {
	l.pos += n
	if l.pos > len(l.input) {
		l.pos = len(l.input)
	}
}

func (l *Lexer) backup() {
	if l.pos > 0 {
		l.pos--
	}
}

func (l *Lexer) tokenLen() int {
	return l.pos - l.tokStart
}

func (l *Lexer) startToken() {
	l.tokStart = l.pos
}

func (l *Lexer) eof() bool {
	return l.pos >= len(l.input)
}

func (l *Lexer) findKeyword(length int) int {
	if length == 0 {
		return 0
	}
	text := l.input[l.tokStart : l.tokStart+length]
	upper := toUpper(text)
	return l.tokenConfig.LookupKeyword(upper)
}

func (l *Lexer) returnToken(t Token) Token {
	l.hintAllowed = TokenIsHintable(t.Type)
	return t
}

// Matches MySQL's int_token() in sql_lex.cc.
func (l *Lexer) intToken(length int) int {
	str := l.input[l.tokStart : l.tokStart+length]
	return ClassifyInteger(str)
}

func (l *Lexer) Lex() Token {
	if l.inHintComment {
		return l.lexHintToken()
	}

	l.startToken()
	if l.peek() == 0 && l.pos < len(l.input) {
		// NUL is the lexer sentinel, not whitespace or a comment. Record it so
		// callers can distinguish a malformed no-token input from comments.
		l.sawNonComment = true
	}
	state := l.nextState
	l.nextState = MY_LEX_START

	for {
		result, handled := l.dispatchState(state)
		// Fallback for unregistered states. Shouldn't happen.
		if !handled {
			c := byte(0)
			if l.tokStart < len(l.input) {
				c = l.input[l.tokStart]
			}
			return Token{Type: int(c), Start: l.tokStart, End: l.pos}
		}

		switch result.kind {
		case lexEmit:
			l.hintAllowed = TokenIsHintable(result.token.Type)
			return result.token
		case lexEmitAndPrime:
			l.hintAllowed = TokenIsHintable(result.token.Type)
			l.nextState = result.nextState
			return result.token
		case lexContinue:
			state = result.nextState
		}
	}
}
