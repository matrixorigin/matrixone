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

func (l *Lexer) lexHintToken() Token {
	l.startToken()

	// Skip whitespace
	l.skipHintWhitespace()
	l.startToken()

	// Check for end of hint comment */
	if l.peek() == '*' && l.peekN(1) == '/' {
		return l.lexHintClose()
	}

	// Check for EOF (unclosed hint)
	if l.eof() {
		return l.lexHintEOF()
	}

	c := l.advance()

	// Dispatch based on first character
	switch {
	case isIdentStart(c):
		return l.lexHintIdentOrKeyword()
	case isDigit(c):
		return l.lexHintNumber()
	case c == '\'':
		return l.lexHintQuoted('\'', false)
	case c == '`':
		return l.lexHintQuoted('`', true)
	case c == '"':
		return l.lexHintQuoted('"', (l.sqlMode&MODE_ANSI_QUOTES) != 0)
	default:
		return l.lexHintChar(c)
	}
}

func (l *Lexer) skipHintWhitespace() {
	for isSpace(l.peek()) {
		l.skip()
	}
}

func (l *Lexer) lexHintClose() Token {
	l.skip() // *
	l.skip() // /
	l.inHintComment = false
	return l.returnToken(Token{Type: TOK_HINT_COMMENT_CLOSE, Start: l.tokStart, End: l.pos})
}

func (l *Lexer) lexHintEOF() Token {
	l.inHintComment = false
	return l.returnToken(Token{
		Type:  ABORT_SYM,
		Start: l.tokStart,
		End:   l.pos,
		Err:   NewLexError(l.tokStart, ErrUnterminatedHint, ""),
	})
}

func (l *Lexer) lexHintIdentOrKeyword() Token {
	for isIdentChar(l.peek()) {
		l.skip()
	}
	length := l.tokenLen()

	// Check if it's a hint keyword
	text := l.input[l.tokStart : l.tokStart+length]
	if tok, ok := HintKeywords[toUpper(text)]; ok {
		return l.returnToken(Token{Type: tok, Start: l.tokStart, End: l.pos})
	}

	// Return as IDENT
	return l.returnToken(Token{Type: IDENT, Start: l.tokStart, End: l.pos})
}

func (l *Lexer) lexHintNumber() Token {
	for isDigit(l.peek()) {
		l.skip()
	}
	if l.peek() == '.' {
		l.skip()
		if !isDigit(l.peek()) {
			return l.returnToken(Token{
				Type:  ABORT_SYM,
				Start: l.tokStart,
				End:   l.pos,
				Err:   NewLexError(l.tokStart, "invalid optimizer hint number", ""),
			})
		}
		for isDigit(l.peek()) {
			l.skip()
		}
		if isIdentChar(l.peek()) {
			for isIdentChar(l.peek()) {
				l.skip()
			}
			return l.returnToken(Token{Type: IDENT, Start: l.tokStart, End: l.pos})
		}
		return l.returnToken(Token{Type: NUM, Start: l.tokStart, End: l.pos})
	}
	if isIdentChar(l.peek()) {
		if l.peek() == 'K' || l.peek() == 'M' || l.peek() == 'G' {
			l.skip()
			if !isIdentChar(l.peek()) {
				return l.returnToken(Token{Type: NUM, Start: l.tokStart, End: l.pos})
			}
		}
		for isIdentChar(l.peek()) {
			l.skip()
		}
		return l.returnToken(Token{Type: IDENT, Start: l.tokStart, End: l.pos})
	}
	return l.returnToken(Token{Type: NUM, Start: l.tokStart, End: l.pos})
}

func (l *Lexer) lexHintQuoted(quote byte, ident bool) Token {
	for {
		ch := l.peek()
		if l.eof() {
			return l.returnToken(Token{
				Type:  ABORT_SYM,
				Start: l.tokStart,
				End:   l.pos,
				Err:   NewLexError(l.tokStart, ErrUnterminatedString, ""),
			})
		}
		l.skip()
		if ch == quote {
			if l.peek() == quote {
				l.skip()
				continue
			}
			break
		}
	}
	tokenType := TEXT_STRING
	if ident {
		tokenType = IDENT_QUOTED
	}
	return l.returnToken(Token{Type: tokenType, Start: l.tokStart, End: l.pos})
}

func (l *Lexer) lexHintChar(c byte) Token {
	return l.returnToken(Token{Type: int(c), Start: l.tokStart, End: l.pos})
}
