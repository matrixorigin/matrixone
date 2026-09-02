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

type lexResultKind int

const (
	lexContinue     lexResultKind = iota // transition to nextState within current Lex()
	lexEmit                              // emit token, done
	lexEmitAndPrime                      // emit token AND set starting state for next Lex()
)

type lexResult struct {
	kind      lexResultKind
	token     Token
	nextState LexState // lexContinue: state to continue to; lexEmitAndPrime: next Lex() start state
}

func done(t Token) lexResult {
	return lexResult{kind: lexEmit, token: t}
}

func doneWithNext(t Token, nextLex LexState) lexResult {
	return lexResult{kind: lexEmitAndPrime, token: t, nextState: nextLex}
}

func cont(state LexState) lexResult {
	return lexResult{kind: lexContinue, nextState: state}
}

func (l *Lexer) handleStart() lexResult {
	for getStateMap(l.peek()) == MY_LEX_SKIP {
		l.skip()
	}
	l.startToken()
	c := l.advance()
	if c == 0 && l.tokStart < len(l.input) {
		// NUL is the lexer sentinel, not whitespace or a comment. Record it
		// even when it follows a comment handled in the same Lex call.
		l.sawNonComment = true
	}
	return cont(getStateMap(c))
}

func (l *Lexer) handleSkip() lexResult {
	l.skip()
	return cont(MY_LEX_START)
}

func (l *Lexer) handleEOL() lexResult {
	return done(Token{Type: END_OF_INPUT, Start: l.tokStart, End: l.pos})
}

func (l *Lexer) handleLineComment() lexResult {
	l.sawComment = true
	l.hintAllowed = false
	l.scanLineComment()
	return cont(MY_LEX_START)
}

func (l *Lexer) handleAsterisks() lexResult {
	c := l.input[l.tokStart]
	if l.inVersionComment {
		if c == '*' && l.peek() == '/' {
			l.skip()
			l.skip()
			l.inVersionComment = false
			l.hintAllowed = false
			return cont(MY_LEX_START)
		}
	}
	return done(Token{Type: int(c), Start: l.tokStart, End: l.pos})
}

func (l *Lexer) handleCharToken() lexResult {
	c := l.input[l.tokStart]
	return done(Token{Type: int(c), Start: l.tokStart, End: l.pos})
}

func (l *Lexer) handleIdentOrHex() lexResult {
	if l.peek() == '\'' {
		return cont(MY_LEX_HEX_NUMBER)
	}
	return cont(MY_LEX_IDENT)
}

func (l *Lexer) handleIdentOrBin() lexResult {
	if l.peek() == '\'' {
		return cont(MY_LEX_BIN_NUMBER)
	}
	return cont(MY_LEX_IDENT)
}

func (l *Lexer) handleIntOrReal() lexResult {
	if l.peek() != '.' {
		return done(Token{Type: l.intToken(l.tokenLen()), Start: l.tokStart, End: l.pos})
	}
	l.skip()
	return cont(MY_LEX_REAL)
}

func (l *Lexer) handleRealOrPoint() lexResult {
	if isDigit(l.peek()) {
		return cont(MY_LEX_REAL)
	}
	c := l.input[l.tokStart]
	return done(Token{Type: int(c), Start: l.tokStart, End: l.pos})
}

func (l *Lexer) handleStringOrDelimiter() lexResult {
	if (l.sqlMode & MODE_ANSI_QUOTES) != 0 {
		return cont(MY_LEX_USER_VARIABLE_DELIMITER)
	}
	return cont(MY_LEX_STRING)
}

func (l *Lexer) handleChar() lexResult {
	// Get the character at token start
	if l.tokStart >= len(l.input) {
		return done(Token{Type: END_OF_INPUT, Start: l.tokStart, End: l.pos})
	}
	c := l.input[l.tokStart]

	// Check for special two-char sequences with '-'
	if c == '-' && l.peek() == '-' {
		// Check for "-- " comment
		nextChar := l.peekN(1)
		if isSpace(nextChar) || isCntrl(nextChar) {
			return cont(MY_LEX_COMMENT)
		}
	}

	// Check for JSON arrow operators
	if c == '-' && l.peek() == '>' {
		l.skip() // consume '>'
		if l.peek() == '>' {
			l.skip() // consume second '>'
			return doneWithNext(Token{Type: JSON_UNQUOTED_SEPARATOR_SYM, Start: l.tokStart, End: l.pos}, MY_LEX_START)
		}
		return doneWithNext(Token{Type: JSON_SEPARATOR_SYM, Start: l.tokStart, End: l.pos}, MY_LEX_START)
	}

	// Check for placeholder '?' in prepare mode
	if c == '?' && l.stmtPrepareMode && !isIdentChar(l.peek()) {
		return doneWithNext(Token{Type: PARAM_MARKER, Start: l.tokStart, End: l.pos}, MY_LEX_START)
	}

	// Close paren does NOT allow signed numbers after (don't set nextState)
	if c == ')' {
		return done(Token{Type: int(c), Start: l.tokStart, End: l.pos})
	}

	// All other chars set nextState = MY_LEX_START to allow signed numbers
	return doneWithNext(Token{Type: int(c), Start: l.tokStart, End: l.pos}, MY_LEX_START)
}

func (l *Lexer) handleIdent() lexResult {
	// Scan identifier
	for isIdentChar(l.peek()) {
		l.skip()
	}

	length := l.tokenLen()

	// Check if followed by '.' and identifier char
	if l.peek() == '.' && isIdentChar(l.peekN(1)) {
		// Still do keyword lookup for system variable scopes
		if tokval := l.findKeyword(length); tokval != 0 {
			return doneWithNext(l.returnToken(Token{Type: tokval, Start: l.tokStart, End: l.tokStart + length}), MY_LEX_IDENT_SEP)
		}
		return doneWithNext(l.returnToken(Token{Type: IDENT, Start: l.tokStart, End: l.tokStart + length}), MY_LEX_IDENT_SEP)
	}

	l.backup() // Unget the non-ident char

	// Check if it's a keyword
	if tokval := l.findKeyword(length); tokval != 0 {
		tokval = l.adjustKeywordForSQLMode(tokval)
		l.skip() // Re-skip the character we ungot
		return doneWithNext(l.returnToken(Token{Type: tokval, Start: l.tokStart, End: l.tokStart + length}), MY_LEX_START)
	}
	l.skip() // Re-skip

	// Return as IDENT
	return done(l.returnToken(Token{Type: IDENT, Start: l.tokStart, End: l.tokStart + length}))
}

// handleIdentSep handles MY_LEX_IDENT_SEP state, dot between identifiers.
func (l *Lexer) handleIdentSep() lexResult {
	c := l.advance()
	if isIdentChar(l.peek()) {
		return doneWithNext(Token{Type: int(c), Start: l.tokStart, End: l.pos}, MY_LEX_IDENT_START)
	}
	return doneWithNext(Token{Type: int(c), Start: l.tokStart, End: l.pos}, MY_LEX_START)
}

func (l *Lexer) handleIdentStart() lexResult {
	for isIdentChar(l.peek()) {
		l.skip()
	}
	length := l.tokenLen()
	if l.peek() == '.' && isIdentChar(l.peekN(1)) {
		return doneWithNext(Token{Type: IDENT, Start: l.tokStart, End: l.tokStart + length}, MY_LEX_IDENT_SEP)
	}
	return done(Token{Type: IDENT, Start: l.tokStart, End: l.tokStart + length})
}

// handleCmpOp handles MY_LEX_CMP_OP state ( >, >=, =, != operators).
func (l *Lexer) handleCmpOp() lexResult {
	nextState := getStateMap(l.peek())
	if nextState == MY_LEX_CMP_OP || nextState == MY_LEX_LONG_CMP_OP {
		l.skip()
	}
	length := l.tokenLen()
	if tokval := l.findKeyword(length); tokval != 0 {
		return doneWithNext(Token{Type: tokval, Start: l.tokStart, End: l.pos}, MY_LEX_START)
	}
	return cont(MY_LEX_CHAR)
}

// handleLongCmpOp handles MY_LEX_LONG_CMP_OP state (<, <=, <>, <=> operators).
func (l *Lexer) handleLongCmpOp() lexResult {
	nextState := getStateMap(l.peek())
	if nextState == MY_LEX_CMP_OP || nextState == MY_LEX_LONG_CMP_OP {
		l.skip()
		if getStateMap(l.peek()) == MY_LEX_CMP_OP {
			l.skip()
		}
	}
	length := l.tokenLen()
	if tokval := l.findKeyword(length); tokval != 0 {
		return doneWithNext(Token{Type: tokval, Start: l.tokStart, End: l.pos}, MY_LEX_START)
	}
	return cont(MY_LEX_CHAR)
}

// handleBool handles MY_LEX_BOOL state (&& || operators).
func (l *Lexer) handleBool() lexResult {
	c := l.input[l.tokStart]
	if l.peek() != c {
		return done(Token{Type: int(c), Start: l.tokStart, End: l.pos})
	}
	l.skip()
	if tokval := l.boolToken(c); tokval != 0 {
		return doneWithNext(Token{Type: tokval, Start: l.tokStart, End: l.pos}, MY_LEX_START)
	}
	return done(Token{Type: int(c), Start: l.tokStart, End: l.pos})
}

func (l *Lexer) boolToken(c byte) int {
	if c == '|' {
		if l.sqlMode&MODE_PIPES_AS_CONCAT != 0 {
			return OR_OR_SYM
		}
		return OR2_SYM
	}
	return l.findKeyword(2)
}

func (l *Lexer) adjustKeywordForSQLMode(tok int) int {
	if tok == NOT_SYM && l.sqlMode&MODE_HIGH_NOT_PRECEDENCE != 0 {
		return NOT2_SYM
	}
	return tok
}

// handleSetVar handles MY_LEX_SET_VAR (:= operator).
func (l *Lexer) handleSetVar() lexResult {
	c := l.input[l.tokStart]
	if l.peek() != '=' {
		return done(Token{Type: int(c), Start: l.tokStart, End: l.pos})
	}
	l.skip()
	return done(Token{Type: SET_VAR, Start: l.tokStart, End: l.pos})
}

// handleUserVariable handles MY_LEX_USER_END state for user variables (@var, @@var, etc.).
// This matches MySQL's handling in sql_lexer.cc lines 1206-1221.
//
// MySQL behavior:
// - @ followed by @ → next state is MY_LEX_SYSTEM_VAR (for @@var system variables)
// - @ followed by string/quoted delimiter → keep as MY_LEX_START (handles naturally)
// - @ followed by identifier → next state is MY_LEX_HOSTNAME (returns LEX_HOSTNAME token)
func (l *Lexer) handleUserVariable() lexResult {
	// '@' has already been consumed by handleStart
	// Check what follows the @
	nextState := getStateMap(l.peek())

	switch nextState {
	case MY_LEX_STRING, MY_LEX_USER_VARIABLE_DELIMITER, MY_LEX_STRING_OR_DELIMITER:
		// String-quoted variable name (@'var', @`var`, @"var")
		// Let the normal lexer handle it
		return doneWithNext(Token{Type: int('@'), Start: l.tokStart, End: l.pos}, MY_LEX_START)
	case MY_LEX_USER_END:
		// Another @ follows - this is a system variable (@@var)
		return doneWithNext(Token{Type: int('@'), Start: l.tokStart, End: l.pos}, MY_LEX_SYSTEM_VAR)
	default:
		// Identifier follows - user variable, use MY_LEX_HOSTNAME to return LEX_HOSTNAME
		return doneWithNext(Token{Type: int('@'), Start: l.tokStart, End: l.pos}, MY_LEX_HOSTNAME)
	}
}

// handleHostname handles MY_LEX_HOSTNAME state for user variable names.
// The variable name after @ is returned as LEX_HOSTNAME token which gets
// normalized to ? in digest output (since LEX_HOSTNAME is a string literal).
func (l *Lexer) handleHostname() lexResult {
	// Scan identifier characters (alphanumeric, '.', '_', '$')
	l.startToken()
	for {
		c := l.peek()
		if !isAlnum(c) && c != '.' && c != '_' && c != '$' {
			break
		}
		l.skip()
	}

	if l.tokenLen() == 0 {
		// No identifier found - just return to start
		return cont(MY_LEX_START)
	}

	return done(Token{Type: LEX_HOSTNAME, Start: l.tokStart, End: l.pos})
}

// handleSystemVar handles MY_LEX_SYSTEM_VAR state for system variables (@@var).
// Returns the second @ token and sets up to parse the variable name as an identifier.
func (l *Lexer) handleSystemVar() lexResult {
	// We're positioned at the second @
	l.startToken()
	l.skip() // Skip the second '@'

	// Check if next char is a quoted delimiter (@@`var`)
	if getStateMap(l.peek()) == MY_LEX_USER_VARIABLE_DELIMITER {
		return doneWithNext(Token{Type: int('@'), Start: l.tokStart, End: l.pos}, MY_LEX_START)
	}

	// Otherwise, parse as identifier or keyword
	return doneWithNext(Token{Type: int('@'), Start: l.tokStart, End: l.pos}, MY_LEX_IDENT_OR_KEYWORD)
}

// handleIdentOrKeyword handles MY_LEX_IDENT_OR_KEYWORD state.
// This is used after @@ for system variables.
func (l *Lexer) handleIdentOrKeyword() lexResult {
	l.startToken()

	// Scan identifier characters
	for isIdentChar(l.peek()) {
		l.skip()
	}

	length := l.tokenLen()
	if length == 0 {
		return cont(MY_LEX_START)
	}

	// Check if followed by '.' and identifier char
	if l.peek() == '.' && isIdentChar(l.peekN(1)) {
		// Check for keyword (like GLOBAL, SESSION)
		if tokval := l.findKeyword(length); tokval != 0 {
			return doneWithNext(Token{Type: tokval, Start: l.tokStart, End: l.tokStart + length}, MY_LEX_IDENT_SEP)
		}
		return doneWithNext(Token{Type: IDENT, Start: l.tokStart, End: l.tokStart + length}, MY_LEX_IDENT_SEP)
	}

	// Check if it's a keyword
	if tokval := l.findKeyword(length); tokval != 0 {
		return doneWithNext(Token{Type: tokval, Start: l.tokStart, End: l.tokStart + length}, MY_LEX_START)
	}

	// Return as IDENT
	return done(Token{Type: IDENT, Start: l.tokStart, End: l.tokStart + length})
}

func (l *Lexer) handleNumberIdent() lexResult {
	c := l.input[l.tokStart]
	// Check for 0x (hex) or 0b (binary) prefix
	if c == '0' {
		nextC := l.advance()
		switch {
		case nextC == 'x' || nextC == 'X':
			return l.handleHexLiteral0x()
		case nextC == 'b' || nextC == 'B':
			return l.handleBinLiteral0b()
		default:
			l.backup() // Put back the char after '0'
		}
	}

	return l.handleDigitSequence()
}

// handleHexLiteral0x handles 0x... hex literals.
// Called after '0x' or '0X' prefix has been consumed.
func (l *Lexer) handleHexLiteral0x() lexResult {
	// Consume hex digits
	for isHexDigit(l.peek()) {
		l.skip()
	}

	// Valid hex if length >= 3 (0x + at least one digit) and not followed by ident char
	if l.tokenLen() >= 3 && !isIdentChar(l.peek()) {
		return done(Token{Type: HEX_NUM, Start: l.tokStart, End: l.pos})
	}

	// Not valid hex - treat as identifier
	l.backup()
	return cont(MY_LEX_IDENT_START)
}

// handleBinLiteral0b handles 0b... binary literals.
// Called after '0b' or '0B' prefix has been consumed.
func (l *Lexer) handleBinLiteral0b() lexResult {
	// Consume binary digits
	for {
		peek := l.peek()
		if peek != '0' && peek != '1' {
			break
		}
		l.skip()
	}

	// Valid binary if length >= 3 (0b + at least one digit) and not followed by ident char
	if l.tokenLen() >= 3 && !isIdentChar(l.peek()) {
		return done(Token{Type: BIN_NUM, Start: l.tokStart, End: l.pos})
	}

	// Not valid binary - treat as identifier
	l.backup()
	return cont(MY_LEX_IDENT_START)
}

func (l *Lexer) handleDigitSequence() lexResult {
	// Consume remaining digits
	for isDigit(l.peek()) {
		l.skip()
	}

	// Check what follows the digits
	nextC := l.peek()
	if !isIdentChar(nextC) {
		// Pure number, check for decimal or stay as int
		return cont(MY_LEX_INT_OR_REAL)
	}

	// Check for exponent (e/E)
	if nextC == 'e' || nextC == 'E' {
		if result, ok := l.tryParseExponent(); ok {
			return result
		}
	}

	// Number followed by identifier chars - becomes identifier
	return cont(MY_LEX_IDENT_START)
}

// tryParseExponent attempts to parse an exponent suffix (e.g., e10, e+10, e-10).
// Returns the lexResult and true if successful, or false if the exponent is invalid.
func (l *Lexer) tryParseExponent() (lexResult, bool) {
	// Save position before consuming exponent in case it's invalid
	savedPos := l.pos
	l.skip() // consume e/E

	peek := l.peek()
	if isDigit(peek) {
		// 1e10 format
		l.skip()
		for isDigit(l.peek()) {
			l.skip()
		}
		return done(Token{Type: FLOAT_NUM, Start: l.tokStart, End: l.pos}), true
	}

	if peek == '+' || peek == '-' {
		l.skip() // consume sign
		if isDigit(l.peek()) {
			l.skip()
			for isDigit(l.peek()) {
				l.skip()
			}
			return done(Token{Type: FLOAT_NUM, Start: l.tokStart, End: l.pos}), true
		}
	}

	// Not a valid exponent - restore position
	l.pos = savedPos
	return lexResult{}, false
}

// handleHexNumber handles MY_LEX_HEX_NUMBER state, X'hex' literals.
func (l *Lexer) handleHexNumber() lexResult {
	l.skip() // Skip the opening '
	for {
		c := l.advance()
		if c == '\'' {
			break
		}
		if c == 0 {
			return done(Token{
				Type:  ABORT_SYM,
				Start: l.tokStart,
				End:   l.pos,
				Err:   NewLexError(l.tokStart, ErrInvalidHexLiteral, ""),
			})
		}
		if !isHexDigit(c) {
			return done(Token{
				Type:  ABORT_SYM,
				Start: l.tokStart,
				End:   l.pos,
				Err:   NewLexError(l.tokStart, ErrInvalidHexLiteral, ""),
			})
		}
	}
	// Valid hex requires even number of hex digits
	length := l.tokenLen()
	if (length % 2) == 0 {
		return done(Token{
			Type:  ABORT_SYM,
			Start: l.tokStart,
			End:   l.pos,
			Err:   NewLexError(l.tokStart, ErrInvalidHexLiteral, ""),
		})
	}
	return done(Token{Type: HEX_NUM, Start: l.tokStart, End: l.pos})
}

// handleBinNumber handles MY_LEX_BIN_NUMBER state, B'bin' literals.
func (l *Lexer) handleBinNumber() lexResult {
	l.skip() // Skip the '
	for {
		c := l.advance()
		if c == '\'' {
			break
		}
		if c == 0 {
			return done(Token{
				Type:  ABORT_SYM,
				Start: l.tokStart,
				End:   l.pos,
				Err:   NewLexError(l.tokStart, ErrInvalidBinaryLiteral, ""),
			})
		}
		if c != '0' && c != '1' {
			return done(Token{
				Type:  ABORT_SYM,
				Start: l.tokStart,
				End:   l.pos,
				Err:   NewLexError(l.tokStart, ErrInvalidBinaryLiteral, ""),
			})
		}
	}
	return done(Token{Type: BIN_NUM, Start: l.tokStart, End: l.pos})
}

// handleReal handles MY_LEX_REAL state, fractional part of decimal numbers.
func (l *Lexer) handleReal() lexResult {
	for isDigit(l.peek()) {
		l.skip()
	}

	nextC := l.peek()
	if nextC == 'e' || nextC == 'E' {
		// Save position before consuming exponent in case it's invalid
		savedPos := l.pos
		l.skip() // consume e/E
		peek := l.peek()
		if peek == '+' || peek == '-' {
			l.skip() // consume sign
		}
		if !isDigit(l.peek()) {
			// Not a valid exponent - restore position and return as DECIMAL_NUM
			l.pos = savedPos
			return done(Token{Type: DECIMAL_NUM, Start: l.tokStart, End: l.pos})
		}
		for isDigit(l.peek()) {
			l.skip()
		}
		return done(Token{Type: FLOAT_NUM, Start: l.tokStart, End: l.pos})
	}

	return done(Token{Type: DECIMAL_NUM, Start: l.tokStart, End: l.pos})
}

type QuoteScanMode int

const (
	QuoteModeString     QuoteScanMode = iota // allows backslash escapes
	QuoteModeIdentifier                      // no backslash escapes
)

// scanQuoted scans a quoted string or identifier.
// The opening quote has already been consumed by the caller.
// sep is the quote character (', ", or `).
// mode determines whether backslash escapes are processed.
func (l *Lexer) scanQuoted(sep byte, mode QuoteScanMode, tokenType int) lexResult {
	allowBackslashEscape := mode == QuoteModeString && (l.sqlMode&MODE_NO_BACKSLASH_ESCAPES) == 0

	for {
		c := l.advance()
		if c == 0 {
			// Unterminated quoted literal
			return done(Token{
				Type:  ABORT_SYM,
				Start: l.tokStart,
				End:   l.pos,
				Err:   NewLexError(l.tokStart, ErrUnterminatedString, l.input),
			})
		}

		// Handle backslash escapes in string mode
		if allowBackslashEscape && c == '\\' {
			if l.peek() != 0 {
				l.skip() // Skip the escaped character
			}
			continue
		}

		// Handle quote character
		if c == sep {
			if l.peek() == sep {
				// Doubled quote = escape
				l.skip()
				continue
			}
			// End of quoted literal
			break
		}
	}

	return done(Token{Type: tokenType, Start: l.tokStart, End: l.pos})
}

func (l *Lexer) handleString() lexResult {
	sep := l.input[l.tokStart]
	return l.scanQuoted(sep, QuoteModeString, TEXT_STRING)
}

func (l *Lexer) handleQuotedIdent() lexResult {
	sep := l.input[l.tokStart]
	return l.scanQuoted(sep, QuoteModeIdentifier, IDENT_QUOTED)
}

// handleNChar handles MY_LEX_IDENT_OR_NCHAR state, N'string' or identifier.
func (l *Lexer) handleNChar() lexResult {
	if l.peek() != '\'' {
		return cont(MY_LEX_IDENT)
	}
	// Found N'string' - parse as NCHAR_STRING
	l.skip() // Skip the opening '
	return l.scanQuoted('\'', QuoteModeString, NCHAR_STRING)
}

func (l *Lexer) handleDollarQuoted() lexResult {
	// c is '$' (already consumed)
	if l.peek() == '$' {
		// $$...$$ anonymous dollar-quoted string
		l.skip() // consume second $
		return done(l.scanDollarQuotedString(""))
	}

	// Check for $tag$...$tag$ (tag is identifier chars between two $)
	tagStart := l.pos
	for isIdentChar(l.peek()) && l.peek() != '$' {
		l.skip()
	}

	if l.peek() == '$' && l.pos > tagStart {
		// We have $tag$ - this is a tagged dollar-quoted string
		tag := l.input[tagStart:l.pos]
		l.skip() // consume the closing $ of the tag
		return done(l.scanDollarQuotedString(tag))
	}

	// Not a dollar-quoted string - treat as identifier
	for isIdentChar(l.peek()) {
		l.skip()
	}

	length := l.tokenLen()
	return done(l.returnToken(Token{Type: IDENT, Start: l.tokStart, End: l.tokStart + length}))
}

func (l *Lexer) handleLongComment() lexResult {
	l.sawComment = true
	c := l.input[l.tokStart]
	if l.peek() != '*' {
		// Not a comment, just a '/' character (division operator)
		return l.handleDivisionOp(c)
	}

	// Skip the '*'
	l.skip()

	// Check for optimizer hint /*+
	if l.peek() == '+' {
		return l.handleOptimizerHint()
	}

	// Check for version comment /*!
	if l.peek() == '!' {
		return l.handleVersionComment()
	}

	// Regular block comment /* ... */
	return l.handleBlockComment()
}

// handleDivisionOp handles the case where '/' is not followed by '*'.
// This is the division operator, not a comment.
func (l *Lexer) handleDivisionOp(c byte) lexResult {
	return done(l.returnToken(Token{Type: int(c), Start: l.tokStart, End: l.pos}))
}

func (l *Lexer) handleOptimizerHint() lexResult {
	l.skip() // Skip '+'

	// Check if last token was a hintable keyword (SELECT, INSERT, etc.)
	if l.hintAllowed {
		// Enter hint mode
		l.inHintComment = true
		return done(l.returnToken(Token{Type: TOK_HINT_COMMENT_OPEN, Start: l.tokStart, End: l.pos}))
	}

	// Not after hintable keyword - treat as regular comment
	return l.consumeBlockComment()
}

// handleVersionComment handles /*! version comments.
func (l *Lexer) handleVersionComment() lexResult {
	l.hintAllowed = false
	l.skip() // Skip '!'

	// Check for version number (5 or 6 digits)
	version, digitCount := l.scanVersionNumber()

	if digitCount >= 5 {
		// Skip the version digits
		l.skipN(digitCount)

		// Check if version is <= configured MySQL version
		if version <= l.mysqlVersionInt() {
			// Execute the content as code - restart lexing
			l.inVersionComment = true
			return cont(MY_LEX_START)
		}
		// Version is too high - skip as comment
		return l.consumeBlockComment()
	}

	if digitCount == 0 {
		// /*! without version - always execute
		l.inVersionComment = true
		return cont(MY_LEX_START)
	}

	// Invalid version format (1-4 digits) - skip as comment
	return l.consumeBlockComment()
}

// handleBlockComment handles regular block comments /* ... */.
func (l *Lexer) handleBlockComment() lexResult {
	l.hintAllowed = false
	return l.consumeBlockComment()
}

func (l *Lexer) consumeBlockComment() lexResult {
	l.hintAllowed = false
	if !l.scanComment() {
		return done(Token{
			Type:  ABORT_SYM,
			Start: l.tokStart,
			End:   l.pos,
			Err:   NewLexError(l.tokStart, ErrUnterminatedComment, l.input),
		})
	}
	return cont(MY_LEX_START)
}

// scanComment consumes comment until closing */.
// Returns true if comment was properly closed, false if EOF reached.
func (l *Lexer) scanComment() bool {
	for !l.eof() {
		c := l.advance()
		if c == '*' && l.peek() == '/' {
			l.skip() // Skip the '/'
			return true
		}
	}
	return false // Unclosed comment
}

// scanLineComment consumes a single-line comment (# or --) until EOL.
func (l *Lexer) scanLineComment() {
	for {
		c := l.advance()
		if c == 0 || c == '\n' {
			break
		}
	}
}

func (l *Lexer) scanVersionNumber() (version int, digitCount int) {
	for i := 0; i < 6; i++ {
		ch := l.peekN(i)
		if isDigit(ch) {
			version = version*10 + int(ch-'0')
			digitCount++
		} else {
			break
		}
	}
	return version, digitCount
}

func (l *Lexer) scanDollarQuotedString(tag string) Token {
	closingDelim := "$" + tag + "$"
	closingLen := len(closingDelim)

	for !l.eof() {
		if l.pos+closingLen <= len(l.input) && l.input[l.pos:l.pos+closingLen] == closingDelim {
			l.pos += closingLen
			return l.returnToken(Token{Type: DOLLAR_QUOTED_STRING_SYM, Start: l.tokStart, End: l.pos})
		}
		l.pos++
	}
	return l.returnToken(Token{
		Type:  ABORT_SYM,
		Start: l.tokStart,
		End:   l.pos,
		Err:   NewLexError(l.tokStart, ErrUnterminatedDollar, l.input),
	})
}
