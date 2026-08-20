// Copyright 2021 Matrix Origin
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

package mysql

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

const eofChar = 0x100

// maxPoolSQLSize is the size threshold beyond which a scanner will not be kept
// in the pool and large fields will be cleared to release memory.
const maxPoolSQLSize = 1 << 20 // 1 MiB

var scannerPool = sync.Pool{
	New: func() any {
		return &Scanner{}
	},
}

type Scanner struct {
	LastToken           string
	LastError           error
	posVarIndex         int
	dialectType         dialect.DialectType
	sqlMode             SQLModeFlags
	MysqlSpecialComment *Scanner

	CommentFlag          bool
	Pos                  int
	Line                 int
	Col                  int
	PrePos               int
	buf                  string
	executableCommentEnd int

	strBuilder bytes.Buffer
}

func (s *Scanner) reset(clearLargeOnly bool, oversized bool) {
	// Reset light-weight state shared by both setSql and PutScanner
	s.LastToken = ""
	s.LastError = nil
	s.posVarIndex = 0
	s.MysqlSpecialComment = nil
	s.CommentFlag = false
	s.Pos = 0
	s.Line = 0
	s.Col = 0
	s.PrePos = 0
	s.executableCommentEnd = 0
	s.sqlMode = 0

	if clearLargeOnly {
		if oversized {
			// Oversized by SQL size: drop both to avoid retaining huge memory.
			s.buf = ""
			s.strBuilder = bytes.Buffer{}
		}
	}
}

func (s *Scanner) setSql(sql string) {
	// This is a mysql scanner, so we set the dialect type to mysql
	s.dialectType = dialect.MYSQL
	// Reset transient fields but do not aggressively clear buffers here so that
	// small capacities can be reused.
	s.reset(false, false)
	s.buf = sql
	// Reset length to 0; this keeps capacity for small cases.
	s.strBuilder.Reset()
}

func (s *Scanner) setSQLMode(mode SQLModeFlags) {
	s.sqlMode = mode
}

func NewScanner(dialectType dialect.DialectType, sql string) *Scanner {
	scanner := scannerPool.Get().(*Scanner)
	scanner.setSql(sql)
	return scanner
}

func NewScannerWithSQLMode(dialectType dialect.DialectType, sql string, sqlMode SQLModeFlags) *Scanner {
	scanner := NewScanner(dialectType, sql)
	scanner.setSQLMode(sqlMode)
	return scanner
}

func PutScanner(scanner *Scanner) {
	oversized := len(scanner.buf) > maxPoolSQLSize
	// Reset shared state. Only clear buffers/strings when oversized.
	scanner.reset(true, oversized)
	if oversized {
		return
	}
	scannerPool.Put(scanner)
}

func (s *Scanner) Scan() (int, string) {
	if s.MysqlSpecialComment != nil {
		msc := s.MysqlSpecialComment
		tok, val := msc.Scan()
		if tok != 0 {
			return tok, val
		}
		s.MysqlSpecialComment = nil
	}
	s.PrePos = s.Pos
	s.skipBlank()
	if size := supplementaryUTF8SequenceSizeAt(s.buf, s.Pos); size != 0 {
		start := s.Pos
		s.incN(size)
		return LEX_ERROR, s.buf[start:s.Pos]
	}
	switch ch := s.cur(); {
	case ch == '@':
		tokenID := AT_ID
		s.inc()
		s.skipBlank()
		if s.cur() == '@' {
			tokenID = AT_AT_ID
			s.inc()
		} else if s.cur() == '\'' || s.cur() == '"' {
			return int('@'), ""
		} else if s.cur() == ',' {
			return tokenID, ""
		}
		var tID int
		var tBytes string
		if s.cur() == '`' {
			s.inc()
			tID, tBytes = s.scanLiteralIdentifier()
		} else if s.cur() == eofChar {
			return LEX_ERROR, ""
		} else {
			tID, tBytes = s.scanIdentifier(true)
		}
		if tID == LEX_ERROR {
			return tID, ""
		}
		return tokenID, tBytes
	case isUnquotedIdentifierLetterAt(s.buf, s.Pos):
		if ch == 'X' || ch == 'x' {
			if s.peek(1) == '\'' {
				s.incN(2)
				return s.scanHex()
			}
		}
		if ch == 'B' || ch == 'b' {
			if s.peek(1) == '\'' {
				s.incN(2)
				return s.scanBitLiteral()
			}
		}
		if ch == '$' {
			typ, str := s.scanIdentifier(false)
			if s.cur() != '$' {
				return typ, str
			} else {
				// this is a dollar sign string
				strTyp, strStr := s.scanString('$', STRING)
				tagTyp, tagStr := s.scanIdentifier(false)
				if tagTyp == LEX_ERROR {
					return tagTyp, tagStr
				}
				if tagStr != str {
					return LEX_ERROR, string(byte(s.cur()))
				}
				s.inc()
				return strTyp, strStr
			}

		}

		if ch == '_' {
			if s.isCharsetIntroducer() {
				s.incN(8)
				s.skipBlank()
				s.inc()
				return s.scanString('\'', STRING)
			}
			return s.scanIdentifier(false)
		}
		return s.scanIdentifier(false)

	case isDigit(ch):
		return s.scanNumber()
	case ch == ':':
		if s.peek(1) == '=' {
			s.incN(2)
			return ASSIGNMENT, ""
		}
		if s.peek(1) == ':' {
			s.incN(2)
			return TYPECAST, ""
		}

		// Like mysql -h ::1 ?
		id, str := s.scanBindVar()
		if id == LEX_ERROR {
			// test for 'label:'
			s.skipBlank()
			// LOOP WHILE REPEAT
			if s.cur() != 'L' && s.cur() != 'l' && s.cur() != 'W' && s.cur() != 'w' && s.cur() != 'R' && s.cur() != 'r' {
				return id, str
			}
			return ':', ""
		} else {
			return id, str
		}
	case ch == ';':
		s.inc()
		return ';', ""
	case ch == '.' && isDigit(s.peek(1)):
		return s.scanNumber()
	case ch == '/':
		s.inc()
		switch s.cur() {
		case '/':
			s.inc()
			id, str := s.scanCommentTypeLine(2)
			if id == LEX_ERROR {
				return id, str
			}
			return s.Scan()
		case '*':
			s.inc()
			switch s.cur() {
			case '!':
				s.CommentFlag = true
				s.inc()
				if !s.readVersion() {
					return LEX_ERROR, ""
				}
				return s.Scan()
			default:
				id, str := s.scanCommentTypeBlock()
				if id == LEX_ERROR {
					return id, str
				}
				return s.Scan()
			}
		default:
			return int(ch), ""
		}
	case ch == '*':
		if !s.CommentFlag {
			return s.stepBackOneChar(ch)
		}
		s.inc()
		switch s.cur() {
		case '/':
			s.CommentFlag = false
			s.inc()
			if s.executableCommentEnd == 0 {
				s.executableCommentEnd = s.Pos
			}
			return s.Scan()
		default:
			return s.stepBackOneChar(ch)
		}
	case ch == '\'':
		if !s.CommentFlag {
			return s.stepBackOneChar(ch)
		}
		s.inc()
		switch {
		case s.cur() == '+':
			s.inc()
			switch s.cur() {
			case '\'':
				return s.Scan()
			default:
				return s.scanStringAddPlus(ch, STRING)
			}
		case isLetter(s.cur()):
			return s.scanString(ch, STRING)
		case s.cur() == '-':
			return s.scanString(ch, STRING)
		case s.cur() == '\'':
			return s.scanString(ch, STRING)
		case s.cur() == '|':
			return s.scanString(ch, STRING)
		case isDigit(s.cur()):
			return s.scanString(ch, STRING)
		default:
			return s.Scan()
		}
	case ch == '#':
		s.inc()
		id, str := s.scanCommentTypeLine(1)
		if id == LEX_ERROR {
			return id, str
		}
		return s.Scan()
	default:
		return s.stepBackOneChar(ch)
	}
}

// TakeExecutableCommentEnd returns the byte offset immediately after the first
// executable-comment terminator scanned since the previous call.
func (s *Scanner) TakeExecutableCommentEnd() int {
	end := s.executableCommentEnd
	s.executableCommentEnd = 0
	return end
}

func (s *Scanner) isCharsetIntroducer() bool {
	if s.peek(1) != 'u' || s.peek(2) != 't' || s.peek(3) != 'f' || s.peek(4) != '8' ||
		s.peek(5) != 'm' || s.peek(6) != 'b' || s.peek(7) != '4' {
		return false
	}

	pos := s.Pos + len("_utf8mb4")
	for pos < len(s.buf) {
		switch s.buf[pos] {
		case ' ', '\n', '\r', '\t':
			pos++
			continue
		}
		break
	}
	return pos < len(s.buf) && s.buf[pos] == '\''
}

// ScanComment finds all Comment (/*  */, //) until gets EOF or LEX_ERROR
func (s *Scanner) ScanComment() (int, string) {
	s.PrePos = s.Pos
	for {
		s.skipBlank()
		ch := s.cur()
		for ch != '/' && ch != eofChar {
			s.inc()
			ch = s.cur()
		}

		if ch == eofChar {
			break
		}

		s.inc()
		switch s.cur() {
		case '/': // //
			s.inc()
			return s.scanCommentTypeLine(2)
		case '*': // /*
			s.inc()
			return s.scanCommentTypeBlock()
		}
	}
	return eofChar, ""
}

func EofChar() int {
	return eofChar
}

func (s *Scanner) readVersion() bool {
	if s.Pos < len(s.buf) {
		if isDigit(s.cur()) {
			if s.Pos+4 < len(s.buf) {
				for i := 0; i < 5; i++ {
					if !isDigit(s.cur()) {
						return false
					}
					s.inc()
				}
				return true
			}
			return false
		}
	}
	return true
}

func (s *Scanner) stepBackOneChar(ch uint16) (int, string) {
	s.inc()
	switch ch {
	case eofChar:
		return 0, ""
	case '=', ',', '(', ')', '+', '*', '%', '^', '~', '{', '}':
		return int(ch), ""
	case '&':
		if s.cur() == '&' {
			s.inc()
			return AND, ""
		}
		return int(ch), ""
	case '|':
		if s.cur() == '|' {
			s.inc()
			if s.sqlMode.Has(SQLModePipesAsConcat) {
				return PIPE_CONCAT, ""
			}
			return OR, ""
		}
		return int(ch), ""
	case '?':
		// mysql's situation
		s.posVarIndex++
		buf := make([]byte, 0, 8)
		buf = append(buf, ":v"...)
		buf = strconv.AppendInt(buf, int64(s.posVarIndex), 10)
		return VALUE_ARG, string(buf)
	case '.':
		return int(ch), ""
	case '#':
		return s.scanCommentTypeLine(1)
	case '-':
		switch s.cur() {
		case '-':
			nextChar := s.peek(1)
			if nextChar == ' ' || nextChar == '\n' || nextChar == '\t' || nextChar == '\r' || nextChar == eofChar {
				s.inc()
				id, str := s.scanCommentTypeLine(2)
				if id == LEX_ERROR {
					return id, str
				}
				return s.Scan()
			}
		case '>':
			s.inc()
			if s.cur() == '>' {
				s.inc()
				return LONG_ARROW, ""
			}
			return ARROW, ""
		}
		return int(ch), ""
	case '<':
		switch s.cur() {
		case '>':
			s.inc()
			return NE, ""
		case '<':
			s.inc()
			return SHIFT_LEFT, ""
		case '=':
			s.inc()
			switch s.cur() {
			case '>':
				s.inc()
				return NULL_SAFE_EQUAL, ""
			default:
				return LE, ""
			}
		default:
			return int(ch), ""
		}
	case '>':
		switch s.cur() {
		case '=':
			s.inc()
			return GE, ""
		case '>':
			s.inc()
			return SHIFT_RIGHT, ""
		default:
			return int(ch), ""
		}
	case '!':
		if s.cur() == '=' {
			s.inc()
			return NE, ""
		}
		return int(ch), ""
	case '\'':
		return s.scanString(ch, STRING)
	case '"':
		if s.sqlMode.Has(SQLModeANSIQuotes) {
			return s.scanLiteralIdentifierWithDelim('"')
		}
		return s.scanString(ch, STRING)
	case '`':
		return s.scanLiteralIdentifier()
	default:
		return LEX_ERROR, string(byte(ch))
	}
}

func (s *Scanner) scanString(delim uint16, typ int) (int, string) {
	if delim == '$' {
		s.inc() // advance the first '$'
	}
	ch := s.cur()
	buf := &s.strBuilder
	defer s.strBuilder.Reset()
	for s.Pos < len(s.buf) {
		if ch == delim {
			if delim != '$' {
				s.inc()
			} else {
				return typ, buf.String()
			}
			if s.cur() != delim {
				return typ, buf.String()
			}
		} else if ch == '\\' && delim != '$' && !s.sqlMode.Has(SQLModeNoBackslashEscapes) {
			ch = handleEscape(s, buf)
			if ch == eofChar {
				break
			}
		}
		buf.WriteByte(byte(ch))
		if s.Pos < len(s.buf) {
			s.inc()
			ch = s.cur()
		}
	}
	return LEX_ERROR, buf.String()
}

func (s *Scanner) scanStringAddPlus(delim uint16, typ int) (int, string) {
	if delim == '$' {
		s.inc() // advance the first '$'
	}
	ch := s.cur()
	buf := &s.strBuilder
	defer s.strBuilder.Reset()
	buf.WriteByte(byte('+'))
	for s.Pos < len(s.buf) {
		if ch == delim {
			if delim != '$' {
				s.inc()
			} else {
				return typ, buf.String()
			}
			if s.cur() != delim {
				return typ, buf.String()
			}
		} else if ch == '\\' && delim != '$' && !s.sqlMode.Has(SQLModeNoBackslashEscapes) {
			ch = handleEscape(s, buf)
			if ch == eofChar {
				break
			}
		}
		buf.WriteByte(byte(ch))
		if s.Pos < len(s.buf) {
			s.inc()
			ch = s.cur()
		}
	}
	return LEX_ERROR, buf.String()
}

func handleEscape(s *Scanner, buf *bytes.Buffer) uint16 {
	s.inc()
	ch0 := s.cur()
	switch ch0 {
	case 'n':
		ch0 = '\n'
	case '0':
		ch0 = 0
	case 'b':
		ch0 = 8
	case 'Z':
		ch0 = 26
	case 'r':
		ch0 = '\r'
	case 't':
		ch0 = '\t'
	case '%', '_':
		buf.WriteByte('\\')
	}
	return ch0
}

// scanLiteralIdentifier scans an identifier enclosed by backticks. If the identifier
// is a simple literal, it'll be returned as a slice of the input buffer. If the identifier
// contains escape sequences, this function will fall back to scanLiteralIdentifierSlow
func (s *Scanner) scanLiteralIdentifier() (int, string) {
	return s.scanLiteralIdentifierWithDelim('`')
}

func (s *Scanner) scanLiteralIdentifierWithDelim(delim uint16) (int, string) {
	start := s.Pos
	for {
		switch s.cur() {
		case delim:
			if s.peek(1) != delim {
				if s.Pos == start {
					return LEX_ERROR, ""
				}
				s.inc()
				return QUOTE_ID, s.buf[start : s.Pos-1]
			}

			var buf strings.Builder
			buf.WriteString(s.buf[start:s.Pos])
			s.inc()
			return s.scanLiteralIdentifierSlow(&buf, delim)
		case eofChar:
			// Premature EOF.
			return LEX_ERROR, s.buf[start:s.Pos]
		default:
			s.inc()
		}
	}
}

// scanLiteralIdentifierSlow scans an identifier surrounded by backticks which may
// contain escape sequences instead of it. This method is only called from
// scanLiteralIdentifier once the first escape sequence is found in the identifier.
// The provided `buf` contains the contents of the identifier that have been scanned
// so far.
func (s *Scanner) scanLiteralIdentifierSlow(buf *strings.Builder, delim uint16) (int, string) {
	delimSeen := true
	for {
		if delimSeen {
			if s.cur() != delim {
				break
			}
			delimSeen = false
			buf.WriteByte(byte(delim))
			s.inc()
			continue
		}
		// The previous char was not the identifier delimiter.
		switch s.cur() {
		case delim:
			delimSeen = true
		case eofChar:
			// Premature EOF.
			return LEX_ERROR, buf.String()
		default:
			buf.WriteByte(byte(s.cur()))
			// keep scanning
		}
		s.inc()
	}
	return QUOTE_ID, buf.String()
}

// scanCommentTypeBlock scans a '/*' delimited comment;
// assumes the opening prefix has already been scanned
func (s *Scanner) scanCommentTypeBlock() (int, string) {
	start := s.Pos - 2
	for {
		if s.cur() == '*' {
			s.inc()
			if s.cur() == '/' {
				s.inc()
				break
			}
			continue
		}
		if s.cur() == eofChar {
			return LEX_ERROR, s.buf[start:s.Pos]
		}
		s.inc()
	}
	return COMMENT, s.buf[start:s.Pos]
}

// scanMySQLSpecificComment scans a MySQL comment pragma, which always starts with '//*`
/*func (s *Scanner) scanMySQLSpecificComment() (int, string) {
	start := s.Pos - 3
	for {
		if s.cur() == '*' {
			s.inc()
			if s.cur() == '/' {
				s.inc()
				break
			}
			continue
		}
		if s.cur() == eofChar {
			return LEX_ERROR, s.buf[start:s.Pos]
		}
		s.inc()
	}

	_, sql := ExtractMysqlComment(s.buf[start:s.Pos])

	s.MysqlSpecialComment = NewScanner(s.dialectType, sql)

	return s.Scan()
}*/

// ExtractMysqlComment extracts the version and SQL from a comment-only query
// such as /*!50708 sql here */
func ExtractMysqlComment(sql string) (string, string) {
	sql = sql[3 : len(sql)-2]

	digitCount := 0
	endOfVersionIndex := strings.IndexFunc(sql, func(c rune) bool {
		digitCount++
		return !unicode.IsDigit(c) || digitCount == 6
	})
	if endOfVersionIndex < 0 {
		return "", ""
	}
	if endOfVersionIndex < 5 {
		endOfVersionIndex = 0
	}
	version := sql[0:endOfVersionIndex]
	innerSQL := strings.TrimFunc(sql[endOfVersionIndex:], unicode.IsSpace)

	return version, innerSQL
}

// scanCommentTypeLine scans a SQL line-comment, which is applied until the end
// of the line. The given prefix length varies based on whether the comment
// is started with '//', '--' or '#'.
func (s *Scanner) scanCommentTypeLine(prefixLen int) (int, string) {
	start := s.Pos - prefixLen
	for s.cur() != eofChar {
		if s.cur() == '\n' {
			s.inc()
			break
		}
		s.inc()
	}
	return COMMENT, s.buf[start:s.Pos]
}

// ?
// scanBindVar scans a bind variable; assumes a ':' has been scanned right before
func (s *Scanner) scanBindVar() (int, string) {
	start := s.Pos
	token := VALUE_ARG

	s.inc()
	if s.cur() == ':' {
		token = LIST_ARG
		s.inc()
	}
	if !isLetter(s.cur()) {
		return LEX_ERROR, s.buf[start:s.Pos]
	}
	for {
		ch := s.cur()
		if !isLetter(ch) && !isDigit(ch) && ch != '.' {
			break
		}
		s.inc()
	}
	return token, s.buf[start:s.Pos]
}

// scanNumber scans any SQL numeric literal, either floating point or integer
func (s *Scanner) scanNumber() (int, string) {
	start := s.Pos
	token := INTEGRAL
	canPromoteToIdentifier := true

	if s.cur() == '.' {
		token = FLOAT
		canPromoteToIdentifier = false
		s.inc()
		s.scanMantissa(10)
		goto exponent
	}

	// 0x construct.
	if s.cur() == '0' {
		s.inc()
		if s.cur() == 'x' || s.cur() == 'X' {
			token = HEXNUM
			s.inc()
			p1 := s.Pos
			s.scanMantissa(16)
			p2 := s.Pos
			if p1 == p2 || isDigit(s.cur()) {
				token = ID
				if s.cur() != eofChar {
					if typ, str := s.scanIdentifier(false); typ == LEX_ERROR {
						return typ, str
					}
				}
				return token, toLowerASCII(s.buf[start:s.Pos])
			}

			goto exit
		} else if s.cur() == 'b' || s.cur() == 'B' {
			token = BIT_LITERAL
			s.inc()
			p1 := s.Pos
			s.scanMantissa(2)
			p2 := s.Pos
			if p1 == p2 || isDigit(s.cur()) {
				token = ID
				if s.cur() != eofChar {
					if typ, str := s.scanIdentifier(false); typ == LEX_ERROR {
						return typ, str
					}
				}
				return token, toLowerASCII(s.buf[start:s.Pos])
			}

			goto exit
		}
	}

	s.scanMantissa(10)

	if s.cur() == '.' {
		token = FLOAT
		canPromoteToIdentifier = false
		s.inc()
		s.scanMantissa(10)
	}

exponent:
	if s.cur() == 'e' || s.cur() == 'E' {
		if s.peek(1) == '+' || s.peek(1) == '-' {
			token = FLOAT
			canPromoteToIdentifier = false
			s.incN(2)
		} else if digitVal(s.peek(1)) < 10 {
			token = FLOAT
			s.inc()
		} else {
			goto exit
		}
		s.scanMantissa(10)
	}

exit:
	if canPromoteToIdentifier && isUnquotedIdentifierLetterAt(s.buf, s.Pos) {
		// TODO: optimize
		token = ID
		s.scanIdentifier(false)
	}

	return token, toLowerASCII(s.buf[start:s.Pos])
}

func (s *Scanner) scanIdentifier(isVariable bool) (int, string) {
	start := s.Pos
	if size := supplementaryUTF8SequenceSizeAt(s.buf, s.Pos); size != 0 {
		s.incN(size)
		return LEX_ERROR, s.buf[start:s.Pos]
	}
	if ch := s.cur(); !isUnquotedIdentifierLetterAt(s.buf, s.Pos) && !isDigit(ch) && !(isVariable && isCarat(ch)) {
		s.inc()
		return LEX_ERROR, s.buf[start:s.Pos]
	}

	dollarFlag := false
	if s.cur() == '$' {
		dollarFlag = true
	}
	s.inc()

	for {
		ch := s.cur()
		if ch == '$' && dollarFlag {
			break
		}
		if !isUnquotedIdentifierLetterAt(s.buf, s.Pos) && !isDigit(ch) && ch != '@' && !(isVariable && isCarat(ch)) {
			break
		}
		if ch == '@' {
			break
		}

		s.inc()
	}
	keywordName := s.buf[start:s.Pos]
	lower := strings.ToLower(keywordName)
	if keywordID, found := keywords[lower]; found {
		if lower == "within" {
			if s.withinGroupPhraseAhead(s.Pos) {
				return keywordID, keywordName
			}
			return ID, keywordName
		}
		if lower == "asof" {
			if s.asofJoinPhraseAhead(start, s.Pos) {
				return keywordID, keywordName
			}
			return ID, keywordName
		}
		if lower == "offset" && !s.offsetClauseAhead() {
			return ID, keywordName
		}
		// make transaction statements coexist with plsql
		if lower == "begin" {
			cur := s.Pos
			s.skipBlank()
			if s.cur() == ';' || s.cur() == eofChar { // "begin ;" situation
				s.Pos = cur
				return keywordID, keywordName
			}
			typ, str := s.scanIdentifier(false) // "begin work / begin transaction" situation
			if typ == LEX_ERROR {
				return typ, str
			}
			if typ == WORK || typ == TRANSACTION {
				s.Pos = cur
				return keywordID, keywordName
			}
			s.Pos = cur
			return SPBEGIN, keywordName
		} else {
			return keywordID, keywordName
		}
	}
	// dual must always be case-insensitive
	if lower == "dual" {
		return ID, keywordName
	}
	return ID, keywordName
}

func (s *Scanner) asofJoinPhraseAhead(start, pos int) bool {
	prev := s.previousSignificantPos(start - 1)
	if prev >= 0 && (s.buf[prev] == '.' || s.buf[prev] == ',' || s.buf[prev] == '(' || s.buf[prev] == ')') {
		return false
	}
	end := prev + 1
	for prev >= 0 && (isLetter(uint16(s.buf[prev])) || isDigit(uint16(s.buf[prev])) || s.buf[prev] == '_') {
		prev--
	}
	if end > prev+1 {
		switch strings.ToLower(s.buf[prev+1 : end]) {
		case "from", "as", "into", "join", "update", "table":
			return false
		}
	}
	pos = s.skipBlankAndCommentsFrom(pos)
	if hasKeywordAt(s.buf, pos, "join") {
		return s.asofDerivedLeftRelation(start) || s.asofTemporalPredicateInJoin(pos+len("join"), s.asofLeftRelationName(start))
	}
	if !hasKeywordAt(s.buf, pos, "left") {
		return false
	}
	pos = s.skipBlankAndCommentsFrom(pos + len("left"))
	if hasKeywordAt(s.buf, pos, "join") {
		return s.asofDerivedLeftRelation(start) || s.asofTemporalPredicateInJoin(pos+len("join"), s.asofLeftRelationName(start))
	}
	if !hasKeywordAt(s.buf, pos, "outer") {
		return false
	}
	pos = s.skipBlankAndCommentsFrom(pos + len("outer"))
	return hasKeywordAt(s.buf, pos, "join") && (s.asofDerivedLeftRelation(start) || s.asofTemporalPredicateInJoin(pos+len("join"), s.asofLeftRelationName(start)))
}

// asofLeftRelationName returns the unqualified name immediately preceding the
// contextual ASOF spelling. It is used only to distinguish an actual temporal
// predicate from an ordinary implicit alias whose ON clause happens to contain
// an unrelated inequality.
func (s *Scanner) asofLeftRelationName(start int) string {
	end := s.previousSignificantPos(start - 1)
	if end < 0 {
		return ""
	}
	i := end
	for i >= 0 && (isLetter(uint16(s.buf[i])) || isDigit(uint16(s.buf[i])) || s.buf[i] == '_') {
		i--
	}
	return strings.ToLower(s.buf[i+1 : end+1])
}

func (s *Scanner) asofDerivedLeftRelation(start int) bool {
	prev := s.previousSignificantPos(start - 1)
	if prev < 0 {
		return false
	}
	for prev >= 0 && (isLetter(uint16(s.buf[prev])) || isDigit(uint16(s.buf[prev])) || s.buf[prev] == '_') {
		prev--
	}
	prev = s.previousSignificantPos(prev)
	return prev >= 0 && s.buf[prev] == ')'
}

func (s *Scanner) asofTemporalPredicateInJoin(pos int, leftName string) bool {
	// The ASOF/implicit-alias spelling is ambiguous at the lexer level.  Treat
	// it as the modifier when the ON clause contains a temporal inequality,
	// which is the semantic distinction from the legacy implicit-alias spelling.
	// Scan SQL tokens so relation/alias names, quoted strings, comments, and
	// later joins cannot influence this decision.
	inOn, escaped, leftRef, asofRef, pendingIneq := false, false, false, false, false
	rightName := s.asofRightRelationName(pos)
	var quote byte
	for i := pos; i < len(s.buf); {
		ch := s.buf[i]
		if quote != 0 {
			if escaped {
				escaped = false
				i++
				continue
			}
			if ch == '\\' {
				escaped = true
				i++
				continue
			}
			if ch == quote {
				quote = 0
			}
			i++
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			i++
			continue
		}
		if ch == '#' || strings.HasPrefix(s.buf[i:], "//") ||
			(strings.HasPrefix(s.buf[i:], "--") && (i+2 == len(s.buf) || isMySQLDashCommentBlank(s.buf[i+2]))) {
			for i < len(s.buf) && s.buf[i] != '\n' {
				i++
			}
			continue
		}
		if strings.HasPrefix(s.buf[i:], "/*") {
			if end := strings.Index(s.buf[i+2:], "*/"); end >= 0 {
				i += end + 4
			} else {
				return false
			}
			continue
		}
		if inOn && (ch == '<' || ch == '>') {
			pendingIneq = true
			if leftName != "" && leftRef && (!asofRef || rightName == "asof") {
				return true
			}
		}
		if inOn && ch == '=' && (i == 0 || (s.buf[i-1] != '<' && s.buf[i-1] != '>')) {
			leftRef, asofRef, pendingIneq = false, false, false
		}
		if !isUnquotedIdentifierLetterAt(s.buf, i) && !isDigit(uint16(s.buf[i])) {
			i++
			continue
		}
		start := i
		for i < len(s.buf) && (isUnquotedIdentifierLetterAt(s.buf, i) || isDigit(uint16(s.buf[i])) || s.buf[i] == '_') {
			i++
		}
		word := strings.ToLower(s.buf[start:i])
		if inOn && leftName != "" && word == leftName {
			next := s.skipBlankAndCommentsFrom(i)
			leftRef = next < len(s.buf) && s.buf[next] == '.'
			if pendingIneq && leftRef && (!asofRef || rightName == "asof") {
				return true
			}
		}
		if inOn {
			next := s.skipBlankAndCommentsFrom(i)
			qualified := next < len(s.buf) && s.buf[next] == '.'
			if qualified {
				if word == "asof" {
					asofRef = true
				}
			}
		}
		if !inOn {
			if word == "on" {
				inOn = true
				continue
			}
			if word == "join" || word == "where" || word == "group" || word == "order" || word == "limit" {
				continue
			}
			continue
		}
		if word == "where" || word == "group" || word == "order" || word == "limit" || word == "having" || word == "union" {
			break
		}
		if word == "join" {
			break
		}
		if word == "tolerance" {
			return true
		}
	}
	return false
}

func (s *Scanner) asofRightRelationName(pos int) string {
	pos = s.skipBlankAndCommentsFrom(pos)
	if pos >= len(s.buf) || !isUnquotedIdentifierLetterAt(s.buf, pos) {
		return ""
	}
	end := pos + 1
	for end < len(s.buf) && (isUnquotedIdentifierLetterAt(s.buf, end) || isDigit(uint16(s.buf[end])) || s.buf[end] == '_') {
		end++
	}
	return strings.ToLower(s.buf[pos:end])
}

func (s *Scanner) previousSignificantPos(pos int) int {
	quote, comment := byte(0), byte(0)
	escaped, last := false, -1
	for i := 0; i <= pos; i++ {
		if comment != 0 {
			if comment == 'b' && i > 0 && s.buf[i-1] == '*' && s.buf[i] == '/' {
				comment = 0
			}
			if comment != 'b' && s.buf[i] == '\n' {
				comment = 0
			}
			continue
		}
		ch := s.buf[i]
		if quote != 0 {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == quote {
				quote = 0
			}
			continue
		}
		if ch == '\'' || ch == '"' || ch == '`' {
			quote = ch
			continue
		}
		if ch == '#' || strings.HasPrefix(s.buf[i:], "//") || (strings.HasPrefix(s.buf[i:], "--") && (i+2 == len(s.buf) || isMySQLDashCommentBlank(s.buf[i+2]))) {
			comment = 'l'
			continue
		}
		if strings.HasPrefix(s.buf[i:], "/*") {
			comment = 'b'
			i++
			continue
		}
		if ch != ' ' && ch != '\n' && ch != '\r' && ch != '\t' {
			last = i
		}
	}
	return last
}

// offsetClauseAhead distinguishes the query clause from OFFSET used as an
// implicit alias. OFFSET is intentionally non-reserved, so a trailing OFFSET
// or one followed by another clause must remain an identifier. If the next
// token can start an expression, the parser receives the OFFSET keyword.
func (s *Scanner) offsetClauseAhead() bool {
	// Preserve OFFSET(...) as a non-reserved function name. OFFSET followed by
	// a parenthesized clause expression remains available with whitespace, as
	// in OFFSET (1).
	if s.Pos < len(s.buf) && s.buf[s.Pos] == '(' {
		return false
	}

	pos := s.skipBlankAndCommentsFrom(s.Pos)
	if hasKeywordAt(s.buf, pos, "offset") {
		return false
	}

	lookahead := *s
	next, _ := lookahead.Scan()

	return !isOffsetAliasFollower(next)
}

// isOffsetAliasFollower is the complete union of tokens that may immediately
// follow a projection or table alias in the query grammar. Keep the groups in
// grammar order: SELECT clauses, query suffixes, set operations, joins, table
// index hints, and statement/table-reference terminators.
func isOffsetAliasFollower(next int) bool {
	switch next {
	case 0, int(','), int(')'), int('}'), int(';'),
		FROM, WHERE, GROUP, HAVING,
		INTERVAL, FILL, ORDER, LIMIT, OFFSET, BY, INTO, FOR, LOCK,
		UNION, EXCEPT, INTERSECT, MINUS, RETURNING,
		JOIN, STRAIGHT_JOIN, LEFT, RIGHT, FULL, INNER, OUTER, CROSS, NATURAL,
		APPLY, DEDUP, CENTROIDX, ON, USING, SET, USE, FORCE, IGNORE:
		return true
	default:
		return false
	}
}

func isTableFactorStart(previous int) bool {
	switch previous {
	case FROM, JOIN, STRAIGHT_JOIN, APPLY, USING:
		return true
	default:
		return false
	}
}

// offsetAliasColumnListAhead reports whether the text following OFFSET is a
// parenthesized identifier list. The lexer uses it only after a closing ')'
// to preserve the pre-existing derived-table form `(...) offset (c1, c2)`.
func (s *Scanner) offsetAliasColumnListAhead() bool {
	pos := s.skipBlankAndCommentsFrom(s.Pos)
	if pos >= len(s.buf) || s.buf[pos] != '(' {
		return false
	}
	pos++

	for {
		pos = s.skipBlankAndCommentsFrom(pos)
		var ok bool
		pos, ok = scanAliasIdentifierFrom(s.buf, pos, s.sqlMode.Has(SQLModeANSIQuotes))
		if !ok {
			return false
		}
		pos = s.skipBlankAndCommentsFrom(pos)
		if pos >= len(s.buf) {
			return false
		}
		switch s.buf[pos] {
		case ')':
			return true
		case ',':
			pos++
		default:
			return false
		}
	}
}

func scanAliasIdentifierFrom(sql string, pos int, ansiQuotes bool) (int, bool) {
	if pos >= len(sql) {
		return pos, false
	}
	if sql[pos] == '`' || (sql[pos] == '"' && ansiQuotes) {
		quote := sql[pos]
		for pos = pos + 1; pos < len(sql); pos++ {
			if sql[pos] != quote {
				continue
			}
			if pos+1 < len(sql) && sql[pos+1] == quote {
				pos++
				continue
			}
			return pos + 1, true
		}
		return pos, false
	}
	if !isLetter(uint16(sql[pos])) {
		return pos, false
	}
	for pos++; pos < len(sql); pos++ {
		ch := uint16(sql[pos])
		if !isLetter(ch) && !isDigit(ch) && ch != '@' {
			break
		}
	}
	return pos, true
}

func (s *Scanner) withinGroupPhraseAhead(pos int) bool {
	pos = s.skipBlankAndCommentsFrom(pos)
	if !hasKeywordAt(s.buf, pos, "group") {
		return false
	}
	pos += len("group")
	pos = s.skipBlankAndCommentsFrom(pos)
	return pos < len(s.buf) && s.buf[pos] == '('
}

func (s *Scanner) skipBlankAndCommentsFrom(pos int) int {
	for {
		for pos < len(s.buf) {
			switch s.buf[pos] {
			case ' ', '\n', '\r', '\t':
				pos++
				continue
			}
			break
		}
		if pos >= len(s.buf) {
			return pos
		}
		switch {
		case strings.HasPrefix(s.buf[pos:], "/*"):
			end := strings.Index(s.buf[pos+2:], "*/")
			if end < 0 {
				return pos
			}
			pos += 2 + end + 2
			continue
		case strings.HasPrefix(s.buf[pos:], "//"):
			pos = skipLineCommentFrom(s.buf, pos+2)
			continue
		case s.buf[pos] == '#':
			pos = skipLineCommentFrom(s.buf, pos+1)
			continue
		case strings.HasPrefix(s.buf[pos:], "--") &&
			(pos+2 == len(s.buf) || isMySQLDashCommentBlank(s.buf[pos+2])):
			pos = skipLineCommentFrom(s.buf, pos+2)
			continue
		}
		return pos
	}
}

func hasKeywordAt(sql string, pos int, keyword string) bool {
	if pos+len(keyword) > len(sql) {
		return false
	}
	if !strings.EqualFold(sql[pos:pos+len(keyword)], keyword) {
		return false
	}
	if pos+len(keyword) == len(sql) {
		return true
	}
	nextPos := pos + len(keyword)
	next := uint16(sql[nextPos])
	return !isUnquotedIdentifierLetterAt(sql, nextPos) && !isDigit(next) && next != '@'
}

func skipLineCommentFrom(sql string, pos int) int {
	for pos < len(sql) {
		if sql[pos] == '\n' {
			return pos + 1
		}
		pos++
	}
	return pos
}

func isMySQLDashCommentBlank(ch byte) bool {
	return ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t'
}

func (s *Scanner) scanBitLiteral() (int, string) {
	start := s.Pos
	s.scanMantissa(2)
	bit := "0b" + s.buf[start:s.Pos]
	if s.cur() != '\'' {
		return LEX_ERROR, bit
	}
	s.inc()
	return BIT_LITERAL, bit
}

func (s *Scanner) scanHex() (int, string) {
	start := s.Pos
	s.scanMantissa(16)
	hex := "0x" + s.buf[start:s.Pos]
	if s.cur() != '\'' {
		return LEX_ERROR, hex
	}
	s.inc()
	if len(hex)%2 != 0 {
		return LEX_ERROR, hex
	}
	return HEXNUM, hex
}

func (s *Scanner) scanMantissa(base int) {
	for digitVal(s.cur()) < base {
		s.inc()
	}
}

// PositionedErr holds context related to parser errros
type PositionedErr struct {
	Err    string
	Line   int
	Col    int
	Near   string
	LenStr string
}

func (p PositionedErr) Error() string {
	return fmt.Sprintf("%s at line %d column %d near \"%s\"%s;", p.Err, p.Line+1, p.Col, p.Near, p.LenStr)
}

func (s *Scanner) skipBlank() {
	ch := s.cur()
	for ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
		s.inc()
		ch = s.cur()
	}
}

func (s *Scanner) cur() uint16 {
	return s.peek(0)
}

func (s *Scanner) inc() {
	if s.Pos >= len(s.buf) {
		return
	}
	if s.buf[s.Pos] == '\n' {
		s.Line++
		s.Col = 0
	}
	s.Pos++
	s.Col++
}

func (s *Scanner) incN(dist int) {
	for i := 0; i < dist; i++ {
		s.inc()
	}
}

func (s *Scanner) peek(dist int) uint16 {
	if s.Pos+dist >= len(s.buf) {
		return eofChar
	}
	return uint16(s.buf[s.Pos+dist])
}

// isUnquotedIdentifierLetterAt preserves invalid UTF-8 bytes from single-byte
// client encodings while rejecting valid supplementary UTF-8 sequences. MySQL
// permits Unicode identifier characters only in the BMP.
func isUnquotedIdentifierLetterAt(sql string, pos int) bool {
	if pos < 0 || pos >= len(sql) {
		return false
	}
	ch := uint16(sql[pos])
	if isLetter(ch) {
		return true
	}
	if ch < utf8.RuneSelf {
		return false
	}
	r, size := utf8.DecodeRuneInString(sql[pos:])
	return (size == 1 && r == utf8.RuneError) || r <= '\uFFFF'
}

func supplementaryUTF8SequenceSizeAt(sql string, pos int) int {
	if pos < 0 || pos >= len(sql) || sql[pos] < utf8.RuneSelf {
		return 0
	}
	r, size := utf8.DecodeRuneInString(sql[pos:])
	if size == 4 && r > '\uFFFF' {
		return size
	}
	return 0
}

// toLowerASCII retains raw client bytes. strings.ToLower cannot be used on an
// identifier that may contain invalid UTF-8 because it replaces those bytes
// with utf8.RuneError.
func toLowerASCII(value string) string {
	for i := 0; i < len(value); i++ {
		if value[i] >= 'A' && value[i] <= 'Z' {
			lower := []byte(value)
			for j := i; j < len(lower); j++ {
				if lower[j] >= 'A' && lower[j] <= 'Z' {
					lower[j] += 'a' - 'A'
				}
			}
			return string(lower)
		}
	}
	return value
}

func isLetter(ch uint16) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_' || ch == '$'
}

func isCarat(ch uint16) bool {
	return ch == '.' || ch == '"' || ch == '`' || ch == '\''
}

func digitVal(ch uint16) int {
	switch {
	case '0' <= ch && ch <= '9':
		return int(ch) - '0'
	case 'a' <= ch && ch <= 'f':
		return int(ch) - 'a' + 10
	case 'A' <= ch && ch <= 'F':
		return int(ch) - 'A' + 10
	}
	return 16 // larger than any legal digit val
}

func isDigit(ch uint16) bool {
	return '0' <= ch && ch <= '9'
}
