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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

const eofChar = 0x100

var scannerPool = sync.Pool{
	New: func() any {
		return &Scanner{
			strBuilder: new(bytes.Buffer),
		}
	},
}

type Scanner struct {
	LastToken           string
	LastError           error
	posVarIndex         int
	dialectType         dialect.DialectType
	MysqlSpecialComment *Scanner

	CommentFlag bool
	Pos         int
	Line        int
	Col         int
	PrePos      int
	buf         string

	strBuilder *bytes.Buffer
}

func NewScanner(dialectType dialect.DialectType, sql string) *Scanner {
	scanner := scannerPool.Get().(*Scanner)
	scanner.dialectType = dialectType
	scanner.LastToken = ""
	scanner.LastError = nil
	scanner.posVarIndex = 0
	scanner.MysqlSpecialComment = nil
	scanner.Pos = 0
	scanner.Line = 0
	scanner.Col = 0
	scanner.PrePos = 0
	scanner.buf = sql
	scanner.strBuilder.Reset()
	return scanner
}

func PutScanner(scanner *Scanner) {
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
	case isLetter(ch):
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
				_, tagStr := s.scanIdentifier(false)
				if tagStr != str {
					return LEX_ERROR, string(byte(s.cur()))
				}
				s.inc()
				return strTyp, strStr
			}

		} else {
			return s.scanIdentifier(false)
		}
	case isDigit(ch):
		return s.scanNumber()
	case ch == ':':
		if s.peek(1) == '=' {
			s.incN(2)
			return ASSIGNMENT, ""
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
		default:
			return s.Scan()
		}
	default:
		return s.stepBackOneChar(ch)
	}
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
			return PIPE_CONCAT, ""
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
	case '\'', '"':
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
	buf := s.strBuilder
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
		} else if ch == '\\' {
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
	buf := s.strBuilder
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
		} else if ch == '\\' {
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
	start := s.Pos
	for {
		switch s.cur() {
		case '`':
			if s.peek(1) != '`' {
				if s.Pos == start {
					return LEX_ERROR, ""
				}
				s.inc()
				return QUOTE_ID, s.buf[start : s.Pos-1]
			}

			var buf strings.Builder
			buf.WriteString(s.buf[start:s.Pos])
			s.inc()
			return s.scanLiteralIdentifierSlow(&buf)
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
func (s *Scanner) scanLiteralIdentifierSlow(buf *strings.Builder) (int, string) {
	backTickSeen := true
	for {
		if backTickSeen {
			if s.cur() != '`' {
				break
			}
			backTickSeen = false
			buf.WriteByte('`')
			s.inc()
			continue
		}
		// The previous char was not a backtick.
		switch s.cur() {
		case '`':
			backTickSeen = true
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

	if s.cur() == '.' {
		token = FLOAT
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
			s.scanMantissa(16)
			goto exit
		} else if s.cur() == 'b' || s.cur() == 'B' {
			token = BIT_LITERAL
			p1 := s.Pos
			s.inc()
			s.scanMantissa(2)
			p2 := s.Pos
			if p1 == p2 || isDigit(s.cur()) {
				token = ID
				s.scanIdentifier(false)
				return token, strings.ToLower(s.buf[start:s.Pos])
			}

			goto exit
		}
	}

	s.scanMantissa(10)

	if s.cur() == '.' {
		token = FLOAT
		s.inc()
		s.scanMantissa(10)
	}

exponent:
	if s.cur() == 'e' || s.cur() == 'E' {
		if s.peek(1) == '+' || s.peek(1) == '-' {
			token = FLOAT
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
	if isLetter(s.cur()) {
		// TODO: optimize
		token = ID
		s.scanIdentifier(false)
	}

	return token, strings.ToLower(s.buf[start:s.Pos])
}

func (s *Scanner) scanIdentifier(isVariable bool) (int, string) {
	dollarFlag := false
	if s.cur() == '$' {
		dollarFlag = true
	}
	start := s.Pos
	s.inc()

	for {
		ch := s.cur()
		if ch == '$' && dollarFlag {
			break
		}
		if !isLetter(ch) && !isDigit(ch) && ch != '@' && !(isVariable && isCarat(ch)) {
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
		// make transaction statements coexist with plsql
		if lower == "begin" {
			cur := s.Pos
			s.skipBlank()
			if s.cur() == ';' || s.cur() == eofChar { // "begin ;" situation
				s.Pos = cur
				return keywordID, keywordName
			}
			typ, _ := s.scanIdentifier(false) // "begin work / begin transaction" situation
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

func (s *Scanner) scanBitLiteral() (int, string) {
	start := s.Pos
	s.scanMantissa(2)
	bit := s.buf[start:s.Pos]
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
