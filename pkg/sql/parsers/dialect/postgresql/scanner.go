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

package postgresql

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

const eofChar = 0x100

type Scanner struct {
	LastToken           string
	LastError           error
	posVarIndex         int
	dialectType         dialect.DialectType
	MysqlSpecialComment *Scanner

	Pos int
	buf string
}

func NewScanner(dialectType dialect.DialectType, sql string) *Scanner {

	return &Scanner{
		buf: sql,
	}
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

	s.skipBlank()
	switch ch := s.cur(); {
	case ch == '@':
		tokenID := AT_ID
		s.skip(1)
		s.skipBlank()
		if s.cur() == '@' {
			tokenID = AT_AT_ID
			s.skip(1)
		} else if s.cur() == '\'' || s.cur() == '"' {
			return int('@'), ""
		} else if s.cur() == ',' {
			return tokenID, ""
		}
		var tID int
		var tBytes string
		if s.cur() == '`' {
			s.skip(1)
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
				s.skip(2)
				return s.scanHex()
			}
		}
		if ch == 'B' || ch == 'b' {
			if s.peek(1) == '\'' {
				s.skip(2)
				return s.scanBitLiteral()
			}
		}
		return s.scanIdentifier(false)
	case isDigit(ch):
		return s.scanNumber()
	case ch == ':':
		if s.peek(1) == '=' {
			s.skip(2)
			return ASSIGNMENT, ""
		}
		// Like mysql -h ::1 ?
		return s.scanBindVar()
	case ch == ';':
		s.skip(1)
		return ';', ""
	case ch == '.' && isDigit(s.peek(1)):
		return s.scanNumber()
	case ch == '/':
		s.skip(1)
		switch s.cur() {
		case '/':
			s.skip(1)
			id, str := s.scanCommentTypeLine(2)
			if id == LEX_ERROR {
				return id, str
			}
			return s.Scan()
		case '*':
			s.skip(1)
			switch {
			case s.cur() == '!' && s.dialectType == dialect.MYSQL:
				// TODO: ExtractMysqlComment
				return s.scanMySQLSpecificComment()
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
	default:
		return s.stepBackOneChar(ch)
	}
}

func (s *Scanner) stepBackOneChar(ch uint16) (int, string) {
	s.skip(1)
	switch ch {
	case eofChar:
		return 0, ""
	case '=', ',', '(', ')', '+', '*', '%', '^', '~':
		return int(ch), ""
	case '&':
		if s.cur() == '&' {
			s.skip(1)
			return AND, ""
		}
		return int(ch), ""
	case '|':
		if s.cur() == '|' {
			s.skip(1)
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
				s.skip(1)
				return s.scanCommentTypeLine(2)
			}
		case '>':
			s.skip(1)
			// TODO:
			// JSON_UNQUOTE_EXTRACT_OP
			// JSON_EXTRACT_OP
			return 0, ""
		}
		return int(ch), ""
	case '<':
		switch s.cur() {
		case '>':
			s.skip(1)
			return NE, ""
		case '<':
			s.skip(1)
			return SHIFT_LEFT, ""
		case '=':
			s.skip(1)
			switch s.cur() {
			case '>':
				s.skip(1)
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
			s.skip(1)
			return GE, ""
		case '>':
			s.skip(1)
			return SHIFT_RIGHT, ""
		default:
			return int(ch), ""
		}
	case '!':
		if s.cur() == '=' {
			s.skip(1)
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

// scanString scans a string surrounded by the given `delim`, which can be
// either single or double quotes. Assumes that the given delimiter has just
// been scanned. If the skin contains any escape sequences, this function
// will fall back to scanStringSlow
func (s *Scanner) scanString(delim uint16, typ int) (int, string) {
	start := s.Pos

	for {
		switch s.cur() {
		case delim:
			if s.peek(1) != delim {
				s.skip(1)
				return typ, s.buf[start : s.Pos-1]
			}
			fallthrough

		case '\\':
			var buffer strings.Builder
			buffer.WriteString(s.buf[start:s.Pos])
			return s.scanStringSlow(&buffer, delim, typ)

		case eofChar:
			return LEX_ERROR, s.buf[start:s.Pos]
		}

		s.skip(1)
	}
}

// scanString scans a string surrounded by the given `delim` and containing escape
// sequencse. The given `buffer` contains the contents of the string that have
// been scanned so far.
func (s *Scanner) scanStringSlow(buffer *strings.Builder, delim uint16, typ int) (int, string) {
	for {
		ch := s.cur()
		if ch == eofChar {
			// Unterminated string.
			return LEX_ERROR, buffer.String()
		}

		if ch != delim && ch != '\\' {
			start := s.Pos
			for ; s.Pos < len(s.buf); s.Pos++ {
				ch = uint16(s.buf[s.Pos])
				if ch == delim || ch == '\\' {
					break
				}
			}

			buffer.WriteString(s.buf[start:s.Pos])
			if s.Pos >= len(s.buf) {
				s.skip(1)
				continue
			}
		}
		s.skip(1)

		if ch == '\\' {
			ch = s.cur()
			switch ch {
			case eofChar:
				return LEX_ERROR, buffer.String()
			case 'n':
				ch = '\n'
			case '0':
				ch = '\x00'
			case 'b':
				ch = 8
			case 'Z':
				ch = 26
			case 'r':
				ch = '\r'
			case 't':
				ch = '\t'
			case '%', '_':
				buffer.WriteByte(byte('\\'))
				continue
			case '\\', delim:
			default:
				continue
			}
		} else if ch == delim && s.cur() != delim {
			break
		}
		buffer.WriteByte(byte(ch))
		s.skip(1)
	}

	return typ, buffer.String()
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
				s.skip(1)
				return ID, s.buf[start : s.Pos-1]
			}

			var buf strings.Builder
			buf.WriteString(s.buf[start:s.Pos])
			s.skip(1)
			return s.scanLiteralIdentifierSlow(&buf)
		case eofChar:
			// Premature EOF.
			return LEX_ERROR, s.buf[start:s.Pos]
		default:
			s.skip(1)
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
			s.skip(1)
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
		s.skip(1)
	}
	return ID, buf.String()
}

// scanCommentTypeBlock scans a '/*' delimited comment;
// assumes the opening prefix has already been scanned
func (s *Scanner) scanCommentTypeBlock() (int, string) {
	start := s.Pos - 2
	for {
		if s.cur() == '*' {
			s.skip(1)
			if s.cur() == '/' {
				s.skip(1)
				break
			}
			continue
		}
		if s.cur() == eofChar {
			return LEX_ERROR, s.buf[start:s.Pos]
		}
		s.skip(1)
	}
	return COMMENT, s.buf[start:s.Pos]
}

// scanMySQLSpecificComment scans a MySQL comment pragma, which always starts with '//*`
func (s *Scanner) scanMySQLSpecificComment() (int, string) {
	start := s.Pos - 3
	for {
		if s.cur() == '*' {
			s.skip(1)
			if s.cur() == '/' {
				s.skip(1)
				break
			}
			continue
		}
		if s.cur() == eofChar {
			return LEX_ERROR, s.buf[start:s.Pos]
		}
		s.skip(1)
	}

	_, sql := ExtractMysqlComment(s.buf[start:s.Pos])

	s.MysqlSpecialComment = NewScanner(s.dialectType, sql)

	return s.Scan()
}

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
			s.skip(1)
			break
		}
		s.skip(1)
	}
	return COMMENT, s.buf[start:s.Pos]
}

// ?
// scanBindVar scans a bind variable; assumes a ':' has been scanned right before
func (s *Scanner) scanBindVar() (int, string) {
	start := s.Pos
	token := VALUE_ARG

	s.skip(1)
	if s.cur() == ':' {
		token = LIST_ARG
		s.skip(1)
	}
	if !isLetter(s.cur()) {
		return LEX_ERROR, s.buf[start:s.Pos]
	}
	for {
		ch := s.cur()
		if !isLetter(ch) && !isDigit(ch) && ch != '.' {
			break
		}
		s.skip(1)
	}
	return token, s.buf[start:s.Pos]
}

// scanNumber scans any SQL numeric literal, either floating point or integer
func (s *Scanner) scanNumber() (int, string) {
	start := s.Pos
	token := INTEGRAL

	if s.cur() == '.' {
		token = FLOAT
		s.skip(1)
		s.scanMantissa(10)
		goto exponent
	}

	// 0x construct.
	if s.cur() == '0' {
		s.skip(1)
		if s.cur() == 'x' || s.cur() == 'X' {
			token = HEXNUM
			s.skip(1)
			s.scanMantissa(16)
			goto exit
		} else if s.cur() == 'b' || s.cur() == 'B' {
			token = BIT_LITERAL
			s.skip(1)
			s.scanMantissa(2)
			goto exit
		}
	}

	s.scanMantissa(10)

	if s.cur() == '.' {
		token = FLOAT
		s.skip(1)
		s.scanMantissa(10)
	}

exponent:
	if s.cur() == 'e' || s.cur() == 'E' {
		if s.peek(1) == '+' || s.peek(1) == '-' {
			token = FLOAT
			s.skip(2)
		} else if digitVal(s.peek(1)) < 10 {
			token = FLOAT
			s.skip(1)
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
	start := s.Pos
	s.skip(1)

	for {
		ch := s.cur()
		if !isLetter(ch) && !isDigit(ch) && ch != '@' && !(isVariable && isCarat(ch)) {
			break
		}
		if ch == '@' {
			isVariable = true
		}
		s.skip(1)
	}
	keywordName := s.buf[start:s.Pos]
	lower := strings.ToLower(keywordName)
	if keywordID, found := keywords[lower]; found {
		return keywordID, lower
	}
	// dual must always be case-insensitive
	if lower == "dual" {
		return ID, lower
	}
	return ID, lower
}

func (s *Scanner) scanBitLiteral() (int, string) {
	start := s.Pos
	s.scanMantissa(2)
	bit := s.buf[start:s.Pos]
	if s.cur() != '\'' {
		return LEX_ERROR, bit
	}
	s.skip(1)
	return BIT_LITERAL, bit
}

func (s *Scanner) scanHex() (int, string) {
	start := s.Pos
	s.scanMantissa(16)
	hex := s.buf[start:s.Pos]
	if s.cur() != '\'' {
		return LEX_ERROR, hex
	}
	s.skip(1)
	if len(hex)%2 != 0 {
		return LEX_ERROR, hex
	}
	return HEXNUM, hex
}

func (s *Scanner) scanMantissa(base int) {
	for digitVal(s.cur()) < base {
		s.skip(1)
	}
}

// PositionedErr holds context related to parser errros
type PositionedErr struct {
	Err  string
	Pos  int
	Near string
}

func (p PositionedErr) Error() string {
	if p.Near != "" {
		return fmt.Sprintf("%s at position %v near '%s';", p.Err, p.Pos, p.Near)
	}
	return fmt.Sprintf("%s at position %v;", p.Err, p.Pos)
}

func (s *Scanner) skipBlank() {
	ch := s.cur()
	for ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' {
		s.skip(1)
		ch = s.cur()
	}
}

func (s *Scanner) cur() uint16 {
	return s.peek(0)
}

func (s *Scanner) skip(dist int) {
	s.Pos += dist
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
