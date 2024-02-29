// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package csvparser

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/spkg/bom"
)

var (
	errUnterminatedQuotedField = moerr.NewSyntaxErrorNoCtx("syntax error: unterminated quoted field")
	errDanglingBackslash       = moerr.NewSyntaxErrorNoCtx("syntax error: no character after backslash")
	errUnexpectedQuoteField    = moerr.NewSyntaxErrorNoCtx("syntax error: cannot have consecutive fields without separator")
	// LargestEntryLimit is the max size for reading file to buf
	LargestEntryLimit       = 120 * 1024 * 1024
	BufferSizeScale         = int64(5)
	ReadBlockSize     int64 = 64 * 1024
)

type Field struct {
	Val    string
	IsNull bool
}

type escapeFlavor uint8

const (
	escapeFlavorNone escapeFlavor = iota
	escapeFlavorMySQL
	escapeFlavorMySQLWithNull
)

type CSVConfig struct {
	// they can only be used by LOAD DATA
	// https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-field-line-handling
	LinesStartingBy   string
	LinesTerminatedBy string

	FieldsTerminatedBy string
	FieldsEnclosedBy   string
	FieldsEscapedBy    string

	Null              []string
	Header            bool
	HeaderSchemaMatch bool
	TrimLastSep       bool
	NotNull           bool

	AllowEmptyLine bool
	// For non-empty FieldsEnclosedBy (for example quotes), null elements inside quotes are not considered as null except for
	// `\N` (when escape-by is `\`). That is to say, `\N` is special for null because it always means null.
	QuotedNullIsText bool
	// ref https://dev.mysql.com/doc/refman/8.0/en/load-data.html
	// > If the field begins with the ENCLOSED BY character, instances of that character are recognized as terminating a
	// > field value only if followed by the field or line TERMINATED BY sequence.
	// This means we will meet unescaped quote in a quoted field
	// > The "BIG" boss      -> The "BIG" boss
	// This means we will meet unescaped quote in an unquoted field
	UnescapedQuote bool

	// see csv.Reader
	Comment byte
}

// CSVParser is basically a copy of encoding/csv, but special-cased for MySQL-like input.
type CSVParser struct {
	cfg *CSVConfig

	comma          []byte
	quote          []byte
	newLine        []byte
	startingBy     []byte
	escapedBy      string
	unescapeRegexp *regexp.Regexp

	// These variables are used with IndexAnyByte to search a byte slice for the
	// first index which some special character may appear.
	// quoteByteSet is used inside quoted fields (so the first characters of
	// the closing delimiter and backslash are special).
	// unquoteByteSet is used outside quoted fields (so the first characters
	// of the opening delimiter, separator, terminator and backslash are
	// special).
	// newLineByteSet is used in strict-format CSV dividing (so the first
	// characters of the terminator are special).
	quoteByteSet   byteSet
	unquoteByteSet byteSet
	newLineByteSet byteSet

	// recordBuffer holds the unescaped fields, one after another.
	// The fields can be accessed by using the indexes in fieldIndexes.
	// E.g., For the row `a,"b","c""d",e`, recordBuffer will contain `abc"de`
	// and fieldIndexes will contain the indexes [1, 2, 5, 6].
	recordBuffer []byte

	// fieldIndexes is an index of fields inside recordBuffer.
	// The width field ends at offset fieldIndexes[i] in recordBuffer.
	fieldIndexes  []int
	fieldIsQuoted []bool

	lastRecord []field

	escFlavor escapeFlavor
	// if set to true, csv parser will treat the first non-empty line as header line
	shouldParseHeader bool
	// in LOAD DATA, empty line should be treated as a valid field
	allowEmptyLine   bool
	quotedNullIsText bool
	unescapedQuote   bool

	reader io.Reader
	// stores data that has NOT been parsed yet, it shares same memory as appendBuf.
	buf []byte
	// used to read data from the reader, the data will be moved to other buffers.
	blockBuf    []byte
	isLastChunk bool

	// The list of column names of the last INSERT statement.
	columns []string

	lastRow []Field

	// the reader position we have parsed, if the underlying reader is not
	// a compressed file, it's the file position we have parsed too.
	// this value may go backward when failed to read quoted field, but it's
	// for printing error message, and the parser should not be used later,
	// so it's ok, see readQuotedField.
	pos int64

	// cache
	remainBuf *bytes.Buffer
	appendBuf *bytes.Buffer

	reuseRow bool

	// see csv.Reader
	comment byte
}

type field struct {
	content string
	quoted  bool
}

// NewCSVParser creates a CSV parser.
func NewCSVParser(
	cfg *CSVConfig,
	reader io.Reader,
	blockBufSize int64,
	shouldParseHeader bool,
	reuseRow bool,
) (*CSVParser, error) {
	// see csv.Reader
	if !validDelim(rune(cfg.FieldsTerminatedBy[0])) || (cfg.Comment != 0 && !validDelim(rune(cfg.Comment))) || cfg.Comment == cfg.FieldsTerminatedBy[0] {
		return nil, moerr.NewInvalidInputNoCtx("invalid field or comment delimiter")
	}

	var err error
	var separator, delimiter, terminator string

	separator = cfg.FieldsTerminatedBy
	delimiter = cfg.FieldsEnclosedBy
	terminator = cfg.LinesTerminatedBy

	if terminator == "\r\n" {
		terminator = "\n"
	}

	var quoteStopSet, newLineStopSet []byte
	unquoteStopSet := []byte{separator[0]}
	if len(delimiter) > 0 {
		quoteStopSet = []byte{delimiter[0]}
		unquoteStopSet = append(unquoteStopSet, delimiter[0])
	}
	if len(terminator) > 0 {
		newLineStopSet = []byte{terminator[0]}
	} else {
		// The character set encoding of '\r' and '\n' is the same in UTF-8 and GBK.
		newLineStopSet = []byte{'\r', '\n'}
	}
	unquoteStopSet = append(unquoteStopSet, newLineStopSet...)

	if len(cfg.LinesStartingBy) > 0 {
		if strings.Contains(cfg.LinesStartingBy, terminator) {
			return nil, moerr.NewInvalidInputNoCtx(fmt.Sprintf("STARTING BY '%s' cannot contain LINES TERMINATED BY '%s'", cfg.LinesStartingBy, terminator))
		}
	}

	escFlavor := escapeFlavorNone
	var r *regexp.Regexp

	if len(cfg.FieldsEscapedBy) > 0 {
		escFlavor = escapeFlavorMySQL
		quoteStopSet = append(quoteStopSet, cfg.FieldsEscapedBy[0])
		unquoteStopSet = append(unquoteStopSet, cfg.FieldsEscapedBy[0])
		// we need special treatment of the NULL value \N, used by MySQL.
		if !cfg.NotNull && slices.Contains(cfg.Null, cfg.FieldsEscapedBy+`N`) {
			escFlavor = escapeFlavorMySQLWithNull
		}
		r, err = regexp.Compile(`(?s)` + regexp.QuoteMeta(cfg.FieldsEscapedBy) + `.`)
		if err != nil {
			return nil, err
		}
	}
	return &CSVParser{
		reader:            reader,
		blockBuf:          make([]byte, blockBufSize*BufferSizeScale),
		remainBuf:         &bytes.Buffer{},
		appendBuf:         &bytes.Buffer{},
		cfg:               cfg,
		comma:             []byte(separator),
		quote:             []byte(delimiter),
		newLine:           []byte(terminator),
		startingBy:        []byte(cfg.LinesStartingBy),
		escapedBy:         cfg.FieldsEscapedBy,
		unescapeRegexp:    r,
		escFlavor:         escFlavor,
		quoteByteSet:      makeByteSet(quoteStopSet),
		unquoteByteSet:    makeByteSet(unquoteStopSet),
		newLineByteSet:    makeByteSet(newLineStopSet),
		shouldParseHeader: shouldParseHeader,
		allowEmptyLine:    cfg.AllowEmptyLine,
		quotedNullIsText:  cfg.QuotedNullIsText,
		unescapedQuote:    cfg.UnescapedQuote,
		reuseRow:          reuseRow,
	}, nil
}
func (parser *CSVParser) Read() (row []Field, err error) {
	if parser.reuseRow {
		row, err = parser.readRow(parser.lastRow)
		parser.lastRow = row
	} else {
		row, err = parser.readRow(nil)
	}
	return row, err
}

func (parser *CSVParser) Pos() int64 {
	return parser.pos
}

func validDelim(r rune) bool {
	return r != 0 && r != '"' && r != '\r' && r != '\n' && utf8.ValidRune(r) && r != utf8.RuneError
}

// readRow reads a row from the datafile.
func (parser *CSVParser) readRow(row []Field) ([]Field, error) {
	// skip the header first
	if parser.shouldParseHeader {
		err := parser.readColumns()
		if err != nil {
			return nil, err
		}
		parser.shouldParseHeader = false
	}

	records, err := parser.readRecord(parser.lastRecord)
	if err != nil {
		return nil, err
	}
	parser.lastRecord = records
	// remove the last empty value
	if parser.cfg.TrimLastSep {
		i := len(records) - 1
		if i >= 0 && len(records[i].content) == 0 {
			records = records[:i]
		}
	}
	row = row[:0]
	if cap(row) < len(records) {
		row = make([]Field, len(records))
	}
	row = row[:len(records)]
	for i, record := range records {
		unescaped, isNull, err := parser.unescapeString(record)
		if err != nil {
			return nil, err
		}
		row[i].IsNull = isNull
		row[i].Val = unescaped
	}

	return row, nil
}

func (parser *CSVParser) unescapeString(input field) (unescaped string, isNull bool, err error) {
	// Convert the input from another charset to utf8mb4 before we return the string.
	unescaped = input.content
	if parser.escFlavor == escapeFlavorMySQLWithNull && unescaped == parser.escapedBy+`N` {
		return input.content, true, nil
	}
	if parser.cfg.FieldsEnclosedBy != "" && !input.quoted && unescaped == "NULL" {
		return input.content, true, nil
	}
	if len(parser.escapedBy) > 0 {
		unescaped = unescape(unescaped, "", parser.escFlavor, parser.escapedBy[0], parser.unescapeRegexp)
	}
	if !(len(parser.quote) > 0 && parser.quotedNullIsText && input.quoted) {
		// this branch represents "quote is not configured" or "quoted null is null" or "this field has no quote"
		// we check null for them
		isNull = !parser.cfg.NotNull &&
			slices.Contains(parser.cfg.Null, unescaped)
		// avoid \\N becomes NULL
		if parser.escFlavor == escapeFlavorMySQLWithNull && unescaped == parser.escapedBy+`N` {
			isNull = false
		}
	}
	return
}

// csvToken is a type representing either a normal byte or some CSV-specific
// tokens such as the separator (comma), delimiter (quote) and terminator (new
// line).
type csvToken int16

const (
	// csvTokenAnyUnquoted is a placeholder to represent any unquoted character.
	csvTokenAnyUnquoted csvToken = 0
	// csvTokenEscaped is a mask indicating an escaped character.
	// The actual token is represented like `csvTokenEscaped | 'n'`.
	csvTokenEscaped csvToken = 0x100
	// csvTokenComma is the CSV separator token.
	csvTokenComma csvToken = 0x200
	// csvTokenNewLine is the CSV terminator token.
	csvTokenNewLine csvToken = 0x400
	// csvTokenDelimiter is the CSV delimiter token.
	csvTokenDelimiter csvToken = 0x800
)

func (parser *CSVParser) readByte() (byte, error) {
	if len(parser.buf) == 0 {
		if err := parser.readBlock(); err != nil {
			return 0, err
		}
	}
	if len(parser.buf) == 0 {
		return 0, io.EOF
	}
	b := parser.buf[0]
	parser.buf = parser.buf[1:]
	parser.pos++
	return b, nil
}

func (parser *CSVParser) peekBytes(cnt int) ([]byte, error) {
	if len(parser.buf) < cnt {
		if err := parser.readBlock(); err != nil {
			return nil, err
		}
	}
	if len(parser.buf) == 0 {
		return nil, io.EOF
	}
	if len(parser.buf) < cnt {
		cnt = len(parser.buf)
	}
	return parser.buf[:cnt], nil
}

func (parser *CSVParser) skipBytes(n int) {
	parser.buf = parser.buf[n:]
	parser.pos += int64(n)
}

// tryPeekExact peeks the bytes ahead, and if it matches `content` exactly will
// return (true, false, nil). If meet EOF it will return (false, true, nil).
// For other errors it will return (false, false, err).
func (parser *CSVParser) tryPeekExact(content []byte) (matched bool, eof bool, err error) {
	if len(content) == 0 {
		return true, false, nil
	}
	bs, err := parser.peekBytes(len(content))
	if err == nil {
		if bytes.Equal(bs, content) {
			return true, false, nil
		}
	} else if err == io.EOF {
		return false, true, nil
	}
	return false, false, err
}

// tryReadExact peeks the bytes ahead, and if it matches `content` exactly will
// consume it (advance the cursor) and return `true`.
func (parser *CSVParser) tryReadExact(content []byte) (bool, error) {
	matched, _, err := parser.tryPeekExact(content)
	if matched {
		parser.skipBytes(len(content))
	}
	return matched, err
}

func (parser *CSVParser) tryReadNewLine(b byte) (bool, error) {
	if len(parser.newLine) == 0 {
		return b == '\r' || b == '\n', nil
	}
	if b != parser.newLine[0] {
		return false, nil
	}
	return parser.tryReadExact(parser.newLine[1:])
}

func (parser *CSVParser) tryReadOpenDelimiter(b byte) (bool, error) {
	if len(parser.quote) == 0 || parser.quote[0] != b {
		return false, nil
	}
	return parser.tryReadExact(parser.quote[1:])
}

// tryReadCloseDelimiter is currently equivalent to tryReadOpenDelimiter until
// we support asymmetric delimiters.
func (parser *CSVParser) tryReadCloseDelimiter(b byte) (bool, error) {
	if parser.quote[0] != b {
		return false, nil
	}
	return parser.tryReadExact(parser.quote[1:])
}

func (parser *CSVParser) tryReadComma(b byte) (bool, error) {
	if parser.comma[0] != b {
		return false, nil
	}
	return parser.tryReadExact(parser.comma[1:])
}

func (parser *CSVParser) tryReadEscaped(bs byte) (bool, byte, error) {
	if parser.escapedBy == "" {
		return false, 0, nil
	}
	if bs != parser.escapedBy[0] || parser.escFlavor == escapeFlavorNone {
		return false, 0, nil
	}
	b, err := parser.readByte()
	return true, b, parser.replaceEOF(err, errDanglingBackslash)
}

// readQuoteToken reads a token inside quoted fields.
func (parser *CSVParser) readQuotedToken(b byte) (csvToken, error) {
	if ok, err := parser.tryReadCloseDelimiter(b); ok || err != nil {
		return csvTokenDelimiter, err
	}
	if ok, eb, err := parser.tryReadEscaped(b); ok || err != nil {
		return csvTokenEscaped | csvToken(eb), err
	}
	return csvToken(b), nil
}

// readUnquoteToken reads a token outside quoted fields.
func (parser *CSVParser) readUnquoteToken(b byte) (csvToken, error) {
	if ok, err := parser.tryReadNewLine(b); ok || err != nil {
		return csvTokenNewLine, err
	}
	if ok, err := parser.tryReadComma(b); ok || err != nil {
		return csvTokenComma, err
	}
	if ok, err := parser.tryReadOpenDelimiter(b); ok || err != nil {
		return csvTokenDelimiter, err
	}
	if ok, eb, err := parser.tryReadEscaped(b); ok || err != nil {
		return csvTokenEscaped | csvToken(eb), err
	}
	return csvToken(b), nil
}

func (parser *CSVParser) appendCSVTokenToRecordBuffer(token csvToken) {
	if token&csvTokenEscaped != 0 {
		parser.recordBuffer = append(parser.recordBuffer, parser.escapedBy[0])
	}
	parser.recordBuffer = append(parser.recordBuffer, byte(token))
}

// readUntil reads the buffer until any character from the `chars` set is found.
// that character is excluded from the final buffer.
func (parser *CSVParser) readUntil(chars *byteSet) ([]byte, byte, error) {
	index := IndexAnyByte(parser.buf, chars)
	if index >= 0 {
		ret := parser.buf[:index]
		parser.buf = parser.buf[index:]
		parser.pos += int64(index)
		return ret, parser.buf[0], nil
	}

	// not found in parser.buf, need allocate and loop.
	var buf []byte
	for {
		buf = append(buf, parser.buf...)
		if len(buf) > LargestEntryLimit {
			return buf, 0, moerr.NewInternalErrorNoCtx("size of row cannot exceed the max value of txn-entry-size-limit")
		}
		parser.buf = nil
		if err := parser.readBlock(); err != nil || len(parser.buf) == 0 {
			if err == nil {
				err = io.EOF
			}
			parser.pos += int64(len(buf))
			return buf, 0, err
		}
		index := IndexAnyByte(parser.buf, chars)
		if index >= 0 {
			buf = append(buf, parser.buf[:index]...)
			parser.buf = parser.buf[index:]
			parser.pos += int64(len(buf))
			return buf, parser.buf[0], nil
		}
	}
}

func (parser *CSVParser) readRecord(dst []field) ([]field, error) {
	parser.recordBuffer = parser.recordBuffer[:0]
	parser.fieldIndexes = parser.fieldIndexes[:0]
	parser.fieldIsQuoted = parser.fieldIsQuoted[:0]

	isEmptyLine := true
	whitespaceLine := true
	foundStartingByThisLine := false
	prevToken := csvTokenNewLine
	fieldIsQuoted := false
	var firstToken csvToken

outside:
	for {
		// we should drop
		// 1. the whole line if it does not contain startingBy
		// 2. any character before startingBy
		// since we have checked startingBy does not contain terminator, we can
		// split at terminator to check the substring contains startingBy. Even
		// if the terminator is inside a quoted field which means it's not the
		// end of a line, the substring can still be dropped by rule 2.
		if len(parser.startingBy) > 0 && !foundStartingByThisLine {
			oldPos := parser.pos
			content, _, err := parser.readUntilTerminator()
			if err != nil {
				if len(content) == 0 {
					return nil, err
				}
				// if we reached EOF, we should still check the content contains
				// startingBy and try to put back and parse it.
			}
			idx := bytes.Index(content, parser.startingBy)
			if idx == -1 {
				continue
			}
			foundStartingByThisLine = true
			content = content[idx+len(parser.startingBy):]
			parser.buf = append(content, parser.buf...)
			parser.pos = oldPos + int64(idx+len(parser.startingBy))
		}

		content, firstByte, err := parser.readUntil(&parser.unquoteByteSet)

		if len(content) > 0 {
			isEmptyLine = false
			if prevToken == csvTokenDelimiter {
				return nil, errUnexpectedQuoteField
			}
			parser.recordBuffer = append(parser.recordBuffer, content...)
			prevToken = csvTokenAnyUnquoted
		}

		if err != nil {
			if isEmptyLine || err != io.EOF {
				return nil, err
			}
			// treat EOF as the same as trailing \n.
			firstToken = csvTokenNewLine
		} else {
			parser.skipBytes(1)
			firstToken, err = parser.readUnquoteToken(firstByte)
			if err != nil {
				return nil, err
			}
		}

		switch firstToken {
		case csvTokenComma:
			whitespaceLine = false
			parser.fieldIndexes = append(parser.fieldIndexes, len(parser.recordBuffer))
			parser.fieldIsQuoted = append(parser.fieldIsQuoted, fieldIsQuoted)
			fieldIsQuoted = false
		case csvTokenDelimiter:
			if prevToken != csvTokenComma && prevToken != csvTokenNewLine {
				if parser.unescapedQuote {
					whitespaceLine = false
					parser.recordBuffer = append(parser.recordBuffer, parser.quote...)
					continue
				}
				return nil, errUnexpectedQuoteField
			}
			if err = parser.readQuotedField(); err != nil {
				return nil, err
			}
			fieldIsQuoted = true
			whitespaceLine = false
		case csvTokenNewLine:
			foundStartingByThisLine = false
			// new line = end of field (ignore empty lines)
			prevToken = firstToken
			if !parser.allowEmptyLine {
				if isEmptyLine {
					continue
				}
				// skip lines only contain whitespaces
				if err == nil && whitespaceLine && len(bytes.TrimSpace(parser.recordBuffer)) == 0 {
					parser.recordBuffer = parser.recordBuffer[:0]
					continue
				}
			}
			// skip lines start with comment
			if err == nil && parser.comment != 0 && parser.recordBuffer[0] == parser.comment {
				parser.recordBuffer = parser.recordBuffer[:0]
				parser.fieldIndexes = parser.fieldIndexes[:0]
				parser.fieldIsQuoted = parser.fieldIsQuoted[:0]

				isEmptyLine = true
				whitespaceLine = true
				foundStartingByThisLine = false
				prevToken = csvTokenNewLine
				fieldIsQuoted = false
				continue
			}
			if bytes.Equal(parser.newLine, []byte{'\n'}) {
				if n := len(parser.recordBuffer); n > 1 && parser.recordBuffer[n-1] == '\r' {
					parser.recordBuffer = parser.recordBuffer[:n-1]
				}
			}
			parser.fieldIndexes = append(parser.fieldIndexes, len(parser.recordBuffer))
			parser.fieldIsQuoted = append(parser.fieldIsQuoted, fieldIsQuoted)
			// the loop is end, no need to reset fieldIsQuoted
			break outside
		default:
			if prevToken == csvTokenDelimiter {
				return nil, errUnexpectedQuoteField
			}
			parser.appendCSVTokenToRecordBuffer(firstToken)
		}
		prevToken = firstToken
		isEmptyLine = false
	}
	// Create a single string and create slices out of it.
	// This pins the memory of the fields together, but allocates once.
	str := string(parser.recordBuffer) // Convert to string once to batch allocations
	dst = dst[:0]
	if cap(dst) < len(parser.fieldIndexes) {
		dst = make([]field, len(parser.fieldIndexes))
	}
	dst = dst[:len(parser.fieldIndexes)]
	var preIdx int
	for i, idx := range parser.fieldIndexes {
		dst[i].content = str[preIdx:idx]
		dst[i].quoted = parser.fieldIsQuoted[i]
		preIdx = idx
	}

	// Check or update the expected fields per field.
	return dst, nil
}

func (parser *CSVParser) readQuotedField() error {
	for {
		prevPos := parser.pos
		content, terminator, err := parser.readUntil(&parser.quoteByteSet)
		if err != nil {
			if err == io.EOF {
				// return the position of quote to the caller.
				// because we return an error here, the parser won't
				// use the `pos` again, so it's safe to modify it here.
				parser.pos = prevPos - 1
				// set buf to parser.buf in order to print err log
				parser.buf = content
				err = parser.replaceEOF(err, errUnterminatedQuotedField)
			}
			return err
		}
		parser.recordBuffer = append(parser.recordBuffer, content...)
		parser.skipBytes(1)

		token, err := parser.readQuotedToken(terminator)
		if err != nil {
			return err
		}

		switch token {
		case csvTokenDelimiter:
			// encountered '"' -> continue if we're seeing '""'.
			doubledDelimiter, err := parser.tryReadExact(parser.quote)
			if err != nil {
				return err
			}
			if doubledDelimiter {
				// consume the double quotation mark and continue
				parser.recordBuffer = append(parser.recordBuffer, parser.quote...)
			} else if parser.unescapedQuote {
				// allow unescaped quote inside quoted field, so we only finish
				// reading the field when we see a delimiter + comma/newline.
				comma, _, err2 := parser.tryPeekExact(parser.comma)
				if comma || err2 != nil {
					return err2
				}
				newline, eof, err2 := parser.tryPeekExact(parser.newLine)
				if eof || newline {
					return nil
				}
				if err2 != nil {
					return err2
				}
				parser.recordBuffer = append(parser.recordBuffer, parser.quote...)
			} else {
				// the field is completed, exit.
				return nil
			}
		default:
			parser.appendCSVTokenToRecordBuffer(token)
		}
	}
}

func (parser *CSVParser) replaceEOF(err error, replaced error) error {
	if err == nil || err != io.EOF {
		return err
	}
	return replaced
}

// readColumns reads the columns of this CSV file.
func (parser *CSVParser) readColumns() error {
	columns, err := parser.readRecord(nil)
	if err != nil {
		return err
	}
	if !parser.cfg.HeaderSchemaMatch {
		return nil
	}
	parser.columns = make([]string, 0, len(columns))
	for _, colName := range columns {
		colNameStr, _, err := parser.unescapeString(colName)
		if err != nil {
			return err
		}
		parser.columns = append(parser.columns, strings.ToLower(colNameStr))
	}
	return nil
}

// readUntilTerminator seeks the file until the terminator token is found, and
// returns
// - the content with terminator, or the content read before meet error
// - the file offset beyond the terminator, or the offset when meet error
// - error
// Note that the terminator string pattern may be the content of a field, which
// means it's inside quotes. Caller should make sure to handle this case.
func (parser *CSVParser) readUntilTerminator() ([]byte, int64, error) {
	var ret []byte
	for {
		content, firstByte, err := parser.readUntil(&parser.newLineByteSet)
		ret = append(ret, content...)
		if err != nil {
			return ret, parser.pos, err
		}
		parser.skipBytes(1)
		ret = append(ret, firstByte)
		if ok, err := parser.tryReadNewLine(firstByte); ok || err != nil {
			if len(parser.newLine) >= 1 {
				ret = append(ret, parser.newLine[1:]...)
			}
			return ret, parser.pos, err
		}
	}
}

func (parser *CSVParser) readBlock() error {
	n, err := io.ReadFull(parser.reader, parser.blockBuf)

	switch {
	case errors.Is(err, io.ErrUnexpectedEOF), err == io.EOF:
		parser.isLastChunk = true
		fallthrough
	case err == nil:
		// `parser.buf` reference to `appendBuf.Bytes`, so should use remainBuf to
		// hold the `parser.buf` rest data to prevent slice overlap
		parser.remainBuf.Reset()
		parser.remainBuf.Write(parser.buf)
		parser.appendBuf.Reset()
		parser.appendBuf.Write(parser.remainBuf.Bytes())
		blockData := parser.blockBuf[:n]
		if parser.pos == 0 {
			bomCleanedData := bom.Clean(blockData)
			parser.pos += int64(n - len(bomCleanedData))
			blockData = bomCleanedData
		}
		parser.appendBuf.Write(blockData)
		parser.buf = parser.appendBuf.Bytes()
		return nil
	default:
		return err
	}
}

func (parser *CSVParser) Columns() []string {
	return parser.columns
}

func (parser *CSVParser) SetColumns(columns []string) {
	parser.columns = columns
}

func unescape(
	input string,
	delim string,
	escFlavor escapeFlavor,
	escChar byte,
	unescapeRegexp *regexp.Regexp,
) string {
	if len(delim) > 0 {
		delim2 := delim + delim
		if strings.Contains(input, delim2) {
			input = strings.ReplaceAll(input, delim2, delim)
		}
	}
	if escFlavor != escapeFlavorNone && strings.IndexByte(input, escChar) != -1 {
		input = unescapeRegexp.ReplaceAllStringFunc(input, func(substr string) string {
			switch substr[1] {
			case '0':
				return "\x00"
			case 'b':
				return "\b"
			case 'n':
				return "\n"
			case 'r':
				return "\r"
			case 't':
				return "\t"
			case 'Z':
				return "\x1a"
			default:
				return substr[1:]
			}
		})
	}
	return input
}

// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package bytes implements functions for the manipulation of byte slices.
// It is analogous to the facilities of the strings package.

// this part is copy from `bytes/bytes.go`

// byteSet is a 32-byte value, where each bit represents the presence of a
// given byte value in the set.
type byteSet [8]uint32

// makeByteSet creates a set of byte value.
func makeByteSet(chars []byte) (as byteSet) {
	for i := 0; i < len(chars); i++ {
		c := chars[i]
		as[c>>5] |= 1 << uint(c&31)
	}
	return as
}

// contains reports whether c is inside the set.
func (as *byteSet) contains(c byte) bool {
	return (as[c>>5] & (1 << uint(c&31))) != 0
}

// IndexAnyByte returns the byte index of the first occurrence in s of any in the byte
// points in chars. It returns -1 if  there is no code point in common.
func IndexAnyByte(s []byte, as *byteSet) int {
	for i, c := range s {
		if as.contains(c) {
			return i
		}
	}
	return -1
}
