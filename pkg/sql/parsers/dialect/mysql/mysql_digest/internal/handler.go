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

import "strings"

type tokenHandler struct {
	lexer              *Lexer
	store              *tokenStore
	reducer            *reducer
	rejectParamMarkers bool
	sawToken           bool
	sawDDL             bool
	ddlTable           bool
	parenDepth         int
	columnAttrDepth    int
	expressionDepth    int
	inColumnDefinition bool
	inColumnExpression bool
}

// SetRejectParameterMarkers makes the handler reject prepared-statement
// markers. The general digest API keeps accepting them because it is also used
// to normalize prepared statements; STATEMENT_DIGEST enables this option.
func (h *tokenHandler) SetRejectParameterMarkers(enabled bool) {
	h.rejectParamMarkers = enabled
}

func (h *tokenHandler) SawToken() bool {
	return h.sawToken
}

// NewTokenHandler creates a new token handler.
func NewTokenHandler(lexer *Lexer, store *tokenStore, reducer *reducer) *tokenHandler {
	return &tokenHandler{
		lexer:   lexer,
		store:   store,
		reducer: reducer,
	}
}

func (h *tokenHandler) ProcessAll() error {
	for {
		tok := h.lexer.Lex()

		if tok.Type == END_OF_INPUT {
			return nil
		}
		if tok.Type == ABORT_SYM {
			return tok.Err
		}
		h.sawToken = true
		if tok.Type == PARAM_MARKER && h.rejectParamMarkers {
			return NewLexError(tok.Start, ErrParameterMarker, h.lexer.input)
		}
		// A full digest buffer must not stop lexical validation. MySQL parses
		// the complete statement even when max_digest_length is zero.
		if h.store.full {
			continue
		}

		if err := h.handleToken(tok); err != nil {
			return err
		}
	}
}

func (h *tokenHandler) handleToken(tok Token) error {
	var err error
	switch {
	case isNumericLiteral(tok.Type):
		h.handleNumericLiteral()

	case isStringLiteral(tok.Type):
		h.handleLiteral()

	case tok.Type == NULL_SYM:
		h.handleNull()

	case tok.Type == ROLLUP_SYM && h.store.last() == WITH:
		// MySQL's lexer emits the internal WITH_ROLLUP_SYM token even when
		// whitespace or ordinary comments separate the two words.
		h.store.pop(1)
		h.store.push(WITH_ROLLUP_SYM)

	case tok.Type == ')':
		h.handleCloseParen()

	case tok.Type == IDENT || tok.Type == IDENT_QUOTED:
		err = h.handleIdentifier(tok)

	default:
		h.store.push(tok.Type)
		h.reducer.reduceAll()
	}
	if err == nil {
		h.observeToken(tok.Type)
	}
	return err
}

// Absorbs any preceding unary +/- signs before normalizing.
func (h *tokenHandler) handleNumericLiteral() {
	h.reducer.reduceUnarySign()
	h.store.push(TOK_GENERIC_VALUE)
	h.reducer.reduceAfterValue()
}

func (h *tokenHandler) handleLiteral() {
	h.store.push(TOK_GENERIC_VALUE)
	h.reducer.reduceAfterValue()
}

// NULL is kept as a keyword after IS/IS NOT, otherwise normalized to a value.
func (h *tokenHandler) handleNull() {
	if h.isNullKeywordContext() || h.isNullResponse() {
		h.store.push(NULL_SYM)
	} else {
		h.store.push(TOK_GENERIC_VALUE)
		h.reducer.reduceAfterValue()
	}
}

func (h *tokenHandler) isNullResponse() bool {
	// Both JSON_TABLE and JSON_VALUE use NULL ON EMPTY/ERROR as syntax.
	// DEFAULT NULL is still a literal, including in JSON_VALUE responses.
	if h.store.last() == DEFAULT_SYM {
		return false
	}
	// Lexer state is value-owned apart from the read-only token configuration.
	// Look ahead without consuming tokens or hiding subsequent lexical errors.
	lookahead := *h.lexer
	if lookahead.Lex().Type != ON_SYM {
		return false
	}
	next := lookahead.Lex().Type
	return next == EMPTY_SYM || next == ERROR_SYM
}

func (h *tokenHandler) handleCloseParen() {
	h.store.push(')')
	h.reducer.reduceAll()
}

func (h *tokenHandler) handleIdentifier(tok Token) error {
	text, err := h.lexer.TokenText(tok)
	if err != nil {
		return err
	}
	if tok.Type == IDENT_QUOTED {
		text = stripIdentifierQuotes(text)
	}
	if tok.Type != IDENT_QUOTED && isCharsetIntroducer(text) && h.lexer.followedBySingleQuote(tok.End) {
		h.store.push(UNDERSCORE_CHARSET)
		return nil
	}
	h.store.pushIdent(text)
	if h.inColumnDefinition && h.inColumnExpression && h.parenDepth == h.expressionDepth && isSimpleDefaultTemporal(text) {
		// CURRENT_TIMESTAMP and its temporal synonyms are the other valid
		// unparenthesized DEFAULT forms. The next NULL/NOT NULL is a column
		// attribute, not part of that temporal expression.
		h.inColumnExpression = false
	}
	return nil
}

func isSimpleDefaultTemporal(text string) bool {
	switch strings.ToUpper(text) {
	case "CURRENT_TIMESTAMP", "CURRENT_TIME", "CURRENT_DATE", "LOCALTIME", "LOCALTIMESTAMP":
		return true
	default:
		return false
	}
}

// isNullKeywordContext checks if NULL should be kept as a keyword.
// Returns true for IS NULL or IS NOT NULL.
func (h *tokenHandler) isNullKeywordContext() bool {
	if h.store.len() == 0 {
		return false
	}

	last := h.store.last()
	if last == IS {
		return true
	}

	if h.inColumnDefinition && !h.inColumnExpression && h.parenDepth == h.columnAttrDepth && last != DEFAULT_SYM {
		return true
	}

	if last == SET_SYM && h.store.len() >= 2 {
		prev, _ := h.store.peek2()
		if prev == DELETE_SYM || prev == UPDATE_SYM {
			return true
		}
	}

	if last == EQ && h.store.len() >= 2 {
		prev, _ := h.store.peek2()
		switch prev {
		case SECONDARY_ENGINE_SYM, PRIVILEGE_CHECKS_USER_SYM, SOURCE_TLS_CIPHERSUITES_SYM:
			return true
		}
	}

	// Check for IS NOT pattern
	if (last == NOT_SYM || last == NOT2_SYM) && h.store.len() >= 2 {
		prev, _ := h.store.peek2()
		if prev == IS {
			return true
		}
	}

	return false
}

func (h *tokenHandler) observeToken(tok int) {
	if !h.ddlTable {
		if tok == CREATE || tok == ALTER {
			h.sawDDL = true
		} else if h.sawDDL && tok == TABLE_SYM {
			h.ddlTable = true
		}
	}
	if h.inColumnDefinition && h.inColumnExpression && h.parenDepth == h.expressionDepth &&
		(isNumericLiteral(tok) || isStringLiteral(tok) || tok == NULL_SYM || tok == NOW_SYM ||
			tok == CURDATE || tok == CURTIME ||
			tok == TRUE_SYM || tok == FALSE_SYM) {
		// An unparenthesized DEFAULT is a simple literal (including boolean)
		// or temporal value.
		// Once that value is consumed, a following NULL/NOT NULL belongs to
		// the column attributes rather than the DEFAULT expression. Complex
		// defaults remain inside parentheses and are closed by the ')' case.
		h.inColumnExpression = false
	}

	switch tok {
	case AS:
		// CREATE TABLE ... AS SELECT is a query, not a column-definition
		// list. Type-like expression tokens in the query must not enable the
		// DDL NULL heuristic.
		if h.ddlTable && h.parenDepth == 0 {
			h.ddlTable = false
			h.inColumnDefinition = false
			h.inColumnExpression = false
		}
	case SELECT_SYM:
		// AS is optional in CREATE TABLE ... SELECT. A parenthesized
		// select_stmt can also start immediately after the table name. Once a
		// SELECT starts outside a column definition, its expressions are query
		// expressions, not DDL column attributes.
		if h.ddlTable && (h.parenDepth == 0 || !h.inColumnDefinition) {
			h.ddlTable = false
			h.inColumnDefinition = false
			h.inColumnExpression = false
		}
	case '(':
		h.parenDepth++
	case ')':
		if h.parenDepth > 0 {
			h.parenDepth--
		}
		if h.inColumnExpression && h.parenDepth == h.expressionDepth {
			h.inColumnExpression = false
		}
		if h.inColumnDefinition && h.parenDepth < h.columnAttrDepth {
			h.inColumnDefinition = false
			h.inColumnExpression = false
		}
	case ',':
		if h.inColumnExpression && h.parenDepth == h.expressionDepth {
			h.inColumnExpression = false
		}
		if h.inColumnDefinition && h.parenDepth == h.columnAttrDepth {
			h.inColumnDefinition = false
			h.inColumnExpression = false
		}
	case DEFAULT_SYM:
		if h.inColumnDefinition {
			h.inColumnExpression = true
			h.expressionDepth = h.parenDepth
		}
	case CHECK_SYM:
		if h.ddlTable {
			// CHECK always owns an expression, whether it appears in a column
			// definition or as a table-level constraint. Record its own base
			// depth so a preceding column cannot leak DDL attribute state into it.
			h.inColumnExpression = true
			h.expressionDepth = h.parenDepth
		}
	default:
		if h.ddlTable && !h.inColumnDefinition && !h.inColumnExpression && isColumnTypeToken(tok) {
			h.inColumnDefinition = true
			h.columnAttrDepth = h.parenDepth
		}
	}
}

// MySQL recognizes an underscore introducer only when the suffix names a
// supported character set. Keep unknown underscore-prefixed names as normal
// identifiers so their spelling remains significant to the digest.
var supportedCharsetIntroducers = map[string]struct{}{
	"armscii8": {}, "ascii": {}, "big5": {}, "binary": {}, "cp1250": {},
	"cp1251": {}, "cp1256": {}, "cp1257": {}, "cp850": {}, "cp852": {},
	"cp866": {}, "cp932": {}, "dec8": {}, "eucjpms": {}, "euckr": {},
	"gb18030": {}, "gb2312": {}, "gbk": {}, "geostd8": {}, "greek": {},
	"hebrew": {}, "hp8": {}, "keybcs2": {}, "koi8r": {}, "koi8u": {},
	"latin1": {}, "latin2": {}, "latin5": {}, "latin7": {}, "macce": {},
	"macroman": {}, "sjis": {}, "swe7": {}, "tis620": {}, "ucs2": {},
	"ujis": {}, "utf16": {}, "utf16le": {}, "utf32": {}, "utf8": {},
	"utf8mb3": {}, "utf8mb4": {},
}

func isCharsetIntroducer(text string) bool {
	if len(text) < 2 || text[0] != '_' {
		return false
	}
	_, ok := supportedCharsetIntroducers[strings.ToLower(text[1:])]
	return ok
}

func isColumnTypeToken(tok int) bool {
	switch tok {
	case INT_SYM, TINYINT_SYM, SMALLINT_SYM, MEDIUMINT_SYM, BIGINT_SYM,
		REAL_SYM, DOUBLE_SYM, FLOAT_SYM, DECIMAL_SYM, NUMERIC_SYM, FIXED_SYM,
		BIT_SYM, BOOL_SYM, BOOLEAN_SYM, CHAR_SYM, NCHAR_SYM, NATIONAL_SYM,
		BINARY_SYM, VARCHAR_SYM, NVARCHAR_SYM, VARBINARY_SYM, YEAR_SYM,
		DATE_SYM, TIME_SYM, TIMESTAMP_SYM, DATETIME_SYM, TINYBLOB_SYM,
		BLOB_SYM, MEDIUMBLOB_SYM, LONGBLOB_SYM, LONG_SYM, TINYTEXT_SYN,
		TEXT_SYM, MEDIUMTEXT_SYM, LONGTEXT_SYM, ENUM_SYM, SET_SYM,
		SERIAL_SYM, JSON_SYM, GEOMETRY_SYM, GEOMETRYCOLLECTION_SYM,
		POINT_SYM, MULTIPOINT_SYM, LINESTRING_SYM, MULTILINESTRING_SYM,
		POLYGON_SYM, MULTIPOLYGON_SYM:
		return true
	}
	return false
}

func isNumericLiteral(t int) bool {
	switch t {
	case NUM, LONG_NUM, ULONGLONG_NUM, DECIMAL_NUM, FLOAT_NUM, BIN_NUM, HEX_NUM:
		return true
	}
	return false
}

func isStringLiteral(t int) bool {
	switch t {
	case LEX_HOSTNAME, TEXT_STRING, NCHAR_STRING, PARAM_MARKER:
		return true
	}
	return false
}
