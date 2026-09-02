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
	sawDDL             bool
	ddlTable           bool
	parenDepth         int
	columnAttrDepth    int
	inColumnDefinition bool
	lastLiteralNull    bool
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
		if h.store.full {
			return nil
		}
		tok := h.lexer.Lex()

		if tok.Type == END_OF_INPUT {
			return nil
		}
		if tok.Type == ABORT_SYM {
			return tok.Err
		}

		if err := h.handleToken(tok); err != nil {
			return err
		}
	}
}

func (h *tokenHandler) handleToken(tok Token) error {
	if tok.Type == ON_SYM && h.lastLiteralNull && h.store.last() == TOK_GENERIC_VALUE {
		// JSON_TABLE's "NULL ON EMPTY/ERROR" uses NULL as syntax, not as a
		// literal. The grammar decides this one token after NULL.
		h.store.pop(1)
		h.store.push(NULL_SYM)
	}
	h.lastLiteralNull = false

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
	if h.isNullKeywordContext() {
		h.store.push(NULL_SYM)
	} else {
		h.store.push(TOK_GENERIC_VALUE)
		h.reducer.reduceAfterValue()
		h.lastLiteralNull = true
	}
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
	if strings.EqualFold(text, "_utf8mb4") && h.lexer.followedBySingleQuote(tok.End) {
		h.store.push(UNDERSCORE_CHARSET)
		return nil
	}
	h.store.pushIdent(text)
	return nil
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

	if h.inColumnDefinition && h.parenDepth == h.columnAttrDepth && last != DEFAULT_SYM {
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
	if last == NOT_SYM && h.store.len() >= 2 {
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

	switch tok {
	case '(':
		h.parenDepth++
	case ')':
		if h.parenDepth > 0 {
			h.parenDepth--
		}
		if h.inColumnDefinition && h.parenDepth < h.columnAttrDepth {
			h.inColumnDefinition = false
		}
	case ',':
		if h.inColumnDefinition && h.parenDepth == h.columnAttrDepth {
			h.inColumnDefinition = false
		}
	default:
		if h.ddlTable && isColumnTypeToken(tok) {
			h.inColumnDefinition = true
			h.columnAttrDepth = h.parenDepth
		}
	}
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
