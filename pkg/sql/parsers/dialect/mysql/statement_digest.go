// Copyright 2026 Matrix Origin
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

package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// These are MySQL's stable digest-encoding values, not MO parser token IDs.
const (
	digestValue uint16 = 1100 + iota
	digestValueList
	digestRow
	digestRowList
	digestTuple
	digestTupleList
	digestIdentifier
	digestIdentifierAt
	digestHintOpen
	digestHintClose
	digestInList
)

const digestStorageSize = 1024

// Digest arguments are data, potentially one per vector row. Bound parsing as
// well as retained digest bytes; a full prefix never excuses an invalid suffix.
type statementDigestSyntax struct{ nulls map[int]struct{} }
type digestSymbol struct {
	id       uint16
	function bool
}
type digestToken struct {
	id   uint16
	name string
}
type statementDigest struct {
	tokens []digestToken
	size   int
	full   bool
}

// StatementDigest parses, but never executes or resolves, one SQL statement.
// Its text and SHA256 share MySQL 8.4's normalized token-byte representation.
func StatementDigest(ctx context.Context, sql, sqlMode string) (text, hash string, err error) {
	limits := ParseLimits{Input: 1 << 20, Tokens: 1 << 16, Work: 8 << 20}
	if parent, ok := ctx.Value(parseLimitsKey{}).(ParseLimits); ok {
		limits.Input = min(limits.Input, parent.Input)
		limits.Tokens = min(limits.Tokens, parent.Tokens)
		limits.Work = min(limits.Work, parent.Work)
	}
	ctx = WithParseLimits(ctx, limits)
	syntax := &statementDigestSyntax{nulls: make(map[int]struct{})}
	stmts, parseErr := parseWithSQLMode(ctx, sql, 0, sqlMode, syntax)
	if parseErr != nil {
		if errors.Is(parseErr, ErrParseLimit) || errors.Is(parseErr, context.Cause(ctx)) {
			return "", "", parseErr
		}
		return "", "", moerr.NewDigestParse(ctx)
	}
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	if len(stmts) != 1 || strings.TrimSpace(sql) == "" || !digestSyntaxValid(stmts[0]) {
		return "", "", moerr.NewDigestParse(ctx)
	}

	control := &parseControl{ctx: ctx, limits: limits}
	defer func() {
		if failure := recover(); failure != nil {
			abort, own := failure.(parseAbort)
			if !own || abort.control != control {
				panic(failure)
			}
			text, hash, err = "", "", abort.err
		}
	}()
	scanner := NewScannerWithSQLMode(dialect.MYSQL, sql, ParseSQLModeFlags(sqlMode))
	defer PutScanner(scanner)
	scanner.digestMode, scanner.parseControl = true, control
	digest := statementDigest{tokens: make([]digestToken, 0, 64)}
	if err = digest.scan(scanner, syntax, false); err != nil {
		return "", "", err
	}
	encoded := make([]byte, 0, digest.size)
	var formatted strings.Builder
	for _, token := range digest.tokens {
		encoded = binary.LittleEndian.AppendUint16(encoded, token.id)
		if token.id == digestIdentifier || token.id == digestIdentifierAt {
			encoded = binary.LittleEndian.AppendUint16(encoded, uint16(len(token.name)))
			encoded = append(encoded, token.name...)
			formatted.WriteByte('`')
			formatted.WriteString(token.name)
			formatted.WriteByte('`')
		} else {
			formatted.WriteString(digestSpelling(token.id))
		}
		if token.id != '@' && token.id != digestIdentifierAt {
			formatted.WriteByte(' ')
		}
	}
	sum := sha256.Sum256(encoded)
	return strings.TrimRight(formatted.String(), " "), hex.EncodeToString(sum[:]), nil
}

func (d *statementDigest) scan(s *Scanner, syntax *statementDigestSyntax, hint bool) error {
	mode := s.sqlMode
	afterDot, afterSemicolon, hintAllowed := false, false, false
	previousEnd := 0
	previousRaw := ""
	for {
		var kind int
		var value string
		if hint {
			kind, value = scanDigestHint(s)
		} else if afterDot {
			kind, value = s.ScanWithCommentsAfterDot()
		} else {
			kind, value = s.ScanWithComments()
		}
		s.parseControl.token()
		if kind == 0 {
			return nil
		}
		raw := strings.TrimSpace(s.buf[s.PrePos:s.Pos])
		if kind == LEX_ERROR && hint {
			// Invalid hints are warnings, not failures of the surrounding SQL.
			// Keep the token prefix before the hint lexer stopped.
			return nil
		}
		if kind == LEX_ERROR || kind == VALUE_ARG {
			return moerr.NewDigestParse(s.parseControl.ctx)
		}
		if kind == COMMENT {
			if !hint && hintAllowed && strings.HasPrefix(raw, "/*+") && strings.TrimSpace(s.buf[previousEnd:s.PrePos]) == "" {
				if err := d.hint(s, raw[3:len(raw)-2]); err != nil {
					return err
				}
			}
			hintAllowed = false
			continue
		}
		if !hint && (afterSemicolon || kind == ';' && len(d.tokens) == 0) {
			return moerr.NewDigestParse(s.parseControl.ctx)
		}
		wasAfterDot := afterDot
		afterDot, hintAllowed = kind == '.', false
		priorEnd, priorRaw := previousEnd, previousRaw
		previousEnd, previousRaw = s.Pos, raw
		if kind == ';' {
			afterSemicolon = true
		}
		switch kind {
		case INTEGRAL, FLOAT, HEXNUM, BIT_LITERAL:
			d.add(digestValue, "", true)
			continue
		case STRING:
			if !hint && d.last(0) == digestIdentifier && strings.TrimSpace(s.buf[priorEnd:s.PrePos]) == "" {
				if strings.EqualFold(priorRaw, "n") && priorEnd == s.Pos-len(raw) {
					d.pop(1)
				} else if digestCharsetIntroducer(priorRaw) {
					d.pop(1)
					d.add(digestCharset, "", false)
				}
			}
			if !hint && strings.HasPrefix(strings.ToLower(raw), "_utf8mb4") {
				d.add(digestCharset, "", false)
			}
			d.add(digestValue, "", false)
			continue
		case NULL:
			if !hint {
				if _, literal := syntax.nulls[s.Pos]; literal {
					d.add(digestValue, "", false)
					continue
				}
			}
		case '@':
			if !hint {
				break
			}
			if d.last(0) == digestIdentifier && priorEnd == s.Pos-len(raw) {
				name := d.tokens[len(d.tokens)-1].name
				d.pop(1)
				d.add(digestIdentifierAt, name, false)
			}
			d.add('@', "", false)
			continue
		case AT_ID, AT_AT_ID:
			d.add('@', "", false)
			if kind == AT_AT_ID {
				d.add('@', "", false)
				parts := strings.Split(value, ".")
				for i, part := range parts {
					if i != 0 {
						d.add('.', "", false)
					}
					if symbol, ok := digestSymbols[strings.ToUpper(part)]; i == 0 && len(parts) > 1 && ok {
						d.add(symbol.id, "", false)
					} else {
						d.add(digestIdentifier, part, false)
					}
				}
			} else if strings.Contains(raw, "`") {
				d.add(digestIdentifier, value, false)
			} else {
				d.add(digestValue, "", false)
			}
			continue
		}
		if !hint && strings.EqualFold(raw, "NOT") && mode.Has(SQLModeHighNotPrecedence) {
			d.add(digestNotHigh, "", false)
			continue
		}
		switch raw {
		case "||":
			id := digestOr
			if mode.Has(SQLModePipesAsConcat) {
				id = digestConcat
			}
			d.add(id, "", false)
			continue
		case ":=":
			d.add(digestAssign, "", false)
			continue
		case "->":
			d.add(digestJSONExtract, "", false)
			continue
		case "->>":
			d.add(digestJSONUnquote, "", false)
			continue
		}
		if kind == QUOTE_ID || wasAfterDot && kind == ID {
			d.add(digestIdentifier, value, false)
			continue
		}
		upper := strings.ToUpper(raw)
		if !hint && digestCharsetIntroducer(raw) {
			d.add(digestCharset, "", false)
			continue
		}
		if !hint && upper == "ROLLUP" && d.last(0) == digestSymbols["WITH"].id {
			d.pop(1)
			d.add(digestWithRollup, "", false)
			continue
		}
		if hint && len(raw) > 0 && isDigit(uint16(raw[0])) {
			d.add(digestValue, "", true)
			continue
		}
		if hint {
			if id, ok := digestHintSymbols[upper]; ok {
				d.add(id, "", false)
				continue
			}
		} else if symbol, ok := digestSymbols[upper]; ok {
			position := s.Pos
			if mode.Has(SQLModeIgnoreSpace) {
				for position < len(s.buf) && strings.IndexByte(" \t\r\n\v\f", s.buf[position]) >= 0 {
					position++
				}
			}
			if !symbol.function || position < len(s.buf) && s.buf[position] == '(' {
				d.add(symbol.id, "", false)
				switch upper {
				case "SELECT", "INSERT", "REPLACE", "UPDATE", "DELETE":
					hintAllowed = true
				}
				continue
			}
		}
		if kind < 256 {
			d.add(uint16(kind), "", false)
		} else {
			d.add(digestIdentifier, value, false)
		}
	}
}

func digestCharsetIntroducer(word string) bool {
	switch strings.ToLower(word) {
	case "_armscii8", "_ascii", "_big5", "_binary", "_cp1250", "_cp1251", "_cp1256", "_cp1257", "_cp850", "_cp852", "_cp866", "_cp932", "_dec8", "_eucjpms", "_euckr", "_gb18030", "_gb2312", "_gbk", "_geostd8", "_greek", "_hebrew", "_hp8", "_keybcs2", "_koi8r", "_koi8u", "_latin1", "_latin2", "_latin5", "_latin7", "_macce", "_macroman", "_sjis", "_swe7", "_tis620", "_ucs2", "_ujis", "_utf16", "_utf16le", "_utf32", "_utf8", "_utf8mb3", "_utf8mb4":
		return true
	default:
		return false
	}
}

func digestRowConstructorValid(parent *Scanner) bool {
	s := NewScannerWithSQLMode(dialect.MYSQL, parent.buf, parent.sqlMode)
	defer PutScanner(s)
	s.Pos, s.CommentFlag = parent.Pos, parent.CommentFlag
	s.digestMode, s.parseControl = true, parent.parseControl
	kind, _ := s.Scan()
	if kind != '(' {
		return false
	}
	depth, commas := 1, 0
	for depth > 0 {
		kind, _ = s.Scan()
		switch kind {
		case 0, LEX_ERROR:
			return false
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 1 {
				commas++
			}
		}
	}
	return commas > 0
}

func digestSyntaxValid(statement tree.Statement) bool {
	// MySQL rejects NULL defaults of NOT NULL columns while contextualizing
	// CREATE TABLE, even though digest parsing never opens the target table.
	if create, ok := statement.(*tree.CreateTable); ok {
		for _, definition := range create.Defs {
			column, ok := definition.(*tree.ColumnTableDef)
			if !ok {
				continue
			}
			notNull, nullDefault := false, false
			for _, attribute := range column.Attributes {
				switch attr := attribute.(type) {
				case *tree.AttributeNull:
					notNull = !attr.Is
				case *tree.AttributeDefault:
					if value, ok := attr.Expr.(*tree.NumVal); ok {
						nullDefault = value.ValType == tree.P_null
					}
				}
			}
			if notNull && nullDefault {
				return false
			}
		}
	}
	return true
}

// Hint punctuation is not SQL punctuation: '?' is not a bind parameter,
// compound operators are separate bytes, and '--'/'#' are not comments.
// Reuse the lexical primitives without borrowing the SQL token dispatcher.
func scanDigestHint(s *Scanner) (int, string) {
	for {
		s.scanCheckpoint()
		s.PrePos = s.Pos
		s.skipBlank()
		ch := s.cur()
		switch {
		case ch == eofChar:
			return 0, ""
		case supplementaryUTF8SequenceSizeAt(s.buf, s.Pos) != 0:
			return LEX_ERROR, ""
		case isUnquotedIdentifierLetterAt(s.buf, s.Pos):
			return s.scanIdentifier(false)
		case isDigit(ch) || ch == '.' && isDigit(s.peek(1)):
			return s.scanNumber()
		case ch == '.':
			// A nonnumeric dot is discarded by the MySQL hint lexer.
			s.inc()
		case ch == '\'' || ch == '"' || ch == '`':
			return s.stepBackOneChar(ch, false)
		default:
			s.inc()
			if ch < 128 {
				return int(ch), ""
			}
			return LEX_ERROR, ""
		}
	}
}

func (d *statementDigest) hint(parent *Scanner, sql string) error {
	s := NewScannerWithSQLMode(dialect.MYSQL, sql, parent.sqlMode)
	defer PutScanner(s)
	// Hint words are classified by digestHintSymbols, never SQL keywords.
	s.digestMode, s.parseControl, s.identifierAfterDot = true, parent.parseControl, true
	d.add(digestHintOpen, "", false)
	if err := d.scan(s, nil, true); err != nil {
		return err
	}
	d.add(digestHintClose, "", false)
	return nil
}

func (d *statementDigest) last(offset int) uint16 {
	if len(d.tokens) <= offset {
		return 0
	}
	return d.tokens[len(d.tokens)-1-offset].id
}

func (d *statementDigest) pop(count int) {
	// Once the storage is full, neither appending nor suffix reductions may
	// change its retained prefix. Scanning still validates the complete input.
	if d.full {
		return
	}
	for range count {
		token := d.tokens[len(d.tokens)-1]
		d.size -= digestTokenSize(token.id, token.name)
		d.tokens = d.tokens[:len(d.tokens)-1]
	}
}

func digestTokenSize(id uint16, name string) int {
	if id == digestIdentifier || id == digestIdentifierAt {
		return 4 + len(name)
	}
	return 2
}

func (d *statementDigest) add(id uint16, name string, numeric bool) {
	if d.full {
		return
	}
	if numeric {
		for (d.last(0) == '+' || d.last(0) == '-') && digestExpressionPrefixes[d.last(1)] {
			d.pop(1)
		}
	}
	if id == digestValue && d.last(0) == ',' && (d.last(1) == digestValue || d.last(1) == digestValueList) {
		d.pop(2)
		id = digestValueList
	}
	if id == ')' && d.last(1) == '(' && (d.last(0) == digestValue || d.last(0) == digestValueList) {
		if d.last(0) == digestValue {
			id = digestRow
		} else {
			id = digestTuple
		}
		d.pop(2)
		if d.last(0) == digestIn {
			d.pop(1)
			id = digestInList
		} else if d.last(0) == ',' && (d.last(1) == id || d.last(1) == id+1) {
			d.pop(2)
			id++
		}
	}
	size := digestTokenSize(id, name)
	if d.size+size > digestStorageSize {
		d.full = true
		return
	}
	d.tokens = append(d.tokens, digestToken{id: id, name: name})
	d.size += size
}

func digestSpelling(id uint16) string {
	switch id {
	case digestValue:
		return "?"
	case digestValueList:
		return "?, ..."
	case digestRow:
		return "(?)"
	case digestRowList:
		return "(?) /* , ... */"
	case digestTuple:
		return "(...)"
	case digestTupleList:
		return "(...) /* , ... */"
	case digestHintOpen:
		return "/*+"
	case digestHintClose:
		return "*/"
	case digestInList:
		return "IN (...)"
	}
	if id < 256 {
		return string(rune(id))
	}
	return digestTokenText[id]
}
