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
type statementDigestSyntaxKey struct{}
type statementDigestSyntax struct {
	nulls      map[int]struct{}
	normalized *statementDigest
}
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
func StatementDigest(ctx context.Context, sql, sqlMode string) (string, string, error) {
	limits := ParseLimits{Input: 1 << 20, Tokens: 1 << 16, Work: 8 << 20}
	if parent, ok := ctx.Value(parseLimitsKey{}).(ParseLimits); ok {
		limits.Input = min(limits.Input, parent.Input)
		limits.Tokens = min(limits.Tokens, parent.Tokens)
		limits.Work = min(limits.Work, parent.Work)
	}
	ctx = WithParseLimits(ctx, limits)
	syntax := &statementDigestSyntax{
		nulls:      make(map[int]struct{}),
		normalized: &statementDigest{},
	}
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
	// Both scanner passes finish at the parser's approved budget boundary.
	// Publish text/hash only after parsing, contextualization and normalization.
	digest := syntax.normalized
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
	hintStarted := false
	if hint {
		defer func() {
			if hintStarted {
				d.add(digestHintClose, "", false)
			}
		}()
	}
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
		if hint && !hintStarted {
			// Empty hints (including discarded dots or an initial lexical error)
			// contribute no tokens, even at the retained-prefix boundary.
			d.add(digestHintOpen, "", false)
			hintStarted = true
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
			if !hint && d.last(0) == digestIdentifier && d.last(1) != '.' && strings.TrimSpace(s.buf[priorEnd:s.PrePos]) == "" {
				if strings.EqualFold(priorRaw, "n") && strings.HasPrefix(raw, "'") && priorEnd == s.Pos-len(raw) {
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
						if len(part) > 0 && (part[0] == '\'' || part[0] == '"' && !mode.Has(SQLModeANSIQuotes)) {
							d.add(digestValue, "", false)
							continue
						}
						// The combined variable token retains quoting on a qualified
						// component; identifier encoding needs its decoded bytes.
						if len(part) >= 2 && part[0] == part[len(part)-1] &&
							(part[0] == '`' || part[0] == '"' && mode.Has(SQLModeANSIQuotes)) {
							quote := part[:1]
							part = strings.ReplaceAll(part[1:len(part)-1], quote+quote, quote)
						}
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
	switch stmt := statement.(type) {
	case *tree.CreateTable:
		for _, definition := range stmt.Defs {
			if column, ok := definition.(*tree.ColumnTableDef); ok && !digestColumnValid(column, true) {
				return false
			}
		}
	case *tree.AlterTable:
		for _, option := range stmt.Options {
			switch option := option.(type) {
			case *tree.AlterAddCol:
				if !digestColumnValid(option.Column, true) {
					return false
				}
			case *tree.AlterTableModifyColumnClause:
				if !digestColumnValid(option.NewColumn, false) {
					return false
				}
			case *tree.AlterTableChangeColumnClause:
				if !digestColumnValid(option.NewColumn, false) {
					return false
				}
			case *tree.AlterTableAlterColumnClause:
				// Its signed-literal/parenthesized grammar is narrower than a
				// column definition, but the parenthesized body is not itemized
				// as a column expression (variables/subqueries remain valid).
				if option.OptionType == tree.AlterColumnOptionSetDefault &&
					!digestAlterDefaultValid(option.DefaultExpr.Expr) {
					return false
				}
			}
		}
	}
	return true
}

func digestColumnValid(column *tree.ColumnTableDef, allowReferences bool) bool {
	// Mirror column-attribute contextualization, not DDL execution. Attribute
	// order matters; a table-level primary key is not applied here.
	if typ, ok := column.Type.(*tree.T); ok &&
		(typ.InternalType.Family == tree.TimestampFamily || typ.InternalType.Family == tree.TimeFamily) &&
		(typ.InternalType.Scale < 0 || typ.InternalType.Scale > 6) {
		return false
	}
	notNull, nullDefault, hasDefault, autoIncrement, generated := false, false, false, false, false
	generatedDisallowedAttribute, hasCollation := false, false
	for _, attribute := range column.Attributes {
		switch attr := attribute.(type) {
		case *tree.AttributeNull:
			notNull = !attr.Is
		case *tree.AttributePrimaryKey:
			notNull = true
		case *tree.AttributeAutoIncrement:
			autoIncrement = true
		case *tree.AttributeGeneratedAlways:
			if !digestColumnExpressionValid(attr.Expr, true) {
				return false
			}
			generated = true
		case *tree.AttributeReference:
			// CREATE/ADD admit inline REFERENCES, but MODIFY/CHANGE do not.
			if !allowReferences {
				return false
			}
		case *tree.AttributeOnUpdate:
			if !digestTemporalAttributeValid(column, attr.Expr, true) {
				return false
			}
			generatedDisallowedAttribute = true
		case *tree.AttributeCollate:
			if hasCollation {
				return false
			}
			hasCollation = true
		case *tree.AttributeColumnFormat, *tree.AttributeStorage:
			generatedDisallowedAttribute = true
		case *tree.AttributeDefault:
			if !digestColumnExpressionValid(attr.Expr, false) || !digestTemporalAttributeValid(column, attr.Expr, false) {
				return false
			}
			if digestDefaultHasColumnCollation(attr.Expr) {
				if hasCollation {
					return false
				}
				hasCollation = true
			}
			hasDefault = true
			expression := attr.Expr
			if collated, ok := expression.(*tree.CollateExpr); ok {
				expression = collated.Expr
			}
			value, literal := expression.(*tree.NumVal)
			nullDefault = literal && value.ValType == tree.P_null
		}
	}
	return !(notNull && nullDefault || autoIncrement && hasDefault && !nullDefault || generated && (hasDefault || autoIncrement || generatedDisallowedAttribute))
}

func digestColumnExpressionValid(expression tree.Expr, generated bool) bool {
	syntax := tree.InspectColumnExpressionSyntax(expression)
	if syntax.VariableOrSubquery || syntax.FullTextMatch {
		return false
	}
	for _, function := range syntax.Functions {
		if function.IsGeneric || function.FuncName != nil && !digestColumnFunctionValid(function.FuncName.Origin(), generated) {
			return false
		}
	}
	return true
}

func digestAlterDefaultValid(expression tree.Expr) bool {
	if !digestDefaultLiteralValid(expression, false) {
		return false
	}
	// Parentheses do not turn a temporal literal into an expression default.
	// CAST/function/operator wrappers are distinct expressions and remain valid.
	for {
		parenthesized, ok := expression.(*tree.ParenExpr)
		if !ok {
			break
		}
		expression = parenthesized.Expr
	}
	literal, temporal := expression.(*tree.FuncExpr)
	return !temporal || !literal.IsTemporalLiteral
}

// Match the signed-literal or parenthesized-expression domain, not an arbitrary
// bit_expr. A trailing COLLATE belongs to column definitions only.
func digestDefaultLiteralValid(expression tree.Expr, allowCollation bool) bool {
	collated := false
	if value, ok := expression.(*tree.CollateExpr); ok && allowCollation {
		expression, collated = value.Expr, true
	}
	switch value := expression.(type) {
	case *tree.NumVal, *tree.ParenExpr:
		return true
	case *tree.Subquery:
		// Subquery parentheses belong to the query itself. Only an additional
		// grouping pair supplies SET DEFAULT's '(' expression ')' production.
		if !value.Exists {
			if query, ok := value.Select.(*tree.ParenSelect); ok {
				wrapper := query.Select
				_, grouped := wrapper.Select.(*tree.ParenSelect)
				// The entire query layer must be just a grouping pair. A nested
				// ParenSelect followed by this layer's ORDER/LIMIT/etc. is still
				// query syntax, not DEFAULT's outer expression parentheses.
				return grouped && len(wrapper.OrderBy) == 0 && wrapper.Limit == nil &&
					wrapper.With == nil && wrapper.TimeWindow == nil && wrapper.RankOption == nil &&
					wrapper.Ep == nil && len(wrapper.IntoVars) == 0 && wrapper.SelectLockInfo == nil &&
					!wrapper.IsPerform && wrapper.RewriteOption == nil && !wrapper.DeprecatedInto
			}
		}
	case *tree.UnaryExpr:
		operand := value.Expr
		if value, ok := operand.(*tree.CollateExpr); ok && allowCollation && !collated {
			operand = value.Expr
		}
		if literal, ok := operand.(*tree.NumVal); ok &&
			(value.Op == tree.UNARY_PLUS || value.Op == tree.UNARY_MINUS) {
			switch literal.ValType {
			case tree.P_int64, tree.P_uint64, tree.P_float64, tree.P_decimal:
				return true
			}
		}
	}
	return false
}

func digestDefaultHasColumnCollation(expression tree.Expr) bool {
	if _, ok := expression.(*tree.CollateExpr); ok {
		return true
	}
	if unary, ok := expression.(*tree.UnaryExpr); ok {
		_, collated := unary.Expr.(*tree.CollateExpr)
		return collated
	}
	return false
}

func digestTemporalAttributeValid(column *tree.ColumnTableDef, expression tree.Expr, onUpdate bool) bool {
	if !onUpdate {
		if digestDefaultLiteralValid(expression, true) {
			return true
		}
		if collated, ok := expression.(*tree.CollateExpr); ok {
			expression = collated.Expr
		}
	}
	function, direct := expression.(*tree.FuncExpr)
	if !direct || function.IsTemporalLiteral || function.IsGeneric {
		return false
	}
	name := ""
	if function.FuncName != nil {
		name = strings.ToLower(function.FuncName.Origin())
	}
	if !isDigestTemporalFunctionName(name) {
		return false
	}
	typ, ok := column.Type.(*tree.T)
	if !ok || typ.InternalType.Family != tree.TimestampFamily || len(function.Exprs) > 1 {
		return false
	}
	var precision int64
	if len(function.Exprs) == 1 {
		literal, ok := function.Exprs[0].(*tree.NumVal)
		if !ok {
			return false
		}
		precision, ok = literal.Int64()
		if !ok {
			return false
		}
	}
	return precision >= 0 && precision <= 6 && precision == int64(typ.InternalType.Scale)
}

// These are MySQL value-generator pre-validation rules, not a determinism
// heuristic or execution-time function registry. In particular SYSDATE passes
// this parsing phase, while UNIX_TIMESTAMP with an argument still fails for a
// generated column. DEFAULT expressions have their own, less restrictive set.
func digestColumnFunctionValid(name string, generated bool) bool {
	switch strings.ToLower(name) {
	case "version", "last_insert_id", "found_rows", "row_count", "benchmark", "sleep",
		"get_lock", "is_free_lock", "is_used_lock", "release_lock", "release_all_locks",
		"source_pos_wait", "master_pos_wait", "load_file", "values", "name_const":
		return false
	case "current_timestamp", "now", "localtime", "localtimestamp",
		"current_date", "curdate", "utc_date", "current_time", "curtime", "utc_time", "utc_timestamp",
		"unix_timestamp", "connection_id", "current_user", "user", "session_user", "system_user",
		"database", "schema", "current_role", "roles_graphml", "uuid", "uuid_short", "rand",
		"statement_digest", "statement_digest_text":
		return !generated
	default:
		return true
	}
}

func isDigestTemporalFunctionName(name string) bool {
	switch strings.ToLower(name) {
	case "current_timestamp", "now", "localtime", "localtimestamp":
		return true
	default:
		return false
	}
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
			kind, value := scanDigestHintNumber(s)
			if kind != skippedScannerToken {
				return kind, value
			}
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

func scanDigestHintNumber(s *Scanner) (int, string) {
	start := s.Pos
	s.scanMantissa(10)
	if s.cur() == '.' {
		s.inc()
		fractionStart := s.Pos
		s.scanMantissa(10)
		if s.Pos == fractionStart || isUnquotedIdentifierLetterAt(s.buf, s.Pos) {
			// An invalid decimal prefix is discarded before scanning the
			// trailing hint word; it is not a numeric token plus an alias.
			return skippedScannerToken, ""
		}
		return FLOAT, s.buf[start:s.Pos]
	}
	end := s.Pos
	if isUnquotedIdentifierLetterAt(s.buf, s.Pos) {
		kind, value := s.scanIdentifier(false)
		if kind == LEX_ERROR {
			return kind, value
		}
		// Hints recognize decimal/size literals, but not SQL exponents or
		// hex/bin literals. Thus 16K is a number while 1e2 is an identifier.
		if s.Pos != end+1 || !strings.ContainsAny(s.buf[end:s.Pos], "KMG") {
			return ID, s.buf[start:s.Pos]
		}
	}
	return INTEGRAL, s.buf[start:s.Pos]
}

func (d *statementDigest) hint(parent *Scanner, sql string) error {
	s := NewScannerWithSQLMode(dialect.MYSQL, sql, parent.sqlMode)
	defer PutScanner(s)
	// Hint words are classified by digestHintSymbols, never SQL keywords.
	s.digestMode, s.parseControl, s.identifierAfterDot = true, parent.parseControl, true
	return d.scan(s, nil, true)
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
