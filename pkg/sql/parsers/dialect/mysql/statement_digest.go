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
	"unicode/utf8"

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
	// Lexical candidates only enable the final contextual walk. The same
	// spelling can be an alias or appear in an unused CTE, so do not reject it.
	checkTableNames bool
}
type digestSymbol struct {
	id       uint16
	function bool
	reserved bool
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
			// MySQL's utf8mb4-to-utf8mb3 display buffer admits at most 64
			// source bytes. Ellipsis does not add a following separator; hash
			// encoding above still includes the complete retained identifier.
			if len(token.name) > 64 {
				formatted.WriteString("...")
				continue
			}
			// Conversion errors suppress the entire displayed identifier, not
			// its stored bytes. This includes valid supplementary utf8mb4.
			if token.name == "" || !digestSystemCharsetValid(token.name) {
				continue
			}
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
	hintPrevious := 0
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
			kind, value = scanDigestHint(s, hintPrevious)
		} else if afterDot {
			kind, value = s.ScanWithCommentsAfterDot()
		} else {
			kind, value = s.ScanWithComments()
		}
		s.parseControl.token()
		if kind == 0 {
			return nil
		}
		// Only scanner whitespace is removable; Unicode spaces can be
		// identifier bytes and must not turn a name into a keyword.
		raw := strings.Trim(s.buf[s.PrePos:s.Pos], " \t\r\n")
		if hint {
			hintPrevious = kind
			if kind == QUOTE_ID {
				hintPrevious = ID
			}
			if kind == LEX_ERROR {
				// MySQL's hint error recovery continues scanning. A failed
				// lexical item contributes no bytes, but later valid items do.
				continue
			}
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
		case STRING, DIGEST_INTRODUCED_STRING:
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
		case digestHintQueryBlock:
			d.add('@', "", false)
			d.add(digestIdentifier, value, false)
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
		if kind == QUOTE_ID || !hint && kind == ID && s.qualifiedIdentifier {
			id := digestIdentifier
			if hint && s.cur() == '@' {
				id = digestIdentifierAt
			}
			d.add(id, value, false)
			continue
		}
		upper := digestKeywordUpper(raw)
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
				hintPrevious = 0 // A keyword is not a query-block name prefix.
				d.add(id, "", false)
				continue
			}
		} else if symbol, ok := digestSymbols[upper]; ok {
			if !symbol.function || s.digestFunctionCallAhead() {
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
			id := digestIdentifier
			if hint && s.cur() == '@' {
				id = digestIdentifierAt
			}
			d.add(id, value, false)
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
	case *tree.ShowColumns:
		return digestShowFilterValid(stmt.Like, stmt.Where, true)
	case *tree.ShowDatabases:
		return digestShowFilterValid(stmt.Like, stmt.Where, true)
	case *tree.ShowTables:
		return digestShowFilterValid(stmt.Like, stmt.Where, true)
	case *tree.ShowTableStatus:
		return digestShowFilterValid(stmt.Like, stmt.Where, true)
	case *tree.ShowCollation:
		return digestShowFilterValid(stmt.Like, stmt.Where, true)
	case *tree.ShowVariables:
		return digestShowFilterValid(stmt.Like, stmt.Where, true)
	case *tree.ShowStatus:
		return digestShowFilterValid(stmt.Like, stmt.Where, true)
	case *tree.ShowFunctionOrProcedureStatus:
		return digestShowFilterValid(stmt.Like, stmt.Where, true)
	case *tree.ShowTarget:
		if stmt.Type == tree.ShowConfig {
			return false // MO-only target, not MySQL SHOW syntax.
		}
		allowed := stmt.Type == tree.ShowCharset || stmt.Type == tree.ShowTriggers || stmt.Type == tree.ShowEvents
		return digestShowFilterValid(stmt.Like, stmt.Where, allowed)
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

// Only the SHOW root's filters participate. A WHERE inside a filter subquery
// is not another SHOW filter, and LIKE alone is not the same as LIKE+WHERE.
func digestShowFilterValid(like *tree.ComparisonExpr, where *tree.Where, allowed bool) bool {
	return like == nil && where == nil || allowed && (like == nil || where == nil)
}

func digestColumnValid(column *tree.ColumnTableDef, allowReferences bool) bool {
	// Mirror column-attribute contextualization, not DDL execution. Attribute
	// order matters; a table-level primary key is not applied here. New column
	// definitions check character length, not execution-time empty/trailing-
	// space rules. Old names in CHANGE/DROP/RENAME have a different contract.
	if column.Name.NumParts != 1 {
		return false
	}
	if name := column.Name.ColNameOrigin(); len(name) > 64 && utf8.RuneCountInString(name) > 64 {
		return false
	}
	if typ, ok := column.Type.(*tree.T); ok &&
		(typ.InternalType.Family == tree.TimestampFamily || typ.InternalType.Family == tree.TimeFamily) &&
		(typ.InternalType.Scale < 0 || typ.InternalType.Scale > 6) {
		return false
	}
	if typ, ok := column.Type.(*tree.T); ok {
		t := typ.InternalType
		switch digestKeywordUpper(t.FamilyString) {
		case "CHAR", "CHARACTER", "NCHAR", "BINARY":
			if t.DisplayWith > 255 {
				return false
			}
		}
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
		switch value := expression.(type) {
		case *tree.ParenExpr:
			expression = value.Expr
			continue
		case *tree.UnaryExpr:
			if value.Op == tree.UNARY_PLUS {
				expression = value.Expr
				continue
			}
		}
		break
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
const digestHintQueryBlock = -2

func scanDigestHint(s *Scanner, previous int) (int, string) {
	start := s.Pos
	for strings.ContainsRune(" \t\r\n\v\f", rune(s.cur())) {
		s.inc()
	}
	s.PrePos = s.Pos
	ch := s.cur()
	switch {
	case ch == eofChar:
		return 0, ""
	case ch == '@' && (previous == '(' || previous == ID && s.Pos == start):
		s.inc()
		var kind int
		var value string
		if s.cur() == '`' || s.cur() == '"' {
			kind, value = scanDigestHintQuoted(s)
			if kind != QUOTE_ID {
				return LEX_ERROR, ""
			}
		} else if isLetter(s.cur()) || isDigit(s.cur()) || s.cur() >= utf8.RuneSelf && s.cur() != eofChar {
			kind, value = scanDigestHintIdentifier(s)
			if kind == LEX_ERROR {
				return kind, value
			}
		} else {
			return LEX_ERROR, ""
		}
		return digestHintQueryBlock, value
	case isDigit(ch) || ch == '.':
		return scanDigestHintNumber(s)
	case isLetter(ch) || ch >= utf8.RuneSelf:
		return scanDigestHintIdentifier(s)
	case ch == '\'' || ch == '"' || ch == '`':
		return scanDigestHintQuoted(s)
	default:
		s.inc()
		return int(ch), ""
	}
}

// Hint names allow repeated '$' and all valid utf8mb4 sequences. They never
// enter SQL keyword, native-function, variable, or qualified-name dispatch.
func scanDigestHintIdentifier(s *Scanner) (int, string) {
	start := s.Pos
	for s.cur() != eofChar {
		if isLetter(s.cur()) || isDigit(s.cur()) {
			s.inc()
		} else if s.cur() >= utf8.RuneSelf {
			_, size := utf8.DecodeRuneInString(s.buf[s.Pos:])
			s.incN(size)
			if size == 1 {
				return LEX_ERROR, ""
			}
		} else {
			break
		}
	}
	return ID, s.buf[start:s.Pos]
}

func scanDigestHintQuoted(s *Scanner) (int, string) {
	quote := s.cur()
	s.inc()
	// Unlike SQL identifiers, an empty hint name fails without consuming
	// the closing quote; recovery starts a new quoted item at that byte.
	if s.cur() == quote && s.peek(1) != quote {
		return LEX_ERROR, ""
	}
	start := s.Pos
	kind, value := s.scanLiteralIdentifierWithDelim(quote)
	if kind == LEX_ERROR || !utf8.ValidString(value) {
		return LEX_ERROR, ""
	}
	// MySQL's hint compact() rewrites from the original client bytes when
	// doubled delimiters occur, overriding the earlier charset conversion.
	// Without doubling, the system-charset conversion owns stored bytes.
	if s.Pos-start-1 == len(value) {
		value = digestSystemIdentifier(value)
	}
	if quote == '\'' || quote == '"' && !s.sqlMode.Has(SQLModeANSIQuotes) {
		kind = STRING
	}
	return kind, value
}

func scanDigestHintNumber(s *Scanner) (int, string) {
	start := s.Pos
	s.scanMantissa(10)
	if s.cur() == '.' {
		s.inc()
		fractionStart := s.Pos
		s.scanMantissa(10)
		if s.Pos == fractionStart || isLetter(s.cur()) || s.cur() >= utf8.RuneSelf && s.cur() != eofChar {
			// Error recovery discards the prefix but retains later hint words.
			return LEX_ERROR, ""
		}
		return FLOAT, s.buf[start:s.Pos]
	}
	// A size suffix stops before a multibyte name, but ASCII name/digit
	// continuations belong to the same identifier (1K名 versus 1Kx).
	if strings.ContainsRune("KMG", rune(s.cur())) && !isLetter(s.peek(1)) && !isDigit(s.peek(1)) {
		s.inc()
		return INTEGRAL, s.buf[start:s.Pos]
	}
	if isLetter(s.cur()) || s.cur() >= utf8.RuneSelf && s.cur() != eofChar {
		kind, value := scanDigestHintIdentifier(s)
		if kind == LEX_ERROR {
			return kind, value
		}
		return ID, s.buf[start:s.Pos]
	}
	return INTEGRAL, s.buf[start:s.Pos]
}

func (d *statementDigest) hint(parent *Scanner, sql string) error {
	s := NewScannerWithSQLMode(dialect.MYSQL, sql, parent.sqlMode)
	defer PutScanner(s)
	// Hint words are classified by digestHintSymbols, never SQL keywords.
	s.digestMode, s.parseControl = true, parent.parseControl
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
