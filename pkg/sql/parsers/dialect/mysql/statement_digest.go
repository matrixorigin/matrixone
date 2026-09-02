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

package mysql

import (
	"context"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

type digestTokenKind uint8

const (
	digestTokenPlain digestTokenKind = iota
	digestTokenValue
	digestTokenValueList
	digestTokenRowSingle
	digestTokenRowSingleList
	digestTokenRowMultiple
	digestTokenRowMultipleList
)

type digestToken struct {
	kind digestTokenKind
	text string
	size int
}

// NormalizeStatementDigest parses sql and returns the token-normalized form used
// by MySQL's STATEMENT_DIGEST_TEXT. It validates MatrixOne's grammar first,
// with narrowly scoped compatibility rewrites for MySQL-only digest syntax.
func NormalizeStatementDigest(ctx context.Context, sql, sqlMode string, maxDigestLength int) (string, error) {
	if !utf8.ValidString(sql) {
		return "", moerr.NewParseError(ctx, "the argument is not valid UTF-8")
	}
	stmt, err := parseStatementDigestSQL(ctx, sql, sqlMode)
	if err != nil {
		return "", err
	}
	defer stmt.Free()

	sqlModeFlags := ParseSQLModeFlags(sqlMode)
	scanner := NewScannerWithSQLMode(dialect.MYSQL, sql, sqlModeFlags)
	defer PutScanner(scanner)

	tokens := make([]digestToken, 0, 16)
	storedSize := 0
	sawStatementToken := false
	storing := true
	bangRun := 0
	skipQuotedUserVarValue := false
	lastScanPos := 0
	for {
		typ, value := scanner.Scan()
		scannedSource := sql[lastScanPos:scanner.Pos]
		lastScanPos = scanner.Pos
		if typ == 0 || typ == EofChar() {
			break
		}
		if typ == LEX_ERROR {
			return "", moerr.NewParseErrorf(ctx, "lexical error near %q", value)
		}
		if typ == VALUE_ARG {
			return "", moerr.NewParseError(ctx, "parameter markers are not permitted")
		}

		sawStatementToken = true
		if skipQuotedUserVarValue {
			skipQuotedUserVarValue = false
			continue
		}
		if !storing {
			continue
		}

		rawSource := sql[scanner.PrePos:scanner.Pos]
		for _, hintToken := range digestOptimizerHintTokens(scannedSource, digestCanStartOptimizerHint(tokens), sqlModeFlags.Has(SQLModeANSIQuotes)) {
			if maxDigestLength >= 0 && storedSize+hintToken.size > maxDigestLength {
				storing = false
				break
			}
			tokens = append(tokens, hintToken)
			storedSize += hintToken.size
		}
		if !storing {
			continue
		}

		if typ == '!' {
			if bangRun == 0 || len(rawSource) != 1 {
				bangRun = 1
			} else {
				bangRun++
			}
			// MySQL's lexer emits every adjacent "!!" pair as one NOT2 token.
			if bangRun%2 == 0 {
				continue
			}
		} else {
			bangRun = 0
		}
		if typ == '@' && scanner.Pos < len(sql) {
			quote := sql[scanner.Pos]
			quotedUserVar := strings.TrimSpace(rawSource) == "@" && (quote == '\'' || quote == '"')
			if !quotedUserVar && len(scannedSource) >= 2 {
				quotedUserVar = strings.HasSuffix(scannedSource, "@'") || strings.HasSuffix(scannedSource, "@\"")
			}
			if quotedUserVar {
				userVarToken := digestToken{kind: digestTokenValue, text: "@?", size: 2}
				if maxDigestLength >= 0 && storedSize+userVarToken.size > maxDigestLength {
					storing = false
					continue
				}
				tokens = append(tokens, userVarToken)
				storedSize += userVarToken.size
				skipQuotedUserVarValue = true
				continue
			}
		}

		// N'text' is one NCHAR_STRING token in MySQL, while the MatrixOne
		// scanner exposes an identifier followed by a string token.
		if typ == ID && strings.EqualFold(value, "n") &&
			scanner.Pos < len(sql) && sql[scanner.Pos] == '\'' {
			continue
		}
		if typ == ID && isDigestCharsetName(value) {
			if !digestIsCharsetIntroducer(sql, scanner.Pos, sqlModeFlags.Has(SQLModeANSIQuotes)) {
				return "", moerr.NewParseErrorf(ctx, "character set introducer %s is not followed by a string literal", value)
			}
			charsetToken := digestToken{kind: digestTokenPlain, text: "(_charset)", size: 2}
			if maxDigestLength >= 0 && storedSize+charsetToken.size > maxDigestLength {
				storing = false
				continue
			}
			tokens = append(tokens, charsetToken)
			storedSize += charsetToken.size
			continue
		}

		if typ == STRING && digestHasCharsetIntroducer(rawSource) {
			charsetToken := digestToken{kind: digestTokenPlain, text: "(_charset)", size: 2}
			if maxDigestLength >= 0 && storedSize+charsetToken.size > maxDigestLength {
				storing = false
				continue
			}
			tokens = append(tokens, charsetToken)
			storedSize += charsetToken.size
		}

		if isDigestValueToken(typ, tokens) {
			storedSize -= removeUnarySigns(&tokens)
			if !canReduceDigestValue(tokens) &&
				maxDigestLength >= 0 && storedSize+2 > maxDigestLength {
				storing = false
				continue
			}
			storedSize += appendDigestValue(&tokens)
			continue
		}

		if typ == ')' {
			if !canReduceDigestGroup(tokens) &&
				maxDigestLength >= 0 && storedSize+2 > maxDigestLength {
				storing = false
				continue
			}
			storedSize += closeDigestGroup(&tokens)
			continue
		}

		text := digestTokenText(typ, value, strings.TrimSpace(rawSource))
		if text == "" {
			return "", moerr.NewParseErrorf(ctx, "unsupported token %d", typ)
		}
		tokenSize := 2
		if typ == ID || typ == QUOTE_ID {
			tokenSize += 2 + len(value)
		}
		if maxDigestLength >= 0 && storedSize+tokenSize > maxDigestLength {
			storing = false
			continue
		}
		tokens = append(tokens, digestToken{kind: digestTokenPlain, text: text, size: tokenSize})
		storedSize += tokenSize
	}

	if len(tokens) == 0 {
		if sawStatementToken {
			return "", nil
		}
		return "", moerr.NewParseError(ctx, "the argument is empty or contains only comments")
	}

	parts := make([]string, 0, len(tokens))
	for i := range tokens {
		parts = append(parts, tokens[i].text)
	}
	return strings.Join(parts, " "), nil
}

// parseStatementDigestSQL validates the input with MatrixOne's parser. A
// handful of MySQL constructs are intentionally accepted by the digest
// function even though they have no direct MatrixOne AST production; these
// are rewritten only for validation, while the original text is always used
// for token normalization below.
func parseStatementDigestSQL(ctx context.Context, sql, sqlMode string) (tree.Statement, error) {
	parsed, err := ParseOneWithSQLMode(ctx, sql, 0, sqlMode)
	if err == nil {
		return parsed, nil
	}
	rewritten := rewriteDigestParserCompatibility(sql)
	if rewritten == sql {
		return nil, err
	}
	parsed, rewrittenErr := ParseOneWithSQLMode(ctx, rewritten, 0, sqlMode)
	if rewrittenErr == nil {
		return parsed, nil
	}
	return nil, err
}

func rewriteDigestParserCompatibility(sql string) string {
	var b strings.Builder
	b.Grow(len(sql))
	changed := false
	for i := 0; i < len(sql); {
		ch := sql[i]
		switch ch {
		case '\'', '"', '`':
			end, closed := skipDigestParserQuoted(sql, i, ch)
			if !closed {
				b.WriteString(sql[i:])
				return b.String()
			}
			b.WriteString(sql[i:end])
			i = end
		case '#':
			end := strings.IndexByte(sql[i:], '\n')
			if end < 0 {
				b.WriteString(sql[i:])
				return b.String()
			}
			end += i
			b.WriteString(sql[i:end])
			i = end
		case '/':
			if i+1 < len(sql) && sql[i+1] == '*' {
				end := strings.Index(sql[i+2:], "*/")
				if end < 0 {
					b.WriteString(sql[i:])
					return b.String()
				}
				end += i + 4
				b.WriteString(sql[i:end])
				i = end
				continue
			}
			b.WriteByte(ch)
			i++
		case '-':
			if i+2 < len(sql) && sql[i+1] == '-' && isDigestHintSpace(sql[i+2]) {
				end := strings.IndexByte(sql[i+3:], '\n')
				if end < 0 {
					b.WriteString(sql[i:])
					return b.String()
				}
				end += i + 3
				b.WriteString(sql[i:end])
				i = end
				continue
			}
			b.WriteByte(ch)
			i++
		case '@':
			if i+1 < len(sql) && (sql[i+1] == '\'' || sql[i+1] == '"' || sql[i+1] == '`') {
				end, closed := skipDigestParserQuoted(sql, i+1, sql[i+1])
				if closed {
					b.WriteString("@digest_var")
					i = end
					changed = true
					continue
				}
			}
			b.WriteByte(ch)
			i++
		default:
			if !isDigestRewriteWordStart(ch) {
				b.WriteByte(ch)
				i++
				continue
			}
			start := i
			for i < len(sql) && isDigestRewriteWordPart(sql[i]) {
				i++
			}
			word := sql[start:i]
			upper := strings.ToUpper(word)
			switch upper {
			case "NCHAR":
				b.WriteString("CHAR")
				changed = true
			case "ROW":
				j := i
				for j < len(sql) && isDigestHintSpace(sql[j]) {
					j++
				}
				if j < len(sql) && sql[j] == '(' && digestRowConstructorHasComma(sql, j) {
					changed = true
					i = j
				} else {
					b.WriteString(word)
				}
			case "SOUNDS":
				j := i
				for j < len(sql) && isDigestHintSpace(sql[j]) {
					j++
				}
				if j+4 <= len(sql) && strings.EqualFold(sql[j:j+4], "LIKE") &&
					(j+4 == len(sql) || !isDigestRewriteWordPart(sql[j+4])) {
					changed = true
					i = j
				} else {
					b.WriteString(word)
				}
			default:
				b.WriteString(word)
			}
		}
	}
	if !changed {
		return sql
	}
	return b.String()
}

func skipDigestParserQuoted(source string, start int, delimiter byte) (int, bool) {
	for i := start + 1; i < len(source); i++ {
		if source[i] == '\\' && delimiter != '`' && i+1 < len(source) {
			i++
			continue
		}
		if source[i] != delimiter {
			continue
		}
		if i+1 < len(source) && source[i+1] == delimiter {
			i++
			continue
		}
		return i + 1, true
	}
	return len(source), false
}

func digestRowConstructorHasComma(source string, start int) bool {
	depth := 0
	for i := start; i < len(source); i++ {
		switch source[i] {
		case '\'', '"', '`':
			end, closed := skipDigestParserQuoted(source, i, source[i])
			if !closed {
				return false
			}
			i = end - 1
		case '#':
			if end := strings.IndexByte(source[i+1:], '\n'); end >= 0 {
				i += end
			} else {
				return false
			}
		case '/':
			if i+1 < len(source) && source[i+1] == '*' {
				end := strings.Index(source[i+2:], "*/")
				if end < 0 {
					return false
				}
				i += end + 3
			}
		case '-':
			if i+2 < len(source) && source[i+1] == '-' && isDigestHintSpace(source[i+2]) {
				end := strings.IndexByte(source[i+3:], '\n')
				if end >= 0 {
					i += end + 2
				} else {
					return false
				}
			}
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return false
			}
		case ',':
			if depth == 1 {
				return true
			}
		}
	}
	return false
}

func isDigestRewriteWordStart(ch byte) bool {
	return ch == '_' || ch == '$' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

func isDigestRewriteWordPart(ch byte) bool {
	return isDigestRewriteWordStart(ch) || ch >= '0' && ch <= '9'
}

func isDigestValueToken(typ int, tokens []digestToken) bool {
	switch typ {
	case STRING, INTEGRAL, FLOAT, DECIMAL_VALUE, HEXNUM, BIT_LITERAL:
		return true
	case NULL:
		return !digestNullIsKeyword(tokens)
	default:
		return false
	}
}

func digestNullIsKeyword(tokens []digestToken) bool {
	n := len(tokens)
	if (n > 0 && tokens[n-1].text == "IS") ||
		(n > 1 && tokens[n-2].text == "IS" && tokens[n-1].text == "NOT") {
		return true
	}
	if n > 1 && tokens[n-2].text == "FILL" && tokens[n-1].text == "(" {
		return true
	}
	if n > 0 && tokens[n-1].text == "SET" {
		for i := n - 2; i >= 0 && i >= n-5; i-- {
			if tokens[i].text == "DELETE" || tokens[i].text == "UPDATE" {
				return i > 0 && tokens[i-1].text == "ON"
			}
		}
	}
	return digestNullIsColumnAttribute(tokens)
}

func digestNullIsColumnAttribute(tokens []digestToken) bool {
	if !digestIsTableDDL(tokens) {
		return false
	}
	foundType := false
	for i := len(tokens) - 1; i >= 0; i-- {
		token := tokens[i]
		if token.kind == digestTokenPlain && (token.text == "," || token.text == "(") {
			break
		}
		switch token.text {
		case "DEFAULT", "CHECK", "AS", "GENERATED", "REFERENCES":
			return false
		}
		if digestIsColumnTypeToken(token.text) {
			foundType = true
		}
	}
	return foundType
}

func digestIsTableDDL(tokens []digestToken) bool {
	if len(tokens) < 2 || tokens[0].text != "CREATE" && tokens[0].text != "ALTER" {
		return false
	}
	for i := 1; i < len(tokens) && i < 6; i++ {
		if tokens[i].text == "TABLE" {
			return true
		}
	}
	return false
}

func digestIsColumnTypeToken(text string) bool {
	switch text {
	case "ARRAY", "INT8", "BINARY", "BIT", "BLOB", "BOOL", "BOOLEAN", "CHARACTER", "DATALINK",
		"DATE", "DATETIME", "DECIMAL", "ENUM", "FLOAT4", "FLOAT8", "GEOGRAPHY", "GEOGRAPHY32",
		"GEOMETRY", "GEOMETRY32", "GEOMETRYCOLLECTION", "GEOMETRYCOLLECTION32", "INTEGER", "JSON",
		"LINESTRING", "LINESTRING32", "LONG", "LONGBLOB", "LONGTEXT", "MEDIUMBLOB", "MIDDLEINT",
		"MEDIUMTEXT", "MULTILINESTRING", "MULTILINESTRING32", "MULTIPOINT", "MULTIPOINT32",
		"MULTIPOLYGON", "MULTIPOLYGON32", "NUMERIC", "POINT", "POINT32", "POLYGON", "POLYGON32",
		"REAL", "SET", "SMALLINT", "STRING", "TEXT", "TIME", "TIMESTAMP", "TINYBLOB", "TINYINT",
		"TINYTEXT", "UUID", "VARBINARY", "VARCHARACTER", "VECBF16", "VECF16", "VECF32", "VECF64",
		"VECINT8", "VECUINT8", "YEAR":
		return true
	default:
		return false
	}
}

func canReduceDigestValue(tokens []digestToken) bool {
	n := len(tokens)
	return n >= 2 && tokens[n-1].text == "," &&
		(tokens[n-2].kind == digestTokenValue || tokens[n-2].kind == digestTokenValueList)
}

func appendDigestValue(tokens *[]digestToken) int {
	t := *tokens
	n := len(t)
	if canReduceDigestValue(t) {
		oldSize := t[n-2].size + t[n-1].size
		t[n-2] = digestToken{kind: digestTokenValueList, text: "?, ...", size: 2}
		*tokens = t[:n-1]
		return 2 - oldSize
	}
	*tokens = append(t, digestToken{kind: digestTokenValue, text: "?", size: 2})
	return 2
}

func canReduceDigestGroup(tokens []digestToken) bool {
	n := len(tokens)
	return n >= 2 && tokens[n-2].text == "(" &&
		(tokens[n-1].kind == digestTokenValue || tokens[n-1].kind == digestTokenValueList)
}

func closeDigestGroup(tokens *[]digestToken) int {
	t := *tokens
	n := len(t)
	delta := 2
	if canReduceDigestGroup(t) {
		delta -= t[n-2].size + t[n-1].size
		row := digestToken{kind: digestTokenRowSingle, text: "(?)", size: 2}
		if t[n-1].kind == digestTokenValueList {
			row.kind = digestTokenRowMultiple
			row.text = "(...)"
		}
		t = append(t[:n-2], row)
	} else {
		t = append(t, digestToken{kind: digestTokenPlain, text: ")", size: 2})
		*tokens = t
		return delta
	}

	n = len(t)
	if n >= 2 && t[n-2].text == "IN" {
		delta += 2 - t[n-2].size - t[n-1].size
		t[n-2] = digestToken{kind: digestTokenPlain, text: "IN (...)", size: 2}
		t = t[:n-1]
	} else if n >= 3 && t[n-2].text == "," {
		var list digestToken
		switch {
		case (t[n-3].kind == digestTokenRowSingle || t[n-3].kind == digestTokenRowSingleList) &&
			t[n-1].kind == digestTokenRowSingle:
			list = digestToken{kind: digestTokenRowSingleList, text: "(?) /* , ... */", size: 2}
		case (t[n-3].kind == digestTokenRowMultiple || t[n-3].kind == digestTokenRowMultipleList) &&
			t[n-1].kind == digestTokenRowMultiple:
			list = digestToken{kind: digestTokenRowMultipleList, text: "(...) /* , ... */", size: 2}
		}
		if list.text != "" {
			delta += list.size - t[n-3].size - t[n-2].size - t[n-1].size
			t[n-3] = list
			t = t[:n-2]
		}
	}
	*tokens = t
	return delta
}

func removeUnarySigns(tokens *[]digestToken) int {
	removed := 0
	for {
		t := *tokens
		n := len(t)
		if n == 0 || (t[n-1].text != "+" && t[n-1].text != "-") {
			return removed
		}
		if n < 2 || !digestTokenStartsExpression(t[n-2]) {
			return removed
		}
		removed += t[n-1].size
		*tokens = t[:n-1]
	}
}

func digestTokenStartsExpression(token digestToken) bool {
	switch token.text {
	case "(", ",", "EVERY", "@", "STARTS", "ENDS", "DEFAULT", "RETURN", "IF", "ELSEIF",
		"CASE", "WHEN", "WHILE", "UNTIL", "SELECT", "OR", "||", "XOR", "AND", "&&", "NOT",
		"BETWEEN", "LIKE", "RLIKE", "|", "&", "<<", ">>", "+", "-", "INTERVAL", "*", "/",
		"%", "DIV", "MOD", "^":
		return true
	default:
		return false
	}
}

func digestHasCharsetIntroducer(source string) bool {
	trimmed := strings.TrimSpace(source)
	const introducer = "_utf8mb4"
	return len(trimmed) > len(introducer) &&
		strings.EqualFold(trimmed[:len(introducer)], introducer) &&
		trimmed[len(introducer)] == '\''
}

func digestIsCharsetIntroducer(sql string, pos int, ansiQuotes bool) bool {
	pos = skipDigestWhitespaceAndComments(sql, pos)
	if pos >= len(sql) {
		return false
	}
	if sql[pos] == '\'' || sql[pos] == '"' && !ansiQuotes {
		return true
	}
	return pos+1 < len(sql) && (sql[pos] == 'x' || sql[pos] == 'X' || sql[pos] == 'b' || sql[pos] == 'B') && sql[pos+1] == '\''
}

func isDigestCharsetName(name string) bool {
	switch strings.ToLower(name) {
	case "_armscii8", "_ascii", "_big5", "_binary", "_cp1250", "_cp1251", "_cp1256", "_cp1257",
		"_cp850", "_cp852", "_cp866", "_cp932", "_dec8", "_eucjpms", "_euckr", "_gb18030",
		"_gb2312", "_gbk", "_geostd8", "_greek", "_hebrew", "_hp8", "_keybcs2", "_koi8r",
		"_koi8u", "_latin1", "_latin2", "_latin5", "_latin7", "_macce", "_macroman", "_sjis",
		"_swe7", "_tis620", "_ucs2", "_ujis", "_utf16", "_utf16le", "_utf32", "_utf8",
		"_utf8mb3", "_utf8mb4":
		return true
	default:
		return false
	}
}

func skipDigestWhitespaceAndComments(sql string, pos int) int {
	for pos < len(sql) {
		switch sql[pos] {
		case ' ', '\t', '\r', '\n':
			pos++
			continue
		case '#':
			if newline := strings.IndexByte(sql[pos+1:], '\n'); newline >= 0 {
				pos += newline + 2
				continue
			}
			return len(sql)
		case '/':
			if pos+1 < len(sql) && sql[pos+1] == '*' {
				if end := strings.Index(sql[pos+2:], "*/"); end >= 0 {
					pos += end + 4
					continue
				}
				return len(sql)
			}
		case '-':
			if pos+2 < len(sql) && sql[pos+1] == '-' && isDigestHintSpace(sql[pos+2]) {
				if newline := strings.IndexByte(sql[pos+3:], '\n'); newline >= 0 {
					pos += newline + 4
					continue
				}
				return len(sql)
			}
		}
		return pos
	}
	return pos
}

func digestCanStartOptimizerHint(tokens []digestToken) bool {
	if len(tokens) == 0 {
		return false
	}
	switch tokens[len(tokens)-1].text {
	case "SELECT", "INSERT", "UPDATE", "DELETE", "REPLACE":
		return true
	default:
		return false
	}
}

func digestOptimizerHintTokens(source string, allowed, ansiQuotes bool) []digestToken {
	if !allowed {
		return nil
	}

	// MySQL recognizes one optimizer-hint comment only when it is the first
	// non-whitespace text after the statement keyword. A normal comment before
	// it, or a second /*+ ... */ comment, is ignored by digest normalization.
	source = strings.TrimLeft(source, " \t\r\n")
	if !strings.HasPrefix(source, "/*+") {
		return nil
	}
	endOffset := strings.Index(source[3:], "*/")
	if endOffset < 0 {
		return nil
	}
	end := 3 + endOffset
	body := normalizeDigestOptimizerHint(source[3:end], ansiQuotes)
	if len(body) == 0 {
		return nil
	}
	result := []digestToken{{kind: digestTokenPlain, text: "/*+", size: 2}}
	result = append(result, body...)
	result = append(result, digestToken{kind: digestTokenPlain, text: "*/", size: 2})
	return result
}

func skipDigestQuoted(source string, start int, delimiter byte) (int, bool) {
	for i := start + 1; i < len(source); i++ {
		if source[i] != delimiter {
			continue
		}
		if i+1 < len(source) && source[i+1] == delimiter {
			i++
			continue
		}
		return i + 1, true
	}
	return len(source), false
}

func normalizeDigestOptimizerHint(source string, ansiQuotes bool) []digestToken {
	tokens := make([]digestToken, 0, 8)
	depth := 0
	for i := 0; i < len(source); {
		if isDigestHintSpace(source[i]) {
			i++
			continue
		}

		ch := source[i]
		switch {
		case ch == '\'' || ch == '"':
			end, closed := skipDigestQuoted(source, i, ch)
			if !closed {
				return tokens
			}
			if ch == '"' && ansiQuotes {
				value := source[i+1 : end-1]
				value = strings.ReplaceAll(value, `""`, `"`)
				tokens = append(tokens, digestToken{kind: digestTokenPlain, text: "`" + value + "`", size: 4 + len(value)})
			} else {
				appendDigestValue(&tokens)
			}
			i = end
		case ch == '`':
			end, closed := skipDigestQuoted(source, i, ch)
			if !closed {
				return tokens
			}
			value := source[i+1 : end-1]
			value = strings.ReplaceAll(value, "``", "`")
			tokens = append(tokens, digestToken{kind: digestTokenPlain, text: "`" + value + "`", size: 4 + len(value)})
			i = end
		case isDigestHintDigit(ch):
			start := i
			var kind digestHintNumberKind
			i, kind = scanDigestHintNumber(source, i)
			switch kind {
			case digestHintNumberIdentifier:
				value := source[start:i]
				tokens = append(tokens, digestToken{kind: digestTokenPlain, text: "`" + value + "`", size: 4 + len(value)})
			case digestHintNumberValue:
				removeUnarySigns(&tokens)
				appendDigestValue(&tokens)
			}
		case ch == '@':
			i++
			if i < len(source) && isDigestHintIdentifierStart(source[i]) {
				start := i
				for i < len(source) && isDigestHintIdentifierPart(source[i]) {
					i++
				}
				value := source[start:i]
				tokens = append(tokens, digestToken{kind: digestTokenPlain, text: "@`" + value + "`", size: 4 + len(value)})
			} else if i < len(source) && (source[i] == '`' || source[i] == '"' && ansiQuotes) {
				delimiter := source[i]
				end, closed := skipDigestQuoted(source, i, delimiter)
				if !closed {
					return tokens
				}
				value := source[i+1 : end-1]
				value = strings.ReplaceAll(value, string([]byte{delimiter, delimiter}), string(delimiter))
				tokens = append(tokens, digestToken{kind: digestTokenPlain, text: "@`" + value + "`", size: 4 + len(value)})
				i = end
			}
		case isDigestHintIdentifierStart(ch):
			start := i
			for i < len(source) && isDigestHintIdentifierPart(source[i]) {
				i++
			}
			value := source[start:i]
			if i < len(source) && source[i] == '@' {
				queryBlockStart := i + 1
				queryBlockEnd := queryBlockStart
				for queryBlockEnd < len(source) && isDigestHintIdentifierPart(source[queryBlockEnd]) {
					queryBlockEnd++
				}
				if queryBlockEnd > queryBlockStart {
					queryBlock := source[queryBlockStart:queryBlockEnd]
					tokens = append(tokens, digestToken{kind: digestTokenPlain, text: "`" + value + "`@`" + queryBlock + "`", size: 8 + len(value) + len(queryBlock)})
					i = queryBlockEnd
					continue
				}
			}
			if keyword, ok := digestHintKeyword(value); depth == 0 && ok {
				tokens = append(tokens, digestToken{kind: digestTokenPlain, text: keyword, size: 2})
			} else {
				tokens = append(tokens, digestToken{kind: digestTokenPlain, text: "`" + value + "`", size: 4 + len(value)})
			}
		case ch == '(':
			tokens = append(tokens, digestToken{kind: digestTokenPlain, text: "(", size: 2})
			depth++
			i++
		case ch == ')':
			closeDigestGroup(&tokens)
			if depth > 0 {
				depth--
			}
			i++
		case ch == '.':
			var kind digestHintNumberKind
			i, kind = scanDigestHintNumber(source, i)
			if kind == digestHintNumberValue {
				removeUnarySigns(&tokens)
				appendDigestValue(&tokens)
			}
		default:
			tokens = append(tokens, digestToken{kind: digestTokenPlain, text: string(ch), size: 2})
			i++
		}
	}
	return tokens
}

func isDigestHintSpace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n'
}

func isDigestHintDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isDigestHintIdentifierStart(ch byte) bool {
	return ch == '_' || ch == '$' || ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z'
}

func isDigestHintIdentifierPart(ch byte) bool {
	return isDigestHintIdentifierStart(ch) || isDigestHintDigit(ch)
}

type digestHintNumberKind uint8

const (
	digestHintNumberError digestHintNumberKind = iota
	digestHintNumberValue
	digestHintNumberIdentifier
)

func scanDigestHintNumber(source string, start int) (int, digestHintNumberKind) {
	i := start
	if source[i] == '.' {
		i++
		if i >= len(source) || !isDigestHintDigit(source[i]) {
			return i, digestHintNumberError
		}
		for i < len(source) && isDigestHintDigit(source[i]) {
			i++
		}
		if i < len(source) && isDigestHintIdentifierStart(source[i]) {
			return i, digestHintNumberError
		}
		return i, digestHintNumberValue
	}

	for i < len(source) && isDigestHintDigit(source[i]) {
		i++
	}
	if i < len(source) && source[i] == '.' {
		i++
		if i >= len(source) || !isDigestHintDigit(source[i]) {
			return i, digestHintNumberError
		}
		for i < len(source) && isDigestHintDigit(source[i]) {
			i++
		}
		if i < len(source) && isDigestHintIdentifierStart(source[i]) {
			return i, digestHintNumberError
		}
		return i, digestHintNumberValue
	}
	if i < len(source) && isDigestHintIdentifierStart(source[i]) {
		identifierStart := i
		i++
		for i < len(source) && isDigestHintIdentifierPart(source[i]) {
			i++
		}
		if i == identifierStart+1 && (source[identifierStart] == 'K' || source[identifierStart] == 'M' || source[identifierStart] == 'G') {
			return i, digestHintNumberValue
		}
		return i, digestHintNumberIdentifier
	}
	return i, digestHintNumberValue
}

func digestHintKeyword(value string) (string, bool) {
	upper := strings.ToUpper(value)
	switch upper {
	case "BKA", "BNL", "DUPSWEEDOUT", "FIRSTMATCH", "INTOEXISTS", "LOOSESCAN", "MATERIALIZATION",
		"MAX_EXECUTION_TIME", "MRR", "NO_BKA", "NO_BNL", "NO_ICP", "NO_MRR", "NO_RANGE_OPTIMIZATION",
		"NO_SEMIJOIN", "QB_NAME", "SEMIJOIN", "SET_VAR", "SUBQUERY", "MERGE", "NO_MERGE", "JOIN_PREFIX",
		"JOIN_SUFFIX", "JOIN_ORDER", "JOIN_FIXED_ORDER", "INDEX_MERGE", "NO_INDEX_MERGE", "RESOURCE_GROUP",
		"SKIP_SCAN", "NO_SKIP_SCAN", "HASH_JOIN", "NO_HASH_JOIN", "INDEX", "NO_INDEX", "JOIN_INDEX",
		"NO_JOIN_INDEX", "GROUP_INDEX", "NO_GROUP_INDEX", "ORDER_INDEX", "NO_ORDER_INDEX",
		"DERIVED_CONDITION_PUSHDOWN", "NO_DERIVED_CONDITION_PUSHDOWN":
		return upper, true
	default:
		return "", false
	}
}

func digestTokenText(typ int, value, source string) string {
	switch typ {
	case ID:
		if strings.EqualFold(value, "dual") {
			return "DUAL"
		}
		if strings.EqualFold(value, "sounds") {
			return "SOUNDS"
		}
		return "`" + value + "`"
	case QUOTE_ID:
		return "`" + value + "`"
	case AT_ID:
		if strings.HasPrefix(source, "@`") {
			return "@`" + value + "`"
		}
		return "@?"
	case AT_AT_ID:
		if scope, name, ok := strings.Cut(value, "."); ok {
			return "@@" + strings.ToUpper(scope) + " . `" + name + "`"
		}
		return "@@`" + value + "`"
	case UNDERSCORE_BINARY:
		return "(_charset)"
	case AND:
		if source == "&&" {
			return "&&"
		}
	case OR:
		if source == "||" {
			return "||"
		}
	case NE:
		return "!="
	case REGEXP:
		return "RLIKE"
	case LE:
		return "<="
	case GE:
		return ">="
	case NULL_SAFE_EQUAL:
		return "<=>"
	case SHIFT_LEFT:
		return "<<"
	case SHIFT_RIGHT:
		return ">>"
	case ASSIGNMENT:
		return ":="
	case TYPECAST:
		return "::"
	case ARROW:
		return "->"
	case LONG_ARROW:
		return "->>"
	case PIPE_CONCAT:
		return "||"
	case INT, INTEGER, INT4:
		return "INTEGER"
	case INT1:
		return "TINYINT"
	case INT2:
		return "SMALLINT"
	case INT3, MEDIUMINT:
		return "MIDDLEINT"
	case BIGINT:
		return "INT8"
	case FLOAT_TYPE:
		return "FLOAT4"
	case DOUBLE:
		return "FLOAT8"
	case CHAR:
		return "CHARACTER"
	case VARCHAR:
		return "VARCHARACTER"
	case ANY:
		return "SOME"
	case DATABASE:
		return "SCHEMA"
	case DATABASES:
		return "SCHEMAS"
	case DESCRIBE:
		return "EXPLAIN"
	case COLUMNS:
		return "FIELDS"
	case DISTINCT:
		return "DISTINCTROW"
	case CURRENT_TIMESTAMP, LOCALTIME, LOCALTIMESTAMP:
		return "NOW"
	case QUARTER:
		return "SQL_TSI_QUARTER"
	case MONTH:
		return "SQL_TSI_MONTH"
	case DAY:
		return "SQL_TSI_DAY"
	case HOUR:
		return "SQL_TSI_HOUR"
	case MINUTE:
		return "SQL_TSI_MINUTE"
	case SECOND:
		return "SQL_TSI_SECOND"
	}
	if typ > 0 && typ < 256 {
		return string(rune(typ))
	}
	return strings.ToUpper(value)
}
