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

// Package sqlquote provides the single, shared SQL identifier-quoting and
// string-literal-escaping helpers used when index plugins (CAGRA / IVF-PQ /
// HNSW / IVF-FLAT / fulltext) and the cuVS CDC/idxcron helpers assemble SQL by
// string interpolation. Routing every such site through here prevents broken
// or injectable SQL when an identifier contains a backtick (e.g. a column named
// `a`b`) or a string literal (e.g. a JSON params blob) contains a single quote.
package sqlquote

import "strings"

// Ident quotes an SQL identifier (database / table / column / alias) with
// backticks, doubling any embedded backtick so the identifier is preserved
// verbatim. e.g. Ident("a`b") == "`a“b`".
//
// Use it everywhere an identifier is interpolated, instead of hand-writing
// "`" + name + "`" (which breaks the moment name contains a backtick).
func Ident(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

// QualifiedIdent quotes each non-empty part and joins them with ".", producing
// e.g. "`db`.`tbl`" or "`src`.`col`". Empty parts are skipped.
func QualifiedIdent(parts ...string) string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p == "" {
			continue
		}
		out = append(out, Ident(p))
	}
	return strings.Join(out, ".")
}

// EscapeString escapes s for embedding inside a single-quoted SQL string
// literal. It escapes BOTH backslash and single quote, because MatrixOne's MySQL
// scanner treats backslash as an escape introducer inside single-quoted literals
// (handleEscape in pkg/sql/parsers/dialect/mysql/scanner.go interprets \n, \t,
// \0, \\, \', ... and a trailing backslash even swallows the closing quote). So
// doubling single quotes alone is NOT enough: a literal backslash would corrupt
// the value (e.g. 'C:\Program' reads back as 'C:Program') or break the literal.
// Each backslash is doubled and each single quote is doubled. The caller supplies
// the surrounding quotes, e.g.
//
//	fmt.Sprintf("... '%s' ...", sqlquote.EscapeString(jsonParams))
//
// This is the escape needed for JSON params / config blobs and any value that
// may contain single quotes or backslashes inside string values.
func EscapeString(s string) string {
	// Backslash first so it does not re-escape anything introduced below; then
	// double single quotes. Both target disjoint bytes, but backslash-first is
	// the conventional, future-proof order.
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "''")
	return s
}

// String wraps s in single quotes after escaping, i.e. a ready-to-interpolate
// SQL string literal: String("a'b") == "'a”b'", String(`a\b`) == `'a\\b'`.
func String(s string) string {
	return "'" + EscapeString(s) + "'"
}
