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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestHasMatrixOneNativeSQLMode(t *testing.T) {
	tests := []struct {
		name string
		mode string
		want bool
	}{
		{
			name: "empty",
			mode: "",
			want: false,
		},
		{
			name: "exact token",
			mode: "MATRIXONE_NATIVE",
			want: true,
		},
		{
			name: "case insensitive with spaces",
			mode: " ansi_quotes , matrixone_native ",
			want: true,
		},
		{
			name: "suffix does not match",
			mode: "MATRIXONE_NATIVE_EXTRA",
			want: false,
		},
		{
			name: "substring does not match",
			mode: "NO_MATRIXONE_NATIVE",
			want: false,
		},
		{
			name: "other tokens only",
			mode: "ANSI_QUOTES,PIPES_AS_CONCAT",
			want: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := HasMatrixOneNativeSQLMode(test.mode); got != test.want {
				t.Fatalf("HasMatrixOneNativeSQLMode(%q) = %v, want %v", test.mode, got, test.want)
			}
		})
	}
}

func TestParserSQLModeCombinations(t *testing.T) {
	modes := ParserSQLModeCombinations()
	wantModes := 1 << len(parserSQLModeTokens)
	if len(modes) != wantModes {
		t.Fatalf("ParserSQLModeCombinations() returned %d modes, want %d", len(modes), wantModes)
	}
	if modes[0] != "" {
		t.Fatalf("first parser mode = %q, want default mode", modes[0])
	}

	seen := make(map[SQLModeFlags]string, len(modes))
	for _, mode := range modes {
		flags := ParseSQLModeFlags(mode)
		if previous, ok := seen[flags]; ok {
			t.Fatalf("modes %q and %q produce duplicate flags %d", previous, mode, flags)
		}
		seen[flags] = mode
	}
}

func TestParseSQLModeFlagsIgnoreSpace(t *testing.T) {
	if !ParseSQLModeFlags(sqlModeIgnoreSpace).Has(SQLModeIgnoreSpace) {
		t.Fatal("IGNORE_SPACE was not parsed")
	}
	if !ParseSQLModeFlags("ansi").Has(SQLModeIgnoreSpace) {
		t.Fatal("ANSI did not enable IGNORE_SPACE")
	}
}

func TestIgnoreSpaceFunctionParsing(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		mode        string
		wantGeneric bool
		wantErr     bool
	}{
		{name: "native function without whitespace", query: "select now()", mode: "STRICT_TRANS_TABLES"},
		{name: "spaced now is generic by default", query: "select now ()", mode: "STRICT_TRANS_TABLES", wantGeneric: true},
		{name: "spaced substring is generic by default", query: "select substring ('abcdef', 2, 3)", mode: "STRICT_TRANS_TABLES", wantGeneric: true},
		{name: "spaced sum is generic by default", query: "select sum (1)", mode: "STRICT_TRANS_TABLES", wantGeneric: true},
		{name: "spaced date add is generic by default", query: "select date_add ('2024-01-01', interval 1 day)", mode: "STRICT_TRANS_TABLES", wantGeneric: true},
		{name: "count star is rejected by default", query: "select count (*) from src", mode: "STRICT_TRANS_TABLES", wantErr: true},
		{name: "ignore space restores now", query: "select now ()", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE"},
		{name: "ignore space restores substring", query: "select substring ('abcdef', 2, 3)", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE"},
		{name: "ignore space restores sum", query: "select sum (1)", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE"},
		{name: "ignore space restores date add", query: "select date_add ('2024-01-01', interval 1 day)", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE"},
		{name: "ignore space restores count star", query: "select count (*) from src", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseOneWithSQLMode(context.Background(), test.query, 1, test.mode)
			if test.wantErr {
				if err == nil {
					if stmt != nil {
						stmt.Free()
					}
					t.Fatalf("ParseOneWithSQLMode(%q, %q) succeeded, want an error", test.query, test.mode)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseOneWithSQLMode(%q, %q) failed: %v", test.query, test.mode, err)
			}
			defer stmt.Free()

			selectStmt, ok := stmt.(*tree.Select)
			if !ok {
				t.Fatalf("parsed %q as %T, want *tree.Select", test.query, stmt)
			}
			selectClause, ok := selectStmt.Select.(*tree.SelectClause)
			if !ok || len(selectClause.Exprs) != 1 {
				t.Fatalf("parsed %q without one select expression", test.query)
			}
			fn, ok := selectClause.Exprs[0].Expr.(*tree.FuncExpr)
			if !ok {
				t.Fatalf("parsed %q expression as %T, want *tree.FuncExpr", test.query, selectClause.Exprs[0].Expr)
			}
			if fn.IsGeneric != test.wantGeneric {
				t.Fatalf("parsed %q IsGeneric = %v, want %v", test.query, fn.IsGeneric, test.wantGeneric)
			}
		})
	}
}

func TestIgnoreSpaceReservedFunctionIdentifiers(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		mode    string
		wantErr bool
	}{
		{name: "table name without whitespace is reserved by default", query: "create table count(i int)", mode: "STRICT_TRANS_TABLES", wantErr: true},
		{name: "table name with whitespace is allowed by default", query: "create table count (i int)", mode: "STRICT_TRANS_TABLES"},
		{name: "table name without whitespace is reserved", query: "create table count(i int)", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE", wantErr: true},
		{name: "table name with whitespace is reserved", query: "create table count (i int)", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE", wantErr: true},
		{name: "column name is allowed by default", query: "create table src(count int)", mode: "STRICT_TRANS_TABLES"},
		{name: "column name is reserved", query: "create table src(count int)", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE", wantErr: true},
		{name: "qualified sensitive identifier is allowed by default", query: "select src.count", mode: "STRICT_TRANS_TABLES"},
		{name: "qualified non-sensitive identifier remains valid", query: "select src.id", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE"},
		{name: "qualified sensitive identifier is reserved", query: "select src.count", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE", wantErr: true},
		{name: "quoted qualified sensitive identifier remains valid", query: "select src.`count`", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE"},
		{name: "quoted table name remains valid", query: "create table `count`(i int)", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE"},
		{name: "quoted column name remains valid", query: "create table src(`count` int)", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseOneWithSQLMode(context.Background(), test.query, 1, test.mode)
			if test.wantErr {
				if err == nil {
					if stmt != nil {
						stmt.Free()
					}
					t.Fatalf("ParseOneWithSQLMode(%q, %q) succeeded, want an error", test.query, test.mode)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseOneWithSQLMode(%q, %q) failed: %v", test.query, test.mode, err)
			}
			stmt.Free()
		})
	}
}

func TestIgnoreSpaceFunctionFormatting(t *testing.T) {
	tests := []struct {
		name  string
		query string
		mode  string
		want  string
	}{
		{name: "generic now", query: "select now ()", mode: "STRICT_TRANS_TABLES", want: "now ()"},
		{name: "generic trim string", query: "select trim (' x ')", mode: "STRICT_TRANS_TABLES", want: "trim (' x ')"},
		{name: "generic trim numeric", query: "select trim (0)", mode: "STRICT_TRANS_TABLES", want: "trim (0)"},
		{name: "native trim string", query: "select trim (' x ')", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE", want: "trim(' x ')"},
		{name: "native trim numeric", query: "select trim (0)", mode: "STRICT_TRANS_TABLES,IGNORE_SPACE", want: "trim(0)"},
		{name: "generic group concat", query: "select group_concat (1)", mode: "STRICT_TRANS_TABLES", want: "group_concat (1)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseOneWithSQLMode(context.Background(), test.query, 1, test.mode)
			if err != nil {
				t.Fatalf("ParseOneWithSQLMode(%q, %q) failed: %v", test.query, test.mode, err)
			}
			defer stmt.Free()

			selectStmt, ok := stmt.(*tree.Select)
			if !ok {
				t.Fatalf("parsed %q as %T, want *tree.Select", test.query, stmt)
			}
			selectClause, ok := selectStmt.Select.(*tree.SelectClause)
			if !ok || len(selectClause.Exprs) != 1 {
				t.Fatalf("parsed %q without one select expression", test.query)
			}
			got := tree.StringWithOpts(selectClause.Exprs[0].Expr, dialect.MYSQL, tree.WithSingleQuoteString())
			if got != test.want {
				t.Fatalf("formatted %q as %q, want %q", test.query, got, test.want)
			}
		})
	}
}

func TestIgnoreSpaceGenericFunctionFormatRoundTrip(t *testing.T) {
	for _, query := range []string{
		"select now ()",
		"select trim (' x ')",
		"select trim (0)",
		"select group_concat (1)",
	} {
		t.Run(query, func(t *testing.T) {
			stmt, err := ParseOneWithSQLMode(context.Background(), query, 1, "STRICT_TRANS_TABLES")
			if err != nil {
				t.Fatalf("ParseOneWithSQLMode(%q) failed: %v", query, err)
			}
			defer stmt.Free()

			formatted := tree.StringWithOpts(stmt, dialect.MYSQL, tree.WithSingleQuoteString())
			roundTripped, err := ParseOneWithSQLMode(context.Background(), formatted, 1, "STRICT_TRANS_TABLES")
			if err != nil {
				t.Fatalf("ParseOneWithSQLMode(%q) failed: %v", formatted, err)
			}
			defer roundTripped.Free()

			originalFn := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.FuncExpr)
			roundTrippedFn := roundTripped.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.FuncExpr)
			if !originalFn.IsGeneric || !roundTrippedFn.IsGeneric {
				t.Fatalf("generic shape was not preserved for %q: original=%v round-tripped=%v", query, originalFn.IsGeneric, roundTrippedFn.IsGeneric)
			}
			if got := tree.StringWithOpts(roundTripped, dialect.MYSQL, tree.WithSingleQuoteString()); got != formatted {
				t.Fatalf("format is not idempotent for %q: got %q, want %q", query, got, formatted)
			}
		})
	}
}
