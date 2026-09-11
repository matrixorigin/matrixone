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
	if len(modes) != 32 {
		t.Fatalf("ParserSQLModeCombinations() returned %d modes, want 32", len(modes))
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
