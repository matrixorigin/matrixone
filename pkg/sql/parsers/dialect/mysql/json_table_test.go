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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// These forms must reach the planner before consumer regressions can run.
func TestJSONTableConsumerSyntax(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"constant_text", `SELECT jt.n FROM JSON_TABLE('[1,2]', '$[*]' COLUMNS(n INT PATH '$')) AS jt ORDER BY jt.n`},
		{"correlated_text", `SELECT t.id, jt.n FROM jt_input AS t, JSON_TABLE(t.doc, '$[*]' COLUMNS(n INT PATH '$')) AS jt ORDER BY t.id, jt.n`},
		{"stage_datalink", `SELECT jt.n FROM JSON_TABLE(CAST('stage://jt_stage/input.json' AS DATALINK), '$[*]' COLUMNS(n INT PATH '$')) AS jt ORDER BY jt.n`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), tc.sql, 1)
			if err != nil {
				t.Fatalf("required JSON_TABLE SQL consumer rejected by parser: %v", err)
			}
			stmt.Free()
		})
	}
}

func TestJSONTableFormatRoundTripSQLModes(t *testing.T) {
	for _, tc := range []struct{ mode, sql string }{
		{"", `SELECT * FROM JSON_TABLE('{}', '$."a\\\\b''c"' COLUMNS(v VARCHAR(20) PATH '$."a\\\\b''c"' DEFAULT '"a\\\\b''c"' ON EMPTY)) jt`},
		{"NO_BACKSLASH_ESCAPES,ANSI_QUOTES", `SELECT * FROM JSON_TABLE('{}', '$."a\\b''c"' COLUMNS(v VARCHAR(20) PATH '$."a\\b''c"' DEFAULT '"a\\b''c"' ON EMPTY)) jt`},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			stmt, err := ParseOneWithSQLMode(context.Background(), tc.sql, 1, tc.mode)
			if err != nil {
				t.Fatal(err)
			}
			defer stmt.Free()
			getSpec := func(stmt tree.Statement) *tree.JSONTable {
				table := stmt.(*tree.Select).Select.(*tree.SelectClause).From.Tables[0]
				for {
					switch node := table.(type) {
					case *tree.JoinTableExpr:
						table = node.Left
					case *tree.AliasedTableExpr:
						table = node.Expr
					case *tree.TableFunction:
						return node.JSONTable
					default:
						t.Fatalf("unexpected FROM shape %T", node)
						return nil
					}
				}
			}
			original := getSpec(stmt)
			if original.Path != `$."a\\b'c"` || original.Columns[0].OnEmpty.Default != `"a\\b'c"` {
				t.Fatalf("literal bytes differ from independent oracle: %#v", original)
			}
			for _, independent := range []bool{false, true} {
				var opts []tree.FmtCtxOption
				if strings.Contains(tc.mode, "NO_BACKSLASH_ESCAPES") {
					opts = append(opts, tree.WithNoBackslashEscape())
				}
				if independent {
					opts = append(opts, tree.WithModeIndependentStringLiterals())
				}
				formatted := tree.StringWithOpts(stmt, dialect.MYSQL, opts...)
				roundTrip, err := ParseOneWithSQLMode(context.Background(), formatted, 1, tc.mode)
				if err != nil {
					t.Fatalf("reparse %q: %v", formatted, err)
				}
				reparsed := getSpec(roundTrip)
				before, after := original.Columns[0], reparsed.Columns[0]
				if original.Path != reparsed.Path || before.Path != after.Path || before.Name != after.Name ||
					before.Kind != after.Kind || before.OnEmpty.Action != after.OnEmpty.Action ||
					before.OnEmpty.Default != after.OnEmpty.Default ||
					before.Type.InternalType.Oid != after.Type.InternalType.Oid ||
					before.Type.InternalType.Width != after.Type.InternalType.Width {
					roundTrip.Free()
					t.Fatalf("path/default/type bytes changed after format: %q", formatted)
				}
				if again := tree.StringWithOpts(roundTrip, dialect.MYSQL, opts...); again != formatted {
					roundTrip.Free()
					t.Fatalf("non-idempotent format: %q != %q", again, formatted)
				}
				roundTrip.Free()
			}
		})
	}
}

func TestJSONTableRejectsDuplicatePolicies(t *testing.T) {
	for _, sql := range []string{
		`SELECT * FROM JSON_TABLE('[]','$[*]' COLUMNS(n INT PATH '$' NULL ON EMPTY ERROR ON EMPTY)) jt`,
		`SELECT * FROM JSON_TABLE('[]','$[*]' COLUMNS(n INT PATH '$' NULL ON ERROR ERROR ON ERROR)) jt`,
		`SELECT * FROM JSON_TABLE('[]',CAST('$[*]' AS VARCHAR) COLUMNS(n INT PATH '$')) jt`,
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		if stmt != nil {
			stmt.Free()
		}
		if err == nil {
			t.Fatalf("unsupported grammar accepted: %s", sql)
		}
	}
}
