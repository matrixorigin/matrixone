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

package plan

import (
	"context"
	"reflect"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestTableDumpParserModesForOriginPreserveEveryParse(t *testing.T) {
	tests := []struct {
		name      string
		origin    string
		wantModes int
	}{
		{name: "ordinary", origin: "concat('a','b')", wantModes: 1},
		{name: "backslash escape", origin: `concat('a\n','b')`, wantModes: 2},
		{name: "double quoted token", origin: `concat("a",'b')`, wantModes: 2},
		{name: "pipes operator", origin: "1 || 2", wantModes: 2},
		{name: "all mode-sensitive syntax", origin: `concat("a\n",'b') || 1`, wantModes: 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selected := tableDumpParserModesForOrigin(tt.origin)
			require.Len(t, selected, tt.wantModes)

			parse := func(modes []string) ([]tree.Expr, []tree.Statement) {
				t.Helper()
				expressions := make([]tree.Expr, 0, len(modes))
				statements := make([]tree.Statement, 0, len(modes))
				for _, mode := range modes {
					stmt, expr, _, err := parseTableDumpExpression(context.Background(), tt.origin, mode)
					require.NoError(t, err, "mode=%q", mode)
					expressions = append(expressions, expr)
					statements = append(statements, stmt)
				}
				return expressions, statements
			}

			allExpressions, allStatements := parse(tableDumpParserModes())
			selectedExpressions, selectedStatements := parse(selected)
			defer func() {
				for _, stmt := range allStatements {
					stmt.Free()
				}
				for _, stmt := range selectedStatements {
					stmt.Free()
				}
			}()

			for modeIndex, expression := range allExpressions {
				foundEquivalent := false
				for _, candidate := range selectedExpressions {
					if reflect.DeepEqual(expression, candidate) {
						foundEquivalent = true
						break
					}
				}
				require.True(t, foundEquivalent,
					"selected modes must retain the AST from full mode %q", tableDumpParserModes()[modeIndex])
			}
		})
	}
}
