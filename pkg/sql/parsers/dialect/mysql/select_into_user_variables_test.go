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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestSelectIntoUserVariablesParse(t *testing.T) {
	testCases := []struct {
		sql            string
		wantSQL        string
		vars           []string
		deprecatedInto bool
	}{
		{
			sql:     "select abs(-5), 'ok' into @out, @label",
			wantSQL: "select abs(-5), ok into @out, @label",
			vars:    []string{"out", "label"},
		},
		{
			sql:     "select id, v from uv_src where id = 2 into @row_id, @row_v",
			wantSQL: "select id, v from uv_src where id = 2 into @row_id, @row_v",
			vars:    []string{"row_id", "row_v"},
		},
		{
			sql:     "select id into @pre_from_id from uv_src where id = 2",
			wantSQL: "select id into @pre_from_id from uv_src where id = 2",
			vars:    []string{"pre_from_id"},
		},
		{
			sql:     "select v from uv_src order by id desc limit 1 into @last_v",
			wantSQL: "select v from uv_src order by id desc limit 1 into @last_v",
			vars:    []string{"last_v"},
		},
		{
			sql:     "(select 3 into @paren_out)",
			wantSQL: "(select 3 into @paren_out)",
			vars:    []string{"paren_out"},
		},
		{
			sql:     "((select 4 into @nested_paren_out))",
			wantSQL: "((select 4 into @nested_paren_out))",
			vars:    []string{"nested_paren_out"},
		},
		{
			sql:            "select 1 union (select 1 into @union_out)",
			wantSQL:        "select 1 union (select 1 into @union_out)",
			vars:           []string{"union_out"},
			deprecatedInto: true,
		},
		{
			sql:            "select 1 union select 1 into @union_terminal",
			wantSQL:        "select 1 union select 1 into @union_terminal",
			vars:           []string{"union_terminal"},
			deprecatedInto: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.sql, func(t *testing.T) {
			statement, err := ParseOne(context.Background(), testCase.sql, 1)
			require.NoError(t, err)
			selectStatement, ok := statement.(*tree.Select)
			require.True(t, ok)
			require.Nil(t, selectStatement.Ep)
			require.Len(t, selectStatement.IntoVars, len(testCase.vars))
			for i, name := range testCase.vars {
				require.Equal(t, name, selectStatement.IntoVars[i].Name)
			}
			require.Equal(t, testCase.deprecatedInto, selectStatement.DeprecatedInto)
			require.Equal(t, tree.OUTPUT_STATUS, selectStatement.StmtKind().OutputType())
			require.Equal(t, testCase.wantSQL, tree.String(statement, dialect.MYSQL))
		})
	}
}

func TestSelectIntoUserVariablesRejectsMisplacedUnionInto(t *testing.T) {
	testCases := []string{
		"select 1 into @bad_non_last union select 1",
		"select 1 union (select 1 into @bad_middle) union select 1",
		"select 1 union (select 1 into @bad_middle) into @bad_terminal",
	}

	for _, testCase := range testCases {
		t.Run(testCase, func(t *testing.T) {
			_, err := ParseOne(context.Background(), testCase, 1)
			require.Error(t, err)
			require.Contains(t, err.Error(), tree.MisplacedIntoClauseMessage)
		})
	}
}
