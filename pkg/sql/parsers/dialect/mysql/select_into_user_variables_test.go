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
		sql     string
		wantSQL string
		vars    []string
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
			sql:     "select v from uv_src order by id desc limit 1 into @last_v",
			wantSQL: "select v from uv_src order by id desc limit 1 into @last_v",
			vars:    []string{"last_v"},
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
			require.Equal(t, tree.OUTPUT_STATUS, selectStatement.StmtKind().OutputType())
			require.Equal(t, testCase.wantSQL, tree.String(statement, dialect.MYSQL))
		})
	}
}
