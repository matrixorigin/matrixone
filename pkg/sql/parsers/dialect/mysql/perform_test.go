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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestParsePerform(t *testing.T) {
	for _, test := range []struct {
		input string
		want  string
	}{
		{input: "perform select 1", want: "perform select 1"},
		{input: "perform with cte as (select 1) select * from cte", want: "perform with cte as (select 1) select * from cte"},
		{input: "perform select 1 union all select 2 order by 1 limit 1", want: "perform select 1 union all select 2 order by 1 limit 1"},
	} {
		t.Run(test.input, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), test.input, 1)
			require.NoError(t, err)
			selectStmt, ok := stmt.(*tree.Select)
			require.True(t, ok)
			require.True(t, selectStmt.IsPerform)
			require.Equal(t, "Perform", selectStmt.GetStatementType())
			require.Equal(t, tree.QueryTypeDQL, selectStmt.GetQueryType())
			require.Equal(t, tree.OUTPUT_STATUS, selectStmt.StmtKind().OutputType())
			require.Equal(t, test.want, tree.String(stmt, dialect.MYSQL))
		})
	}
}

func TestPerformKeywordRemainsUsableAsIdentifier(t *testing.T) {
	for _, sql := range []string{
		"create table perform (perform int)",
		"select perform from perform",
		"select 1 as perform",
	} {
		stmt, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
		stmt.Free()
	}
}

func TestRejectUnsupportedPerformForms(t *testing.T) {
	for _, sql := range []string{
		"perform 1",
		"perform * from t",
		"perfrom select 1",
		"perform values row(1)",
		"perform table t",
		"perform (select 1)",
		"perform (select 1 into outfile 'result.csv')",
		"perform (values row(1))",
		"perform (table t)",
		"perform explain select 1",
		"perform insert into t values (1)",
		"perform",
	} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.Error(t, err, sql)
	}
}

func TestParsePerformSelectIntoOutfile(t *testing.T) {
	stmt, err := ParseOne(context.Background(), "perform select 1 into outfile 'result.csv'", 1)
	require.NoError(t, err)
	selectStmt, ok := stmt.(*tree.Select)
	require.True(t, ok)
	require.True(t, selectStmt.IsPerform)
	require.NotNil(t, selectStmt.Ep)
}
