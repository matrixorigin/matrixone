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

package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestRewriteInformationSchemaConstraintJoin(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want int
	}{
		{
			name: "schema omitted",
			sql: `select A.table_name
from information_schema.key_column_usage A
join information_schema.table_constraints B using (constraint_name, table_name)
join information_schema.referential_constraints R
  on R.constraint_name = B.constraint_name`,
			want: 3,
		},
		{
			name: "already schema qualified",
			sql: `select A.table_name
from information_schema.key_column_usage A
join information_schema.table_constraints B
  using (constraint_schema, constraint_name, table_name)`,
			want: 3,
		},
		{
			name: "ordinary user tables",
			sql:  `select * from app.key_column_usage A join app.table_constraints B using (constraint_name, table_name)`,
			want: 2,
		},
		{
			name: "different aliases",
			sql:  `select * from information_schema.key_column_usage K join information_schema.table_constraints T using (constraint_name, table_name)`,
			want: 3,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, test.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			selectStmt := stmt.(*tree.Select)
			rewriteMySQLMetadataCompatibility(selectStmt)
			join := firstUsingJoin(selectStmt.Select.(*tree.SelectClause).From.Tables[0])
			require.NotNil(t, join)
			columns := join.Cond.(*tree.UsingJoinCond).Cols
			require.Len(t, columns, test.want)
			if test.want == 3 {
				require.Equal(t, tree.Identifier("constraint_schema"), columns[0])
			}
		})
	}
}

func firstUsingJoin(table tree.TableExpr) *tree.JoinTableExpr {
	join, ok := table.(*tree.JoinTableExpr)
	if !ok {
		return nil
	}
	if nested := firstUsingJoin(join.Left); nested != nil {
		return nested
	}
	if _, ok := join.Cond.(*tree.UsingJoinCond); ok {
		return join
	}
	return firstUsingJoin(join.Right)
}
