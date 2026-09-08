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

func TestIssue28372UnparenthesizedTableForms(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		check func(t *testing.T, stmt tree.Statement)
	}{
		{
			name: "query with order and limit",
			sql:  "table src order by a desc limit 1",
			check: func(t *testing.T, stmt tree.Statement) {
				selectStmt, ok := stmt.(*tree.Select)
				require.True(t, ok)
				require.NotNil(t, selectStmt.OrderBy)
				require.NotNil(t, selectStmt.Limit)
				require.Contains(t, tree.String(stmt, dialect.MYSQL), "select * from src")
			},
		},
		{
			name: "query term union",
			sql:  "table src union table src2",
			check: func(t *testing.T, stmt tree.Statement) {
				selectStmt, ok := stmt.(*tree.Select)
				require.True(t, ok)
				_, ok = selectStmt.Select.(*tree.UnionClause)
				require.True(t, ok)
			},
		},
		{
			name: "ctas",
			sql:  "create table dst table src",
			check: func(t *testing.T, stmt tree.Statement) {
				createStmt, ok := stmt.(*tree.CreateTable)
				require.True(t, ok)
				require.True(t, createStmt.IsAsSelect)
				require.NotNil(t, createStmt.AsSource)
			},
		},
		{
			name: "ctas with target columns",
			sql:  "create table dst (extra int default 9) table src",
			check: func(t *testing.T, stmt tree.Statement) {
				createStmt, ok := stmt.(*tree.CreateTable)
				require.True(t, ok)
				require.True(t, createStmt.IsAsSelect)
				require.Len(t, createStmt.Defs, 1)
			},
		},
		{
			name: "insert",
			sql:  "insert into dst table src",
			check: func(t *testing.T, stmt tree.Statement) {
				insertStmt, ok := stmt.(*tree.Insert)
				require.True(t, ok)
				require.NotNil(t, insertStmt.Rows)
			},
		},
		{
			name: "view",
			sql:  "create view v as table src",
			check: func(t *testing.T, stmt tree.Statement) {
				viewStmt, ok := stmt.(*tree.CreateView)
				require.True(t, ok)
				require.NotNil(t, viewStmt.AsSource)
			},
		},
		{
			name: "prepared string",
			sql:  "prepare p from 'table src order by a desc limit 1'",
			check: func(t *testing.T, stmt tree.Statement) {
				_, ok := stmt.(*tree.PrepareString)
				require.True(t, ok)
			},
		},
		{
			name: "replace source remains valid",
			sql:  "replace into dst table src",
			check: func(t *testing.T, stmt tree.Statement) {
				replaceStmt, ok := stmt.(*tree.Replace)
				require.True(t, ok)
				require.NotNil(t, replaceStmt.Rows)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), test.sql, 1)
			require.NoError(t, err)
			require.NotNil(t, stmt)
			test.check(t, stmt)
		})
	}
}

func TestIssue28372ParenthesizedTableFormsRemainValid(t *testing.T) {
	for _, sql := range []string{
		"(table src) order by a desc limit 1",
		"create table dst (table src)",
		"create view v as (table src)",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err)
		})
	}
}

func TestIssue28372TableUnionIntoPlacement(t *testing.T) {
	stmt, err := ParseOne(context.Background(), "table src union select 1 into @v", 1)
	require.NoError(t, err)
	selectStmt, ok := stmt.(*tree.Select)
	require.True(t, ok)
	require.Len(t, selectStmt.IntoVars, 1)
	require.True(t, selectStmt.DeprecatedInto)

	_, err = ParseOne(context.Background(), "table src union select 1 into @v union select 2", 1)
	require.Error(t, err)
}
