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
	"github.com/stretchr/testify/require"
)

func TestParseIcebergCatalogStatements(t *testing.T) {
	ctx := context.Background()
	stmt, err := ParseOne(ctx, "create iceberg catalog if not exists ksa with ('type'='rest','uri'='https://catalog.example')", 1)
	require.NoError(t, err)
	require.IsType(t, &tree.CreateIcebergCatalog{}, stmt)
	require.Equal(t, tree.Identifier("ksa"), stmt.(*tree.CreateIcebergCatalog).Name)

	stmt, err = ParseOne(ctx, "alter iceberg catalog ksa set uri='https://new.example'", 1)
	require.NoError(t, err)
	require.IsType(t, &tree.AlterIcebergCatalog{}, stmt)

	stmt, err = ParseOne(ctx, "alter iceberg catalog ksa set ('uri'='https://new.example','token_secret'='secret://catalog/token')", 1)
	require.NoError(t, err)
	require.IsType(t, &tree.AlterIcebergCatalog{}, stmt)
	formattedAlter := tree.String(stmt, dialect.MYSQL)
	require.Contains(t, formattedAlter, "set (")
	require.NotContains(t, formattedAlter, "catalog/token")
	_, err = ParseOne(ctx, formattedAlter, 1)
	require.NoError(t, err)

	stmt, err = ParseOne(ctx, "drop iceberg catalog if exists ksa", 1)
	require.NoError(t, err)
	require.IsType(t, &tree.DropIcebergCatalog{}, stmt)
}

func TestParseShowIcebergStatements(t *testing.T) {
	ctx := context.Background()
	for _, sql := range []string{
		"show iceberg catalogs",
		"show iceberg namespaces from ksa",
		"show iceberg namespaces in catalog ksa",
		"show iceberg tables",
		"show iceberg tables from ksa",
		"show iceberg tables from ksa.sales",
		"show iceberg tables in catalog ksa",
		"show iceberg tables in namespace 'sales'",
		"show iceberg tables in namespace sales in catalog ksa",
	} {
		_, err := ParseOne(ctx, sql, 1)
		require.NoError(t, err, sql)
	}
}

func TestParseIcebergExternalTable(t *testing.T) {
	stmt, err := ParseOne(context.Background(), "create external table gold_orders (id int) engine = iceberg with ('catalog'='ksa','namespace'='sales','table'='orders','ref'='main','read_mode'='append_only','write_mode'='read_only')", 1)
	require.NoError(t, err)
	createTable := stmt.(*tree.CreateTable)
	require.NotNil(t, createTable.IcebergParam)
	require.Equal(t, "catalog", string(createTable.IcebergParam.Options[0].Key))
	require.Equal(t, "ksa", createTable.IcebergParam.Options[0].Val)

	stmt, err = ParseOne(context.Background(), "create external table gold_orders engine = iceberg with (catalog=ksa, namespace=sales, 'table'=orders, ref=main)", 1)
	require.NoError(t, err)
	createTable = stmt.(*tree.CreateTable)
	require.NotNil(t, createTable.IcebergParam)
	require.Empty(t, createTable.Defs)

	stmt, err = ParseOne(context.Background(), "drop table gold_orders", 1)
	require.NoError(t, err)
	require.IsType(t, &tree.DropTable{}, stmt)
}

func TestParseIcebergInsertOverwrite(t *testing.T) {
	ctx := context.Background()
	stmt, err := ParseOne(ctx, "insert overwrite gold_orders select 1", 1)
	require.NoError(t, err)
	insertStmt := stmt.(*tree.Insert)
	require.True(t, insertStmt.Overwrite)
	require.Equal(t, "insert overwrite gold_orders select 1", tree.String(stmt, dialect.MYSQL))

	stmt, err = ParseOne(ctx, "insert overwrite gold_orders partition(region = 'ksa', day = 20260624) select 1", 1)
	require.NoError(t, err)
	insertStmt = stmt.(*tree.Insert)
	require.True(t, insertStmt.Overwrite)
	require.Empty(t, insertStmt.PartitionNames)
	require.Len(t, insertStmt.PartitionValues, 2)
	require.Equal(t, tree.Identifier("region"), insertStmt.PartitionValues[0].Name)
	require.Equal(t, tree.Identifier("day"), insertStmt.PartitionValues[1].Name)
	require.Equal(t, "insert overwrite gold_orders partition(region = 'ksa', day = 20260624) select 1", tree.String(stmt, dialect.MYSQL))
}

func TestParseIcebergMergeInto(t *testing.T) {
	ctx := context.Background()
	sql := "merge into gold_orders as t using staging_orders as s on t.id = s.id when matched and s.deleted = 1 then delete when matched then update set id = s.id when not matched then insert (id) values (s.id)"
	stmt, err := ParseOne(ctx, sql, 1)
	require.NoError(t, err)
	mergeStmt := stmt.(*tree.Merge)
	require.Len(t, mergeStmt.Clauses, 3)
	require.True(t, mergeStmt.Clauses[0].Matched)
	require.Equal(t, tree.MergeActionDelete, mergeStmt.Clauses[0].Action)
	require.NotNil(t, mergeStmt.Clauses[0].Condition)
	require.True(t, mergeStmt.Clauses[1].Matched)
	require.Equal(t, tree.MergeActionUpdate, mergeStmt.Clauses[1].Action)
	require.Len(t, mergeStmt.Clauses[1].UpdateExprs, 1)
	require.False(t, mergeStmt.Clauses[2].Matched)
	require.Equal(t, tree.MergeActionInsert, mergeStmt.Clauses[2].Action)
	require.Equal(t, tree.IdentifierList{tree.Identifier("id")}, mergeStmt.Clauses[2].InsertColumns)

	formatted := tree.String(stmt, dialect.MYSQL)
	require.Contains(t, formatted, "merge into")
	require.Contains(t, formatted, "when not matched then insert (id) values")
	_, err = ParseOne(ctx, formatted, 1)
	require.NoError(t, err)

	stmt, err = ParseOne(ctx, "with s as (select 1 id) merge into gold_orders t using s on t.id = s.id when not matched then insert values (s.id)", 1)
	require.NoError(t, err)
	require.NotNil(t, stmt.(*tree.Merge).With)
}

func TestParseIcebergTableRefsDoesNotBreakForUpdate(t *testing.T) {
	ctx := context.Background()
	stmt, err := ParseOne(ctx, "select * from gold_orders for iceberg snapshot 123", 1)
	require.NoError(t, err)
	selectStmt := stmt.(*tree.Select)
	join := selectStmt.Select.(*tree.SelectClause).From.Tables[0].(*tree.JoinTableExpr)
	table := join.Left.(*tree.AliasedTableExpr).Expr.(*tree.TableName)
	require.NotNil(t, table.IcebergRef)
	require.Equal(t, tree.IcebergRefSnapshot, table.IcebergRef.Type)

	stmt, err = ParseOne(ctx, "select * from gold_orders for iceberg ref audit_branch", 1)
	require.NoError(t, err)
	selectStmt = stmt.(*tree.Select)
	join = selectStmt.Select.(*tree.SelectClause).From.Tables[0].(*tree.JoinTableExpr)
	table = join.Left.(*tree.AliasedTableExpr).Expr.(*tree.TableName)
	require.NotNil(t, table.IcebergRef)
	require.Equal(t, tree.IcebergRefNamedRef, table.IcebergRef.Type)
	require.Equal(t, tree.Identifier("audit_branch"), table.IcebergRef.RefName)

	_, err = ParseOne(ctx, "select * from gold_orders for update", 1)
	require.NoError(t, err)
}

func TestIcebergKeywordsRemainIdentifiers(t *testing.T) {
	ctx := context.Background()
	for _, sql := range []string{
		"create table t1 (ref int, catalog varchar(10), namespace int, iceberg int, catalogs int, namespaces int, overwrite int)",
		"select ref, catalog, namespace, overwrite from t1",
		"select a from t1 as ref",
	} {
		_, err := ParseOne(ctx, sql, 1)
		require.NoError(t, err, sql)
	}
}
