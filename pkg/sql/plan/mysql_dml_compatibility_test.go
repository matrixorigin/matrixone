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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func buildMySQLDMLCompatibilityPlan(t *testing.T, sql string) (*Plan, error) {
	t.Helper()
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	return BuildPlan(ctx, stmt, false)
}

func buildMySQLDMLCompatibilityPlanWithSQLMode(t *testing.T, sql, sqlMode string) (*Plan, error) {
	t.Helper()
	ctx := NewMockCompilerContext(true)
	ctx.SetSqlModeOverride(sqlMode)
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	return BuildPlan(ctx, stmt, false)
}

func requireMySQLDMLCompatibilityError(t *testing.T, sql string, code uint16, message string) {
	t.Helper()
	_, err := buildMySQLDMLCompatibilityPlan(t, sql)
	require.Error(t, err)
	moErr, ok := err.(*moerr.Error)
	require.True(t, ok, "unexpected error type %T: %v", err, err)
	require.Equal(t, code, moErr.MySQLCode())
	require.Equal(t, message, moErr.Error())
}

func requireMySQLUpdateTargetSubqueryCompatible(t *testing.T, sql string) {
	t.Helper()
	ctx := NewMockCompilerContext(true)
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	updateStmt, ok := stmt.(*tree.Update)
	require.True(t, ok)
	tblInfo, err := getUpdateTableInfo(ctx, updateStmt)
	require.NoError(t, err)
	require.NoError(t, validateUpdateTargetSubqueries(ctx, updateStmt, tblInfo.objRef, tblInfo.tableDefs))
}

func TestMultiTableUpdateRejectsOrderByAndLimit(t *testing.T) {
	requireMySQLDMLCompatibilityError(
		t,
		"UPDATE nation JOIN region ON region.r_regionkey = nation.n_regionkey SET nation.n_name = region.r_name ORDER BY nation.n_nationkey",
		moerr.ER_WRONG_USAGE,
		"Incorrect usage of UPDATE and ORDER BY",
	)
	requireMySQLDMLCompatibilityError(
		t,
		"UPDATE nation JOIN region ON region.r_regionkey = nation.n_regionkey SET nation.n_name = region.r_name LIMIT 1",
		moerr.ER_WRONG_USAGE,
		"Incorrect usage of UPDATE and LIMIT",
	)
}

func TestWindowFunctionsInUpdateAndCheckRespectMatrixOneNativeSQLMode(t *testing.T) {
	updateSQL := "UPDATE nation SET n_regionkey = row_number() over (order by n_nationkey)"
	requireMySQLDMLCompatibilityError(
		t,
		updateSQL,
		moerr.ER_WINDOW_INVALID_WINDOW_FUNC_USE,
		"You cannot use the window function 'row_number' in this context",
	)
	_, err := buildMySQLDMLCompatibilityPlanWithSQLMode(t, updateSQL, "MATRIXONE_NATIVE")
	require.NoError(t, err)

	for _, sql := range []string{
		"CREATE TABLE window_check (id INT PRIMARY KEY, v INT, CHECK (row_number() over (order by v) > 0))",
		"CREATE TABLE column_window_check (id INT PRIMARY KEY, v INT CHECK (row_number() over (order by v) > 0))",
	} {
		for _, sqlMode := range []string{"", "MATRIXONE_NATIVE"} {
			_, err = buildMySQLDMLCompatibilityPlanWithSQLMode(t, sql, sqlMode)
			require.Error(t, err)
			moErr, ok := err.(*moerr.Error)
			require.True(t, ok, "unexpected error type %T: %v", err, err)
			require.Equal(t, uint16(moerr.ER_WINDOW_INVALID_WINDOW_FUNC_USE), moErr.MySQLCode())
			require.Equal(t, "You cannot use the window function 'row_number' in this context", moErr.Error())
		}
	}
}

func TestUpdateRejectsDirectTargetTableSubqueries(t *testing.T) {
	tests := []string{
		"UPDATE nation SET n_name = 'x' WHERE n_nationkey IN (SELECT n_nationkey FROM nation)",
		"UPDATE nation SET n_name = (SELECT max(n_name) FROM nation)",
		"UPDATE nation SET n_name = 'x' WHERE EXISTS (SELECT 1 FROM region WHERE EXISTS (SELECT 1 FROM nation))",
		"UPDATE nation AS dst SET n_name = 'x' WHERE n_nationkey IN (SELECT n_nationkey FROM nation AS src)",
		"UPDATE nation JOIN region ON region.r_regionkey = nation.n_regionkey SET nation.n_name = region.r_name WHERE nation.n_nationkey IN (SELECT n_nationkey FROM nation)",
		"UPDATE nation JOIN region ON EXISTS (SELECT 1 FROM nation) SET nation.n_name = region.r_name",
		"UPDATE nation SET n_name = 'x' FROM region JOIN nation2 ON EXISTS (SELECT 1 FROM nation)",
		"UPDATE nation SET n_name = 'x' WHERE EXISTS (SELECT 1 FROM region UNION ALL SELECT 1 FROM nation)",
		"UPDATE nation SET n_name = 'x' WHERE EXISTS (SELECT 1 FROM region ORDER BY (SELECT max(n_nationkey) FROM nation))",
		"UPDATE nation SET n_name = 'x' WHERE EXISTS (SELECT 1 FROM region GROUP BY r_regionkey HAVING EXISTS (SELECT 1 FROM nation))",
		"UPDATE nation SET n_name = 'x' WHERE EXISTS (SELECT 1 FROM region JOIN nation ON region.r_regionkey = nation.n_regionkey)",
		"UPDATE nation AS dst SET n_name = (SELECT max(dst.n_name) FROM nation AS dst)",
		"UPDATE nation AS dst SET n_name = (SELECT max(src.n_name) FROM nation AS src WHERE EXISTS (SELECT 1 FROM nation AS dst ORDER BY dst.n_nationkey))",
		"UPDATE nation AS dst SET n_name = ((SELECT max(src.n_name) FROM nation AS src WHERE src.n_nationkey <= dst.n_nationkey) UNION ALL (SELECT max(other.n_name) FROM nation AS other))",
	}
	for _, sql := range tests {
		requireMySQLDMLCompatibilityError(
			t,
			sql,
			moerr.ER_UPDATE_TABLE_USED,
			"You can't specify target table 'nation' for update in FROM clause",
		)
	}
}

func TestDeleteRejectsDirectTargetTableSubquery(t *testing.T) {
	for _, sql := range []string{
		"DELETE FROM nation WHERE n_nationkey IN (SELECT n_nationkey FROM nation WHERE n_regionkey > 0)",
		"DELETE nation FROM nation JOIN region ON region.r_regionkey = nation.n_regionkey WHERE EXISTS (SELECT 1 FROM nation)",
		"DELETE nation FROM nation JOIN region ON EXISTS (SELECT 1 FROM nation)",
		"DELETE FROM nation ORDER BY (SELECT max(n_nationkey) FROM nation) LIMIT 1",
	} {
		requireMySQLDMLCompatibilityError(
			t,
			sql,
			moerr.ER_UPDATE_TABLE_USED,
			"You can't specify target table 'nation' for update in FROM clause",
		)
	}
}

func TestMySQLDMLCompatibilityHelpers(t *testing.T) {
	join := &tree.JoinTableExpr{}
	require.True(t, tableExprContainsJoin(tree.NewParenTableExpr(join)))
	require.True(t, tableExprContainsJoin(tree.NewAliasedTableExpr(join, "joined", nil)))
	require.False(t, tableExprContainsJoin(tree.NewTableName(tree.Identifier("nation"), tree.ObjectNamePrefix{}, nil)))

	targets := makeMySQLDMLTargets(
		[]*ObjectRef{nil, {Obj: 99, SchemaName: "resolved_db", ObjName: "resolved_name"}},
		[]*TableDef{nil, {TblId: 7, DbName: "fallback_db", Name: "fallback_name"}},
	)
	require.Equal(t, []mysqlDMLTarget{{
		objID: 99, tableID: 7, schema: "resolved_db", name: "resolved_name",
	}}, targets)

	require.True(t, (mysqlDMLTarget{objID: 99}).matches(&ObjectRef{Obj: 99}, &TableDef{TblId: 1}, "ignored", "ignored"))
	require.True(t, (mysqlDMLTarget{tableID: 7}).matches(nil, &TableDef{TblId: 7}, "ignored", "ignored"))
	require.True(t, (mysqlDMLTarget{schema: "tpch", name: "nation"}).matches(nil, nil, "TPCH", "NATION"))
	require.False(t, (mysqlDMLTarget{schema: "tpch", name: "nation"}).matches(nil, nil, "tpch", "region"))

	qualifiedTargets := make(map[mysqlDMLTarget]map[string]struct{})
	firstSameNameTarget := mysqlDMLTarget{objID: 1, schema: "db1", name: "same_name"}
	secondSameNameTarget := mysqlDMLTarget{objID: 2, schema: "db2", name: "same_name"}
	mysqlAddTargetQualifier(qualifiedTargets, firstSameNameTarget, "first_alias")
	mysqlAddTargetQualifier(qualifiedTargets, secondSameNameTarget, "second_alias")
	require.Equal(t, map[string]struct{}{"first_alias": {}}, qualifiedTargets[firstSameNameTarget])
	require.Equal(t, map[string]struct{}{"second_alias": {}}, qualifiedTargets[secondSameNameTarget])

	outerQualifiers := map[string]struct{}{"dst": {}}
	outerColumn := func() tree.Expr {
		return tree.NewUnresolvedName(tree.NewCStr("dst", 1), tree.NewCStr("n_nationkey", 1))
	}
	otherColumn := func() tree.Expr {
		return tree.NewUnresolvedName(tree.NewCStr("src", 1), tree.NewCStr("n_nationkey", 1))
	}
	selectWrapper := &tree.Select{Select: &tree.SelectClause{}}
	require.False(t, mysqlSelectWrapperReferencesOuterQualifier(selectWrapper, outerQualifiers, nil))
	selectWrapper.TimeWindow = &tree.TimeWindow{Interval: &tree.Interval{Val: outerColumn()}}
	require.True(t, mysqlSelectWrapperReferencesOuterQualifier(selectWrapper, outerQualifiers, nil))
	selectWrapper.TimeWindow = &tree.TimeWindow{
		Interval: &tree.Interval{Val: otherColumn()},
		Sliding:  &tree.Sliding{Val: outerColumn()},
	}
	require.True(t, mysqlSelectWrapperReferencesOuterQualifier(selectWrapper, outerQualifiers, nil))
	selectWrapper.TimeWindow = &tree.TimeWindow{
		Interval: &tree.Interval{Val: otherColumn()},
		Sliding:  &tree.Sliding{Val: otherColumn()},
		Fill:     &tree.Fill{Val: outerColumn()},
	}
	require.True(t, mysqlSelectWrapperReferencesOuterQualifier(selectWrapper, outerQualifiers, nil))

	inherited := map[string]struct{}{"outer": {}}
	visibleCTEs := mysqlCTENames(&tree.With{CTEs: []*tree.CTE{
		nil,
		{Name: nil},
		{Name: &tree.AliasClause{Alias: tree.Identifier("Visible")}},
	}}, inherited)
	require.Contains(t, visibleCTEs, "outer")
	require.Contains(t, visibleCTEs, "visible")
	require.Equal(t, inherited, mysqlCTENames(nil, inherited))

	ctx := NewMockCompilerContext(true)
	_, ok := findMySQLDMLTargetInLimit(ctx, nil, targets, visibleCTEs)
	require.False(t, ok)

	cteName := tree.NewTableName(tree.Identifier("Visible"), tree.ObjectNamePrefix{}, nil)
	_, ok = findMySQLDMLTargetInDirectTableExpr(ctx, cteName, targets, visibleCTEs)
	require.False(t, ok)

	objRef, tableDef, err := ctx.Resolve("tpch", "nation", nil)
	require.NoError(t, err)
	resolvedTargets := makeMySQLDMLTargets([]*ObjectRef{objRef}, []*TableDef{tableDef})
	nationName := tree.NewTableName(tree.Identifier("nation"), tree.ObjectNamePrefix{}, nil)
	aliasedNation := tree.NewAliasedTableExpr(tree.NewParenTableExpr(nationName), "n", nil)
	target, ok := findMySQLDMLTargetInDirectTableExpr(ctx, aliasedNation, resolvedTargets, nil)
	require.True(t, ok)
	require.Equal(t, "nation", target)

	regionName := tree.NewTableName(tree.Identifier("region"), tree.ObjectNamePrefix{}, nil)
	apply := &tree.ApplyTableExpr{Left: regionName, Right: nationName}
	target, ok = findMySQLDMLTargetInDirectTableExpr(ctx, apply, resolvedTargets, nil)
	require.True(t, ok)
	require.Equal(t, "nation", target)
	_, ok = findMySQLDMLTargetInOuterTableExpr(ctx, apply, resolvedTargets, nil)
	require.False(t, ok)
}

func TestMySQLDMLCompatibilityAllowsLegalShapes(t *testing.T) {
	tests := []string{
		"UPDATE nation SET n_name = 'x' ORDER BY n_nationkey LIMIT 1",
		"UPDATE nation SET n_name = 'x' WHERE n_regionkey IN (SELECT r_regionkey FROM region)",
		"UPDATE nation SET n_name = 'x' WHERE n_nationkey IN (SELECT n_nationkey FROM (SELECT n_nationkey FROM nation) AS materialized_nation)",
		"UPDATE nation AS dst JOIN nation AS src ON dst.n_nationkey = src.n_nationkey SET dst.n_name = src.n_name",
		"UPDATE nation SET n_name = (SELECT max(src.n_name) FROM nation AS src WHERE src.n_nationkey <= nation.n_nationkey)",
		"UPDATE nation AS dst SET n_name = (SELECT max(src.n_name) FROM nation AS src WHERE src.n_nationkey <= dst.n_nationkey)",
		"UPDATE nation AS dst SET n_name = (SELECT max(src.n_name) FROM nation AS src WHERE EXISTS (SELECT 1 FROM region WHERE src.n_regionkey = dst.n_regionkey))",
		"UPDATE nation AS dst SET n_name = (SELECT max(src.n_name) FROM nation AS src ORDER BY dst.n_nationkey LIMIT 1)",
		"DELETE FROM nation WHERE n_nationkey IN (SELECT n_nationkey FROM (SELECT n_nationkey FROM nation) AS materialized_nation)",
	}
	for _, sql := range tests {
		_, err := buildMySQLDMLCompatibilityPlan(t, sql)
		require.NoError(t, err, sql)
	}
}

func TestUpdateTargetCompatibilityAllowsNestedJoinCorrelation(t *testing.T) {
	requireMySQLUpdateTargetSubqueryCompatible(t, `
		UPDATE nation AS dst
		SET n_name = (
			SELECT max(src.n_name)
			FROM nation AS src
			JOIN region AS r
				ON EXISTS (
					SELECT 1
					FROM nation AS nested_src
					WHERE nested_src.n_regionkey = dst.n_regionkey
				)
		)`,
	)
}
