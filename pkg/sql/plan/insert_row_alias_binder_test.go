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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func testInsertAliasTable() *planpb.TableDef {
	return &planpb.TableDef{
		Name: "t",
		Cols: []*planpb.ColDef{
			{Name: "id"},
			{Name: "a"},
			{Name: "b"},
		},
		Name2ColIndex: map[string]int32{"id": 0, "a": 1, "b": 2},
	}
}

func testInsertAliasName(parts ...string) *tree.UnresolvedName {
	cstrs := make([]*tree.CStr, len(parts))
	for i, part := range parts {
		cstrs[i] = tree.NewCStr(part, 1)
	}
	return tree.NewUnresolvedName(cstrs...)
}

func testInsertAliasNameWithTableCase(table, column string, lowerCaseTableNames int64) *tree.UnresolvedName {
	return tree.NewUnresolvedName(tree.NewCStr(table, lowerCaseTableNames), tree.NewCStr(column, 1))
}

func TestInsertRowAliasBindingMapsTargetIdentity(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"x", "y"}},
		[]string{"b", "a"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	require.Equal(t, insertRowAliasColumn{targetIdx: 2, incomingPos: 2}, binding.cols["x"])
	require.Equal(t, insertRowAliasColumn{targetIdx: 1, incomingPos: 1}, binding.cols["y"])

	require.NoError(t, binding.remapIncomingPositions(context.Background(), tableDef, map[string]int32{
		"t.a": 4,
		"t.b": 3,
	}))
	require.Equal(t, 3, binding.cols["x"].incomingPos)
	require.Equal(t, 4, binding.cols["y"].incomingPos)
}

func TestInsertRowAliasBindingRemapsGeneratedColumnByIdentity(t *testing.T) {
	tableDef := testInsertAliasTable()
	tableDef.Cols = append(tableDef.Cols, &planpb.ColDef{
		Name:         "g",
		GeneratedCol: &planpb.GeneratedCol{},
	})
	tableDef.Name2ColIndex["g"] = 3
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"id", "g"}},
		[]string{"id", "g"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	require.NoError(t, binding.remapIncomingPositions(context.Background(), tableDef, map[string]int32{
		"t.id": 5,
		"t.g":  2,
	}))
	require.Equal(t, 5, binding.cols["id"].incomingPos)
	require.Equal(t, 2, binding.cols["g"].incomingPos)
}

func TestInsertRowAliasBinderResolvesIncomingAndTargetRows(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"x", "y"}},
		[]string{"b", "a"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	binder := NewOndupUpdateBinder(context.Background(), nil, nil, 11, 7, tableDef, "db", "t", 1, binding)

	expr, err := binder.BindColRef(testInsertAliasName("n", "x"), 0, true)
	require.NoError(t, err)
	require.Equal(t, int32(7), expr.GetCol().RelPos)
	require.Equal(t, int32(2), expr.GetCol().ColPos)

	expr, err = binder.BindColRef(testInsertAliasName("t", "a"), 0, true)
	require.NoError(t, err)
	require.Equal(t, int32(11), expr.GetCol().RelPos)
	require.Equal(t, int32(1), expr.GetCol().ColPos)

	expr, err = binder.BindColRef(testInsertAliasName("n", "x"), 2, true)
	require.NoError(t, err)
	require.Equal(t, int32(2), expr.GetCorr().Depth)
	require.Equal(t, int32(7), expr.GetCorr().RelPos)
	require.Equal(t, int32(2), expr.GetCorr().ColPos)
}

func TestInsertRowAliasBinderUsesVisibleTargetForCorrelation(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"id", "x", "y"}},
		[]string{"id", "a", "b"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	binder := NewOndupUpdateBinder(context.Background(), nil, nil, 11, 7, tableDef, "db", "t", 1, binding)
	binder.SetTargetCorrelationTag(13)

	expr, err := binder.BindColRef(testInsertAliasName("t", "a"), 1, true)
	require.NoError(t, err)
	require.Equal(t, int32(1), expr.GetCorr().Depth)
	require.Equal(t, int32(13), expr.GetCorr().RelPos)
	require.Equal(t, int32(1), expr.GetCorr().ColPos)
}

func TestInsertRowAliasCorrelatedFromBuildPlanKeepsTargetLookupReachable(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
		"insert into constraint_test.dept(deptno, dname, loc) values (1, 'Sales', 'NY') as n(id, name, location) "+
			"on duplicate key update loc = (select e.ename from constraint_test.emp as e "+
			"where e.deptno = constraint_test.dept.deptno)")
	require.NoError(t, err)

	targetScans := 0
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.Name == "dept" {
			targetScans++
		}
	}
	// The target arbitration and DEDUP scans are siblings.  A third target
	// scan proves the correlated lookup was attached below the candidate side
	// before flattening the scalar subquery.
	require.GreaterOrEqual(t, targetScans, 3)
}

func TestInsertRowAliasCorrelatedScalarJoinGuardsNonConflict(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
		"insert into constraint_test.dept(deptno, dname, loc) values (999, 'Sales', 'NY') as n(id, name, location) "+
			"on duplicate key update loc = (select e.ename from constraint_test.emp as e "+
			"where e.deptno = coalesce(constraint_test.dept.deptno, 0))")
	require.NoError(t, err)

	guardedScalarJoin := false
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType != planpb.Node_JOIN || node.JoinType != planpb.Node_SINGLE {
			continue
		}
		for _, predicate := range node.OnList {
			if exprContainsFunc(predicate, "isnotnull") {
				guardedScalarJoin = true
			}
		}
	}
	require.True(t, guardedScalarJoin,
		"target-correlated scalar ODKU subquery must be gated by the target lookup match")
}

func TestInsertRowAliasCorrelatedRejectsOrderedAssignmentComposition(t *testing.T) {
	_, err := runOneStmt(NewMockOptimizer(true), t,
		"insert into constraint_test.dept(deptno, dname, loc) values (1, 'Sales', 'NY') as n(id, name, location) "+
			"on duplicate key update dname = 'changed', loc = (select max(e.ename) from constraint_test.emp as e "+
			"where e.deptno = constraint_test.dept.deptno)")
	require.ErrorContains(t, err, odkuTargetCorrelatedSubqueryCause)
}

func TestInsertRowAliasCorrelatedRejectsMultiRowInput(t *testing.T) {
	_, err := runOneStmt(NewMockOptimizer(true), t,
		"insert into constraint_test.dept(deptno, dname, loc) values (1, 'Sales', 'NY'), (1, 'Marketing', 'LA') as n(id, name, location) "+
			"on duplicate key update loc = (select max(e.ename) from constraint_test.emp as e "+
			"where e.deptno = constraint_test.dept.deptno)")
	require.ErrorContains(t, err, odkuTargetCorrelatedSubqueryCause)
}

func TestInsertRowAliasCorrelatedFromBuildPlanWithUniqueConflict(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
		"insert into constraint_test.dept(deptno, dname, loc) values (999, 'Sales', 'NY') as n(id, name, location) "+
			"on duplicate key update loc = (select e.ename from constraint_test.emp as e "+
			"where e.deptno = constraint_test.dept.deptno)")
	require.NoError(t, err)

	var targetArbiter, targetLookup bool
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == planpb.Node_PRE_INSERT_UK && node.PreInsertUkCtx.GetOdkuTargetArbitration() {
			targetArbiter = true
		}
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.Name == "dept" {
			targetLookup = true
		}
	}
	require.True(t, targetArbiter)
	require.True(t, targetLookup)
}

func TestInsertRowAliasCorrelatedFromBuildPlanSupportsFakePrimaryTarget(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
		"insert into constraint_test.fake_pk_t(a, b) values (1, 'x') as n(k, v) "+
			"on duplicate key update b = (select e.ename from constraint_test.emp as e "+
			"where e.deptno = constraint_test.fake_pk_t.a)")
	require.NoError(t, err)

	var targetArbiter, targetLookup bool
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == planpb.Node_PRE_INSERT_UK && node.PreInsertUkCtx.GetOdkuTargetArbitration() {
			targetArbiter = true
		}
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.Name == "fake_pk_t" {
			targetLookup = true
		}
	}
	require.True(t, targetArbiter)
	require.True(t, targetLookup)
}

func TestInsertRowAliasNestedCorrelationIsRejected(t *testing.T) {
	_, err := runOneStmt(NewMockOptimizer(true), t,
		"insert into constraint_test.dept(deptno, dname, loc) values (1, 'Sales', 'NY') as n(id, name, location) "+
			"on duplicate key update loc = (select max(e.ename) from constraint_test.emp as e "+
			"where e.deptno = (select max(e2.deptno) from constraint_test.emp as e2 "+
			"where e2.deptno = constraint_test.dept.deptno))")
	require.ErrorContains(t, err, odkuTargetCorrelatedSubqueryCause)
}

func TestInsertRowAliasGeneratedDefaultNoKeyFallbackBuilds(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t,
		"insert into constraint_test.fake_pk_no_unique_gen(a, g) values (1, default) as n(x, y) "+
			"on duplicate key update a = n.x")
	require.NoError(t, err)
	require.NotNil(t, logicPlan)
}

func TestInsertRowAliasBinderRejectsAmbiguousAndInvalidNames(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n"}, []string{"a", "b"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	binder := NewOndupUpdateBinder(context.Background(), nil, nil, 11, 7, tableDef, "db", "t", 1, binding)
	_, err = binder.BindColRef(testInsertAliasName("a"), 0, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ambiguous")

	for _, alias := range []*tree.AliasClause{
		{Alias: "t"},
		{Alias: "n", Cols: tree.IdentifierList{"x", "x"}},
		{Alias: "n", Cols: tree.IdentifierList{"x"}},
	} {
		_, err = validateInsertRowAlias(context.Background(), alias, []string{"a", "b"}, tableDef, "db", "t", 1)
		require.Error(t, err)
	}
}

func TestInsertRowAliasColumnNamesIgnoreTableCaseSetting(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "N", Cols: tree.IdentifierList{"K", "X", "Y"}},
		[]string{"id", "a", "b"}, tableDef, "db", "t", 0,
	)
	require.NoError(t, err)
	require.Equal(t, "N", binding.name)
	require.Contains(t, binding.cols, "x")
	binder := NewOndupUpdateBinder(context.Background(), nil, nil, 11, 7, tableDef, "db", "t", 0, binding)
	// The parser stores column identifier parts with the column normalization
	// (lower=1), even when lower_case_table_names=0.
	expr, err := binder.BindColRef(testInsertAliasNameWithTableCase("N", "X", 0), 0, true)
	require.NoError(t, err)
	require.Equal(t, int32(7), expr.GetCol().RelPos)
	require.Equal(t, int32(1), expr.GetCol().ColPos)

	_, err = validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"X", "x", "Y"}},
		[]string{"id", "a", "b"}, tableDef, "db", "t", 0,
	)
	require.Error(t, err)
	require.ErrorContains(t, err, "column name")
}

func TestInsertRowAliasFallbackCollectsNestedParameters(t *testing.T) {
	stmt, err := parsers.ParseOne(
		context.Background(), dialect.MYSQL,
		"insert into t values (1) as n on duplicate key update a = case when ? then n.a + ? else (select ?) end", 1,
	)
	require.NoError(t, err)
	insert := stmt.(*tree.Insert)
	require.Equal(t, []int{1, 2, 3}, collectParamExprOffsets(insert.OnDuplicateUpdate[0].Expr))
}

func TestInsertRowAliasValidationEdges(t *testing.T) {
	tableDef := testInsertAliasTable()
	tests := []struct {
		name        string
		rowAlias    *tree.AliasClause
		insertCols  []string
		table       *planpb.TableDef
		wantFailure bool
	}{
		{name: "nil alias", insertCols: []string{"a"}, table: tableDef},
		{name: "empty alias", rowAlias: &tree.AliasClause{}, insertCols: []string{"a"}, table: tableDef, wantFailure: true},
		{name: "target conflict", rowAlias: &tree.AliasClause{Alias: "t"}, insertCols: []string{"a"}, table: tableDef, wantFailure: true},
		{name: "missing table", rowAlias: &tree.AliasClause{Alias: "n"}, insertCols: []string{"a"}, wantFailure: true},
		{name: "column count mismatch", rowAlias: &tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"x"}}, insertCols: []string{"a", "b"}, table: tableDef, wantFailure: true},
		{name: "empty target column", rowAlias: &tree.AliasClause{Alias: "n"}, insertCols: []string{""}, table: tableDef, wantFailure: true},
		{name: "empty alias column", rowAlias: &tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{""}}, insertCols: []string{"a"}, table: tableDef, wantFailure: true},
		{name: "missing target column", rowAlias: &tree.AliasClause{Alias: "n"}, insertCols: []string{"missing"}, table: tableDef, wantFailure: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			binding, err := validateInsertRowAlias(
				context.Background(), test.rowAlias, test.insertCols, test.table, "db", "t", 1,
			)
			if test.wantFailure {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Nil(t, binding)
		})
	}
}

func TestInsertRowAliasSourceColumnResolutionEdges(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_INSERT, NewMockCompilerContext(true), false, false)
	tableDef := testInsertAliasTable()

	columns, err := builder.getInsertColsForRowAlias(nil, tableDef)
	require.NoError(t, err)
	require.Equal(t, []string{"id", "a", "b"}, columns)

	_, err = builder.getInsertColsForRowAlias(tree.IdentifierList{"a", "A"}, tableDef)
	require.Error(t, err)
	_, err = builder.getInsertColsForRowAlias(tree.IdentifierList{"missing"}, tableDef)
	require.Error(t, err)
	require.Nil(t, cloneInsertRowsForGeneratedRewrite(nil))
}

func TestInsertRowAliasUpdateTargetValidationEdges(t *testing.T) {
	tableDef := testInsertAliasTable()
	valid := func(name *tree.UnresolvedName) *tree.UpdateExpr {
		return &tree.UpdateExpr{Names: []*tree.UnresolvedName{name}}
	}
	tests := []struct {
		name        string
		table       *planpb.TableDef
		updates     tree.UpdateExprs
		wantFailure bool
	}{
		{name: "missing table", updates: tree.UpdateExprs{}, wantFailure: true},
		{name: "nil updates", table: tableDef},
		{name: "nil update item", table: tableDef, updates: tree.UpdateExprs{nil}},
		{name: "empty update", table: tableDef, updates: tree.UpdateExprs{&tree.UpdateExpr{}}, wantFailure: true},
		{name: "nil update name", table: tableDef, updates: tree.UpdateExprs{&tree.UpdateExpr{Names: []*tree.UnresolvedName{nil}}}, wantFailure: true},
		{name: "wrong table", table: tableDef, updates: tree.UpdateExprs{valid(testInsertAliasName("other", "a"))}, wantFailure: true},
		{name: "missing column", table: tableDef, updates: tree.UpdateExprs{valid(testInsertAliasName("missing"))}, wantFailure: true},
		{name: "valid column", table: tableDef, updates: tree.UpdateExprs{valid(testInsertAliasName("a"))}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateOndupUpdateTargets(
				context.Background(), test.updates, test.table, "db", "t", 1,
			)
			if test.wantFailure {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestInsertRowAliasBinderResolvesBareAndRejectsQualifiedEdges(t *testing.T) {
	tableDef := testInsertAliasTable()
	binding, err := validateInsertRowAlias(
		context.Background(),
		&tree.AliasClause{Alias: "n", Cols: tree.IdentifierList{"x", "y"}},
		[]string{"a", "b"}, tableDef, "db", "t", 1,
	)
	require.NoError(t, err)
	binder := NewOndupUpdateBinder(context.Background(), nil, nil, 11, 7, tableDef, "db", "t", 1, binding)

	for _, name := range []*tree.UnresolvedName{
		testInsertAliasName("db", "n", "x"),
		testInsertAliasName("n", "missing"),
		testInsertAliasName("other", "a"),
		testInsertAliasName("t", "missing"),
	} {
		_, err = binder.BindColRef(name, 0, true)
		require.Error(t, err)
	}

	_, err = binder.BindColRef(testInsertAliasName("other", "a"), 1, true)
	require.Error(t, err)

	expr, err := binder.BindColRef(testInsertAliasName("x"), 0, true)
	require.NoError(t, err)
	require.Equal(t, int32(7), expr.GetCol().RelPos)
	require.Equal(t, int32(1), expr.GetCol().ColPos)

	binder.SetTargetCorrelationTag(13)
	expr, err = binder.BindColRef(testInsertAliasName("a"), 1, true)
	require.NoError(t, err)
	require.Equal(t, int32(1), expr.GetCorr().Depth)
	require.Equal(t, int32(13), expr.GetCorr().RelPos)
	require.Equal(t, int32(1), expr.GetCorr().ColPos)

	_, err = binder.BindColRef(testInsertAliasName("missing"), 0, true)
	require.Error(t, err)
}

func TestInsertRowAliasScopeRefsTraverseAndRewrite(t *testing.T) {
	col := func(tag, pos int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: tag, ColPos: pos}}}
	}
	corr := func(tag, pos, depth int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_Corr{Corr: &planpb.CorrColRef{RelPos: tag, ColPos: pos, Depth: depth}}}
	}
	emptyExprs := []*planpb.Expr{
		{Expr: &planpb.Expr_Col{}},
		{Expr: &planpb.Expr_Corr{}},
		{Expr: &planpb.Expr_F{}},
		{Expr: &planpb.Expr_List{}},
		{Expr: &planpb.Expr_W{}},
		{Expr: &planpb.Expr_Sub{}},
	}

	refs := make([]insertScopeRef, 0)
	seen := make(map[[2]int32]struct{})
	collectInsertScopeRefs(nil, &refs, seen)
	for _, expr := range emptyExprs {
		collectInsertScopeRefs(expr, &refs, seen)
	}

	listExpr := &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: []*planpb.Expr{
		col(22, 6), corr(22, 6, 2), col(88, 12),
	}}}}
	windowExpr := &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
		WindowFunc:  col(7, 7),
		PartitionBy: []*planpb.Expr{col(11, 8)},
		OrderBy:     []*planpb.OrderBySpec{nil, {Expr: col(99, 9)}},
		Frame:       &planpb.FrameClause{Start: &planpb.FrameBound{Val: col(7, 10)}, End: &planpb.FrameBound{Val: col(11, 11)}},
	}}}
	subqueryExpr := &planpb.Expr{Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{}}}
	expr := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Args: []*planpb.Expr{
		col(7, 1),
		corr(7, 2, 1),
		corr(7, 3, 2),
		col(11, 3),
		col(99, 4),
		col(0, 5),
		col(7, 1),
		listExpr,
		corr(11, 13, 2),
		corr(88, 13, 2),
		windowExpr,
		subqueryExpr,
	}}}}
	collectInsertScopeRefs(expr, &refs, seen)

	got := make([][2]int32, 0, len(refs))
	for _, ref := range refs {
		got = append(got, [2]int32{ref.tag, ref.pos})
	}
	require.Equal(t, [][2]int32{
		{7, 1}, {7, 2}, {7, 3}, {11, 3}, {99, 4}, {22, 6}, {88, 12},
		{11, 13}, {88, 13}, {7, 7}, {11, 8}, {99, 9}, {7, 10}, {11, 11},
	}, got)

	rewriteInsertScopeRefs(nil, 7, 20, 11, 4, map[[2]int32]int32{})
	for _, empty := range emptyExprs {
		rewriteInsertScopeRefs(empty, 7, 20, 11, 4, map[[2]int32]int32{})
	}
	rewriteInsertScopeRefs(expr, 7, 20, 11, 4, map[[2]int32]int32{
		{99, 4}: 8,
		{22, 6}: 10,
		{99, 9}: 11,
	})

	args := expr.GetF().Args
	require.Equal(t, int32(20), args[0].GetCol().RelPos)
	require.Equal(t, int32(2), args[1].GetCol().ColPos)
	require.Equal(t, int32(20), args[2].GetCorr().RelPos)
	require.Equal(t, int32(7), args[3].GetCol().ColPos)
	require.Equal(t, int32(8), args[4].GetCol().ColPos)
	require.Equal(t, int32(0), args[5].GetCol().RelPos)
	require.Equal(t, int32(10), args[7].GetList().List[0].GetCol().ColPos)
	require.Equal(t, int32(10), args[7].GetList().List[1].GetCorr().ColPos)
	require.Equal(t, int32(88), args[7].GetList().List[2].GetCol().RelPos)
	require.Equal(t, int32(17), args[8].GetCorr().ColPos)
	require.Equal(t, int32(88), args[9].GetCorr().RelPos)
	require.Equal(t, int32(20), args[10].GetW().WindowFunc.GetCol().RelPos)
	require.Equal(t, int32(20), args[10].GetW().OrderBy[1].Expr.GetCol().RelPos)
	require.Equal(t, int32(20), args[10].GetW().Frame.Start.Val.GetCol().RelPos)
}

func TestInsertRowAliasParamOffsetsHandleEmptyExpressions(t *testing.T) {
	require.Nil(t, collectParamExprOffsets(nil))
	stmt, err := parsers.ParseOne(
		context.Background(), dialect.MYSQL,
		"insert into t values (1) as n on duplicate key update a = 1", 1,
	)
	require.NoError(t, err)
	insert := stmt.(*tree.Insert)
	require.Nil(t, collectParamExprOffsets(insert.OnDuplicateUpdate[0].Expr))
}
