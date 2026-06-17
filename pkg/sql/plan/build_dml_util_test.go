// Copyright 2025 Matrix Origin
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
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/buffer"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func Test_runSql(t *testing.T) {
	rt := moruntime.DefaultRuntime()
	moruntime.SetupServiceBasedRuntime("", rt)
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, executor.NewMemExecutor(func(sql string) (executor.Result, error) {
		return executor.Result{}, nil
	}))

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	proc := testutil.NewProcess(t)
	proc.Base.SessionInfo.Buf = buffer.New()

	ctx := context.Background()
	proc.Ctx = context.Background()
	proc.ReplaceTopCtx(ctx)

	compilerContext := NewMockCompilerContext2(ctrl)
	compilerContext.EXPECT().GetProcess().Return(proc).AnyTimes()

	_, err := runSql(compilerContext, "")
	require.Error(t, err, "internal error: no account id in context")
}

func Test_buildPostDmlFullTextIndexAsync(t *testing.T) {
	{
		//invalid json
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":1}`,
		}

		err := buildPostDmlFullTextIndex(nil, nil, nil, nil, nil, nil, 0, idxdef, 0, false, false, false)
		require.NotNil(t, err)
	}

	{

		// async true
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":"true"}`,
		}

		err := buildPostDmlFullTextIndex(nil, nil, nil, nil, nil, nil, 0, idxdef, 0, false, false, false)
		require.Nil(t, err)
	}

}

func Test_buildPreDeleteFullTextIndexAsync(t *testing.T) {
	{
		//invalid json
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":1}`,
		}

		err := buildPreDeleteFullTextIndex(nil, nil, nil, nil, idxdef, 0, nil, nil)
		require.NotNil(t, err)
	}

	{

		// async true
		idxdef := &plan.IndexDef{
			IndexAlgoParams: `{"async":"true"}`,
		}

		err := buildPreDeleteFullTextIndex(nil, nil, nil, nil, idxdef, 0, nil, nil)
		require.Nil(t, err)
	}

}

// Test WITH clause support for INSERT statement (Issue #22583)
func TestBuildWithInsert(t *testing.T) {
	sqls := []string{
		"WITH cte AS (SELECT * FROM t1 WHERE id = 1) INSERT INTO t1 SELECT id + 10, name, value FROM cte",
		"WITH cte AS (SELECT id, name FROM t1) INSERT INTO t1 (id, name, value) SELECT id + 20, name, 100 FROM cte",
		"WITH cte1 AS (SELECT * FROM t1), cte2 AS (SELECT * FROM cte1 WHERE id > 5) INSERT INTO t1 SELECT id + 30, name, value FROM cte2",
	}

	for _, sql := range sqls {
		t.Run(sql, func(t *testing.T) {
			// Just verify the SQL can be parsed and the WITH clause is present
			stmts, err := mysql.Parse(context.TODO(), sql, 1)
			require.NoError(t, err)
			require.Equal(t, 1, len(stmts))

			ins, ok := stmts[0].(*tree.Insert)
			require.True(t, ok)
			require.NotNil(t, ins.With, "INSERT.With should not be nil")
			require.Greater(t, len(ins.With.CTEs), 0, "WITH clause should have at least one CTE")
		})
	}
}

func TestBuildInsertOnDuplicateUpdatePlansReleasesDmlPlanCtxOnError(t *testing.T) {
	oldGet := buildInsertGetDmlPlanCtx
	oldPut := buildInsertPutDmlPlanCtx
	oldBuild := buildInsertUpdatePlans
	defer func() {
		buildInsertGetDmlPlanCtx = oldGet
		buildInsertPutDmlPlanCtx = oldPut
		buildInsertUpdatePlans = oldBuild
	}()

	wantErr := moerr.NewInternalErrorNoCtx("build update plans failed")
	ctxHolder := &dmlPlanCtx{}
	putCalled := false

	buildInsertGetDmlPlanCtx = func() *dmlPlanCtx {
		return ctxHolder
	}
	buildInsertPutDmlPlanCtx = func(ctx *dmlPlanCtx) {
		putCalled = true
		require.Same(t, ctxHolder, ctx)
	}
	buildInsertUpdatePlans = func(ctx CompilerContext, builder *QueryBuilder, bindCtx *BindContext, updatePlanCtx *dmlPlanCtx, addAffectedRows bool) error {
		require.Same(t, ctxHolder, updatePlanCtx)
		require.Equal(t, int32(7), updatePlanCtx.sourceStep)
		require.Equal(t, 3, updatePlanCtx.updateColLength)
		require.Equal(t, 2, updatePlanCtx.rowIdPos)
		require.Equal(t, []int{4, 5}, updatePlanCtx.insertColPos)
		require.Equal(t, map[string]int{"a": 1}, updatePlanCtx.updateColPosMap)
		require.True(t, updatePlanCtx.updatePkCol)
		require.True(t, addAffectedRows)
		return wantErr
	}

	err := buildInsertOnDuplicateUpdatePlans(
		nil,
		nil,
		&ObjectRef{ObjName: "t"},
		&TableDef{Name: "t"},
		7,
		3,
		2,
		[]int{4, 5},
		map[string]int{"a": 1},
		true,
	)
	require.ErrorIs(t, err, wantErr)
	require.True(t, putCalled)
}

func TestMakeInsertValueConstExprGeometry(t *testing.T) {
	proc := testutil.NewProcess(t)
	colType := types.T_geometry.ToType()
	numVal := tree.NewNumVal("POINT(1 1)", "POINT(1 1)", false, tree.P_char)

	expr, err := MakeInsertValueConstExpr(proc, numVal, &colType)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_geometry), expr.Typ.Id)

	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "cast", fn.Func.ObjName)
	require.Equal(t, int32(types.T_varchar), fn.Args[0].Typ.Id)
	require.Equal(t, "POINT(1 1)", fn.Args[0].GetLit().GetSval())
	require.Equal(t, int32(types.T_geometry), fn.Args[1].Typ.Id)
}

func TestAppendIndexPrefixProjection(t *testing.T) {
	newBuilder := func(t *testing.T) (*QueryBuilder, *BindContext, int32) {
		t.Helper()

		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		lastNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT,
			ProjectList: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{Name: "id", ColPos: 0},
					},
				},
				{
					Typ: plan.Type{Id: int32(types.T_text), Width: types.MaxVarcharLen},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{Name: "body", ColPos: 1},
					},
				},
			},
		}, bindCtx)
		return builder, bindCtx, lastNodeID
	}

	t.Run("empty prefix lengths keeps original projection", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)
		useColumns := []int32{1}

		gotNodeID, gotUseColumns, err := appendIndexPrefixProjection(
			builder,
			bindCtx,
			&plan.TableDef{Name: "t"},
			lastNodeID,
			[]string{"body"},
			map[string]int{"body": 1},
			useColumns,
			nil,
		)

		require.NoError(t, err)
		require.Equal(t, lastNodeID, gotNodeID)
		require.Equal(t, useColumns, gotUseColumns)
		require.Len(t, builder.qry.Nodes, 1)
	})

	t.Run("prefix key appends substring projection", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)

		gotNodeID, gotUseColumns, err := appendIndexPrefixProjection(
			builder,
			bindCtx,
			&plan.TableDef{Name: "t"},
			lastNodeID,
			[]string{"body"},
			map[string]int{"id": 0, "body": 1},
			[]int32{1},
			map[string]int{"body": 8},
		)

		require.NoError(t, err)
		require.NotEqual(t, lastNodeID, gotNodeID)
		require.Equal(t, []int32{2}, gotUseColumns)
		require.Len(t, builder.qry.Nodes, 2)

		projectNode := builder.qry.Nodes[gotNodeID]
		require.Equal(t, plan.Node_PROJECT, projectNode.NodeType)
		require.Equal(t, []int32{lastNodeID}, projectNode.Children)
		require.Len(t, projectNode.ProjectList, 3)

		prefixExpr := projectNode.ProjectList[2]
		require.Equal(t, int32(types.T_varchar), prefixExpr.Typ.Id)
		require.Equal(t, int32(types.MaxVarcharLen), prefixExpr.Typ.Width)

		castFn := prefixExpr.GetF()
		require.NotNil(t, castFn)
		require.Equal(t, "cast", castFn.Func.ObjName)
		require.Len(t, castFn.Args, 2)

		substringFn := castFn.Args[0].GetF()
		require.NotNil(t, substringFn)
		require.Equal(t, "substring", substringFn.Func.ObjName)
		require.Len(t, substringFn.Args, 3)
		require.Equal(t, "body", substringFn.Args[0].GetCol().Name)
		require.Equal(t, int64(1), substringFn.Args[1].GetLit().GetI64Val())
		require.Equal(t, int64(8), substringFn.Args[2].GetLit().GetI64Val())
	})

	t.Run("missing and non-positive prefix parts do not append projection", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)
		useColumns := []int32{0}

		gotNodeID, gotUseColumns, err := appendIndexPrefixProjection(
			builder,
			bindCtx,
			&plan.TableDef{Name: "t"},
			lastNodeID,
			[]string{"missing", "id"},
			map[string]int{"id": 0, "body": 1},
			useColumns,
			map[string]int{"missing": 4, "id": 0},
		)

		require.NoError(t, err)
		require.Equal(t, lastNodeID, gotNodeID)
		require.Equal(t, useColumns, gotUseColumns)
		require.Len(t, builder.qry.Nodes, 1)
	})
}

func TestAppendDeleteIndexTablePlanUsesPrefixLookupKey(t *testing.T) {
	newBuilder := func(t *testing.T) (*QueryBuilder, *BindContext, int32) {
		t.Helper()

		builder := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
		bindCtx := NewBindContext(builder, nil)
		lastNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_PROJECT,
			Stats:    &plan.Stats{Selectivity: 1, Outcnt: 1, Cost: 1, TableCnt: 1},
			ProjectList: []*plan.Expr{
				{
					Typ: plan.Type{Id: int32(types.T_int64)},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{Name: "id", ColPos: 0},
					},
				},
				{
					Typ: plan.Type{Id: int32(types.T_text), Width: types.MaxVarcharLen},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{Name: "body", ColPos: 1},
					},
				},
				{
					Typ: plan.Type{Id: int32(types.T_varchar), Width: 32},
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{Name: "tenant", ColPos: 2},
					},
				},
			},
		}, bindCtx)
		return builder, bindCtx, lastNodeID
	}

	indexTableDef := &plan.TableDef{
		Name: "idx_body",
		Cols: []*plan.ColDef{
			{
				Name: catalog.Row_ID,
				Typ:  plan.Type{Id: int32(types.T_Rowid), Width: 16},
			},
			{
				Name: catalog.IndexTableIndexColName,
				Typ:  plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
			},
		},
		Pkey: &plan.PrimaryKeyDef{PkeyColName: catalog.IndexTableIndexColName},
	}
	typMap := map[string]plan.Type{
		"id":     {Id: int32(types.T_int64)},
		"body":   {Id: int32(types.T_text), Width: types.MaxVarcharLen},
		"tenant": {Id: int32(types.T_varchar), Width: 32},
	}
	posMap := map[string]int{
		"id":     0,
		"body":   1,
		"tenant": 2,
	}

	extractLookupExpr := func(t *testing.T, joinNode *plan.Node) *plan.Expr {
		t.Helper()
		require.Equal(t, plan.Node_JOIN, joinNode.NodeType)
		require.Len(t, joinNode.OnList, 1)

		joinFn := joinNode.OnList[0].GetF()
		require.NotNil(t, joinFn)
		require.Equal(t, "=", joinFn.Func.ObjName)
		require.Len(t, joinFn.Args, 2)
		return joinFn.Args[1]
	}
	requirePrefixExpr := func(t *testing.T, expr *plan.Expr, colName string, length int64) {
		t.Helper()

		castFn := expr.GetF()
		require.NotNil(t, castFn)
		require.Equal(t, "cast", castFn.Func.ObjName)
		require.Len(t, castFn.Args, 2)

		substringFn := castFn.Args[0].GetF()
		require.NotNil(t, substringFn)
		require.Equal(t, "substring", substringFn.Func.ObjName)
		require.Len(t, substringFn.Args, 3)
		require.Equal(t, colName, substringFn.Args[0].GetCol().Name)
		require.Equal(t, int32(1), substringFn.Args[0].GetCol().RelPos)
		require.Equal(t, int64(1), substringFn.Args[1].GetLit().GetI64Val())
		require.Equal(t, length, substringFn.Args[2].GetLit().GetI64Val())
	}

	t.Run("single prefix part", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)

		gotNodeID, err := appendDeleteIndexTablePlan(
			builder,
			bindCtx,
			&plan.ObjectRef{ObjName: "idx_body"},
			indexTableDef,
			&plan.IndexDef{
				Parts:           []string{"body"},
				IndexAlgoParams: `{"prefix_lengths":"body:8"}`,
			},
			typMap,
			posMap,
			lastNodeID,
			true,
		)

		require.NoError(t, err)
		lookupExpr := extractLookupExpr(t, builder.qry.Nodes[gotNodeID])
		requirePrefixExpr(t, lookupExpr, "body", 8)
	})

	t.Run("composite prefix part", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)

		gotNodeID, err := appendDeleteIndexTablePlan(
			builder,
			bindCtx,
			&plan.ObjectRef{ObjName: "idx_body_tenant"},
			indexTableDef,
			&plan.IndexDef{
				Parts:           []string{"body", "tenant"},
				IndexAlgoParams: `{"prefix_lengths":"body:8"}`,
			},
			typMap,
			posMap,
			lastNodeID,
			false,
		)

		require.NoError(t, err)
		lookupExpr := extractLookupExpr(t, builder.qry.Nodes[gotNodeID])

		serialFn := lookupExpr.GetF()
		require.NotNil(t, serialFn)
		require.Equal(t, "serial_full", serialFn.Func.ObjName)
		require.Len(t, serialFn.Args, 2)
		requirePrefixExpr(t, serialFn.Args[0], "body", 8)
		require.Equal(t, "tenant", serialFn.Args[1].GetCol().Name)
	})

	t.Run("composite unique prefix part", func(t *testing.T) {
		builder, bindCtx, lastNodeID := newBuilder(t)

		gotNodeID, err := appendDeleteIndexTablePlan(
			builder,
			bindCtx,
			&plan.ObjectRef{ObjName: "idx_body_tenant"},
			indexTableDef,
			&plan.IndexDef{
				Parts:           []string{"body", "tenant"},
				IndexAlgoParams: `{"prefix_lengths":"body:8"}`,
			},
			typMap,
			posMap,
			lastNodeID,
			true,
		)

		require.NoError(t, err)
		lookupExpr := extractLookupExpr(t, builder.qry.Nodes[gotNodeID])

		serialFn := lookupExpr.GetF()
		require.NotNil(t, serialFn)
		require.Equal(t, "serial", serialFn.Func.ObjName)
		require.Len(t, serialFn.Args, 2)
		requirePrefixExpr(t, serialFn.Args[0], "body", 8)
		require.Equal(t, "tenant", serialFn.Args[1].GetCol().Name)
	})
}

func TestPrefixIndexDMLPlansMaterializePrefixKeys(t *testing.T) {
	mock := NewMockOptimizer(true)
	emp := mock.ctxt.tables["emp"]
	require.NotNil(t, emp)
	require.NotEmpty(t, emp.Indexes)
	for _, idxDef := range emp.Indexes {
		idxDef.IndexAlgoParams = `{"prefix_lengths":"ename:4"}`
	}

	assertPlanHasEnamePrefix := func(t *testing.T, sql string) {
		t.Helper()

		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
		require.NotNil(t, logicPlan.GetQuery())

		require.True(
			t,
			queryHasPrefixSubstring(logicPlan.GetQuery(), "ename", 4),
			"expected plan for %q to materialize prefix index key with substring(ename, 1, 4)",
			sql,
		)
	}

	assertPlanHasEnamePrefix(t, "update constraint_test.emp set ename = 'abcdef-long' where empno = 1")
	assertPlanHasEnamePrefix(t, "delete from constraint_test.emp where empno = 1")

	assertPlanHasPrefixCount := func(t *testing.T, sql, colName string, minCount int) {
		t.Helper()

		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err)
		require.NotNil(t, logicPlan.GetQuery())

		got := countQueryPrefixSubstrings(logicPlan.GetQuery(), colName, 4)
		require.GreaterOrEqual(t, got, minCount, "expected plan for %q to materialize at least %d prefix keys", sql, minCount)
	}

	dept := mock.ctxt.tables["dept"]
	require.NotNil(t, dept)
	for _, idxDef := range dept.Indexes {
		if len(idxDef.Parts) == 1 && idxDef.Parts[0] == "dname" {
			idxDef.IndexAlgoParams = `{"prefix_lengths":"dname:4"}`
		}
	}
	assertPlanHasPrefixCount(t, "update constraint_test.dept set dname = 'abcdef-long' where deptno = 1", "dname", 1)
	assertPlanHasPrefixCount(t, "delete from constraint_test.dept where deptno = 1", "dname", 1)

	singleIdx := mock.ctxt.tables["single_idx_t"]
	require.NotNil(t, singleIdx)
	require.Len(t, singleIdx.Indexes, 1)
	singleIdx.Cols[1].Typ = plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	singleIdx.Indexes[0].IndexAlgoParams = `{"prefix_lengths":"val:4"}`
	singleIdxIndexTable := mock.ctxt.tables[singleIdx.Indexes[0].IndexTableName]
	require.NotNil(t, singleIdxIndexTable)
	singleIdxIndexTable.Cols[0].Typ = plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	assertPlanHasPrefixCount(t, "update constraint_test.single_idx_t set val = 'abcdef-long' where id = 1", "val", 2)
	assertPlanHasPrefixCount(t, "delete from constraint_test.single_idx_t where id = 1", "val", 1)
}

func queryHasPrefixSubstring(query *plan.Query, colName string, length int64) bool {
	return countQueryPrefixSubstrings(query, colName, length) > 0
}

func countQueryPrefixSubstrings(query *plan.Query, colName string, length int64) int {
	count := 0
	for _, node := range query.Nodes {
		count += countExprListPrefixSubstrings(node.ProjectList, colName, length)
		count += countExprListPrefixSubstrings(node.OnList, colName, length)
		count += countExprListPrefixSubstrings(node.FilterList, colName, length)
	}
	return count
}

func exprListHasPrefixSubstring(exprs []*plan.Expr, colName string, length int64) bool {
	return countExprListPrefixSubstrings(exprs, colName, length) > 0
}

func countExprListPrefixSubstrings(exprs []*plan.Expr, colName string, length int64) int {
	count := 0
	for _, expr := range exprs {
		count += countExprPrefixSubstrings(expr, colName, length)
	}
	return count
}

func exprHasPrefixSubstring(expr *plan.Expr, colName string, length int64) bool {
	return countExprPrefixSubstrings(expr, colName, length) > 0
}

func countExprPrefixSubstrings(expr *plan.Expr, colName string, length int64) int {
	if expr == nil {
		return 0
	}

	fn := expr.GetF()
	if fn == nil {
		list := expr.GetList()
		if list == nil {
			return 0
		}
		return countExprListPrefixSubstrings(list.List, colName, length)
	}

	count := 0
	if fn.Func.ObjName == "substring" && len(fn.Args) == 3 {
		start := fn.Args[1].GetLit()
		prefixLen := fn.Args[2].GetLit()
		if exprContainsColumn(fn.Args[0], colName) &&
			start != nil && start.GetI64Val() == 1 &&
			prefixLen != nil && prefixLen.GetI64Val() == length {
			count++
		}
	}

	return count + countExprListPrefixSubstrings(fn.Args, colName, length)
}

func exprContainsColumn(expr *plan.Expr, colName string) bool {
	if expr == nil {
		return false
	}

	if col := expr.GetCol(); col != nil {
		if col.Name == "" {
			return true
		}
		return columnNameMatches(col.Name, colName)
	}

	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if exprContainsColumn(arg, colName) {
				return true
			}
		}
		return false
	}

	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprContainsColumn(item, colName) {
				return true
			}
		}
	}

	return false
}

func columnNameMatches(got, want string) bool {
	return got == want || strings.HasSuffix(got, "."+want)
}
