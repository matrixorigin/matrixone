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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestPreparedNumericContextUsesInsertValuesTarget(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{
			name: "binary arithmetic",
			sql:  "insert into constraint_test.emp (sal) values (? + ?)",
			want: types.T_decimal64,
		},
		{
			name: "mod function",
			sql:  "insert into constraint_test.emp (sal) values (mod(?, ?))",
			want: types.T_decimal64,
		},
		{
			name: "double sibling overrides decimal target",
			sql:  "insert into constraint_test.emp (sal) values ((? + ?) + cast(1 as double))",
			want: types.T_float64,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectPlanParamTypes(queryPlan)
			require.Len(t, paramTypes, 2)
			require.Equal(t, test.want, paramTypes[0])
			require.Equal(t, test.want, paramTypes[1])
		})
	}
}

func TestPreparedNumericContextUsesInsertSelectTarget(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		want       types.T
		paramCount int
	}{
		{
			name:       "direct select",
			sql:        "insert into constraint_test.emp (sal) select ? + ?",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "parenthesized select",
			sql:        "insert into constraint_test.emp (sal) (select ? + ?)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "union select",
			sql:        "insert into constraint_test.emp (sal) (select ? + ? union all select ? + ?)",
			want:       types.T_decimal64,
			paramCount: 4,
		},
		{
			name:       "double sibling overrides target",
			sql:        "insert into constraint_test.emp (sal) select (? + ?) + cast(1 as double)",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name:       "group by position",
			sql:        "insert into constraint_test.emp (sal) select ? + ? from constraint_test.emp group by 1",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "independent group by parameters keep default type",
			sql:        "insert into constraint_test.emp (sal) select ? + ? from nation group by ? + ?",
			want:       types.T_float64,
			paramCount: 2,
		},
		{
			name:       "scalar subquery",
			sql:        "insert into constraint_test.emp (sal) select (select ? + ?)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "derived table passthrough",
			sql:        "insert into constraint_test.emp (sal) select x from (select ? + ? as x) d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "nested derived table passthrough",
			sql:        "insert into constraint_test.emp (sal) select x from (select x from (select ? + ? as x) d1) d2",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "cte passthrough",
			sql:        "insert into constraint_test.emp (sal) with c as (select ? + ? as x) select x from c",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "aliased cte passthrough",
			sql:        "insert into constraint_test.emp (sal) with c(y) as (select ? + ?) select q.y from c q",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "derived column alias passthrough",
			sql:        "insert into constraint_test.emp (sal) select d.y from (select ? + ?) d(y)",
			want:       types.T_decimal64,
			paramCount: 2,
		},
		{
			name:       "derived qualified star passthrough",
			sql:        "insert into constraint_test.emp (sal, comm) select d.* from (select ? + ? as x, ? + ? as y) d",
			want:       types.T_decimal64,
			paramCount: 4,
		},
		{
			name: "derived union passthrough",
			sql: "insert into constraint_test.emp (sal) select d.x from " +
				"(select ? + ? as x union all select ? + ?) d",
			want:       types.T_decimal64,
			paramCount: 4,
		},
		{
			name: "chained cte passthrough",
			sql: "insert into constraint_test.emp (sal) " +
				"with c as (select ? + ? as x), d as (select x from c) select x from d",
			want:       types.T_decimal64,
			paramCount: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectUniquePlanParamTypes(t, queryPlan)
			require.Len(t, paramTypes, test.paramCount)
			for _, typ := range paramTypes {
				require.Equal(t, int32(test.want), typ.Id)
				if test.want == types.T_decimal64 {
					require.Equal(t, int32(7), typ.Width)
					require.Equal(t, int32(2), typ.Scale)
				}
			}
		})
	}
}

func TestPreparedNumericContextUsesSampleSuffixTarget(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmt, err := mysql.ParseOne(
		optimizer.CurrentContext().GetContext(),
		"insert into constraint_test.emp (sal, deptno, comm) "+
			"select ? + ?, sample(n_nationkey, 1 rows), ? + ? from nation",
		1,
	)
	require.NoError(t, err)

	queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmt, true)
	require.NoError(t, err)

	paramTypes := collectUniquePlanParamTypes(t, queryPlan)
	require.Len(t, paramTypes, 4)
	for pos, typ := range paramTypes {
		require.Equal(t, int32(types.T_decimal64), typ.Id)
		require.Equal(t, int32(7), typ.Width, "parameter %d has the wrong width", pos)
		require.Equal(t, int32(2), typ.Scale, "parameter %d has the wrong scale", pos)
	}
}

func TestPreparedNumericContextUsesLegacyInsertSelectTarget(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	stmt, err := mysql.ParseOne(
		optimizer.CurrentContext().GetContext(),
		"insert into ext(v) select ? + ? from nation",
		1,
	)
	require.NoError(t, err)

	insertStmt := stmt.(*tree.Insert)
	tableDef := &planpb.TableDef{
		Name:      "ext",
		TableType: catalog.SystemExternalRel,
		Cols: []*planpb.ColDef{{
			Name: "v",
			Typ:  planpb.Type{Id: int32(types.T_decimal64), Width: 7, Scale: 2},
		}},
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, optimizer.CurrentContext(), true, false)
	bindCtx := NewBindContext(builder, nil)
	info := &dmlSelectInfo{tblInfo: &dmlTableInfo{
		tableDefs: []*planpb.TableDef{tableDef},
		objRef:    []*planpb.ObjectRef{{ObjName: "ext"}},
	}}

	_, _, _, err = initInsertStmt(builder, bindCtx, insertStmt, info)
	require.NoError(t, err)

	paramTypes := collectUniquePlanParamTypes(t, &Plan{Plan: &planpb.Plan_Query{Query: builder.qry}})
	require.Len(t, paramTypes, 2)
	for _, typ := range paramTypes {
		require.Equal(t, int32(types.T_decimal64), typ.Id)
		require.Equal(t, int32(7), typ.Width)
		require.Equal(t, int32(2), typ.Scale)
	}
}

func collectUniquePlanParamTypes(t *testing.T, queryPlan *Plan) map[int32]planpb.Type {
	t.Helper()
	result := make(map[int32]planpb.Type)
	query := queryPlan.GetQuery()
	if query == nil {
		return result
	}
	for _, node := range query.Nodes {
		for _, expr := range node.ProjectList {
			collectExprParamTypesByPos(t, expr, result)
		}
		for _, expr := range node.FilterList {
			collectExprParamTypesByPos(t, expr, result)
		}
		for _, expr := range node.GroupBy {
			collectExprParamTypesByPos(t, expr, result)
		}
		for _, expr := range node.AggList {
			collectExprParamTypesByPos(t, expr, result)
		}
		for _, expr := range node.OnUpdateExprs {
			collectExprParamTypesByPos(t, expr, result)
		}
		if dedup := node.DedupJoinCtx; dedup != nil {
			for _, expr := range dedup.UpdateColExprList {
				collectExprParamTypesByPos(t, expr, result)
			}
		}
		if rowset := node.RowsetData; rowset != nil {
			for _, col := range rowset.Cols {
				for _, data := range col.Data {
					collectExprParamTypesByPos(t, data.Expr, result)
				}
			}
		}
	}
	return result
}

func collectExprParamTypesByPos(t *testing.T, expr *planpb.Expr, result map[int32]planpb.Type) {
	t.Helper()
	collectExprEffectiveParamPlanTypesByPos(expr, planpb.Type{}, func(pos int32, typ planpb.Type) {
		if previous, ok := result[pos]; ok {
			require.Equal(t, previous.Id, typ.Id, "parameter %d has inconsistent type id", pos)
			require.Equal(t, previous.Width, typ.Width, "parameter %d has inconsistent width", pos)
			require.Equal(t, previous.Scale, typ.Scale, "parameter %d has inconsistent scale", pos)
			return
		}
		result[pos] = typ
	})
}

func collectExprEffectiveParamPlanTypesByPos(
	expr *planpb.Expr,
	inherited planpb.Type,
	collect func(int32, planpb.Type),
) {
	if expr == nil {
		return
	}
	if param := expr.GetP(); param != nil {
		typ := inherited
		if typ.Id == 0 {
			typ = expr.Typ
		}
		collect(param.Pos, typ)
		return
	}
	if fn := expr.GetF(); fn != nil {
		childType := inherited
		if fn.Func != nil && fn.Func.ObjName == "cast" {
			childType = expr.Typ
		}
		for _, arg := range fn.Args {
			collectExprEffectiveParamPlanTypesByPos(arg, childType, collect)
		}
	}
}

func TestPreparedNonNumericAssignmentKeepsTargetType(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{
			name: "update varchar assignment",
			sql:  "update constraint_test.emp set ename = ? where empno = 1",
			want: types.T_text,
		},
		{
			name: "insert values varchar assignment",
			sql:  "insert into constraint_test.emp (empno, ename) values (1, ?)",
			want: types.T_text,
		},
		{
			name: "insert select varchar assignment",
			sql:  "insert into constraint_test.emp (empno, ename) select 1, ?",
			want: types.T_text,
		},
		{
			name: "on duplicate key update date assignment",
			sql: "insert into constraint_test.emp (empno, hiredate) values (1, '2024-01-01') " +
				"on duplicate key update hiredate = ?",
			want: types.T_date,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectUniquePlanParamTypes(t, queryPlan)
			require.Len(t, paramTypes, 1)
			for _, typ := range paramTypes {
				require.Equal(t, int32(test.want), typ.Id)
			}
		})
	}
}

func TestNumericAssignmentTargetKeepsGroupedProjection(t *testing.T) {
	// a numeric assignment target must not re-bind a projection that is itself a
	// GROUP BY key against the raw scan columns; it should resolve to the grouped
	// column and plan without a "must appear in the GROUP BY clause" error.
	tests := []string{
		"insert into constraint_test.emp (sal) select empno + deptno from constraint_test.emp group by empno + deptno",
		"insert into constraint_test.emp (sal) select empno + deptno as s from constraint_test.emp group by empno + deptno",
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
			require.NoError(t, err)

			_, err = BuildPlan(optimizer.CurrentContext(), stmts[0], false)
			require.NoError(t, err)
		})
	}
}

func TestValuesExprIsFuncCall(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	sql := "insert into constraint_test.emp (sal) values " +
		"(1), (-1), (mod(1, 2)), ((mod(1, 2))), (abs(1)), ((abs(1))), (cast(abs(1) as double))"
	stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
	require.NoError(t, err)

	insertStmt, ok := stmts[0].(*tree.Insert)
	require.True(t, ok)
	valuesClause, ok := insertStmt.Rows.Select.(*tree.ValuesClause)
	require.True(t, ok)

	wants := []bool{false, false, false, false, true, true, true}
	require.Len(t, valuesClause.Rows, len(wants))
	for i, want := range wants {
		require.Equal(t, want, valuesExprIsFuncCall(valuesClause.Rows[i][0]), "row %d", i)
	}
}

func TestBindProjectionListWithSampleFunc(t *testing.T) {
	optimizer := NewMockOptimizer(false)
	sql := "select n_name, sample(n_nationkey, 10 rows) from nation group by n_name"
	stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), sql, 1)
	require.NoError(t, err)

	_, err = BuildPlan(optimizer.CurrentContext(), stmts[0], false)
	require.NoError(t, err)
}

func TestPreparedNumericContextUsesUpdateTarget(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want types.T
	}{
		{
			name: "update decimal assignment",
			sql:  "update constraint_test.emp set sal = ? + ? where empno = 1",
			want: types.T_decimal64,
		},
		{
			name: "update double sibling overrides target",
			sql:  "update constraint_test.emp set sal = (? + ?) + cast(1 as double) where empno = 1",
			want: types.T_float64,
		},
		{
			name: "on duplicate key update decimal assignment",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = ? + ?",
			want: types.T_decimal64,
		},
		{
			name: "on duplicate key update scalar subquery",
			sql: "insert into constraint_test.emp (empno, sal) values (1, 1) " +
				"on duplicate key update sal = (select ? + ?)",
			want: types.T_decimal64,
		},
		{
			name: "update from derived source",
			sql: "update constraint_test.emp set sal = d.x " +
				"from (select ? + ? as x) d where emp.empno = 1",
			want: types.T_decimal64,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false)
			stmts, err := mysql.Parse(optimizer.CurrentContext().GetContext(), test.sql, 1)
			require.NoError(t, err)

			queryPlan, err := BuildPlan(optimizer.CurrentContext(), stmts[0], true)
			require.NoError(t, err)

			paramTypes := collectUniquePlanParamTypes(t, queryPlan)
			require.Len(t, paramTypes, 2)
			for _, typ := range paramTypes {
				require.Equal(t, int32(test.want), typ.Id)
			}
			if test.name == "on duplicate key update scalar subquery" {
				for _, node := range queryPlan.GetQuery().Nodes {
					if node.DedupJoinCtx == nil {
						continue
					}
					for _, expr := range node.DedupJoinCtx.UpdateColExprList {
						require.False(t, exprContainsSubqueryRef(expr))
					}
				}
			}
		})
	}
}

func exprContainsSubqueryRef(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetSub() != nil {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if exprContainsSubqueryRef(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprContainsSubqueryRef(item) {
				return true
			}
		}
	}
	return false
}
