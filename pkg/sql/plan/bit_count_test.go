// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestBitCountDerivedSubqueryProjectionBinding(t *testing.T) {
	for _, tc := range []struct {
		name    string
		selects string
		want    int32
	}{
		{
			name: "correlated projection materializes",
			selects: `select cast(e.sal as double) x,
				(select max(i.sal) from constraint_test.emp i where i.empno=e.empno) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "row-dependent wrapper materializes",
			selects: `select cast(e.sal as double) x,
				e.sal+(select max(i.sal) from constraint_test.emp i) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "volatile wrapper materializes",
			selects: `select cast(e.sal as double) x,
				rand()+(select max(i.sal) from constraint_test.emp i) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "uncorrelated subquery remains mergeable",
			selects: `select cast(e.sal as double) x,
				(select max(i.sal) from constraint_test.emp i) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "tableless scalar remains reducible",
			selects: `select cast(e.sal as double) x, (select rand()) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "WHERE-only subquery remains mergeable",
			selects: `select cast(e.sal as double) x from constraint_test.emp e
				where exists (select 1 from constraint_test.emp i where i.empno=e.empno)`,
			want: 16,
		},
		{
			name: "direct IN with literal RHS reduces",
			selects: `select cast(e.sal as double) x, e.empno in (select 1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with NULL RHS reduces",
			selects: `select cast(e.sal as double) x, e.empno in (select null) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with volatile RHS reduces",
			selects: `select cast(e.sal as double) x, e.empno in (select rand()) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN reduces nested scalar RHS",
			selects: `select cast(e.sal as double) x, e.empno in (select (select 1)) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN retains nested physical scalar RHS",
			selects: `select cast(e.sal as double) x,
				e.empno in (select (select (select max(i.empno) from constraint_test.emp i))) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct IN keeps wrapped scalar RHS subquery",
			selects: `select cast(e.sal as double) x, e.empno in (select abs((select 1))) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct IN retains subquery through GROUP BY ordinal",
			selects: `select cast(e.sal as double) x,
				e.empno in (select abs((select 1)) group by 1) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct IN keeps filtered empty RHS subquery",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where false) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct IN reduces true-filtered RHS",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where 1=1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN reduces constant-true comparison filter",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where 2>1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with numeric true filter",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where 1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with null test true filter",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where 1 is not null) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with boolean true filter",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where 1=1 or 1=2) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN with NULL filter",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 where null) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct IN reduces DISTINCT RHS",
			selects: `select cast(e.sal as double) x, e.empno in (select distinct 1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN reduces grouped RHS",
			selects: `select cast(e.sal as double) x, e.empno in (select 1 group by 1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN reduces one-row VALUES RHS",
			selects: `select cast(e.sal as double) x, e.empno in (values row(1)) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "direct IN retains multirow VALUES RHS",
			selects: `select cast(e.sal as double) x, e.empno in (values row(1), row(2)) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "NOT IN remains a subquery",
			selects: `select cast(e.sal as double) x, e.empno not in (select 1) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "wrapped IN remains a subquery",
			selects: `select cast(e.sal as double) x, (e.empno in (select 1)) + 0 z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "equality ANY reduces like IN",
			selects: `select cast(e.sal as double) x, e.empno = any (select 1) z
				from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "non-equality ANY remains a subquery",
			selects: `select cast(e.sal as double) x, e.empno > any (select 1) z
				from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			queryPlan, err := runOneStmt(optimizer, t, "select bit_count(x) from ("+tc.selects+") s")
			require.NoError(t, err)
			fn := findPlanFunctionExpr(queryPlan, "bit_count")
			require.NotNil(t, fn)
			_, overload := function.DecodeOverloadID(fn.GetF().GetFunc().GetObj())
			require.Equal(t, tc.want, overload)
		})
	}
}

func TestBitCountCorrelatedScalarProjectionBinding(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want int32
	}{
		{
			name: "direct scalar field",
			sql:  `select bit_count((select e.sal)) from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct scalar explicit cast",
			sql:  `select bit_count((select cast(e.sal as double))) from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "reducible scalar does not materialize sibling projection",
			sql:  `select bit_count(x) from (select cast(e.sal as double) x, (select e.sal) z from constraint_test.emp e) s`,
			want: 16,
		},
		{
			name: "unreduced scalar field",
			sql:  `select bit_count((select i.sal from constraint_test.emp i limit 1))`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "IN subquery retains its dependent left operand",
			sql:  `select bit_count(x) from (select cast(e.sal as double) x, e.sal in (select i.sal from constraint_test.emp i) z from constraint_test.emp e) s`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "uncorrelated tableless scalar remains reducible",
			sql:  `select bit_count((select rand()))`,
			want: function.BitCountFloat64ExpressionOverload,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			table := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")]
			salPos, ok := tableColumnPosition(table, "sal")
			require.True(t, ok)
			table.Cols[salPos].Typ.Id = int32(types.T_float64)
			queryPlan, err := runOneStmt(optimizer, t, tc.sql)
			require.NoError(t, err)
			fn := findPlanFunctionExpr(queryPlan, "bit_count")
			require.NotNil(t, fn)
			_, overload := function.DecodeOverloadID(fn.GetF().GetFunc().GetObj())
			require.Equal(t, tc.want, overload)
		})
	}
}

func TestTableDumpReleasedBitCountBindings(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	const ddl = "create table t (d double not null, g bigint generated always as " +
		"(bit_count(cast(d as double)) + bit_count(cast(-d as double))) stored)"
	current := tableDumpDDLCatalog(tableDumpDDLCreate(t, compiler, ddl))
	released := DeepCopyTableDef(current, true)
	var calls []*pb.Function
	require.NoError(t, pb.VisitExprTree(released.Cols[1].GeneratedCol.Expr, func(e *pb.Expr) error {
		if fn := e.GetF(); fn != nil && fn.Func.ObjName == "bit_count" {
			fid, overload := function.DecodeOverloadID(fn.Func.Obj)
			require.Equal(t, int32(function.BIT_COUNT), fid)
			require.Equal(t, int32(16), overload)
			// 4.2.8 stored BIT_COUNT overload 9 over CAST overload 0.
			fn.Func.Obj = fn.Func.Obj&^0xffffffff | 9
			cast := fn.Args[0].GetF()
			require.Equal(t, "cast", cast.Func.ObjName)
			cast.Func.Obj &^= 0xffffffff
			cast.SyntaxExplicitCast = false
			calls = append(calls, fn)
		}
		return nil
	}))
	require.Len(t, calls, 2)

	sensitive, err := AnalyzeTableDumpBindings(compiler, released, released)
	require.NoError(t, err)
	require.True(t, sensitive, "DUMP must carry the released bound expression")
	sensitive, err = AnalyzeTableDumpBindings(compiler, current, released)
	require.NoError(t, err)
	require.True(t, sensitive)
	calls[0].Func.Obj = calls[0].Func.Obj&^0xffffffff | 16
	calls[0].Args[0].GetF().Func.Obj |= 1
	_, err = AnalyzeTableDumpBindings(compiler, current, released)
	require.ErrorContains(t, err, "cannot verify bound generated expression", "mixed modes have no SQL binding")
}
