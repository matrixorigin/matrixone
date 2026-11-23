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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

// A projected source determines its own conversion. Unrelated SELECT-list
// subqueries must not alter that decision, including when their filters fold.
func TestBitCountProjectionConversionIgnoresSibling(t *testing.T) {
	for _, tc := range []struct {
		name, sibling, limit string
		want                 int32
	}{
		{name: "no sibling", want: 16},
		{name: "true subquery", sibling: ", e.empno in (select 1 where 1=1) z", want: 16},
		{name: "false subquery", sibling: ", e.empno in (select 1 where false) z", want: 16},
		{name: "null subquery", sibling: ", e.empno in (select 1 where null) z", want: 16},
		{name: "unreachable cast", sibling: ", e.empno in (select 1 where length(lpad('a',case when 1=1 then 4 else cast(-1 as unsigned) end,'b'))=4) z", want: 16},
		{name: "correlated subquery", sibling: ", (select max(i.sal) from constraint_test.emp i where i.empno=e.empno) z", want: 16},
		{name: "explicit limit", sibling: ", e.empno in (select 1 where false) z", limit: " limit 1", want: function.BitCountFloat64FieldOverload},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			p, err := runOneStmt(optimizer, t, "select bit_count(x) from (select cast(e.sal as double) x"+tc.sibling+" from constraint_test.emp e"+tc.limit+") s")
			require.NoError(t, err)
			fn := findPlanFunctionExpr(p, "bit_count")
			require.NotNil(t, fn)
			_, overload := function.DecodeOverloadID(fn.GetF().GetFunc().GetObj())
			require.Equal(t, tc.want, overload)
		})
	}
}

// An alias of a selector is a field at its consumer, whether or not a later
// optimizer pass removes the projection. Direct selectors remain lazy.
func TestBitCountProjectedSelectorUsesFieldConversion(t *testing.T) {
	for _, query := range []string{
		"select bit_count(x) from (select if(e.empno=1,e.sal,cast(e.sal as double)) x from constraint_test.emp e) s",
		"select x,bit_count(x) from (select if(e.empno=1,e.sal,cast(e.sal as double)) x from constraint_test.emp e) s",
		"select bit_count(x) from (select if(e.empno=1,e.sal,cast(e.sal as double)) x,e.empno in (select 1 where false) z from constraint_test.emp e) s",
	} {
		t.Run(query, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			p, err := runOneStmt(optimizer, t, query)
			require.NoError(t, err)
			fn := findPlanFunctionExpr(p, "bit_count")
			require.NotNil(t, fn)
			_, overload := function.DecodeOverloadID(fn.GetF().GetFunc().GetObj())
			require.Equal(t, int32(function.BitCountFloat64FieldOverload), overload)
		})
	}
}

func TestBitCountConditionalBinding(t *testing.T) {
	optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
	queryPlan, err := runOneStmt(optimizer, t, `select
		bit_count(case when e.empno=1 then cast(e.sal as double)+0 else cast(e.sal as double) end)
		from constraint_test.emp e`)
	require.NoError(t, err)
	selector := findPlanFunctionExpr(queryPlan, "case")
	require.NotNil(t, selector)
	require.Equal(t, int32(types.T_uint64), selector.Typ.Id)
	for i, want := range []int32{function.BitCountFloat64ExpressionOverload, 16} {
		arg := selector.GetF().GetArgs()[i+1]
		require.NotNil(t, arg.GetF())
		fid, overload := function.DecodeOverloadID(arg.GetF().GetFunc().GetObj())
		require.Equal(t, int32(function.BIT_COUNT), fid)
		require.Equal(t, want, overload)
	}
}

func TestBitCountMergedSelectorFilterReachesJoinInput(t *testing.T) {
	optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
	ctx := optimizer.CurrentContext().(*MockCompilerContext)
	table := ctx.tables["emp"]
	for _, col := range table.Cols {
		if col.Name == "sal" {
			// FLOAT field conversion has no row diagnostic and can be pushed
			// below the join. DOUBLE retains the normal diagnostic barrier.
			col.Typ = pb.Type{Id: int32(types.T_float32)}
		}
	}
	ctx.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")] = table
	p, err := runOneStmt(optimizer, t, `select s.empno
		from (select e.empno, if(e.empno=1, e.sal, cast(e.sal as float)) as x
		      from constraint_test.emp e cross join constraint_test.emp d) s
		where bit_count(s.x)=2`)
	require.NoError(t, err)
	query := p.GetQuery()
	join := findNodeUnder(query, query.Steps[len(query.Steps)-1], pb.Node_JOIN)
	require.NotNil(t, join)
	filtered := 0
	for _, child := range join.Children {
		scan := findNodeUnder(query, child, pb.Node_TABLE_SCAN)
		require.NotNil(t, scan)
		for _, filter := range scan.FilterList {
			if exprCallsFunc(filter, "bit_count") {
				filtered++
			}
		}
	}
	require.Equal(t, 1, filtered, "the merged selector predicate should filter one scan before the cross join")
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
			name: "nested scalar field",
			sql:  `select bit_count((select (select e.sal))) from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
		},
		{
			name: "direct scalar explicit cast",
			sql:  `select bit_count((select cast(e.sal as double))) from constraint_test.emp e`,
			want: 16,
		},
		{
			name: "nested dependent scalar with limit",
			sql:  `select bit_count((select (select cast(e.sal as double) limit 1))) from constraint_test.emp e`,
			want: function.BitCountFloat64FieldOverload,
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
			name: "IN subquery does not change sibling conversion",
			sql:  `select bit_count(x) from (select cast(e.sal as double) x, e.sal in (select i.sal from constraint_test.emp i) z from constraint_test.emp e) s`,
			want: 16,
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

func TestBitCountFoldedSourceSurvivesPlanCopies(t *testing.T) {
	original := &pb.Expr{
		Typ: pb.Type{Id: int32(types.T_float64)},
		Expr: &pb.Expr_Lit{Lit: &pb.Literal{
			Value: &pb.Literal_Dval{Dval: 3.9}, FloatConversionSource: foldedFloatArithmetic,
		}},
	}
	copied := DeepCopyExpr(original)
	require.Equal(t, foldedFloatArithmetic, copied.GetLit().GetFloatConversionSource())
	data, err := copied.Marshal()
	require.NoError(t, err)
	var restored pb.Expr
	require.NoError(t, restored.Unmarshal(data))
	require.Equal(t, foldedFloatArithmetic, restored.GetLit().GetFloatConversionSource())
}

func TestBitCountFoldedSourceDoesNotChangeCatalogDefinitions(t *testing.T) {
	compiler := NewMockCompilerContext(false, newPlanTestProcess(t))
	const ddl = `create table t (d double default (cast(3.9 as double)+0),
		g double generated always as (cast(3.9 as double)+0) stored)`
	table := tableDumpDDLCatalog(tableDumpDDLCreate(t, compiler, ddl))
	for _, expression := range []*pb.Expr{table.Cols[0].Default.Expr, table.Cols[1].GeneratedCol.Expr} {
		require.NoError(t, pb.VisitExprTree(expression, func(expr *pb.Expr) error {
			require.Zero(t, expr.GetLit().GetFloatConversionSource())
			return nil
		}))
	}
	_, err := AnalyzeTableDumpBindings(compiler, table, table)
	require.NoError(t, err)
}

// The nested-scalar shortcut belongs only to BIT_COUNT's direct operand.
// An unrelated scalar in the same statement or in the operand's WHERE keeps
// the ordinary dependent program and its output metadata.
func TestBitCountScalarShortcutStaysWithItsOperand(t *testing.T) {
	for _, sql := range []string{
		`select bit_count((select (select e.sal))), (select (select e.sal)) from constraint_test.emp e`,
		`select bit_count((select max(i.sal) from constraint_test.emp i where (select (select e.sal)) > 0)) from constraint_test.emp e`,
	} {
		t.Run(sql, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			table := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")]
			pos, ok := tableColumnPosition(table, "sal")
			require.True(t, ok)
			table.Cols[pos].Typ.Id = int32(types.T_float64)
			p, err := runOneStmt(optimizer, t, sql)
			require.NoError(t, err)
			require.NotNil(t, p.GetQuery().Correlation)
			require.NoError(t, p.GetQuery().ValidateCorrelation())
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

func TestBitCountLowersReducedScalarSelector(t *testing.T) {
	for _, tc := range []struct {
		name, operand string
		selector      int32
	}{
		{"IF", "if(e.empno=1, e.sal, cast(e.sal as double))", function.IFF},
		{"CASE", "case when e.empno=1 then e.sal else cast(e.sal as double) end", function.CASE},
	} {
		for _, shape := range []struct{ name, prefix, suffix string }{
			{"single", "(select ", ")"},
			{"nested", "(select (select ", "))"},
			{"nested plus", "(select +(select ", "))"},
			{"nested repeated plus", "(select +(+(select ", ")))"},
		} {
			t.Run(tc.name+"/"+shape.name, func(t *testing.T) {
				optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
				table := optimizer.ctxt.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")]
				salPos, ok := tableColumnPosition(table, "sal")
				require.True(t, ok)
				table.Cols[salPos].Typ.Id = int32(types.T_float64)
				operand := shape.prefix + tc.operand + shape.suffix
				p, err := runOneStmt(optimizer, t, "select bit_count("+operand+") from constraint_test.emp e")
				require.NoError(t, err)
				q := p.GetQuery()
				root := q.Nodes[q.Steps[len(q.Steps)-1]]
				require.Len(t, root.ProjectList, 1)
				selector := root.ProjectList[0]
				require.NotNil(t, selector.GetF())
				id, _ := function.DecodeOverloadID(selector.GetF().GetFunc().GetObj())
				require.Equal(t, tc.selector, id)
				require.Equal(t, int32(types.T_uint64), selector.Typ.Id)
				for i, want := range []int32{function.BitCountFloat64FieldOverload, 16} {
					arm := selector.GetF().GetArgs()[i+1]
					require.NotNil(t, arm.GetF())
					id, overload := function.DecodeOverloadID(arm.GetF().GetFunc().GetObj())
					require.Equal(t, int32(function.BIT_COUNT), id)
					require.Equal(t, want, overload)
				}
			})
		}
	}
}

func TestBitCountFilteredSingletonRetainsVolatileRowComparison(t *testing.T) {
	optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
	p, err := runOneStmt(optimizer, t,
		"select (1,0) < (select get_lock('bit_count_group_test',0),1 where true)")
	require.NoError(t, err)
	require.NotNil(t, findPlanFunctionExpr(p, "get_lock"))
}

func TestTablelessSubqueryInlinesLiteralTrueOnly(t *testing.T) {
	for _, tc := range []struct {
		name, subquery string
		wantMark       bool
	}{
		{name: "true", subquery: "select 1 where true"},
		{name: "false", subquery: "select 1 where false", wantMark: true},
		{name: "null", subquery: "select 1 where null", wantMark: true},
		{name: "volatile value", subquery: "select rand() where true", wantMark: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			p, err := runOneStmt(optimizer, t, "select e.empno in ("+tc.subquery+") from constraint_test.emp e")
			require.NoError(t, err)
			q := p.GetQuery()
			found := false
			var visit func(int32)
			visit = func(id int32) {
				node := q.Nodes[id]
				found = found || node.NodeType == pb.Node_JOIN && node.JoinType == pb.Node_MARK
				for _, child := range node.Children {
					visit(child)
				}
			}
			for _, root := range q.Steps {
				visit(root)
			}
			require.Equal(t, tc.wantMark, found)
		})
	}
}

func TestBitCountGroupedSelectorKeepsGroupingContract(t *testing.T) {
	const selector = "if(e.empno=1,e.sal,cast(e.sal as double))"
	for _, tc := range []struct {
		name, mode, operand, keys string
		wantTuple, wantArms       bool
	}{
		{"determinant", "ONLY_FULL_GROUP_BY", selector, "e.empno,e.sal," + selector, false, true},
		{"unproved determinant uses group key", "ONLY_FULL_GROUP_BY", selector, selector, false, false},
		{"permissive representative", "", selector, selector, true, true},
		{"native exact group key", "ONLY_FULL_GROUP_BY,MATRIXONE_NATIVE", selector, selector, false, false},
		{"rollup grouping NULL", "", selector, selector + " with rollup", false, false},
		{"warning-bearing cast", "", "if(cast(e.ename as signed)=1,e.sal,cast(e.sal as double))", "k", false, false},
		{"masked binary-to-text cast", "", "if(e.empno<0,if(e.ename=cast(x'ff' as char(16)),e.sal,cast(e.sal as double)),e.sal)", "k", false, false},
		{"volatile condition", "", "if(rand()>0.5,e.sal,cast(e.sal as double))", "k", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			ctx := optimizer.CurrentContext().(*MockCompilerContext)
			ctx.SetSqlModeOverride(tc.mode)
			table := ctx.tables["emp"]
			for _, col := range table.Cols {
				if col.Name == "sal" {
					col.Typ = pb.Type{Id: int32(types.T_float64)}
				}
			}
			ctx.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")] = table
			selectList := "bit_count(" + tc.operand + ")"
			if tc.keys == "k" {
				selectList = tc.operand + " as k, " + selectList
			}
			p, err := runOneStmt(optimizer, t, "select "+selectList+" from constraint_test.emp e group by "+tc.keys)
			require.NoError(t, err)
			require.Equal(t, tc.wantTuple, findPlanFunctionExpr(p, "serial_full") != nil)
			if !tc.wantArms {
				return
			}
			q := p.GetQuery()
			proj := q.Nodes[q.Steps[len(q.Steps)-1]].ProjectList[0]
			require.NotNil(t, proj.GetF())
			selectorID, _ := function.DecodeOverloadID(proj.GetF().GetFunc().GetObj())
			require.Equal(t, int32(function.IFF), selectorID)
			require.Len(t, proj.GetF().GetArgs(), 3)
			want := []int32{function.BitCountFloat64FieldOverload, 16}
			for i, arg := range proj.GetF().GetArgs()[1:] {
				id, overload := function.DecodeOverloadID(arg.GetF().GetFunc().GetObj())
				require.Equal(t, int32(function.BIT_COUNT), id)
				require.Equal(t, want[i], overload)
			}
		})
	}

	t.Run("ungrouped column remains invalid", func(t *testing.T) {
		optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
		optimizer.ctxt.SetSqlModeOverride("ONLY_FULL_GROUP_BY")
		_, err := runOneStmt(optimizer, t, "select e.empno,bit_count("+selector+") from constraint_test.emp e group by "+selector)
		require.ErrorContains(t, err, "must appear in the GROUP BY clause")
	})
}

func TestBitCountGroupedTextConditionStoresBoolean(t *testing.T) {
	const yes = "if(e.ename='yes',e.sal,cast(e.sal as double))"
	const no = "if(e.ename='no',e.sal,cast(e.sal as double))"
	const mixed = "if(e.ename=e.job,e.sal,cast(e.sal as double))"
	for _, tc := range []struct {
		name, sql string
		bools     int
		wantText  bool
		wantTuple bool
	}{
		{"shared SELECT and HAVING", "select bit_count(" + yes + ") n,bit_count(" + yes + ") from constraint_test.emp e group by " + yes + " having bit_count(" + yes + ")>=0 order by n", 1, false, true},
		{"distinct predicates", "select bit_count(" + yes + "),bit_count(" + no + ") from constraint_test.emp e group by " + yes + "," + no, 2, false, true},
		{"text and value grouped", "select bit_count(" + yes + ") from constraint_test.emp e group by e.ename,e.sal," + yes, 0, false, false},
		{"mixed grouped and input text", "select bit_count(" + mixed + ") from constraint_test.emp e group by e.ename," + mixed, 0, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			ctx := optimizer.CurrentContext().(*MockCompilerContext)
			ctx.SetSqlModeOverride("")
			table := ctx.tables["emp"]
			pos, ok := tableColumnPosition(table, "sal")
			require.True(t, ok)
			table.Cols[pos].Typ = pb.Type{Id: int32(types.T_float64)}
			ctx.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")] = table
			p, err := runOneStmt(optimizer, t, tc.sql)
			require.NoError(t, err)
			tuple := findPlanFunctionExpr(p, "serial_full")
			if !tc.wantTuple {
				require.Nil(t, tuple)
				return
			}
			require.NotNil(t, tuple)
			bools := 0
			foundText := false
			for _, arg := range tuple.GetF().Args {
				if types.T(arg.Typ.Id) == types.T_bool {
					bools++
				}
				foundText = foundText || types.T(arg.Typ.Id).IsMySQLString()
			}
			require.Equal(t, tc.bools, bools)
			require.Equal(t, tc.wantText, foundText)
		})
	}
}

func TestBitCountSharedImplicitGroupRepresentative(t *testing.T) {
	const selector = "if(e.empno=1,e.sal,cast(e.sal as double))"
	for _, tc := range []struct {
		name, project, suffix string
		fields                int
	}{
		{"expression and having", "if(e.empno=1,bit_count(" + selector + "),0) n", " having e.ename is not null order by n", 3},
		{"having only", "count(*)", " having bit_count(" + selector + ")=1", 2},
		{"legacy having", "count(*)", " having e.ename is not null", 0},
		{"legacy HAVING BETWEEN", "count(*)", " having e.empno between e.empno and 2", 0},
		{"HAVING BETWEEN shares row", "bit_count(" + selector + ")", " having e.empno between e.empno and 2", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			ctx := optimizer.CurrentContext().(*MockCompilerContext)
			ctx.SetSqlModeOverride("")
			table := ctx.tables["emp"]
			pos, ok := tableColumnPosition(table, "sal")
			require.True(t, ok)
			table.Cols[pos].Typ = pb.Type{Id: int32(types.T_float64)}
			ctx.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")] = table
			p, err := runOneStmt(optimizer, t, "select "+tc.project+" from constraint_test.emp e group by "+selector+tc.suffix)
			require.NoError(t, err)
			var tuples []*Expr
			for _, node := range p.GetQuery().Nodes {
				for _, agg := range node.AggList {
					args := agg.GetF().GetArgs()
					if len(args) == 1 && args[0].GetF().GetFunc().GetObjName() == "serial_full" {
						tuples = append(tuples, args[0])
					}
				}
			}
			if tc.fields == 0 {
				require.Empty(t, tuples)
				return
			}
			require.Len(t, tuples, 1)
			require.Len(t, tuples[0].GetF().GetArgs(), tc.fields)
		})
	}
}

func TestBitCountGroupedHavingBoundReads(t *testing.T) {
	const selector = "if(e.empno=1,e.sal,cast(e.sal as double))"
	const predicate = " having length(e.ename) between 1 and 3 and length(e.job)>0"
	for _, tc := range []struct {
		name, project string
		shared        bool
		explicit      bool
	}{
		{"qualified ENUM and SET", "bit_count(" + selector + ")", true, false},
		{"separate explicit aggregate", "bit_count(" + selector + "), any_value(e.ename)", true, true},
		{"inactive representative", "count(*)", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
			ctx := optimizer.CurrentContext().(*MockCompilerContext)
			ctx.SetSqlModeOverride("")
			table := ctx.tables["emp"]
			for name, typ := range map[string]pb.Type{
				"sal":   {Id: int32(types.T_float64)},
				"ename": {Id: int32(types.T_enum), Enumvalues: "yes,no"},
				"job":   {Id: int32(types.T_uint64), Enumvalues: "z,a"},
			} {
				pos, ok := tableColumnPosition(table, name)
				require.True(t, ok)
				table.Cols[pos].Typ = typ
			}
			ctx.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")] = table
			p, err := runOneStmt(optimizer, t, "select "+tc.project+" from constraint_test.emp e group by "+selector+predicate)
			require.NoError(t, err)
			tuple := findPlanFunctionExpr(p, "serial_full")
			if !tc.shared {
				require.Nil(t, tuple)
				return
			}
			require.NotNil(t, tuple)
			var domains []string
			for _, input := range tuple.GetF().Args {
				require.NotNil(t, input.GetCol())
				if input.Typ.Enumvalues != "" {
					domains = append(domains, input.Typ.Enumvalues)
				}
			}
			require.Len(t, tuple.GetF().Args, 4, "BETWEEN copies must reuse the ENUM field")
			require.ElementsMatch(t, []string{"yes,no", "z,a"}, domains)
			filterDomains := make(map[string]bool)
			independentEnum := false
			for _, node := range p.GetQuery().Nodes {
				for _, filter := range node.FilterList {
					require.NoError(t, pb.VisitExprTree(filter, func(expr *Expr) error {
						fn := expr.GetF()
						if fn.GetFunc().GetObjName() == "serial_extract" {
							if domain := fn.Args[2].Typ.Enumvalues; domain != "" {
								filterDomains[domain] = true
							}
						}
						return nil
					}))
				}
				for _, agg := range node.AggList {
					fn := agg.GetF()
					if fn.GetFunc().GetObjName() == "any_value" && fn.Args[0].GetF().GetFunc().GetObjName() == moEnumCastIndexToValueFun {
						independentEnum = true
					}
				}
			}
			require.Equal(t, map[string]bool{"yes,no": true, "z,a": true}, filterDomains)
			require.Equal(t, tc.explicit, independentEnum, "the explicit ENUM aggregate keeps its own input")
		})
	}
}

func TestBitCountGroupedOuterJoinRowPresence(t *testing.T) {
	optimizer := NewMockOptimizer(false, newPlanTestProcess(t))
	ctx := optimizer.CurrentContext().(*MockCompilerContext)
	ctx.SetSqlModeOverride("")
	ctx.isDml = true // Keep the hidden row identity exposed by the engine catalog.
	left := ctx.tables["emp"]
	pos, ok := tableColumnPosition(left, "sal")
	require.True(t, ok)
	left.Cols[pos].Typ = pb.Type{Id: int32(types.T_float64)}
	ctx.tablesByQualifiedName[mockQualifiedTableName("constraint_test", "emp")] = left

	ddl, err := runOneStmt(optimizer, t, `create table constraint_test.group_virtual (
		id int primary key, ts timestamp null, g int generated always as (year(ts)) virtual)`)
	require.NoError(t, err)
	right := ddl.GetDdl().GetCreateTable().GetTableDef()
	require.NotNil(t, right)
	right.TblId = 9001
	// A table read from the engine catalog exposes a hidden row identity.
	if _, ok := tableColumnPosition(right, catalog.Row_ID); !ok {
		right.Cols = append(right.Cols, &pb.ColDef{Name: catalog.Row_ID, Hidden: true,
			ColId: uint64(len(right.Cols)), Typ: types.T_Rowid.ToType().PlanType()})
	}
	right.Name2ColIndex = make(map[string]int32, len(right.Cols))
	for i, col := range right.Cols {
		right.Name2ColIndex[col.Name] = int32(i)
	}
	rightName := mockQualifiedTableName("constraint_test", "group_virtual")
	ctx.tablesByQualifiedName[rightName] = right
	ctx.objectsByQualifiedName[rightName] = &pb.ObjectRef{SchemaName: "constraint_test", ObjName: "group_virtual", Obj: int64(right.TblId)}

	p, err := runOneStmt(optimizer, t, `select r.g, r.g+1, s.g,
		bit_count(if(l.empno=1,l.sal,cast(l.sal as double)))
		from constraint_test.emp l left join constraint_test.group_virtual r on l.empno=r.id
		left join constraint_test.group_virtual s on l.mgr=s.id
		group by if(l.empno=1,l.sal,cast(l.sal as double))
		having r.g is null order by ifnull(s.g,r.g)`)
	require.NoError(t, err)
	tuple := findPlanFunctionExpr(p, "serial_full")
	require.NotNil(t, tuple)
	guards := make(map[[2]int32]struct{})
	count := 0
	for _, input := range tuple.GetF().Args {
		fn := input.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) != 1 || fn.Args[0].Typ.Id != int32(types.T_Rowid) {
			continue
		}
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		require.Equal(t, int32(function.ISNULL), id)
		require.Equal(t, int32(types.T_bool), input.Typ.Id)
		rowID := fn.Args[0].GetCol()
		require.NotNil(t, rowID)
		guards[[2]int32{rowID.RelPos, rowID.ColPos}] = struct{}{}
		count++
	}
	require.Len(t, guards, 2, "each nullable join binding contributes one presence field")
	require.Equal(t, len(guards), count, "repeated reads reuse each binding's presence field")
	for _, node := range p.GetQuery().Nodes {
		for _, agg := range node.AggList {
			fn := agg.GetF()
			if fn.GetFunc().GetObjName() == "any_value" {
				require.NotEqual(t, int32(types.T_Rowid), fn.Args[0].Typ.Id, "no separate row identity aggregate")
			}
		}
	}
}
