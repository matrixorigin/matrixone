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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func TestPreparedMultiInsertLockRowsMatchPrimaryKeyType(t *testing.T) {
	sql := "insert first when n_name = ? then into region (r_regionkey, r_name, r_comment) " +
		"values (n_nationkey + ?, n_name, n_comment) else into test_idx (n_nationkey, n_name) " +
		"values (n_nationkey + ?, n_name) select n_nationkey, n_name, n_comment from nation " +
		"where n_nationkey >= ?"
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t, fmt.Sprintf("prepare stmt1 from '%s'", sql))
	require.NoError(t, err)
	prepare := logicPlan.GetDcl().GetPrepare()
	require.NotNil(t, prepare)
	preservedWrites := preparedDMLWriteExpressions(prepare.Plan.GetQuery())
	preparedLockTargets := 0
	for _, node := range prepare.Plan.GetQuery().Nodes {
		if node.NodeType != plan.Node_LOCK_OP || len(node.Children) != 1 {
			continue
		}
		input := prepare.Plan.GetQuery().Nodes[node.Children[0]]
		for _, target := range node.LockTargets {
			preparedLockTargets++
			expr := input.ProjectList[target.PrimaryColIdxInBat]
			require.Contains(t, preservedWrites, expr, "lock input expression: %s", expr.String())
		}
	}
	require.NotZero(t, preparedLockTargets)
	params := []any{
		ParamValue{Value: "hot", SourceType: types.T_varchar.ToType(), HasSourceType: true},
		ParamValue{Value: "10", SourceType: types.T_int64.ToType(), HasSourceType: true},
		ParamValue{Value: "1000", SourceType: types.T_int64.ToType(), HasSourceType: true},
		ParamValue{Value: "2", SourceType: types.T_int64.ToType(), HasSourceType: true},
	}
	filled, _, err := FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(
		context.Background(), prepare.Plan, params)
	require.NoError(t, err)
	query := filled.GetQuery()
	require.NotNil(t, query)
	lockTargetCount := 0
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_LOCK_OP {
			continue
		}
		require.Len(t, node.Children, 1)
		input := query.Nodes[node.Children[0]]
		for _, target := range node.LockTargets {
			lockTargetCount++
			require.Less(t, int(target.PrimaryColIdxInBat), len(input.ProjectList))
			require.Equal(t, target.PrimaryColTyp.Id,
				input.ProjectList[target.PrimaryColIdxInBat].Typ.Id,
				"lock input expression: %s", input.ProjectList[target.PrimaryColIdxInBat].String())
		}
	}
	require.NotZero(t, lockTargetCount)
}

func countNodeTypes(qry *plan.Query) map[plan.Node_NodeType]int {
	counts := make(map[plan.Node_NodeType]int)
	for _, node := range qry.Nodes {
		counts[node.NodeType]++
	}
	return counts
}

func stepRootTypes(qry *plan.Query) []plan.Node_NodeType {
	typs := make([]plan.Node_NodeType, len(qry.Steps))
	for i, step := range qry.Steps {
		typs[i] = qry.Nodes[step].NodeType
	}
	return typs
}

// collectFilterTree returns every FILTER node reachable from root.
func collectFilterNodes(qry *plan.Query, root int32, out *[]*plan.Node) {
	node := qry.Nodes[root]
	if node.NodeType == plan.Node_FILTER {
		*out = append(*out, node)
	}
	for _, child := range node.Children {
		collectFilterNodes(qry, child, out)
	}
}

func TestMultiInsertUnconditionalFansOutOverOneSink(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"insert all into dept into t2 (a, b) values (deptno, deptno + 1) select deptno, dname, loc from dept")
	require.NoError(t, err)
	qry := logicPlan.GetQuery()
	require.Equal(t, plan.Query_INSERT, qry.StmtType)

	// step 0 materializes the source; one write step per INTO clause.
	require.Equal(t, []plan.Node_NodeType{plan.Node_SINK, plan.Node_MULTI_UPDATE, plan.Node_MULTI_UPDATE}, stepRootTypes(qry))
	counts := countNodeTypes(qry)
	require.Equal(t, 1, counts[plan.Node_SINK])
	require.Equal(t, 2, counts[plan.Node_SINK_SCAN])
	require.Equal(t, 2, counts[plan.Node_MULTI_UPDATE])
	require.Zero(t, counts[plan.Node_FILTER], "unconditional INSERT ALL routes every row to every target")

	// dept has a PK plus a unique and a secondary index: its MULTI_UPDATE writes
	// all three tables; t2 has only a PK.
	var deptCtxs, t2Ctxs int
	for _, node := range qry.Nodes {
		if node.NodeType != plan.Node_MULTI_UPDATE {
			continue
		}
		switch node.UpdateCtxList[0].TableDef.Name {
		case "dept":
			deptCtxs = len(node.UpdateCtxList)
		case "t2":
			t2Ctxs = len(node.UpdateCtxList)
		}
	}
	require.Equal(t, 3, deptCtxs)
	require.Equal(t, 1, t2Ctxs)
	testDeepCopy(logicPlan)
}

func TestMultiInsertFirstAndElseRouting(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"insert first"+
			" when deptno < 10 then into t2 (a, b) values (deptno, deptno)"+
			" when deptno < 20 then into t3 (a) values (deptno)"+
			" else into dept (deptno, dname) values (deptno * 2, dname)"+
			" select deptno, dname, loc from dept")
	require.NoError(t, err)
	qry := logicPlan.GetQuery()
	require.Len(t, qry.Steps, 4)

	// Each target step carries exactly one FILTER with a single conjunct. With
	// INSERT FIRST the route column already names the WHEN that claimed the
	// row, so a branch is one integer test: no exclusion terms for a WHEN, and
	// ELSE is the single "nothing claimed it" value rather than one term per
	// WHEN.
	routeTest := func(t *testing.T, step int32) int32 {
		t.Helper()
		var filters []*plan.Node
		collectFilterNodes(qry, step, &filters)
		require.Len(t, filters, 1)
		require.Len(t, filters[0].FilterList, 1)
		fn := filters[0].FilterList[0].GetF()
		require.NotNil(t, fn)
		require.Equal(t, "=", fn.Func.ObjName)
		require.NotNil(t, fn.Args[0].GetCol(), "the test must read a materialized column, not re-evaluate")
		lit := fn.Args[1].GetLit()
		require.NotNil(t, lit)
		return lit.GetI32Val()
	}
	// the two WHEN branches select their own route value, in order
	require.Equal(t, int32(0), routeTest(t, qry.Steps[1]))
	require.Equal(t, int32(1), routeTest(t, qry.Steps[2]))
	// ELSE selects the no-route value
	require.Equal(t, noMultiInsertRoute, routeTest(t, qry.Steps[3]))
	// every branch reads the same materialized column
	routeCols := map[int32]int{}
	for _, step := range qry.Steps[1:] {
		var filters []*plan.Node
		collectFilterNodes(qry, step, &filters)
		routeCols[filters[0].FilterList[0].GetF().Args[0].GetCol().ColPos]++
	}
	require.NotEmpty(t, routeCols)
	testDeepCopy(logicPlan)
}

// INSERT FIRST must not evaluate a later WHEN for a row an earlier WHEN
// already claimed: an unreachable predicate that errors would otherwise turn a
// correct first-match route into a statement failure. The plan expresses this
// by nesting each later claim inside the false branch of
// if(route >= 0, route, ...), and EvalIff evaluates a false branch only on the
// rows whose condition was false.
func TestMultiInsertFirstMasksLaterConditions(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"insert first"+
			" when deptno < 1 then into t3 (a) values (deptno)"+
			" when deptno < 2 then into t2 (a, b) values (deptno, deptno)"+
			" when deptno < 3 then into dept (deptno, dname) values (deptno, dname)"+
			" select deptno, dname from dept")
	require.NoError(t, err)
	qry := logicPlan.GetQuery()

	// The route is one nested chain, outermost WHEN first:
	//
	//   if(cond_0, 0, if(cond_1, 1, if(cond_2, 2, -1)))
	//
	// Each level's false branch is the rest of the chain, which EvalIff runs
	// only on the rows the level did not claim -- that is the laziness. The
	// nesting order is the first-match order, and nothing rebuilds a prefix
	// from the earlier conditions.
	var routes []*plan.Expr
	for _, node := range qry.Nodes {
		for _, expr := range node.ProjectList {
			if fn := expr.GetF(); fn != nil && (fn.Func.ObjName == "if" || fn.Func.ObjName == "iff") {
				routes = append(routes, expr)
			}
		}
	}
	require.Len(t, routes, 1, "the whole route is a single projected expression")

	expr := routes[0]
	for i := 0; i < 3; i++ {
		fn := expr.GetF()
		require.NotNil(t, fn, "level %d", i)
		require.Contains(t, []string{"if", "iff"}, fn.Func.ObjName, "level %d", i)
		require.Len(t, fn.Args, 3)
		require.NotNil(t, fn.Args[0].GetF(), "level %d: the condition itself is the test", i)
		lit := fn.Args[1].GetLit()
		require.NotNil(t, lit, "level %d: a claimed row takes the WHEN index", i)
		require.Equal(t, int32(i), lit.GetI32Val(), "the chain is in first-match order")
		expr = fn.Args[2]
	}
	last := expr.GetLit()
	require.NotNil(t, last, "the innermost false branch is the no-route value")
	require.Equal(t, noMultiInsertRoute, last.GetI32Val())

	// Nothing rebuilds an OR chain over the earlier conditions.
	var walk func(*plan.Expr)
	walk = func(e *plan.Expr) {
		if e == nil {
			return
		}
		if fn := e.GetF(); fn != nil {
			require.NotEqual(t, "or", fn.Func.ObjName, "routing must not rebuild an OR chain")
			for _, arg := range fn.Args {
				walk(arg)
			}
		}
	}
	for _, node := range qry.Nodes {
		for _, e := range node.ProjectList {
			walk(e)
		}
	}

	// Each condition still appears exactly once: masking must not duplicate it.
	var lessThan int
	var countLessThan func(*plan.Expr)
	countLessThan = func(expr *plan.Expr) {
		if expr == nil {
			return
		}
		if fn := expr.GetF(); fn != nil {
			if fn.Func.ObjName == "<" {
				lessThan++
			}
			for _, arg := range fn.Args {
				countLessThan(arg)
			}
		}
	}
	for _, node := range qry.Nodes {
		for _, expr := range node.ProjectList {
			countLessThan(expr)
		}
		for _, expr := range node.FilterList {
			countLessThan(expr)
		}
	}
	require.Equal(t, 3, lessThan, "each of the 3 WHEN predicates must be bound exactly once")
	testDeepCopy(logicPlan)
}

// Every WHEN must be evaluated exactly once, above the shared sink, and the
// targets must read the materialized boolean. Re-binding the predicate per
// branch makes one WHEN occurrence several independent route decisions, which
// breaks INSERT FIRST partitioning and makes two INTOs of one WHEN disagree
// whenever the predicate is volatile (rand()).
func TestMultiInsertEvaluatesEachWhenOnce(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"insert first"+
			" when deptno < 10 then into t2 (a, b) values (deptno, deptno)"+
			"                       into t3 (a) values (deptno)"+
			" when deptno < 20 then into dept (deptno, dname) values (deptno, dname)"+
			" else into t2 (a, b) values (deptno * 2, deptno)"+
			" select deptno, dname from dept")
	require.NoError(t, err)
	qry := logicPlan.GetQuery()

	// The comparison that implements each WHEN appears exactly once in the whole
	// plan: in the selector projection feeding the sink.
	var lessThan int
	var countLessThan func(*plan.Expr)
	countLessThan = func(expr *plan.Expr) {
		if expr == nil {
			return
		}
		if fn := expr.GetF(); fn != nil {
			if fn.Func.ObjName == "<" {
				lessThan++
			}
			for _, arg := range fn.Args {
				countLessThan(arg)
			}
		}
	}
	for _, node := range qry.Nodes {
		for _, expr := range node.ProjectList {
			countLessThan(expr)
		}
		for _, expr := range node.FilterList {
			countLessThan(expr)
		}
	}
	require.Equal(t, 2, lessThan, "each of the 2 WHEN predicates must be bound exactly once")

	// No FILTER re-evaluates a predicate: every conjunct tests the materialized
	// route column against a constant, and every branch reads the same column.
	routeCols := map[int32]int{}
	routeValues := map[int32]int{}
	for _, node := range qry.Nodes {
		if node.NodeType != plan.Node_FILTER {
			continue
		}
		for _, expr := range node.FilterList {
			fn := expr.GetF()
			require.NotNil(t, fn)
			require.Equal(t, "=", fn.Func.ObjName)
			col := fn.Args[0].GetCol()
			require.NotNil(t, col, "a branch must read the materialized route")
			lit := fn.Args[1].GetLit()
			require.NotNil(t, lit)
			routeCols[col.ColPos]++
			routeValues[lit.GetI32Val()]++
		}
	}
	require.NotEmpty(t, routeCols)
	// Both INTO clauses of the first WHEN consume the identical decision, so
	// two branches select route 0; the second WHEN and ELSE take one each.
	require.Equal(t, 2, routeValues[0], "both INTOs of WHEN 1 share one decision")
	require.Equal(t, 1, routeValues[1])
	require.Equal(t, 1, routeValues[noMultiInsertRoute])
	testDeepCopy(logicPlan)
}

func TestMultiInsertAllConditionalDoesNotExcludeEarlierBranches(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"insert all"+
			" when deptno < 10 then into t2 (a, b) values (deptno, deptno)"+
			" when dname = 'x' then into t3 (a) values (deptno)"+
			" select deptno, dname from dept")
	require.NoError(t, err)
	qry := logicPlan.GetQuery()
	require.Len(t, qry.Steps, 3)
	for _, step := range qry.Steps[1:] {
		var filters []*plan.Node
		collectFilterNodes(qry, step, &filters)
		require.Len(t, filters, 1)
		require.Len(t, filters[0].FilterList, 1, "INSERT ALL never negates earlier WHENs")
	}
}

func TestMultiInsertPositionalTargetUsesEverySourceColumn(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"insert all into t3 into t2 (b, a) values (a + 1, a) select a from t3 where a > 0")
	require.NoError(t, err)
	require.Len(t, logicPlan.GetQuery().Steps, 3)
}

func TestMultiInsertWithClauseFeedsSource(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"with s as (select deptno, dname from dept) insert all into t2 (a, b) values (deptno, deptno) select * from s")
	require.NoError(t, err)
	require.Len(t, logicPlan.GetQuery().Steps, 2)
}

func TestMultiInsertRejectsUnsupportedTargets(t *testing.T) {
	mock := NewMockOptimizer(true)
	tests := []struct {
		sql string
		msg string
	}{
		{
			// emp carries a foreign key
			sql: "insert all into emp (empno, ename) values (deptno, dname) into t3 (a) values (deptno) select deptno, dname from dept",
			msg: "foreign key",
		},
		{
			sql: "insert all into t2 (a, b) values (deptno) select deptno, dname from dept",
			msg: "does not match the number of columns",
		},
		{
			// positional insert: source width must match the table
			sql: "insert all into t3 select deptno, dname from dept",
			msg: "does not match the number of columns",
		},
		{
			sql: "insert all into t3 (a) values (no_such_col) select deptno from dept",
			msg: "no_such_col",
		},
		{
			sql: "insert all when no_such_col > 1 then into t3 (a) values (deptno) select deptno from dept",
			msg: "no_such_col",
		},
		{
			sql: "insert all into t2 (a, zzz) values (deptno, dname) select deptno, dname from dept",
			msg: "zzz",
		},
	}
	for _, test := range tests {
		_, err := runOneStmt(mock, t, test.sql)
		require.Error(t, err, test.sql)
		require.Contains(t, err.Error(), test.msg, test.sql)
	}

	clusterMock := NewMockOptimizer(true)
	clusterMock.ctxt.tables["t2"].TableType = catalog.SystemClusterRel
	_, err := runOneStmt(clusterMock, t,
		"insert all into t2 (a, b) values (deptno, dname) select deptno, dname from dept")
	require.ErrorContains(t, err, "multi-table INSERT into cluster table 't2'")
}

func TestMultiInsertSameTableMergesIntoOneWritePipeline(t *testing.T) {
	mock := NewMockOptimizer(true)
	logicPlan, err := runOneStmt(mock, t,
		"insert first"+
			" when deptno < 10 then into dept (deptno, dname) values (deptno, dname)"+
			" else into dept (deptno) values (deptno + 100)"+
			" select deptno, dname from dept")
	require.NoError(t, err)
	qry := logicPlan.GetQuery()

	// Both clauses write dept: one sink step plus ONE write step, whose input is
	// the UNION ALL of the two filtered branches, so cross-clause duplicate keys
	// meet the same in-batch dedup as a single INSERT ... SELECT.
	require.Equal(t, []plan.Node_NodeType{plan.Node_SINK, plan.Node_MULTI_UPDATE}, stepRootTypes(qry))
	counts := countNodeTypes(qry)
	require.Equal(t, 2, counts[plan.Node_SINK_SCAN])
	require.Equal(t, 1, counts[plan.Node_UNION_ALL])
	require.Equal(t, 1, counts[plan.Node_MULTI_UPDATE])
	require.Equal(t, 2, counts[plan.Node_FILTER])

	// The union carries the union of the clauses' column lists (deptno, dname);
	// the second clause fills dname with its default. The single write step
	// still maintains dept's unique and secondary index tables.
	for _, node := range qry.Nodes {
		switch node.NodeType {
		case plan.Node_UNION_ALL:
			require.Len(t, node.ProjectList, 2)
			for _, child := range node.Children {
				require.Len(t, qry.Nodes[child].ProjectList, 2)
			}
		case plan.Node_MULTI_UPDATE:
			require.Len(t, node.UpdateCtxList, 3)
		}
	}
	testDeepCopy(logicPlan)
}

func TestMultiInsertSameTableMixedColumnListsRejectsMissingNotNull(t *testing.T) {
	mock := NewMockOptimizer(true)
	// t2.b is NOT NULL without a default: a clause that leaves it out cannot be
	// widened, exactly like "insert into t2 (a) ..." fails.
	_, err := runOneStmt(mock, t,
		"insert all into t2 (a, b) values (deptno, deptno) into t2 (a) values (deptno + 1) select deptno from dept")
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid default value for column 'b'")
}

func TestMultiInsertKeepsIrregularIndexMaintenance(t *testing.T) {
	mock := NewMockOptimizer(true)
	for _, sql := range []string{
		// single clause
		"insert all into docs_ft (id, body) values (deptno, dname) select deptno, dname from dept",
		// two clauses merged into one write pipeline
		"insert all into docs_ft (id, body) values (deptno, dname) into docs_ft (id, body) values (deptno + 10, loc) select deptno, dname, loc from dept",
	} {
		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)
		qry := logicPlan.GetQuery()

		// The fulltext index is not a MULTI_UPDATE target; it is maintained by
		// extra steps that tokenize the materialized new-row image. Those steps
		// must survive the insert helpers stripping irregular indexes.
		var multiUpdates, tokenizers int
		for _, node := range qry.Nodes {
			switch node.NodeType {
			case plan.Node_MULTI_UPDATE:
				multiUpdates++
				require.Len(t, node.UpdateCtxList, 1, sql)
			case plan.Node_FUNCTION_SCAN:
				if node.TableDef != nil && node.TableDef.TblFunc != nil && node.TableDef.TblFunc.Name == "fulltext_index_tokenize" {
					tokenizers++
				}
			}
		}
		require.Equal(t, 1, multiUpdates, sql)
		require.Equal(t, 1, tokenizers, "fulltext maintenance step missing: %s", sql)
		require.Greater(t, len(qry.Steps), 2, sql)
	}
}

// A branch that reads only a subset of the source's columns must still project
// the shared sink correctly: createQuery prunes the unreferenced columns from
// the sink, so the branch SINK_SCAN has to be registered for positional repair.
// Before that registration this shape produced an out-of-range panic at runtime.
func TestMultiInsertBranchSinkScansAreRepositionedAfterSinkPruning(t *testing.T) {
	mock := NewMockOptimizer(true)
	for _, sql := range []string{
		// only the 2nd source column is written
		"insert all into t3 (a) values (dname) select deptno, dname from dept",
		// two targets, the middle source column unused
		"insert all into t3 (a) values (deptno) into t2 (a, b) values (loc, loc) select deptno, dname, loc from dept",
		// a later column referenced only by a WHEN
		"insert first when loc > 0 then into t3 (a) values (loc) select deptno, dname, loc from dept",
	} {
		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)
		qry := logicPlan.GetQuery()

		sinkWidth := len(qry.Nodes[qry.Steps[0]].ProjectList)
		require.Greater(t, sinkWidth, 0, sql)
		for _, node := range qry.Nodes {
			if node.NodeType != plan.Node_SINK_SCAN {
				continue
			}
			for i, expr := range node.ProjectList {
				col := expr.GetCol()
				require.NotNil(t, col, sql)
				require.Less(t, col.ColPos, int32(sinkWidth),
					"sink scan projection %d points past the pruned sink (%s)", i, sql)
			}
		}
		testDeepCopy(logicPlan)
	}
}

// Merged same-table clauses must not mix an explicit auto_increment value with a
// generated one: the union branches feed one PRE_INSERT concurrently, so the
// generated values race the explicit ones and can collide.
func TestClassifyAutoIncrValue(t *testing.T) {
	nullLit := &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	intLit := &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 7}}}}
	zeroLit := &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 0}}}}
	zeroFloat := &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 0}}}}
	zeroStr := &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: " 0 "}}}}
	notNullExpr := &plan.Expr{Typ: plan.Type{NotNullable: true}, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
	nullableExpr := &plan.Expr{Typ: plan.Type{NotNullable: false}, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}

	require.Equal(t, autoIncrGenerated, classifyAutoIncrValue(nil, nil), "an omitted column is generated")
	require.Equal(t, autoIncrGenerated, classifyAutoIncrValue(nullLit, nil), "a listed column holding NULL is still generated")
	require.Equal(t, autoIncrExplicit, classifyAutoIncrValue(intLit, nil))

	// Zero counts as generated in EVERY representation and independently of
	// sql_mode: under NO_AUTO_VALUE_ON_ZERO it is really explicit, but calling
	// it generated only refuses a statement that would have been safe, whereas
	// reading the mode here would bake a session bit into the plan that
	// PRE_INSERT re-reads at EXECUTE time.
	require.Equal(t, autoIncrGenerated, classifyAutoIncrValue(zeroLit, nil))
	require.Equal(t, autoIncrGenerated, classifyAutoIncrValue(zeroFloat, nil), "0.0 reaches PRE_INSERT as 0")
	require.Equal(t, autoIncrGenerated, classifyAutoIncrValue(zeroStr, nil), "'0' reaches PRE_INSERT as 0")

	// Not a constant: could still be zero or NULL per row, so it is refused.
	require.Equal(t, autoIncrUnknown, classifyAutoIncrValue(notNullExpr, nil))
	require.Equal(t, autoIncrUnknown, classifyAutoIncrValue(nullableExpr, nil))
}

func TestMultiInsertRejectsTooManyTargets(t *testing.T) {
	mock := NewMockOptimizer(true)
	clause := " into t3 (a) values (deptno)"
	var sb strings.Builder
	sb.WriteString("insert all")
	for i := 0; i <= maxMultiInsertTargets; i++ {
		sb.WriteString(clause)
	}
	sb.WriteString(" select deptno from dept")
	_, err := runOneStmt(mock, t, sb.String())
	require.Error(t, err)
	require.Contains(t, err.Error(), "INTO clauses")
}

// The merged auto_increment guard must judge the value that reaches PRE_INSERT,
// so every representation of zero and NULL counts as "generated" in the default
// sql_mode, whatever literal the user wrote.
func TestMultiInsertMergedAutoIncrRejectsGeneratedMixes(t *testing.T) {
	rejected := []struct {
		name string
		sql  string
	}{
		{"null and literal", "insert all into auto_t (seq, val) values (null, a) into auto_t (seq, val) values (5, a) select a from t3"},
		{"int zero and literal", "insert all into auto_t (seq, val) values (0, a) into auto_t (seq, val) values (5, a) select a from t3"},
		{"float zero and literal", "insert all into auto_t (seq, val) values (0.0, a) into auto_t (seq, val) values (5, a) select a from t3"},
		{"string zero and literal", "insert all into auto_t (seq, val) values ('0', a) into auto_t (seq, val) values (5, a) select a from t3"},
		{"omitted and literal", "insert all into auto_t (val) values (a) into auto_t (seq, val) values (5, a) select a from t3"},
		{"nullable expression and literal", "insert all into auto_t (seq, val) values (a, a) into auto_t (seq, val) values (5, a) select a from t3"},
	}
	for _, test := range rejected {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			_, err := runOneStmt(mock, t, test.sql)
			require.Error(t, err)
			require.Contains(t, err.Error(), "auto_increment")
		})
	}

	accepted := []struct {
		name string
		sql  string
	}{
		{"all generated", "insert all into auto_t (val) values (a) into auto_t (val) values (a + 1) select a from t3"},
		{"all non-zero literals", "insert all into auto_t (seq, val) values (5, a) into auto_t (seq, val) values (6, a) select a from t3"},
	}
	for _, test := range accepted {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			_, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
		})
	}
}

// The routing bookkeeping must stay LINEAR in the number of WHEN clauses.
// Rebuilding "did any earlier selector match" at every level is O(W^2) in plan
// size and per-row work; the carried flag makes each level O(1). Measured on
// the plan: doubling the WHEN count must not quadruple the boolean bookkeeping.
// TestMultiInsertFirstRoutingWorkIsLinear measures the TOTAL expression work
// the plan carries, not a chosen subset of function names: every expression
// node in every ProjectList and FilterList, which is what Projection.Prepare
// turns into executors and Projection.Call runs for every batch.
//
// Each added WHEN also widens the source and reads a NEW source column, which
// is the shape that exposes per-level re-emission: a chain that re-projects
// every source column and every condition carrier at each of the W levels is
// O(W * (M + C)), and with M and C growing with W that is quadratic even when
// the decision itself is one column. A single projection holding one nested
// route expression is O(M + W).
func TestMultiInsertFirstRoutingWorkIsLinear(t *testing.T) {
	planWork := func(whens int) (exprs int, projects int, widest int) {
		mock := NewMockOptimizer(true)
		var sb strings.Builder
		sb.WriteString("insert first")
		for i := 0; i < whens; i++ {
			sb.WriteString(" when c")
			sb.WriteString(strconv.Itoa(i))
			sb.WriteString(" = 0 then into t3 (a) values (c")
			sb.WriteString(strconv.Itoa(i))
			sb.WriteString(")")
		}
		sb.WriteString(" else into t3 (a) values (c0 + 1000) select ")
		for i := 0; i < whens; i++ {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(strconv.Itoa(i))
			sb.WriteString(" as c")
			sb.WriteString(strconv.Itoa(i))
		}
		sb.WriteString(" from dept")
		logicPlan, err := runOneStmt(mock, t, sb.String())
		require.NoError(t, err)

		var walk func(*plan.Expr)
		walk = func(expr *plan.Expr) {
			if expr == nil {
				return
			}
			exprs++
			if fn := expr.GetF(); fn != nil {
				for _, arg := range fn.Args {
					walk(arg)
				}
			}
		}
		for _, node := range logicPlan.GetQuery().Nodes {
			if node.NodeType == plan.Node_PROJECT {
				projects++
				widest = max(widest, len(node.ProjectList))
			}
			for _, expr := range node.ProjectList {
				walk(expr)
			}
			for _, expr := range node.FilterList {
				walk(expr)
			}
		}
		return exprs, projects, widest
	}

	const small, large = 8, 32
	smallExprs, smallProjects, smallWidest := planWork(small)
	largeExprs, largeProjects, largeWidest := planWork(large)
	require.Positive(t, smallExprs)

	// Quadrupling the WHENs quadruples linear work and multiplies quadratic
	// work by ~16. The slack covers the per-target write pipelines and the
	// widening source, both linear in the WHEN count by construction.
	require.Less(t, largeExprs, smallExprs*6,
		"total plan expression work grew faster than linearly: %d for %d WHENs, %d for %d WHENs",
		smallExprs, small, largeExprs, large)

	// Stated as the per-WHEN contract: each added WHEN costs a bounded amount
	// of expression, projection and projection-width work, independent of how
	// many came before.
	require.Less(t, (largeExprs-smallExprs)/(large-small), 40,
		"each added WHEN must cost a bounded number of expressions (%d -> %d)", smallExprs, largeExprs)
	require.LessOrEqual(t, (largeProjects-smallProjects)/(large-small), 3,
		"each added WHEN must cost a bounded number of projections (%d -> %d)", smallProjects, largeProjects)
	// The routing chain lives in ONE projection, so no projection grows a
	// column per WHEN beyond the source's own width.
	require.LessOrEqual(t, (largeWidest-smallWidest)/(large-small), 2,
		"a projection must not gain more than the source column per WHEN (%d -> %d)", smallWidest, largeWidest)
}

// runOneStmtWithRewriteHints parses like runOneStmt but also applies the
// statement's `/*+ {"rewrites": ...} */` hint, which is what puts a
// RewriteOption on the statement in production.
func runOneStmtWithRewriteHints(opt Optimizer, t *testing.T, sql string) (*Plan, error) {
	t.Helper()
	ctx := opt.CurrentContext()
	stmts, err := parsers.Parse(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.NoError(t, parsers.AddRewriteHints(ctx.GetContext(), stmts, sql))
	defer stmts[0].Free()
	return BuildPlan(ctx, stmts[0], false)
}

// A subquery in a WHEN condition or in a clause's VALUES is another read
// performed by this statement, so it must obey the statement's rewrite policy.
// Binding those subqueries in a fresh root context instead of the source's
// declaration scope let them read the base table directly, which is a
// read-policy bypass.
//
// The rewrite maps `dept` to a relation with a single column `x`, so a
// subquery that still resolves `deptno` proves it read the unrewritten table.
// The source reads `nation`, which the rewrite does not touch, so only the
// branch subquery is under test.
func TestMultiInsertBranchSubqueriesObeyRewritePolicy(t *testing.T) {
	const hint = `/*+ {"rewrites": {"tpch.dept": "select 1 as x"}} */ `
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			"WHEN subquery",
			"insert all when exists (select 1 from dept where deptno = 1)" +
				" then into t2 (a, b) values (1, 2) select n_nationkey from nation",
		},
		{
			"unconditional VALUES subquery",
			"insert all into t2 (a, b) values ((select max(deptno) from dept), 2)" +
				" select n_nationkey from nation",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// the control: without the rewrite the statement plans
			_, err := runOneStmtWithRewriteHints(NewMockOptimizer(true), t, tc.sql)
			require.NoError(t, err)

			// under the rewrite the subquery reads the rewritten relation,
			// which has no deptno, so planning must fail
			_, err = runOneStmtWithRewriteHints(NewMockOptimizer(true), t, hint+tc.sql)
			require.Error(t, err, "the branch subquery bypassed the rewrite policy")
			require.Contains(t, err.Error(), "deptno")
		})
	}
}

// A statement-level CTE must be visible to a subquery in a WHEN condition and
// in a clause's VALUES: the WITH belongs to the statement, and those subqueries
// are reads by that statement. Binding them in a fresh root context made the
// CTE name unresolvable.
func TestMultiInsertBranchSubqueriesSeeStatementCTEs(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{
			"WHEN subquery",
			"with vip as (select n_nationkey as k from nation where n_nationkey < 5)" +
				" insert all when exists (select 1 from vip where k = n_nationkey)" +
				" then into t2 (a, b) values (n_nationkey, 2) select n_nationkey from nation",
		},
		{
			"unconditional VALUES subquery",
			"with vip as (select n_nationkey as k from nation where n_nationkey < 5)" +
				" insert all into t2 (a, b) values ((select max(k) from vip), 2)" +
				" select n_nationkey from nation",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			require.NoError(t, err)
			testDeepCopy(logicPlan)
		})
	}
}

// A statement-level WITH and a WITH on the trailing source SELECT are two
// different lexical scopes, and both can be present. WHEN conditions and
// clause VALUES belong to the statement, so they see the statement's CTEs and
// must NOT see CTEs the source query declared privately — those are declared
// after them and are private to the source.
//
// Collapsing the two scopes (moving the statement WITH into the source, then
// deriving the branch contexts from the source) both rejected valid SQL and
// accepted invalid SQL: the outer CTE went missing whenever the source had its
// own WITH, and the source-local CTE became visible to WHEN/VALUES.
func TestMultiInsertBranchSubqueriesSeeStatementScopeOnly(t *testing.T) {
	const (
		outerWith  = "with outer_vip as (select n_nationkey as k from nation where n_nationkey < 5) "
		sourceWith = " with source_only as (select n_nationkey as k from nation) select k from source_only"
	)
	for _, tc := range []struct {
		name    string
		sql     string
		wantErr string // "" means it must plan
	}{
		{
			// the source declares its own WITH, so the statement's must not be
			// pushed into it and lost
			"WHEN sees the statement CTE alongside a source WITH",
			outerWith + "insert all when exists (select 1 from outer_vip where k = 1)" +
				" then into t2 (a, b) values (k, 2)" + sourceWith,
			"",
		},
		{
			"unconditional VALUES sees the statement CTE alongside a source WITH",
			outerWith + "insert all into t2 (a, b) values ((select max(k) from outer_vip), 2)" +
				sourceWith,
			"",
		},
		{
			"WHEN must not see a source-local CTE",
			"insert all when exists (select 1 from source_only where k = 1)" +
				" then into t2 (a, b) values (k, 2)" + sourceWith,
			"source_only",
		},
		{
			"unconditional VALUES must not see a source-local CTE",
			"insert all into t2 (a, b) values ((select max(k) from source_only), 2)" + sourceWith,
			"source_only",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			if tc.wantErr == "" {
				require.NoError(t, err, "a statement CTE must be visible to the branch")
				testDeepCopy(logicPlan)
				return
			}
			require.Error(t, err, "a source-local CTE must not be visible to the branch")
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// The statement's rewrite policy governs every read the statement performs,
// including the body of a statement-level CTE. preprocessCte snapshots one
// declaration context per CTE and that snapshot copies the policy, so the
// policy has to be installed before it runs; assigning it afterwards reached
// the source and branch contexts but left the CTE bodies reading the
// unrewritten base table.
//
// Only the table-rewrite direction is asserted here: the planner mock's
// Resolve ignores the database name and stamps back whatever was asked for, so
// a `remapdb` policy has no observable effect under it. Both travel on the same
// remapOption, which is what this ordering governs.
func TestMultiInsertStatementCTEObeysRewritePolicy(t *testing.T) {
	for _, tc := range []struct {
		name string
		hint string
		sql  string
	}{
		{
			"table rewrite",
			`/*+ {"rewrites": {"tpch.dept": "select 1 as x"}} */ `,
			"with d as (select deptno from dept) insert all into t2 (a, b)" +
				" values (deptno, 2) select deptno from d",
		},
		{
			// the source declaring its own WITH must not change the policy
			"table rewrite with a source WITH",
			`/*+ {"rewrites": {"tpch.dept": "select 1 as x"}} */ `,
			"with d as (select deptno from dept) insert all into t2 (a, b)" +
				" values (k, 2) with local as (select deptno as k from d) select k from local",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// the control: without the policy the statement plans
			_, err := runOneStmtWithRewriteHints(NewMockOptimizer(true), t, tc.sql)
			require.NoError(t, err)

			// under the policy the CTE body must not reach the raw base table
			_, err = runOneStmtWithRewriteHints(NewMockOptimizer(true), t, tc.hint+tc.sql)
			require.Error(t, err, "the statement CTE body bypassed the rewrite policy")
		})
	}
}
