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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

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

	// Each target step carries exactly one FILTER: branch i keeps
	// cond_i AND (cond_1 IS NOT TRUE) ... ; ELSE keeps every cond IS NOT TRUE.
	expectedFilterLens := []int{1, 2, 2}
	for i, step := range qry.Steps[1:] {
		var filters []*plan.Node
		collectFilterNodes(qry, step, &filters)
		require.Len(t, filters, 1, "step %d", i+1)
		require.Len(t, filters[0].FilterList, expectedFilterLens[i], "step %d", i+1)
	}
	// The ELSE step's conditions are all IS NOT TRUE over a selector column.
	var elseFilters []*plan.Node
	collectFilterNodes(qry, qry.Steps[3], &elseFilters)
	for _, expr := range elseFilters[0].FilterList {
		fn := expr.GetF()
		require.NotNil(t, fn)
		require.Equal(t, "isnottrue", fn.Func.ObjName)
		require.NotNil(t, fn.Args[0].GetCol(), "IS NOT TRUE must read a materialized selector, not re-evaluate")
	}
	// The second WHEN keeps its own selector plus one exclusion; the selector is
	// a plain column reference, never a re-bound predicate.
	var secondFilters []*plan.Node
	collectFilterNodes(qry, qry.Steps[2], &secondFilters)
	var bare, notTrue int
	for _, expr := range secondFilters[0].FilterList {
		if expr.GetCol() != nil {
			bare++
			continue
		}
		require.Equal(t, "isnottrue", expr.GetF().Func.ObjName)
		notTrue++
	}
	require.Equal(t, 1, bare)
	require.Equal(t, 1, notTrue)
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

	// No FILTER re-evaluates a predicate: every conjunct is either a selector
	// column or IS NOT TRUE over one.
	for _, node := range qry.Nodes {
		if node.NodeType != plan.Node_FILTER {
			continue
		}
		for _, expr := range node.FilterList {
			if expr.GetCol() != nil {
				continue
			}
			fn := expr.GetF()
			require.NotNil(t, fn)
			require.Equal(t, "isnottrue", fn.Func.ObjName)
			require.NotNil(t, fn.Args[0].GetCol())
		}
	}

	// Both INTO clauses of the first WHEN consume the same selector column.
	selectors := map[int32]int{}
	for _, node := range qry.Nodes {
		if node.NodeType != plan.Node_FILTER {
			continue
		}
		for _, expr := range node.FilterList {
			if col := expr.GetCol(); col != nil {
				selectors[col.ColPos]++
			}
		}
	}
	require.NotEmpty(t, selectors)
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
func TestValidateMergedAutoIncrColumns(t *testing.T) {
	tableDef := &plan.TableDef{
		Name: "t",
		Cols: []*plan.ColDef{
			{Name: "seq", Typ: plan.Type{AutoIncr: true}},
			{Name: "val", Typ: plan.Type{}},
		},
	}
	branch := func(cols ...string) *multiInsertBranch {
		return &multiInsertBranch{insertColumns: cols}
	}
	tests := []struct {
		name     string
		branches []*multiInsertBranch
		rejected bool
	}{
		{name: "mixed explicit and generated", branches: []*multiInsertBranch{branch("seq", "val"), branch("val")}, rejected: true},
		{name: "mixed, different case", branches: []*multiInsertBranch{branch("SEQ"), branch("val")}, rejected: true},
		{name: "mixed across three clauses", branches: []*multiInsertBranch{branch("val"), branch("val"), branch("seq")}, rejected: true},
		{name: "every clause sets it", branches: []*multiInsertBranch{branch("seq", "val"), branch("seq")}},
		{name: "no clause sets it", branches: []*multiInsertBranch{branch("val"), branch("val")}},
		{name: "single clause", branches: []*multiInsertBranch{branch("seq", "val")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateMergedAutoIncrColumns(context.Background(), tableDef, test.branches)
			if test.rejected {
				require.Error(t, err)
				require.Contains(t, err.Error(), "auto_increment")
				require.Contains(t, err.Error(), "seq")
			} else {
				require.NoError(t, err)
			}
		})
	}

	// a table without an auto_increment column is never rejected
	plainDef := &plan.TableDef{Name: "t", Cols: []*plan.ColDef{{Name: "a"}, {Name: "b"}}}
	require.NoError(t, validateMergedAutoIncrColumns(context.Background(),
		plainDef, []*multiInsertBranch{branch("a"), branch("b")}))
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
