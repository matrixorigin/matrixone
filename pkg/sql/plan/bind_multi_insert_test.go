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
	// The ELSE step's conditions are all IS NOT TRUE wrappers.
	var elseFilters []*plan.Node
	collectFilterNodes(qry, qry.Steps[3], &elseFilters)
	for _, expr := range elseFilters[0].FilterList {
		require.Equal(t, "isnottrue", expr.GetF().Func.ObjName)
	}
	// The second WHEN keeps its own condition plus one exclusion.
	var secondFilters []*plan.Node
	collectFilterNodes(qry, qry.Steps[2], &secondFilters)
	names := []string{secondFilters[0].FilterList[0].GetF().Func.ObjName, secondFilters[0].FilterList[1].GetF().Func.ObjName}
	require.Contains(t, names, "isnottrue")
	require.Contains(t, names, "<")
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
