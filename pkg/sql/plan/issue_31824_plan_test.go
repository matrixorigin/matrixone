// Copyright 2021 - 2026 Matrix Origin
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

type repeatedTargetDedupGuard struct {
	rowNumber *plan.ColRef
	active    *plan.ColRef
}

func repeatedTargetFunctionsAtCol(query *plan.Query, pos int32, name string) []*plan.Function {
	var functions []*plan.Function
	for _, node := range query.Nodes {
		if int(pos) >= len(node.ProjectList) {
			continue
		}
		fn := node.ProjectList[pos].GetF()
		if fn != nil && fn.GetFunc().GetObjName() == name {
			functions = append(functions, fn)
		}
	}
	return functions
}

func repeatedTargetExtractGuard(
	t *testing.T, query *plan.Query, dedup *plan.Node,
) repeatedTargetDedupGuard {
	t.Helper()
	require.Len(t, dedup.OnList, 1)
	joinEQ := dedup.OnList[0].GetF()
	require.NotNil(t, joinEQ)
	require.Equal(t, "=", joinEQ.GetFunc().GetObjName())
	require.Len(t, joinEQ.Args, 2)
	parseGuard := func(guardIF *plan.Function) (repeatedTargetDedupGuard, bool) {
		if guardIF == nil || guardIF.GetFunc().GetObjName() != "if" || len(guardIF.Args) != 3 {
			return repeatedTargetDedupGuard{}, false
		}
		guardAND := guardIF.Args[0].GetF()
		if guardAND == nil || guardAND.GetFunc().GetObjName() != "and" || len(guardAND.Args) != 2 {
			return repeatedTargetDedupGuard{}, false
		}
		selectedEQ := guardAND.Args[0].GetF()
		if selectedEQ == nil || selectedEQ.GetFunc().GetObjName() != "=" || len(selectedEQ.Args) != 2 ||
			selectedEQ.Args[0].GetCol() == nil || guardAND.Args[1].GetCol() == nil {
			return repeatedTargetDedupGuard{}, false
		}
		return repeatedTargetDedupGuard{
			rowNumber: selectedEQ.Args[0].GetCol(),
			active:    guardAND.Args[1].GetCol(),
		}, true
	}
	if guard, ok := parseGuard(joinEQ.Args[1].GetF()); ok {
		return guard
	}

	// Some finalized plans retain a projection reference instead of inlining the guard.
	keyRef := joinEQ.Args[1].GetCol()
	require.NotNil(t, keyRef)
	var guards []repeatedTargetDedupGuard
	for _, guardIF := range repeatedTargetFunctionsAtCol(query, keyRef.ColPos, "if") {
		if guard, ok := parseGuard(guardIF); ok {
			guards = append(guards, guard)
		}
	}
	require.Len(t, guards, 1, "the DEDUP key must have one unambiguous row-level guard")
	return guards[0]
}

func repeatedTargetDedupsByIndexTable(query *plan.Query) map[string][]*plan.Node {
	result := make(map[string][]*plan.Node)
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_DEDUP ||
			len(node.Children) != 2 {
			continue
		}
		indexScan := query.Nodes[node.Children[0]]
		if indexScan.TableDef == nil {
			continue
		}
		result[indexScan.TableDef.Name] = append(result[indexScan.TableDef.Name], node)
	}
	return result
}

func TestUpdateRepeatedPhysicalTargetUniqueDedupPlan(t *testing.T) {
	t.Run("one merged physical check per unique index", func(t *testing.T) {
		mock := NewMockOptimizer(true, newPlanTestProcess(t))
		tableDef := mock.ctxt.tables["dept"]
		require.Len(t, tableDef.Indexes, 2)
		secondIndex := *tableDef.Indexes[0]
		secondIndex.IndexTableName = tableDef.Indexes[1].IndexTableName
		secondIndex.Parts = []string{"loc"}
		tableDef.Indexes[1] = &secondIndex
		logicPlan, err := runOneStmt(mock, t,
			"UPDATE dept a JOIN dept b ON a.deptno = b.deptno "+
				"SET a.dname = 'a', a.loc = 'a', b.dname = 'b', b.loc = 'b'")
		require.NoError(t, err)
		query := logicPlan.GetQuery()
		dedups := repeatedTargetDedupsByIndexTable(query)

		var previous *repeatedTargetDedupGuard
		for _, indexDef := range tableDef.Indexes {
			indexDedups := dedups[indexDef.IndexTableName]
			require.Len(t, indexDedups, 1,
				"each physical unique index must be checked once, not once per alias")
			require.Equal(t, plan.Node_FAIL, indexDedups[0].OnDuplicateAction)
			guard := repeatedTargetExtractGuard(t, query, indexDedups[0])
			active := repeatedTargetFunctionsAtCol(query, guard.active.ColPos, "or")
			require.Len(t, active, 1,
				"the unique guard must use the merged physical active selector")
			require.Len(t, active[0].Args, 2)
			require.Equal(t, guard.active.ColPos+1, guard.rowNumber.ColPos,
				"the merged active selector and its physical row_number must be adjacent")
			if previous != nil {
				require.Equal(t, previous.rowNumber, guard.rowNumber,
					"all physical unique checks must share the merged row_number")
				require.Equal(t, previous.active, guard.active)
			}
			previous = &guard
		}
	})

	t.Run("NULL assignment adds no independent alias check", func(t *testing.T) {
		mock := NewMockOptimizer(true, newPlanTestProcess(t))
		tableDef := mock.ctxt.tables["dept"]
		logicPlan, err := runOneStmt(mock, t,
			"UPDATE dept a JOIN dept b ON a.deptno = b.deptno "+
				"SET a.dname = NULL, b.dname = NULL")
		require.NoError(t, err)
		dedups := repeatedTargetDedupsByIndexTable(logicPlan.GetQuery())
		require.Len(t, dedups[tableDef.Indexes[0].IndexTableName], 1)
		require.Empty(t, dedups[tableDef.Indexes[1].IndexTableName])
	})

	t.Run("ordinary non unique index adds no check", func(t *testing.T) {
		mock := NewMockOptimizer(true, newPlanTestProcess(t))
		tableDef := mock.ctxt.tables["dept"]
		logicPlan, err := runOneStmt(mock, t,
			"UPDATE dept a JOIN dept b ON a.deptno = b.deptno "+
				"SET a.loc = 'a', b.loc = 'b'")
		require.NoError(t, err)
		dedups := repeatedTargetDedupsByIndexTable(logicPlan.GetQuery())
		require.Empty(t, dedups[tableDef.Indexes[0].IndexTableName])
		require.Empty(t, dedups[tableDef.Indexes[1].IndexTableName])
	})
}

func TestUpdateIgnoreRepeatedPhysicalTargetKeepsAliasChecks(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	tableDef := mock.ctxt.tables["dept"]
	require.Len(t, tableDef.Indexes, 2)
	secondIndex := *tableDef.Indexes[0]
	secondIndex.IndexTableName = tableDef.Indexes[1].IndexTableName
	secondIndex.Parts = []string{"loc"}
	tableDef.Indexes[1] = &secondIndex
	logicPlan, err := runOneStmt(mock, t,
		"UPDATE IGNORE dept a JOIN dept b ON a.deptno = b.deptno "+
			"SET a.dname = 'a', a.loc = 'a', b.dname = 'b', b.loc = 'b'")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	dedups := repeatedTargetDedupsByIndexTable(query)

	for _, indexDef := range tableDef.Indexes {
		indexDedups := dedups[indexDef.IndexTableName]
		require.Greater(t, len(indexDedups), 1,
			"UPDATE IGNORE must retain independent pre-merge alias checks")
		for _, dedup := range indexDedups {
			require.Equal(t, plan.Node_IGNORE, dedup.OnDuplicateAction)
		}
	}
}
