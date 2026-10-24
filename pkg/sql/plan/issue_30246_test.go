// Copyright 2026 Matrix Origin
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
	"testing"

	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// empUnderCascadeSetNullOverlap wires the mock emp table with two foreign keys
// to dept: one ON DELETE CASCADE (deptno) and one ON DELETE SET NULL (mgr),
// so a child row can own both actions. fkOrder selects which constraint is
// declared first (#30246: the outcome must not depend on declaration order).
func empUnderCascadeSetNullOverlap(t *testing.T, mock *MockOptimizer, fkOrder []plan.ForeignKeyDef_RefAction) {
	t.Helper()

	const (
		mockDeptTableID uint64 = 88888
		mockEmpTableID  uint64 = 88889
	)

	empTable := mock.ctxt.tables["emp"]
	require.NotNil(t, empTable)
	require.NotEmpty(t, empTable.Fkeys)
	deptTable := mock.ctxt.tables["dept"]
	require.NotNil(t, deptTable)

	delete(mock.ctxt.id2name, empTable.TblId)
	delete(mock.ctxt.id2name, deptTable.TblId)
	empTable.TblId = mockEmpTableID
	deptTable.TblId = mockDeptTableID
	mock.ctxt.id2name[mockEmpTableID] = "emp"
	mock.ctxt.id2name[mockDeptTableID] = "dept"
	mock.ctxt.objects["emp"].Obj = int64(mockEmpTableID)
	mock.ctxt.objects["dept"].Obj = int64(mockDeptTableID)

	cascadeFK := &plan.ForeignKeyDef{
		Name:        "fk30246_cascade",
		Cols:        []uint64{7}, // emp.deptno
		ForeignTbl:  mockDeptTableID,
		ForeignCols: []uint64{0}, // dept.deptno
		OnDelete:    fkOrder[0],
		OnUpdate:    plan.ForeignKeyDef_RESTRICT,
	}
	setNullFK := &plan.ForeignKeyDef{
		Name:        "fk30246_setnull",
		Cols:        []uint64{3}, // emp.mgr
		ForeignTbl:  mockDeptTableID,
		ForeignCols: []uint64{0},
		OnDelete:    fkOrder[1],
		OnUpdate:    plan.ForeignKeyDef_RESTRICT,
	}
	empTable.Fkeys = []*plan.ForeignKeyDef{cascadeFK, setNullFK}
	deptTable.RefChildTbls = []uint64{empTable.TblId}
}

// dmlTargetMultisets returns the table-name multiset of the plan's write
// targets (delete and insert contexts of every MULTI_UPDATE/DELETE traverse).
// Declaration-order permutations may reorder sibling action steps but must
// not change the action repertoire they write through.
func dmlTargetMultisets(query *plan.Query) (deletes, inserts []string) {
	for _, node := range query.Nodes {
		if node.DeleteCtx != nil && node.DeleteCtx.TableDef != nil {
			deletes = append(deletes, node.DeleteCtx.TableDef.Name)
		}
		if node.InsertCtx != nil && node.InsertCtx.TableDef != nil {
			inserts = append(inserts, node.InsertCtx.TableDef.Name)
		}
	}
	return deletes, inserts
}

func assertHasCascadeOwnedAntiJoin(t *testing.T, query *plan.Query) {
	t.Helper()
	seen := false
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_JOIN && node.JoinType == plan.Node_ANTI {
			seen = true
			break
		}
	}
	require.True(t, seen, "the SET NULL stream must anti-join cascade-owned rows")
}

// sinkStepsUnderTargetDeleteRoots returns the distinct SourceStep values
// consumed by SINK_SCAN nodes inside the Children-closure of the DELETE nodes
// whose target table belongs to the named family (the shared child table, its
// hidden index-maintenance tables, and its descendant cascade tables). After a
// repeated-source merge (#30246) EVERY deletion-dependent consumer of the
// shared child stream must read the same final union step; anything still
// holding the superseded step is a stale index entry or an orphaned grandchild
// waiting to happen.
func sinkStepsUnderTargetDeleteRoots(query *plan.Query, family map[string]bool) map[int32]bool {
	inBranch := make([]bool, len(query.Nodes))
	var mark func(int32)
	mark = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) || inBranch[nodeID] {
			return
		}
		inBranch[nodeID] = true
		for _, child := range query.Nodes[nodeID].Children {
			mark(child)
		}
	}
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_DELETE || node.DeleteCtx == nil || node.DeleteCtx.TableDef == nil {
			continue
		}
		if family[node.DeleteCtx.TableDef.Name] {
			mark(node.NodeId)
		}
	}
	steps := make(map[int32]bool)
	for i, node := range query.Nodes {
		if inBranch[i] && node.NodeType == plan.Node_SINK_SCAN && len(node.SourceStep) > 0 {
			steps[node.SourceStep[0]] = true
		}
	}
	return steps
}

// #30246 review: with two ON DELETE CASCADE constraints to the same parent on
// a child that also carries secondary/unique indexes and a cascade
// grandchild, the second cascade's source merge must re-point ALL
// deletion-dependent consumers of the first stream — not only the delete
// halves of the shared child table (same TblId) but also the hidden
// index-maintenance branches and the grandchild cascade branches.
func TestIssue30246MergedCascadeRewiresAllDeleteConsumers(t *testing.T) {
	const mockGrandchildTableID uint64 = 88890
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	empUnderCascadeSetNullOverlap(t, mock, []plan.ForeignKeyDef_RefAction{
		plan.ForeignKeyDef_CASCADE, plan.ForeignKeyDef_CASCADE,
	})

	// Cascade grandchild empgc(col0) -> emp(empno): re-uses the dept column
	// conventions by deep copy, without any index definitions of its own.
	empTable := mock.ctxt.tables["emp"]
	require.NotNil(t, empTable)
	gc := DeepCopyTableDef(mock.ctxt.tables["dept"], true)
	require.NotNil(t, gc)
	gc.Name = "empgc"
	gc.TblId = mockGrandchildTableID
	gc.Indexes = nil
	gc.Fkeys = []*plan.ForeignKeyDef{{
		Name:        "fk30246_grandchild",
		Cols:        []uint64{0},
		ForeignTbl:  empTable.TblId,
		ForeignCols: []uint64{0}, // emp.empno
		OnDelete:    plan.ForeignKeyDef_CASCADE,
		OnUpdate:    plan.ForeignKeyDef_RESTRICT,
	}}
	gc.RefChildTbls = nil
	mock.ctxt.objects["empgc"] = &ObjectRef{SchemaName: "tpch", ObjName: "empgc", Obj: int64(mockGrandchildTableID)}
	mock.ctxt.tables["empgc"] = gc
	mock.ctxt.id2name[mockGrandchildTableID] = "empgc"
	empTable.RefChildTbls = []uint64{mockGrandchildTableID}

	logicPlan, err := runOneStmt(mock, t, "DELETE FROM dept WHERE deptno = 10")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	requireQueryStepDependenciesAcyclic(t, query)

	// The shared-child family: emp itself, its hidden index-maintenance tables,
	// and the descendant cascade table. dept's own delete branches legitimately
	// keep reading the parent statement's stream.
	empFamily := map[string]bool{"emp": true, "empgc": true}
	for _, idx := range empTable.Indexes {
		empFamily[idx.IndexTableName] = true
	}
	steps := sinkStepsUnderTargetDeleteRoots(query, empFamily)
	if len(steps) != 1 {
		parents := make(map[int32][]int32)
		for i, n := range query.Nodes {
			for _, c := range n.Children {
				parents[c] = append(parents[c], int32(i))
			}
		}
		for i, n := range query.Nodes {
			tgt := ""
			if n.DeleteCtx != nil && n.DeleteCtx.TableDef != nil {
				tgt = " del:" + n.DeleteCtx.TableDef.Name
			}
			if n.InsertCtx != nil && n.InsertCtx.TableDef != nil {
				tgt += " ins:" + n.InsertCtx.TableDef.Name
			}
			t.Logf("node %d type=%v children=%v parents=%v sourceStep=%v%s", i, n.NodeType, n.Children, parents[int32(i)], n.SourceStep, tgt)
		}
		for s, sn := range query.Steps {
			t.Logf("step %d -> node %d", s, sn)
		}
	}
	require.Len(t, steps, 1,
		"every deletion-dependent consumer of the merged emp stream must read the final union step, found steps %v", steps)
	// …and that step is exactly the stream the child's own delete consumes,
	// i.e. the final merged ownership stream, not a superseded action source.
	var empDeleteSource int32 = -1
	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_DELETE && node.DeleteCtx != nil &&
			node.DeleteCtx.TableDef != nil && node.DeleteCtx.TableDef.Name == "emp" {
			for _, child := range node.Children {
				if query.Nodes[child].NodeType == plan.Node_SINK_SCAN {
					empDeleteSource = query.Nodes[child].SourceStep[0]
				}
			}
		}
	}
	require.NotEqual(t, int32(-1), empDeleteSource)
	require.True(t, steps[empDeleteSource],
		"index-maintenance and descendant consumers must read the emp delete's own merged stream step %d, found %v",
		empDeleteSource, steps)
}

// #30246: overlapping ON DELETE CASCADE and ON DELETE SET NULL actions to the
// same parent must build a plan for a plain parent DELETE in either
// declaration order: no projection/width raggedness (previously a runtime
// panic in one order) and the SET NULL stream excludes cascade-owned rows
// (previously a resurrected orphan in the other order).
func TestIssue30246OverlappingCascadeAndSetNullOrderInvariant(t *testing.T) {
	orders := [][]plan.ForeignKeyDef_RefAction{
		{plan.ForeignKeyDef_CASCADE, plan.ForeignKeyDef_SET_NULL},
		{plan.ForeignKeyDef_SET_NULL, plan.ForeignKeyDef_CASCADE},
	}
	deletes := make([][]string, len(orders))
	inserts := make([][]string, len(orders))
	for i, fkOrder := range orders {
		mock := NewMockOptimizer(true, newPlanTestProcess(t))
		empUnderCascadeSetNullOverlap(t, mock, fkOrder)

		logicPlan, err := runOneStmt(mock, t, "DELETE FROM dept WHERE deptno = 10")
		require.NoError(t, err, "order %d must plan", i)
		query := logicPlan.GetQuery()
		require.NotNil(t, query)
		require.True(t, query.GetHasForeignKeyAction(), "order %d", i)

		assertHasCascadeOwnedAntiJoin(t, query)
		requireQueryStepDependenciesAcyclic(t, query)

		deletes[i], inserts[i] = dmlTargetMultisets(query)
	}
	// Compare the action repertoire on the shared child table: sibling action
	// steps for the parent's own delete may legitimately arrive through
	// different write routes (MULTI_UPDATE for the direct statement), but the
	// child-table referential actions must be identical for both declaration
	// orders.
	empDeletes := func(xs []string) int {
		n := 0
		for _, x := range xs {
			if x == "emp" {
				n++
			}
		}
		return n
	}
	require.Equal(t, empDeletes(deletes[0]), empDeletes(deletes[1]),
		"declaration order must not change the child-side delete actions")
	require.Greater(t, empDeletes(deletes[0]), 0)
	empInserts := func(xs []string) int {
		n := 0
		for _, x := range xs {
			if x == "emp" {
				n++
			}
		}
		return n
	}
	require.Equal(t, empInserts(inserts[0]), empInserts(inserts[1]),
		"declaration order must not change the child-side insert actions")
}
