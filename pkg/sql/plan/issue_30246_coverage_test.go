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

// REPLACE plans a delete-and-reinsert of the parent row in one statement:
// the referential merge runs while INSERT-side nodes already exist in the
// query graph. The merge must fence the insert image off from the re-pointing
// (the image is wider than the flat union of both action streams and once
// panicked downstream after a mis-merge, #30246) while still rewiring every
// deletion consumer. Both declaration orders must produce a valid, acyclic
// plan carrying both the insert image and the cascade delete machinery.
func TestIssue30246ReplaceOverlapFencesInsertImage(t *testing.T) {
	orders := map[string][]plan.ForeignKeyDef_RefAction{
		"setnull_first": {plan.ForeignKeyDef_SET_NULL, plan.ForeignKeyDef_CASCADE},
		"cascade_first": {plan.ForeignKeyDef_CASCADE, plan.ForeignKeyDef_SET_NULL},
	}
	for name, fkOrder := range orders {
		t.Run(name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			empUnderCascadeSetNullOverlap(t, mock, fkOrder)
			addCascadeGrandchild(t, mock)

			logicPlan, err := runOneStmt(mock, t, "REPLACE INTO dept VALUES (10, 'REPLACED', 1)")
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			requireQueryStepDependenciesAcyclic(t, query)

			insertFamily := false
			for _, node := range query.Nodes {
				if node.NodeType == plan.Node_INSERT || node.NodeType == plan.Node_PRE_INSERT ||
					node.InsertCtx != nil || node.PreInsertCtx != nil {
					insertFamily = true
					break
				}
			}
			require.True(t, insertFamily,
				"order %s: REPLACE must plan the replacement-row insert side", name)
			deletes, _ := dmlTargetMultisets(query)
			require.Contains(t, deletes, "emp",
				"order %s: REPLACE cascades the displaced dept row to emp", name)
			require.Contains(t, deletes, "empgc",
				"order %s: the grandchild cascade must follow", name)
		})
	}
}

// addSetNullCascadeGrandchildOverlap registers a grandchild empgc whose two
// foreign keys to emp overlap on deptno with fkOrder actions. Deleting dept
// cascades emp, and the recursive build of empgc's actions is bind-tagged —
// the exclusion helper's tagged early return lives on that path.
func addSetNullCascadeGrandchildOverlap(t *testing.T, mock *MockOptimizer, fkOrder []plan.ForeignKeyDef_RefAction) {
	t.Helper()

	empTable := mock.ctxt.tables["emp"]
	require.NotNil(t, empTable)
	gc := DeepCopyTableDef(mock.ctxt.tables["dept"], true)
	require.NotNil(t, gc)
	gc.Name = "empgcsc"
	gc.TblId = mockGrandchildSCOverlapTableID
	gc.Indexes = nil
	gc.Fkeys = []*plan.ForeignKeyDef{
		{
			Name:        "fk30246_gc_a",
			Cols:        []uint64{0},
			ForeignTbl:  empTable.TblId,
			ForeignCols: []uint64{0},
			OnDelete:    fkOrder[0],
			OnUpdate:    plan.ForeignKeyDef_RESTRICT,
		},
		{
			Name:        "fk30246_gc_b",
			Cols:        []uint64{0},
			ForeignTbl:  empTable.TblId,
			ForeignCols: []uint64{0},
			OnDelete:    fkOrder[1],
			OnUpdate:    plan.ForeignKeyDef_RESTRICT,
		},
	}
	gc.RefChildTbls = nil
	mock.ctxt.objects["empgcsc"] = &ObjectRef{SchemaName: "tpch", ObjName: "empgcsc", Obj: int64(mockGrandchildSCOverlapTableID)}
	mock.ctxt.tables["empgcsc"] = gc
	mock.ctxt.id2name[mockGrandchildSCOverlapTableID] = "empgcsc"
	empTable.RefChildTbls = []uint64{mockGrandchildSCOverlapTableID}
}

const mockGrandchildSCOverlapTableID uint64 = 88891

// A cascade grandchild whose own actions overlap exercises the recursion's
// tagged branch of the cascade-owner exclusion: whichever order empgc
// declares SET NULL and CASCADE, the recursive plan must stay acyclic and
// carry the grandchild's physical delete.
func TestIssue30246TaggedGrandchildOverlapDelete(t *testing.T) {
	orders := map[string][]plan.ForeignKeyDef_RefAction{
		"setnull_first": {plan.ForeignKeyDef_SET_NULL, plan.ForeignKeyDef_CASCADE},
		"cascade_first": {plan.ForeignKeyDef_CASCADE, plan.ForeignKeyDef_SET_NULL},
	}
	for name, fkOrder := range orders {
		t.Run(name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			empUnderCascadeSetNullOverlap(t, mock, fkOrder)
			addSetNullCascadeGrandchildOverlap(t, mock, fkOrder)

			logicPlan, err := runOneStmt(mock, t, "DELETE FROM dept WHERE deptno = 10")
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			requireQueryStepDependenciesAcyclic(t, query)

			deletes := deleteTargetsByTable(query)
			require.Greater(t, deletes["empgcsc"], 0,
				"order %s: cascade must still physically delete grandchild rows", name)
		})
	}
}
