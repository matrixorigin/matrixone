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

const mockGrandchildTableID uint64 = 88890

// addCascadeGrandchild re-uses the shared mock conventions to register a
// grandchild table empgc whose only foreign key cascades deletes from emp
// (#30246 review round 2: descendant referential maintenance must exist
// whichever order the parent's own actions are declared in).
func addCascadeGrandchild(t *testing.T, mock *MockOptimizer) {
	t.Helper()
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
}

// deleteTargetsByTable counts DELETE write contexts per table name.
func deleteTargetsByTable(query *plan.Query) map[string]int {
	out := make(map[string]int)
	for _, node := range query.Nodes {
		if node.DeleteCtx != nil && node.DeleteCtx.TableDef != nil {
			out[node.DeleteCtx.TableDef.Name]++
		}
	}
	return out
}

// #30246 review round 2: with ON DELETE SET NULL declared BEFORE ON DELETE
// CASCADE to the same parent, the SET NULL action registers the shared child
// through its UPDATE-driven plan first. The CASCADE action that follows must
// not merge into that registration and skip the physical delete's
// maintenance: rewiring existing scans cannot supply index-maintenance or
// descendant-cascade actions that were never built. Both the hidden
// index-maintenance deletes and the grandchild cascade delete must exist in
// either declaration order; without them the parent DELETE commits invalid
// state (orphaned unique entries / orphaned grandchild rows).
func TestIssue30246MixedActionCascadeMaintenanceBothOrders(t *testing.T) {
	orders := map[string][]plan.ForeignKeyDef_RefAction{
		"setnull_first": {plan.ForeignKeyDef_SET_NULL, plan.ForeignKeyDef_CASCADE},
		"cascade_first": {plan.ForeignKeyDef_CASCADE, plan.ForeignKeyDef_SET_NULL},
	}
	for name, fkOrder := range orders {
		t.Run(name, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			empUnderCascadeSetNullOverlap(t, mock, fkOrder)
			addCascadeGrandchild(t, mock)
			empTable := mock.ctxt.tables["emp"]
			require.NotEmpty(t, empTable.Indexes, "mock emp must carry hidden index-maintenance tables")

			logicPlan, err := runOneStmt(mock, t, "DELETE FROM dept WHERE deptno = 10")
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.NotNil(t, query)
			requireQueryStepDependenciesAcyclic(t, query)

			deletes := deleteTargetsByTable(query)
			require.Greater(t, deletes["emp"], 0,
				"order %s: a physical DELETE of the shared child must exist", name)
			for _, idx := range empTable.Indexes {
				require.Greater(t, deletes[idx.IndexTableName], 0,
					"order %s: cascade must build index-maintenance delete %s", name, idx.IndexTableName)
			}
			require.Greater(t, deletes["empgc"], 0,
				"order %s: cascade must build the grandchild cascade delete", name)

			// The SET NULL stream keeps excluding cascade-owned rows.
			assertHasCascadeOwnedAntiJoin(t, query)
		})
	}
}
