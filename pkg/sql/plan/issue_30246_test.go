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
