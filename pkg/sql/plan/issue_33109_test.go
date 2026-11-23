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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

const mockReplaceSetNullGrandchildID uint64 = 77920

// addReplaceSetNullGrandchild attaches a grandchild table to replace_fk_cc
// whose only foreign key is ON DELETE SET NULL. REPLACE on the CASCADE parent
// (replace_fk_cp) must then plan a SET NULL update of the grandchild after
// cascading the delete of replace_fk_cc (#33109).
func addReplaceSetNullGrandchild(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	child := mock.ctxt.tables["replace_fk_cc"]
	require.NotNil(t, child)
	rootObj := mock.ctxt.objects["replace_fk_cp"]
	require.NotNil(t, rootObj)

	grandchild := &plan.TableDef{
		TblId: mockReplaceSetNullGrandchildID,
		Name:  "replace_fk_cg",
		Cols: []*plan.ColDef{
			{Name: "gid", ColId: 0, Typ: plan.Type{Id: int32(types.T_int32), Width: 32}},
			{Name: "pid", ColId: 1, Typ: plan.Type{Id: int32(types.T_int32), Width: 32}, Default: &plan.Default{NullAbility: true}},
			{Name: catalog.Row_ID, ColId: 2, Hidden: true, Typ: plan.Type{Id: int32(types.T_Rowid), Width: 16}},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"gid"}, PkeyColName: "gid"},
		Fkeys: []*plan.ForeignKeyDef{{
			Name:        "fk_replace_cg",
			Cols:        []uint64{1},
			ForeignTbl:  child.TblId,
			ForeignCols: []uint64{0},
			OnDelete:    plan.ForeignKeyDef_SET_NULL,
			OnUpdate:    plan.ForeignKeyDef_RESTRICT,
		}},
		Name2ColIndex: map[string]int32{"gid": 0, "pid": 1, catalog.Row_ID: 2},
	}
	mock.ctxt.tables[grandchild.Name] = grandchild
	mock.ctxt.objects[grandchild.Name] = &plan.ObjectRef{
		Obj: int64(grandchild.TblId), SchemaName: rootObj.SchemaName, ObjName: grandchild.Name,
	}
	mock.ctxt.id2name[grandchild.TblId] = grandchild.Name
	child.RefChildTbls = append(append([]uint64{}, child.RefChildTbls...), grandchild.TblId)
}

// #33109: REPLACE on a parent whose ON DELETE CASCADE child is itself the
// parent of an ON DELETE SET NULL grandchild used to fail at remapping with
// "Column remapping failed" on an AGG GroupBy. The equivalent DELETE works.
func TestIssue33109ReplaceCascadeSetNullGrandchild(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addReplaceSetNullGrandchild(t, mock)

	logicPlan, err := runOneStmt(mock, t, "REPLACE INTO replace_fk_cp VALUES (1, 'p1_new')")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	requireQueryStepDependenciesAcyclic(t, query)
	require.True(t, queryDeletesTable(query, "replace_fk_cc"),
		"REPLACE must cascade-delete the child")
	require.True(t, queryUpdatesTable(query, "replace_fk_cg"),
		"REPLACE must SET NULL the grandchild, not fail planning")
}

func TestIssue33109DeleteCascadeSetNullGrandchildStillPlans(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addReplaceSetNullGrandchild(t, mock)

	logicPlan, err := runOneStmt(mock, t, "DELETE FROM replace_fk_cp WHERE id = 1")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.True(t, queryDeletesTable(query, "replace_fk_cc"))
	require.True(t, queryUpdatesTable(query, "replace_fk_cg"))
}
