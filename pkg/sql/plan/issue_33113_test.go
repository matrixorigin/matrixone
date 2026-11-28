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

const (
	mockIssue33113ParentID     uint64 = 77930
	mockIssue33113CascadeID    uint64 = 77931
	mockIssue33113SetNullID    uint64 = 77932
	mockIssue33113GrandchildID uint64 = 77933
)

// addIssue33113Diamond attaches the #33113 schema: a parent with one
// ON DELETE CASCADE child and one ON DELETE SET NULL child, and a
// grandchild that is CASCADE-deleted by the first child and
// UPDATE-CASCADE'd by the second. A parent DELETE must plan both a
// delete and an update of the grandchild, with the update anti-joining
// the already scheduled delete so the shared row is not resurrected.
func addIssue33113Diamond(t *testing.T, mock *MockOptimizer, cascadeFirst bool) {
	t.Helper()
	schema := ""
	if obj := mock.ctxt.objects["replace_fk_cp"]; obj != nil {
		schema = obj.SchemaName
	}

	intTyp := plan.Type{Id: int32(types.T_int32), Width: 32}
	varcharTyp := plan.Type{Id: int32(types.T_varchar), Width: 10}
	rowIDTyp := plan.Type{Id: int32(types.T_Rowid), Width: 16}
	nullableVarchar := plan.Type{Id: int32(types.T_varchar), Width: 10}

	parent := &plan.TableDef{
		TblId: mockIssue33113ParentID,
		Name:  "fk33113_p",
		Cols: []*plan.ColDef{
			{Name: "id", ColId: 0, Typ: intTyp},
			{Name: "code", ColId: 1, Typ: varcharTyp},
			{Name: catalog.Row_ID, ColId: 2, Hidden: true, Typ: rowIDTyp},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Name2ColIndex: map[string]int32{"id": 0, "code": 1, catalog.Row_ID: 2},
	}
	c1 := &plan.TableDef{
		TblId: mockIssue33113CascadeID,
		Name:  "fk33113_c1",
		Cols: []*plan.ColDef{
			{Name: "id", ColId: 0, Typ: intTyp},
			{Name: "pid", ColId: 1, Typ: intTyp, Default: &plan.Default{NullAbility: true}},
			{Name: catalog.Row_ID, ColId: 2, Hidden: true, Typ: rowIDTyp},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Fkeys: []*plan.ForeignKeyDef{{
			Name:        "fk33113_c1_p",
			Cols:        []uint64{1},
			ForeignTbl:  parent.TblId,
			ForeignCols: []uint64{0},
			OnDelete:    plan.ForeignKeyDef_CASCADE,
			OnUpdate:    plan.ForeignKeyDef_RESTRICT,
		}},
		Name2ColIndex: map[string]int32{"id": 0, "pid": 1, catalog.Row_ID: 2},
	}
	c2 := &plan.TableDef{
		TblId: mockIssue33113SetNullID,
		Name:  "fk33113_c2",
		Cols: []*plan.ColDef{
			{Name: "id", ColId: 0, Typ: intTyp},
			{Name: "x", ColId: 1, Typ: nullableVarchar, Default: &plan.Default{NullAbility: true}},
			{Name: catalog.Row_ID, ColId: 2, Hidden: true, Typ: rowIDTyp},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Fkeys: []*plan.ForeignKeyDef{{
			Name:        "fk33113_c2_p",
			Cols:        []uint64{1},
			ForeignTbl:  parent.TblId,
			ForeignCols: []uint64{1},
			OnDelete:    plan.ForeignKeyDef_SET_NULL,
			OnUpdate:    plan.ForeignKeyDef_RESTRICT,
		}},
		Name2ColIndex: map[string]int32{"id": 0, "x": 1, catalog.Row_ID: 2},
	}
	g := &plan.TableDef{
		TblId: mockIssue33113GrandchildID,
		Name:  "fk33113_g",
		Cols: []*plan.ColDef{
			{Name: "id", ColId: 0, Typ: intTyp},
			{Name: "c1id", ColId: 1, Typ: intTyp, Default: &plan.Default{NullAbility: true}},
			{Name: "x", ColId: 2, Typ: nullableVarchar, Default: &plan.Default{NullAbility: true}},
			{Name: catalog.Row_ID, ColId: 3, Hidden: true, Typ: rowIDTyp},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
		Fkeys: []*plan.ForeignKeyDef{
			{
				Name:        "fk33113_g_c1",
				Cols:        []uint64{1},
				ForeignTbl:  c1.TblId,
				ForeignCols: []uint64{0},
				OnDelete:    plan.ForeignKeyDef_CASCADE,
				OnUpdate:    plan.ForeignKeyDef_RESTRICT,
			},
			{
				Name:        "fk33113_g_c2",
				Cols:        []uint64{2},
				ForeignTbl:  c2.TblId,
				ForeignCols: []uint64{1},
				OnDelete:    plan.ForeignKeyDef_RESTRICT,
				OnUpdate:    plan.ForeignKeyDef_CASCADE,
			},
		},
		Name2ColIndex: map[string]int32{"id": 0, "c1id": 1, "x": 2, catalog.Row_ID: 3},
	}

	if cascadeFirst {
		parent.RefChildTbls = []uint64{c1.TblId, c2.TblId}
	} else {
		parent.RefChildTbls = []uint64{c2.TblId, c1.TblId}
	}
	c1.RefChildTbls = []uint64{g.TblId}
	c2.RefChildTbls = []uint64{g.TblId}

	for _, def := range []*plan.TableDef{parent, c1, c2, g} {
		mock.ctxt.tables[def.Name] = def
		mock.ctxt.objects[def.Name] = &plan.ObjectRef{
			Obj: int64(def.TblId), SchemaName: schema, ObjName: def.Name,
		}
		mock.ctxt.id2name[def.TblId] = def.Name
	}
}

func requireIssue33113DeletePlan(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	logicPlan, err := runOneStmt(mock, t, "DELETE FROM fk33113_p WHERE id = 1")
	require.NoError(t, err)
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	requireQueryStepDependenciesAcyclic(t, query)
	require.True(t, queryDeletesTable(query, "fk33113_c1"), "parent DELETE must cascade-delete c1")
	require.True(t, queryUpdatesTable(query, "fk33113_c2"), "parent DELETE must SET NULL c2")
	require.True(t, queryDeletesTable(query, "fk33113_g"), "c1 CASCADE must delete grandchild rows")
	require.True(t, queryUpdatesTable(query, "fk33113_g"), "c2 SET NULL must UPDATE CASCADE grandchild rows")
	require.True(t, queryHasRowIDAntiJoin(query),
		"grandchild UPDATE must anti-join rows already scheduled for DELETE")
}

func queryHasRowIDAntiJoin(query *plan.Query) bool {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_JOIN || node.JoinType != plan.Node_ANTI {
			continue
		}
		for _, on := range node.OnList {
			if exprMentionsRowID(on) {
				return true
			}
		}
	}
	return false
}

func exprMentionsRowID(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		return e.Col != nil && e.Col.Name == catalog.Row_ID
	case *plan.Expr_F:
		if e.F == nil {
			return false
		}
		for _, arg := range e.F.Args {
			if exprMentionsRowID(arg) {
				return true
			}
		}
	case *plan.Expr_List:
		if e.List == nil {
			return false
		}
		for _, arg := range e.List.List {
			if exprMentionsRowID(arg) {
				return true
			}
		}
	}
	return false
}

func TestIssue33113DeleteCascadeAndUpdateCascadeDiamond(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addIssue33113Diamond(t, mock, true)
	requireIssue33113DeletePlan(t, mock)
}

func TestIssue33113DeleteCascadeAndUpdateCascadeDiamondSetNullFirst(t *testing.T) {
	// RefChildTbls lists the SET NULL child first. The planner must still
	// walk the CASCADE child first so the grandchild delete is registered
	// before the update is planned.
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addIssue33113Diamond(t, mock, false)
	requireIssue33113DeletePlan(t, mock)
}

func TestIssue33113OrderReferentialChildTableIDs(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addIssue33113Diamond(t, mock, false)
	builder := NewQueryBuilder(plan.Query_DELETE, mock.CurrentContext(), false, true)
	parent := mock.ctxt.tables["fk33113_p"]

	require.Nil(t, orderReferentialChildTableIDs(builder, nil))
	require.Nil(t, orderReferentialChildTableIDs(builder, &plan.TableDef{}))

	saved := parent.RefChildTbls
	parent.RefChildTbls = []uint64{mockIssue33113SetNullID, mockIssue33113CascadeID, mockIssue33113SetNullID}
	defer func() { parent.RefChildTbls = saved }()
	require.Equal(t,
		[]uint64{mockIssue33113CascadeID, mockIssue33113SetNullID},
		orderReferentialChildTableIDs(builder, parent),
		"CASCADE children come first and duplicate ids are dropped")
}

func TestIssue33113ChildHasOnDeleteCascade(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	addIssue33113Diamond(t, mock, true)
	builder := NewQueryBuilder(plan.Query_DELETE, mock.CurrentContext(), false, true)
	parent := mock.ctxt.tables["fk33113_p"]

	require.False(t, childHasOnDeleteCascade(builder, nil, mockIssue33113CascadeID))
	require.True(t, childHasOnDeleteCascade(builder, parent, mockIssue33113CascadeID))
	require.False(t, childHasOnDeleteCascade(builder, parent, mockIssue33113SetNullID))
	require.False(t, childHasOnDeleteCascade(builder, parent, 987654321), "unknown table")
	require.False(t, childHasOnDeleteCascade(nil, parent, mockIssue33113CascadeID), "no compiler context")
	require.False(t, childHasOnDeleteCascade(builder, parent, 0), "parent has no self reference")

	selfRef := &plan.TableDef{
		TblId: 77939,
		Fkeys: []*plan.ForeignKeyDef{{ForeignTbl: 0, OnDelete: plan.ForeignKeyDef_CASCADE}},
	}
	require.True(t, childHasOnDeleteCascade(builder, selfRef, 0), "self-referencing CASCADE")
}

func TestIssue33113ProjectRowIDHelpers(t *testing.T) {
	rowIDTyp := plan.Type{Id: int32(types.T_Rowid), Width: 16}
	intTyp := plan.Type{Id: int32(types.T_int32), Width: 32}
	project := []*plan.Expr{
		{Typ: intTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0, Name: "id"}}},
		{Typ: intTyp, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}},
		{Typ: rowIDTyp, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 2, Name: catalog.Row_ID}}},
	}
	require.True(t, projectHasRowIDAt(project, 2))
	require.False(t, projectHasRowIDAt(project, 0))
	require.False(t, projectHasRowIDAt(project, 1), "literal is not a rowid column")
	require.False(t, projectHasRowIDAt(project, -1))
	require.False(t, projectHasRowIDAt(project, 3))
	require.Equal(t, int32(2), projectRowIDPos(project))
	require.Equal(t, int32(-1), projectRowIDPos(project[:2]))
}

func TestIssue33113AppendExcludeRegisteredDeleteRowsEdges(t *testing.T) {
	mock := NewMockOptimizer(true, newPlanTestProcess(t))
	rowIDTyp := plan.Type{Id: int32(types.T_Rowid), Width: 16}
	intTyp := plan.Type{Id: int32(types.T_int32), Width: 32}
	col := func(pos int32, name string, typ plan.Type) *plan.Expr {
		return &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos, Name: name}}}
	}
	newBuilder := func() (*QueryBuilder, *BindContext) {
		builder := NewQueryBuilder(plan.Query_DELETE, mock.CurrentContext(), false, true)
		return builder, NewBindContext(builder, nil)
	}
	// registers a delete source whose sink projects deleteProject.
	register := func(builder *QueryBuilder, bindCtx *BindContext, tableID uint64, deleteProject []*plan.Expr) {
		src := builder.appendNode(&plan.Node{NodeType: plan.Node_PROJECT, ProjectList: deleteProject}, bindCtx)
		sink := appendSinkNode(builder, bindCtx, src)
		builder.appendStep(sink)
		builder.deleteNode[tableID] = sink
	}
	input := func(builder *QueryBuilder, bindCtx *BindContext) int32 {
		return builder.appendNode(&plan.Node{
			NodeType:    plan.Node_PROJECT,
			ProjectList: []*plan.Expr{col(0, "id", intTyp), col(1, catalog.Row_ID, rowIDTyp)},
		}, bindCtx)
	}
	const tbl uint64 = 77940

	t.Run("no registered delete", func(t *testing.T) {
		builder, bindCtx := newBuilder()
		in := input(builder, bindCtx)
		out, err := appendExcludeRegisteredDeleteRows(builder, bindCtx, in, tbl, 1)
		require.NoError(t, err)
		require.Equal(t, in, out)
	})
	t.Run("negative rowid position", func(t *testing.T) {
		builder, bindCtx := newBuilder()
		register(builder, bindCtx, tbl, []*plan.Expr{col(0, catalog.Row_ID, rowIDTyp)})
		in := input(builder, bindCtx)
		out, err := appendExcludeRegisteredDeleteRows(builder, bindCtx, in, tbl, -1)
		require.NoError(t, err)
		require.Equal(t, in, out)
	})
	t.Run("registered sink is not a step", func(t *testing.T) {
		builder, bindCtx := newBuilder()
		in := input(builder, bindCtx)
		builder.deleteNode[tbl] = in
		out, err := appendExcludeRegisteredDeleteRows(builder, bindCtx, in, tbl, 1)
		require.NoError(t, err)
		require.Equal(t, in, out)
	})
	t.Run("update rowid out of range", func(t *testing.T) {
		builder, bindCtx := newBuilder()
		register(builder, bindCtx, tbl, []*plan.Expr{col(0, catalog.Row_ID, rowIDTyp)})
		in := input(builder, bindCtx)
		_, err := appendExcludeRegisteredDeleteRows(builder, bindCtx, in, tbl, 5)
		require.ErrorContains(t, err, "update source rowid is unavailable")
	})
	t.Run("registered delete has no rowid", func(t *testing.T) {
		builder, bindCtx := newBuilder()
		register(builder, bindCtx, tbl, []*plan.Expr{col(0, "id", intTyp)})
		in := input(builder, bindCtx)
		_, err := appendExcludeRegisteredDeleteRows(builder, bindCtx, in, tbl, 1)
		require.ErrorContains(t, err, "registered delete rowid is unavailable")
	})
	t.Run("delete rowid found at another position", func(t *testing.T) {
		builder, bindCtx := newBuilder()
		register(builder, bindCtx, tbl, []*plan.Expr{col(0, catalog.Row_ID, rowIDTyp), col(1, "id", intTyp)})
		in := input(builder, bindCtx)
		out, err := appendExcludeRegisteredDeleteRows(builder, bindCtx, in, tbl, 1)
		require.NoError(t, err)
		join := builder.qry.Nodes[out]
		require.Equal(t, plan.Node_JOIN, join.NodeType)
		require.Equal(t, plan.Node_ANTI, join.JoinType)
		require.Len(t, join.ProjectList, 2)
		args := join.OnList[0].GetF().Args
		require.Equal(t, int32(1), args[0].GetCol().ColPos, "candidate rowid")
		require.Equal(t, int32(0), args[1].GetCol().ColPos, "registered delete rowid located by name")
		require.Equal(t, int32(1), args[1].GetCol().RelPos)
	})
}
