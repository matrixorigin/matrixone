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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestRepeatedUpdateDefersAssignmentsDuringSelectBinding(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	ctx.SetSqlModeOverride("MATRIXONE_NATIVE")
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
		"update nation x join nation y on x.n_nationkey=y.n_nationkey "+
			"set x.n_regionkey=y.n_regionkey+1, y.n_regionkey=(select max(r_regionkey) from region where r_regionkey=x.n_regionkey)", 1)
	require.NoError(t, err)
	defer stmt.Free()
	update := stmt.(*tree.Update)
	info, err := getUpdateTableInfo(ctx, update)
	require.NoError(t, err)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	bindCtx := NewBindContext(builder, nil)
	deferred := &repeatedUpdateAssignments{}
	last, _, err := selectUpdateTables(builder, bindCtx, update, info, deferred)
	require.NoError(t, err)
	project := builder.qry.Nodes[last]
	require.Same(t, project, deferred.source.project)
	require.Len(t, deferred.roles, 2)
	require.Len(t, deferred.positions, 2)
	require.Greater(t, len(project.ProjectList), deferred.width)
	for pos, assignment := range deferred.positions {
		require.True(t, project.ProjectList[pos].GetLit().GetIsnull())
		require.NotNil(t, assignment.expr)
		require.NotEmpty(t, assignment.role.inputs)
	}
}

func TestRepeatedUpdateWindowStillUsesMatchColumns(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	ctx.SetSqlModeOverride("MATRIXONE_NATIVE")
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
		"update nation x join nation y on x.n_nationkey=y.n_nationkey "+
			"set x.n_regionkey=row_number() over (partition by y.n_regionkey order by x.n_nationkey), y.n_regionkey=x.n_regionkey", 1)
	require.NoError(t, err)
	defer stmt.Free()
	update := stmt.(*tree.Update)
	info, err := getUpdateTableInfo(ctx, update)
	require.NoError(t, err)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	bindCtx := NewBindContext(builder, nil)
	deferred := &repeatedUpdateAssignments{}
	_, _, err = selectUpdateTables(builder, bindCtx, update, info, deferred)
	require.NoError(t, err)
	require.Len(t, bindCtx.windows, 1)
	window := bindCtx.windows[0].GetW()
	require.NotNil(t, window)
	require.Len(t, window.PartitionBy, 1)
	require.Len(t, window.OrderBy, 1)
	for _, col := range []*planpb.ColRef{window.PartitionBy[0].GetCol(), window.OrderBy[0].Expr.GetCol()} {
		require.NotNil(t, col)
		for _, role := range deferred.roles {
			require.NotEqual(t, role.tag, col.RelPos)
		}
	}
	windowCaptured := false
	for _, expr := range deferred.source.project.ProjectList[deferred.width:] {
		if col := expr.GetCol(); col != nil && col.RelPos == bindCtx.windowTag {
			windowCaptured = true
		}
	}
	require.True(t, windowCaptured, "the computed window value must be selected with its match")
}

func TestRepeatedUpdateChecksOnlyAssignedDatalinkSources(t *testing.T) {
	for _, tc := range []struct {
		name, assignment string
		checks           int
	}{
		{"carried", "x.val=x.val+1,y.val=y.val+1", 0},
		{"assigned", "x.val=x.val+1,y.updated_at=y.updated_at", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockOptimizer(false).CurrentContext().(*MockCompilerContext)
			table := ctx.tables["t_on_update_gen"]
			ensureName2ColIndexForReplace(table)
			table.Cols[2].Typ = planpb.Type{Id: int32(types.T_datalink)}
			table.Cols[2].OnUpdate = nil
			table.Cols[3].GeneratedCol = nil
			table.Indexes = []*planpb.IndexDef{{IndexAlgo: "fulltext2", IndexName: "ft", Parts: []string{"updated_at"}}}
			stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL,
				"update constraint_test.t_on_update_gen x join constraint_test.t_on_update_gen y on x.id=y.id set "+tc.assignment, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			checks := 0
			for _, node := range p.GetQuery().Nodes {
				for _, filter := range node.FilterList {
					if f := filter.GetF(); f != nil && f.GetFunc().GetObjName() == "_check_async_datalink_source" {
						checks++
					}
				}
			}
			require.Equal(t, tc.checks, checks)
		})
	}
}

func TestLegacyUpdateRewritesEveryTargetBlock(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_UPDATE, NewMockCompilerContext(true), false, true)
	widths := []int32{3, 5, 7}
	projections := make([]*planpb.Expr, 0, 2*len(widths))
	contexts := make([]*dmlPlanCtx, 0, len(widths))
	for _, width := range widths {
		target := planpb.Type{Id: int32(types.T_varchar), Width: width}
		projections = append(projections,
			&planpb.Expr{Typ: target, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}},
			&planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{}}},
		)
		contexts = append(contexts, &dmlPlanCtx{
			tableDef:        &planpb.TableDef{Cols: []*planpb.ColDef{{Name: "c", Typ: target}}},
			updateColLength: 1,
			updateColPosMap: map[string]int{"c": 1},
		})
	}
	builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{ProjectList: projections})
	require.NoError(t, rewriteUpdateQueryLastNode(builder, contexts, 0, false))
	for i, width := range widths {
		expr := builder.qry.Nodes[0].ProjectList[2*i+1]
		require.Equal(t, "cast_assign", expr.GetF().GetFunc().GetObjName())
		require.Equal(t, width, expr.Typ.Width)
	}
}

func TestRepeatedUpdateRoleBindsOuterColumnsAtEveryDepth(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(true), false, false)
	outer := NewBindContext(builder, nil)
	typ := &planpb.Type{Id: int32(types.T_int32)}
	binding := &Binding{tag: 11, table: "x", cols: []string{"a"}, types: []*planpb.Type{typ}, colIdByName: map[string]int32{"a": 0}}
	outer.bindingByTable["x"] = binding
	outer.bindingByCol["a"] = binding
	outerBinder := NewProjectionBinder(builder, outer, NewHavingBinder(builder, outer))
	outer.binder = outerBinder
	source := newRepeatedUpdateSource(&planpb.Node{})
	role := source.newRole(71, 3)
	outer.repeatedUpdateRole = role
	name := tree.NewUnresolvedName(tree.NewCStr("x", 1), tree.NewCStr("a", 1))
	col, err := outerBinder.BindColRef(name, 0, true)
	require.NoError(t, err)
	require.EqualValues(t, 11, col.GetCol().RelPos)
	role.captureExpr(col)
	require.EqualValues(t, 71, col.GetCol().RelPos)
	require.EqualValues(t, 3, col.GetCol().ColPos)

	child := NewBindContext(builder, outer)
	childBinder := NewProjectionBinder(builder, child, NewHavingBinder(builder, child))
	child.binder = childBinder
	grandchild := NewBindContext(builder, child)
	grandchildBinder := NewProjectionBinder(builder, grandchild, NewHavingBinder(builder, grandchild))
	corr, err := grandchildBinder.BindColRef(name, 0, true)
	require.NoError(t, err)
	require.EqualValues(t, 71, corr.GetCorr().RelPos)
	require.EqualValues(t, 3, corr.GetCorr().ColPos)
	require.EqualValues(t, 2, corr.GetCorr().Depth)
	require.Equal(t, []int{0}, role.inputs, "the same outer column is selected once")
	require.Len(t, source.project.ProjectList, 1)
	require.EqualValues(t, 11, source.project.ProjectList[0].GetCol().RelPos)

	child.bindingByTable["x"] = &Binding{tag: 22, table: "x", cols: []string{"a"}, types: []*planpb.Type{typ}, colIdByName: map[string]int32{"a": 0}}
	shadow, err := childBinder.BindColRef(name, 0, true)
	require.NoError(t, err)
	require.EqualValues(t, 22, shadow.GetCol().RelPos, "an inner alias shadows the outer role")
	require.Len(t, source.project.ProjectList, 1)

	other := source.newRole(72, 4)
	outer.repeatedUpdateRole = other
	col, err = outerBinder.BindColRef(name, 0, true)
	require.NoError(t, err)
	other.captureExpr(col)
	require.EqualValues(t, 72, col.GetCol().RelPos)
	require.EqualValues(t, 4, col.GetCol().ColPos)
	require.Equal(t, []int{0}, other.inputs, "roles share the raw source but have independent selected slots")
}

func TestUpdateUniqueContextSurvivesPlanCopyAndWire(t *testing.T) {
	original := &planpb.Node{NodeType: planpb.Node_UPDATE_UNIQUE, UpdateUniqueCtx: &planpb.UpdateUniqueCtx{
		RowId: 1, Active: 2,
		Keys:   []planpb.UpdateUniqueKey{{Original: 3, Current: 4, Proposed: 5, SnapshotHit: 6}},
		Values: []planpb.UpdateUniqueValue{{Current: 8, Proposed: 9}},
		Checks: []planpb.UpdateUniqueCheck{{Column: 10, Name: "v", NotNull: true}},
	}, UpdateCountCtx: &planpb.UpdateCountCtx{Column: 7}}
	copy := DeepCopyNode(original)
	require.Equal(t, original.UpdateUniqueCtx, copy.UpdateUniqueCtx)
	require.Equal(t, original.UpdateCountCtx, copy.UpdateCountCtx)
	require.NotSame(t, original.UpdateCountCtx, copy.UpdateCountCtx)
	copy.UpdateUniqueCtx.Keys[0].Proposed = 42
	copy.UpdateUniqueCtx.Values[0].Proposed = 43
	copy.UpdateUniqueCtx.Checks[0].Name = "changed"
	require.EqualValues(t, 5, original.UpdateUniqueCtx.Keys[0].Proposed)
	require.EqualValues(t, 9, original.UpdateUniqueCtx.Values[0].Proposed)
	require.Equal(t, "v", original.UpdateUniqueCtx.Checks[0].Name)
	encoded, err := original.Marshal()
	require.NoError(t, err)
	var decoded planpb.Node
	require.NoError(t, decoded.Unmarshal(encoded))
	require.Equal(t, original.UpdateUniqueCtx, decoded.UpdateUniqueCtx)
	require.Equal(t, original.UpdateCountCtx, decoded.UpdateCountCtx)
	require.Equal(t, original.NodeType, decoded.NodeType)
}
