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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// UpdateRolesTableFunction expands each matched tuple into its present UPDATE
// target roles before distributed per-role RowID aggregation.
const UpdateRolesTableFunction = "_expand_update_roles"

// repeatedUpdateSource records raw column dependencies in the common match
// projection. All roles share the source, but select their own complete match.
type repeatedUpdateSource struct {
	project *planpb.Node
	columns map[[2]int32]int
}

type repeatedUpdateAssignment struct {
	role *repeatedUpdateRole
	expr *planpb.Expr
}

// repeatedUpdateAssignments owns assignment expressions bound in the match
// query's lexical scope but evaluated after each role selects a match.
type repeatedUpdateAssignments struct {
	source    *repeatedUpdateSource
	roles     []*repeatedUpdateRole
	positions map[int]*repeatedUpdateAssignment
	width     int
}

func (assignments *repeatedUpdateAssignments) bindProjection(
	ctx *BindContext, binder *ProjectionBinder, ast tree.Expr, pos int,
) (*planpb.Expr, error) {
	assignment := assignments.positions[pos]
	if assignment == nil {
		return bindProjectionExprWithNumericTarget(ctx, binder, ast, pos)
	}
	ctx.repeatedUpdateRole = assignment.role
	expr, err := bindProjectionExprWithNumericTarget(ctx, binder, ast, pos)
	ctx.repeatedUpdateRole = nil
	if err != nil {
		return nil, err
	}
	assignment.expr = expr
	assignment.role.captureExpr(expr)
	return typedRepeatedNull(expr.Typ), nil
}

// captureExpr rewrites only expressions evaluated in the outer query block.
// Correlated references inside a subquery were captured by its outer binder.
func (role *repeatedUpdateRole) captureExpr(expr *planpb.Expr) {
	role.captureExprFrom(expr, nil)
}

// captureExprFrom optionally limits rewriting to tags in the original match
// domain. Subquery outputs created by flattening stay on the role branch.
func (role *repeatedUpdateRole) captureExprFrom(expr *planpb.Expr, matchTags map[int32]struct{}) {
	if expr == nil {
		return
	}
	switch item := expr.Expr.(type) {
	case *planpb.Expr_Col:
		if matchTags != nil {
			if _, ok := matchTags[item.Col.RelPos]; !ok {
				return
			}
		}
		item.Col.RelPos, item.Col.ColPos = role.capture(item.Col.RelPos, item.Col.ColPos, expr.Typ)
	case *planpb.Expr_F:
		for _, arg := range item.F.Args {
			role.captureExprFrom(arg, matchTags)
		}
	case *planpb.Expr_List:
		for _, arg := range item.List.List {
			role.captureExprFrom(arg, matchTags)
		}
	case *planpb.Expr_Sub:
		role.captureExprFrom(item.Sub.Child, matchTags)
	}
}

// restoreExpr recovers the original binding for an already selected role slot.
// Assignment casts use that provenance to recognize projected ENUM/SET values.
func (role *repeatedUpdateRole) restoreExpr(expr *planpb.Expr) *planpb.Expr {
	expr = DeepCopyExpr(expr)
	var restore func(*planpb.Expr)
	restore = func(current *planpb.Expr) {
		if current == nil {
			return
		}
		switch item := current.Expr.(type) {
		case *planpb.Expr_Col:
			pos := int(item.Col.ColPos) - role.oldWidth
			if item.Col.RelPos == role.tag && pos >= 0 && pos < len(role.inputs) {
				original := DeepCopyExpr(role.source.project.ProjectList[role.inputs[pos]])
				original.Typ = current.Typ
				*current = *original
			}
		case *planpb.Expr_F:
			for _, arg := range item.F.Args {
				restore(arg)
			}
		case *planpb.Expr_List:
			for _, arg := range item.List.List {
				restore(arg)
			}
		case *planpb.Expr_Sub:
			restore(item.Sub.Child)
		}
	}
	restore(expr)
	return expr
}

// appendExpandedUpdateMatches expands the present target roles on each input
// scope and selects a whole match per (role, RowID). Its output keeps the raw
// match columns followed by the role and RowID group keys. Partial GROUPs can
// reduce fanout before the shared source gathers their results.
func appendExpandedUpdateMatches(
	builder *QueryBuilder,
	bindCtx *BindContext,
	matchID int32,
	rowIDPositions []int,
	assignments map[int]*repeatedUpdateAssignment,
	plans []*dmlPlanCtx,
) (nodeID, resultTag int32, stats *repeatedUpdateStatsSource, err error) {
	if len(rowIDPositions) == 0 || matchID < 0 || int(matchID) >= len(builder.qry.Nodes) {
		return -1, 0, nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE match source")
	}
	match := builder.qry.Nodes[matchID]
	if len(match.BindingTags) != 1 {
		return -1, 0, nil, moerr.NewInternalErrorNoCtx("repeated UPDATE match has no output binding")
	}
	matchTag := match.BindingTags[0]
	args := make([]*planpb.Expr, len(rowIDPositions))
	for i, pos := range rowIDPositions {
		if pos < 0 || pos >= len(match.ProjectList) || types.T(match.ProjectList[pos].Typ.Id) != types.T_Rowid {
			return -1, 0, nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE match identity")
		}
		args[i] = GetColExpr(match.ProjectList[pos].Typ, matchTag, int32(pos))
	}
	roleType := planpb.Type{Id: int32(types.T_int32), NotNullable: true}
	rowIDType := planpb.Type{Id: int32(types.T_Rowid), NotNullable: true}
	functionTag := builder.genNewBindTag()
	function := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_FUNCTION_SCAN,
		TableDef: &planpb.TableDef{
			TableType: "func_table", TblFunc: &planpb.TableFunction{Name: UpdateRolesTableFunction},
			Cols: []*planpb.ColDef{{Name: "role", Typ: roleType}, {Name: "row_id", Typ: rowIDType}},
		},
		BindingTags: []int32{functionTag}, TblFuncExprList: args, Stats: &planpb.Stats{},
	}, bindCtx)
	apply := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_APPLY, ApplyType: planpb.Node_CROSSAPPLY, Children: []int32{matchID, function},
	}, bindCtx)
	groupTag, aggTag := builder.genNewBindTag(), builder.genNewBindTag()
	aggs := make([]*planpb.Expr, 0, len(match.ProjectList))
	project := make([]*planpb.Expr, 0, len(match.ProjectList)+2)
	for i, expr := range match.ProjectList {
		if _, deferred := assignments[i]; deferred {
			project = append(project, typedRepeatedNull(expr.Typ))
			continue
		}
		arg := GetColExpr(expr.Typ, matchTag, int32(i))
		agg, bindErr := BindFuncExprImplByPlanExpr(builder.GetContext(), "_first_row", []*planpb.Expr{arg})
		if bindErr != nil {
			return -1, 0, nil, bindErr
		}
		agg.Typ = expr.Typ
		project = append(project, GetColExpr(expr.Typ, aggTag, int32(len(aggs))))
		aggs = append(aggs, agg)
	}
	group := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_AGG, Children: []int32{apply}, BindingTags: []int32{groupTag, aggTag},
		GroupBy: []*planpb.Expr{GetColExpr(roleType, functionTag, 0), GetColExpr(rowIDType, functionTag, 1)},
		AggList: aggs, SpillMem: builder.aggSpillMem,
	}, bindCtx)
	stats = newRepeatedUpdateStatsSource(builder, matchID, apply, rowIDPositions, plans)
	if stats != nil {
		builder.setRepeatedUpdateStats(apply, repeatedUpdateStatsHint{kind: repeatedStatsExpand, source: stats})
		builder.setRepeatedUpdateStats(group, repeatedUpdateStatsHint{kind: repeatedStatsRoles, source: stats})
	}
	project = append(project, GetColExpr(roleType, groupTag, 0), GetColExpr(rowIDType, groupTag, 1))
	resultTag = builder.genNewBindTag()
	nodeID = builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_PROJECT, Children: []int32{group}, BindingTags: []int32{resultTag}, ProjectList: project,
	}, bindCtx)
	return nodeID, resultTag, stats, nil
}

// appendExpandedUpdateRole reads a single role from the shared output of
// appendExpandedUpdateMatches. The role's output tag is the one used while
// binding its assignment expressions.
func appendExpandedUpdateRole(
	builder *QueryBuilder,
	bindCtx *BindContext,
	sourceStep int32,
	role *repeatedUpdateRole,
	oldPositions []int,
	ordinal int32,
) (int32, error) {
	if role == nil || len(oldPositions) != role.oldWidth || sourceStep < 0 || int(sourceStep) >= len(builder.qry.Steps) {
		return -1, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE role source")
	}
	sourceID := builder.qry.Steps[sourceStep]
	source := builder.qry.Nodes[sourceID].ProjectList
	if len(source) < 2 {
		return -1, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE role schema")
	}
	positions := append(append([]int(nil), oldPositions...), role.inputs...)
	for _, pos := range positions {
		if pos < 0 || pos >= len(source)-2 {
			return -1, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE role position")
		}
	}
	scanTag := builder.genNewBindTag()
	scan := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{sourceStep},
		BindingTags: []int32{scanTag}, ProjectList: getProjectionByLastNode(builder, sourceID),
	}, bindCtx)
	if builder.positionalSinkScans == nil {
		builder.positionalSinkScans = make(map[int32]struct{})
	}
	builder.positionalSinkScans[scan] = struct{}{}
	roleColumn := GetColExpr(source[len(source)-2].Typ, scanTag, int32(len(source)-2))
	filter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{
		roleColumn, makePlan2Int32ConstExprWithType(ordinal),
	})
	if err != nil {
		return -1, err
	}
	filtered := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_FILTER, Children: []int32{scan}, FilterList: []*planpb.Expr{filter},
	}, bindCtx)
	project := make([]*planpb.Expr, len(positions))
	for i, pos := range positions {
		project[i] = GetColExpr(source[pos].Typ, scanTag, int32(pos))
	}
	return builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_PROJECT, Children: []int32{filtered},
		BindingTags: []int32{role.tag}, ProjectList: project,
	}, bindCtx), nil
}

// repeatedUpdateRole maps an outer column binding to the corresponding slot
// in this role's selected match. Its scope is the RHS binding of one alias.
type repeatedUpdateRole struct {
	source   *repeatedUpdateSource
	tag      int32
	oldWidth int
	columns  map[[2]int32]int32
	inputs   []int
}

func newRepeatedUpdateSource(project *planpb.Node) *repeatedUpdateSource {
	return &repeatedUpdateSource{project: project, columns: make(map[[2]int32]int)}
}

func (source *repeatedUpdateSource) newRole(tag int32, oldWidth int) *repeatedUpdateRole {
	return &repeatedUpdateRole{
		source: source, tag: tag, oldWidth: oldWidth,
		columns: make(map[[2]int32]int32),
	}
}

func (role *repeatedUpdateRole) capture(rel, col int32, typ planpb.Type) (int32, int32) {
	key := [2]int32{rel, col}
	if pos, ok := role.columns[key]; ok {
		return role.tag, pos
	}
	sourcePos, ok := role.source.columns[key]
	if !ok {
		sourcePos = len(role.source.project.ProjectList)
		role.source.project.ProjectList = append(role.source.project.ProjectList, &planpb.Expr{
			Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rel, ColPos: col}},
		})
		role.source.columns[key] = sourcePos
	}
	pos := int32(role.oldWidth + len(role.inputs))
	role.inputs = append(role.inputs, sourcePos)
	role.columns[key] = pos
	return role.tag, pos
}
