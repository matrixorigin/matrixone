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
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
)

type repeatedRoleSource struct {
	role         *repeatedUpdateRole
	oldPositions []int
	scan         int32
	filter       int32
	project      int32
	ordinal      int32
}

// refreshRepeatedRoleSources completes the schemas of already-bound role
// branches. An assignment cast can discover an additional stored SET value;
// every reader must then see the enlarged match projection at the same offset.
func refreshRepeatedRoleSources(builder *QueryBuilder, sinkID int32, roles []repeatedRoleSource) error {
	sink := builder.qry.Nodes[sinkID]
	if len(sink.ProjectList) < 2 {
		return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE source schema")
	}
	rolePos := len(sink.ProjectList) - 2
	for _, source := range roles {
		scan := builder.qry.Nodes[source.scan]
		scan.ProjectList = getProjectionByLastNode(builder, sinkID)
		scanTag := scan.BindingTags[0]
		roleColumn := GetColExpr(sink.ProjectList[rolePos].Typ, scanTag, int32(rolePos))
		filter, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{
			roleColumn, makePlan2Int32ConstExprWithType(source.ordinal),
		})
		if err != nil {
			return err
		}
		builder.qry.Nodes[source.filter].FilterList = []*planpb.Expr{filter}
		positions := append(append([]int(nil), source.oldPositions...), source.role.inputs...)
		project := make([]*planpb.Expr, len(positions))
		for i, pos := range positions {
			if pos < 0 || pos >= rolePos {
				return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE role dependency")
			}
			project[i] = GetColExpr(sink.ProjectList[pos].Typ, scanTag, int32(pos))
		}
		builder.qry.Nodes[source.project].ProjectList = project
	}
	return nil
}

func provisionalRepeatedMatchSink(builder *QueryBuilder, sinkID, matchID int32) {
	sink := builder.qry.Nodes[sinkID]
	match := builder.qry.Nodes[matchID]
	sink.ProjectList = getProjectionByLastNodeWithTag(builder, matchID, match.BindingTags[0])
	sink.ProjectList = append(sink.ProjectList,
		makePlan2Int32ConstExprWithType(0),
		typedRepeatedNull(planpb.Type{Id: int32(types.T_Rowid)}),
	)
}

// normalizeRepeatedOldRows keeps the original storage values used by the
// writer and index deletes. SQL projection normally displays ENUM/SET labels;
// converting those labels back can lose a valid SET bitmap.
func normalizeRepeatedOldRows(builder *QueryBuilder, match *planpb.Node, plans []*dmlPlanCtx) error {
	offset := 0
	for _, up := range plans {
		for i, col := range up.tableDef.Cols {
			pos := offset + i
			if pos < 0 || pos >= len(match.ProjectList) {
				return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE old row")
			}
			old := match.ProjectList[pos]
			if isEnumOrSetPlanType(&col.Typ) {
				if raw, ok := storedMySQLSpecialTypeExpr(old); ok {
					raw.Typ = col.Typ
					raw.Typ.NotNullable = old.Typ.NotNullable
					match.ProjectList[pos] = raw
				} else if old.Typ.Id != col.Typ.Id || !sameTypeMemberDomain(&old.Typ, &col.Typ) {
					return moerr.NewInternalErrorNoCtx("invalid repeated UPDATE old special-type value")
				}
			} else if isGeometryPlanType(&col.Typ) {
				var err error
				match.ProjectList[pos], err = funcCastForGeometryType(builder.GetContext(), old, col.Typ)
				if err != nil {
					return err
				}
			}
		}
		offset += len(up.tableDef.Cols) + up.updateColLength
	}
	return nil
}

func bindRepeatedAssignment(
	builder *QueryBuilder, bindCtx *BindContext, root int32, role *repeatedUpdateRole,
	value *planpb.Expr, col *planpb.ColDef, table *planpb.TableDef, explicit, isIgnore bool,
) (int32, *planpb.Expr, error) {
	root, value, err := builder.flattenSubqueries(root, value, bindCtx)
	if err != nil {
		return -1, nil, err
	}
	value = role.restoreExpr(value)
	if explicit && isDefaultValExpr(value) {
		value, err = getDefaultExpr(builder.GetContext(), col)
		if err != nil {
			return -1, nil, err
		}
	}
	// UpdateUnique checks NOT NULL for the roles admitted by UPDATE IGNORE.
	if explicit && !isIgnore {
		if err = checkNotNull(builder.GetContext(), value, table, col); err != nil {
			return -1, nil, err
		}
	}
	sourceValue := DeepCopyExpr(value)
	switch {
	case isEnumPlanType(&col.Typ):
		value, err = funcCastForEnumType(builder.GetContext(), value, col.Typ)
	case isSetPlanType(&col.Typ):
		if fn := value.GetF(); isSetDisplayValueExpr(value) && validMySQLSpecialDisplayCall(fn) && sameTypeMemberDomain(&fn.Args[1].Typ, &col.Typ) {
			value, _ = storedSetBitmapExpr(value)
			value.Typ = col.Typ
			value.Typ.NotNullable = sourceValue.Typ.NotNullable
		} else {
			value, err = funcCastForSetType(builder.GetContext(), value, col.Typ)
		}
	case isGeometryPlanType(&col.Typ):
		value, err = funcCastForGeometryType(builder.GetContext(), value, col.Typ)
	default:
		value, err = builder.forceProjectedAssignmentCastExpr(value, sourceValue, col.Typ, isIgnore)
	}
	if err != nil {
		return -1, nil, err
	}
	matchTags := make(map[int32]struct{}, len(role.source.columns))
	for ref := range role.source.columns {
		matchTags[ref[0]] = struct{}{}
	}
	role.captureExprFrom(value, matchTags)
	return root, value, nil
}

// buildRepeatedRoleBranches binds each role's assignments against its selected
// match. It finalizes the shared collector only after casts have exposed all
// raw dependencies. No assignment is evaluated by that collector.
func buildRepeatedRoleBranches(
	builder *QueryBuilder,
	bindCtx *BindContext,
	matchID int32,
	plans []*dmlPlanCtx,
	assignments *repeatedUpdateAssignments,
	isIgnore bool,
) ([]repeatedUpdateBranch, error) {
	if assignments == nil || len(plans) == 0 || len(assignments.roles) != len(plans) ||
		matchID < 0 || int(matchID) >= len(builder.qry.Nodes) {
		return nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE match")
	}
	match := builder.qry.Nodes[matchID]
	if len(match.BindingTags) != 1 || assignments.source.project != match {
		return nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE match binding")
	}
	if err := normalizeRepeatedOldRows(builder, match, plans); err != nil {
		return nil, err
	}
	sink := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_SINK, Children: []int32{matchID},
		BindingTags:  []int32{builder.genNewBindTag()},
		ProjectList:  getProjectionByLastNodeWithTag(builder, matchID, match.BindingTags[0]),
		ExtraOptions: materialized.CTESinkOption,
	}, bindCtx)
	provisionalRepeatedMatchSink(builder, sink, matchID)
	sourceStep := builder.appendStep(sink)
	if builder.preserveSinkProjection == nil {
		builder.preserveSinkProjection = make(map[int32]struct{})
	}
	builder.preserveSinkProjection[sink] = struct{}{}

	branches := make([]repeatedUpdateBranch, len(plans))
	sources := make([]repeatedRoleSource, 0, len(plans))
	rowIDPositions := make([]int, len(plans))
	offset := 0
	for i, up := range plans {
		role := assignments.roles[i]
		oldWidth := len(up.tableDef.Cols)
		matchWidth := oldWidth + up.updateColLength
		if role == nil || role.oldWidth != oldWidth || up.rowIdPos < 0 || up.rowIdPos >= oldWidth {
			return nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE target")
		}
		old := make([]int, oldWidth)
		for j := range old {
			old[j] = offset + j
		}
		rowIDPositions[i] = offset + up.rowIdPos
		root, err := appendExpandedUpdateRole(builder, bindCtx, sourceStep, role, old, int32(i))
		if err != nil {
			return nil, err
		}
		filter := builder.qry.Nodes[root].Children[0]
		scan := builder.qry.Nodes[filter].Children[0]
		sources = append(sources, repeatedRoleSource{
			role: role, oldPositions: old, scan: scan, filter: filter, project: root, ordinal: int32(i),
		})

		cols := make([]string, 0, len(up.updateColPosMap))
		for name := range up.updateColPosMap {
			cols = append(cols, name)
		}
		sort.Slice(cols, func(i, j int) bool { return up.updateColPosMap[cols[i]] < up.updateColPosMap[cols[j]] })
		values := make([]*planpb.Expr, 0, len(cols))
		for _, name := range cols {
			assignment := assignments.positions[offset+up.updateColPosMap[name]]
			if assignment == nil || assignment.expr == nil {
				return nil, moerr.NewInternalErrorNoCtx("missing repeated UPDATE assignment")
			}
			colIdx, ok := up.tableDef.Name2ColIndex[name]
			if !ok || colIdx < 0 || int(colIdx) >= len(up.tableDef.Cols) {
				return nil, moerr.NewInternalErrorNoCtx("missing repeated UPDATE target column")
			}
			var value *planpb.Expr
			root, value, err = bindRepeatedAssignment(builder, bindCtx, root, role, assignment.expr, up.tableDef.Cols[colIdx], up.tableDef, true, isIgnore)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
			provisionalRepeatedMatchSink(builder, sink, matchID)
			if err = refreshRepeatedRoleSources(builder, sink, sources); err != nil {
				return nil, err
			}
		}
		for _, col := range up.tableDef.Cols {
			if col.OnUpdate == nil || col.OnUpdate.Expr == nil {
				continue
			}
			if _, explicit := up.updateColPosMap[col.Name]; explicit {
				continue
			}
			value := DeepCopyExpr(col.OnUpdate.Expr)
			if err = replaceFuncId(builder.GetContext(), value); err != nil {
				return nil, err
			}
			root, value, err = bindRepeatedAssignment(builder, bindCtx, root, role, value, col, up.tableDef, false, isIgnore)
			if err != nil {
				return nil, err
			}
			up.updateColPosMap[col.Name] = oldWidth + len(values)
			up.updateColLength++
			values = append(values, value)
			provisionalRepeatedMatchSink(builder, sink, matchID)
			if err = refreshRepeatedRoleSources(builder, sink, sources); err != nil {
				return nil, err
			}
		}
		project := make([]*planpb.Expr, 0, oldWidth+len(values))
		for j, col := range up.tableDef.Cols {
			project = append(project, GetColExpr(col.Typ, role.tag, int32(j)))
		}
		project = append(project, values...)
		tag := builder.genNewBindTag()
		root = builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_PROJECT, Children: []int32{root}, BindingTags: []int32{tag}, ProjectList: project,
		}, bindCtx)
		branches[i] = repeatedUpdateBranch{nodeID: root, tag: tag, oldWidth: oldWidth, rowIDPos: up.rowIdPos}
		offset += matchWidth
	}

	expanded, expandedTag, stats, err := appendExpandedUpdateMatches(builder, bindCtx, matchID, rowIDPositions, assignments.positions, plans)
	if err != nil {
		return nil, err
	}
	sinkNode := builder.qry.Nodes[sink]
	sinkNode.Children[0] = expanded
	sinkNode.BindingTags[0] = expandedTag
	sinkNode.ProjectList = getProjectionByLastNodeWithTag(builder, expanded, expandedTag)
	builder.tag2NodeID[expandedTag] = sink
	if err = refreshRepeatedRoleSources(builder, sink, sources); err != nil {
		return nil, err
	}
	if stats != nil {
		for i, source := range sources {
			branches[i].stats, branches[i].ordinal = stats, i
			builder.setRepeatedUpdateStats(source.filter, repeatedUpdateStatsHint{kind: repeatedStatsRole, source: stats, role: i, filters: append([]*planpb.Expr(nil), builder.qry.Nodes[source.filter].FilterList...)})
		}
	}
	return branches, nil
}
