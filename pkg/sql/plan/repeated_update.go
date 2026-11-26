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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// repeatedUpdateBranch carries one selected row per target role. Its columns
// contain the immutable old row followed by that role's assignment values.
type repeatedUpdateBranch struct {
	nodeID   int32
	tag      int32
	oldWidth int
	rowIDPos int
	stats    *repeatedUpdateStatsSource
	ordinal  int
}

// appendRepeatedParticipants combines roles writing the same physical table.
// Every role contributes at most one selected row per RowID. A presence marker
// distinguishes an absent role from a present role assigning NULL.
func appendRepeatedParticipants(
	builder *QueryBuilder,
	bindCtx *BindContext,
	roles []repeatedUpdateBranch,
) (nodeID, resultTag int32, markers []int, values [][]int, err error) {
	if len(roles) == 0 || roles[0].oldWidth <= 0 || roles[0].rowIDPos < 0 || roles[0].rowIDPos >= roles[0].oldWidth {
		return -1, 0, nil, nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE participants")
	}
	oldWidth, rowIDPos := roles[0].oldWidth, roles[0].rowIDPos
	markers = make([]int, len(roles))
	values = make([][]int, len(roles))
	width := oldWidth
	for i, role := range roles {
		if role.oldWidth != oldWidth || role.rowIDPos != rowIDPos || role.nodeID < 0 || int(role.nodeID) >= len(builder.qry.Nodes) {
			return -1, 0, nil, nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE role layout")
		}
		project := builder.qry.Nodes[role.nodeID].ProjectList
		if len(project) < oldWidth {
			return -1, 0, nil, nil, moerr.NewInternalErrorNoCtx("invalid repeated UPDATE role columns")
		}
		markers[i] = width
		width++
		for range project[oldWidth:] {
			values[i] = append(values[i], width)
			width++
		}
	}

	var union, unionTag int32
	for i, role := range roles {
		input := builder.qry.Nodes[role.nodeID].ProjectList
		project := make([]*planpb.Expr, 0, width)
		for j := 0; j < oldWidth; j++ {
			project = append(project, GetColExpr(input[j].Typ, role.tag, int32(j)))
		}
		for j, other := range roles {
			marker := makePlan2BoolConstExprWithType(true)
			marker.Typ.NotNullable = false
			if i != j {
				marker = typedRepeatedNull(marker.Typ)
			}
			project = append(project, marker)
			otherCols := builder.qry.Nodes[other.nodeID].ProjectList
			for k, expr := range otherCols[oldWidth:] {
				if i != j {
					project = append(project, typedRepeatedNull(expr.Typ))
					continue
				}
				typ := expr.Typ
				typ.NotNullable = false
				project = append(project, GetColExpr(typ, role.tag, int32(oldWidth+k)))
			}
		}
		tag := builder.genNewBindTag()
		branch := builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_PROJECT, Children: []int32{role.nodeID}, BindingTags: []int32{tag}, ProjectList: project,
		}, bindCtx)
		if i == 0 {
			union, unionTag = branch, tag
			continue
		}
		unionProject := make([]*planpb.Expr, width)
		for j, expr := range builder.qry.Nodes[union].ProjectList {
			unionProject[j] = GetColExpr(expr.Typ, unionTag, int32(j))
		}
		unionTag = builder.genNewBindTag()
		union = builder.appendNode(&planpb.Node{
			NodeType: planpb.Node_UNION_ALL, Children: []int32{union, branch}, BindingTags: []int32{unionTag}, ProjectList: unionProject,
		}, bindCtx)
	}
	if len(roles) == 1 {
		return union, unionTag, markers, values, nil
	}

	input := builder.qry.Nodes[union].ProjectList
	groupTag, aggTag := builder.genNewBindTag(), builder.genNewBindTag()
	aggregates := make([]*planpb.Expr, 0, width-1)
	project := make([]*planpb.Expr, width)
	for i, expr := range input {
		if i == rowIDPos {
			project[i] = GetColExpr(expr.Typ, groupTag, 0)
			continue
		}
		name := "any_value"
		if i < oldWidth {
			name = "_first_row"
		}
		agg, bindErr := BindFuncExprImplByPlanExpr(builder.GetContext(), name, []*planpb.Expr{GetColExpr(expr.Typ, unionTag, int32(i))})
		if bindErr != nil {
			return -1, 0, nil, nil, bindErr
		}
		agg.Typ = expr.Typ
		project[i] = GetColExpr(expr.Typ, aggTag, int32(len(aggregates)))
		aggregates = append(aggregates, agg)
	}
	group := builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_AGG, Children: []int32{union}, BindingTags: []int32{groupTag, aggTag},
		GroupBy: []*planpb.Expr{GetColExpr(input[rowIDPos].Typ, unionTag, int32(rowIDPos))},
		AggList: aggregates, SpillMem: builder.aggSpillMem,
	}, bindCtx)
	if roles[0].stats != nil {
		ordinals := make([]int, len(roles))
		for i, role := range roles {
			if role.stats != roles[0].stats {
				return -1, 0, nil, nil, moerr.NewInternalErrorNoCtx("inconsistent repeated UPDATE statistics")
			}
			ordinals[i] = role.ordinal
		}
		builder.setRepeatedUpdateStats(group, repeatedUpdateStatsHint{kind: repeatedStatsParticipants, source: roles[0].stats, roles: ordinals})
	}
	resultTag = builder.genNewBindTag()
	nodeID = builder.appendNode(&planpb.Node{
		NodeType: planpb.Node_PROJECT, Children: []int32{group}, BindingTags: []int32{resultTag}, ProjectList: project,
	}, bindCtx)
	return nodeID, resultTag, markers, values, nil
}

func typedRepeatedNull(typ planpb.Type) *planpb.Expr {
	result := makePlan2NullConstExprWithType()
	result.Typ = typ
	result.Typ.NotNullable = false
	return result
}
