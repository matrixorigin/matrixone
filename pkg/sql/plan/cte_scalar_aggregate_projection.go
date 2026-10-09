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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

// prepareScalarValueAggregateProjection preserves the original scalar output
// through transparent ORDER BY, DISTINCT and per-identity LIMIT wrappers.
// The row-comparison path uses prepareCorrelatedScalarAggregatePostJoinProjection
// instead: its multiple result columns and HAVING ownership differ.
func (builder *QueryBuilder) prepareScalarValueAggregateProjection(
	subID int32, subCtx *BindContext, joinPreds []*plan.Expr,
) (*plan.Expr, bool, error) {
	if !subCtx.hasSingleRow || len(subCtx.groups) != 0 || len(subCtx.aggregates) == 0 || len(joinPreds) == 0 {
		return nil, false, nil
	}
	project := builder.qry.Nodes[subID]
	if project.NodeType == plan.Node_WINDOW && len(project.Children) == 1 &&
		len(project.FilterList) == 1 && len(project.WinSpecList) == 1 &&
		project.WinSpecList[0].GetW() != nil && project.WinSpecList[0].GetW().Name == "row_number" {
		child := builder.qry.Nodes[project.Children[0]]
		if child.NodeType == plan.Node_PARTITION && len(child.Children) == 1 &&
			builder.qry.Nodes[child.Children[0]].NodeType == plan.Node_PROJECT {
			return builder.prepareScalarValueAggregateProjection(child.Children[0], subCtx, joinPreds)
		}
	}
	if project.NodeType == plan.Node_PROJECT && len(project.BindingTags) == 1 && len(project.Children) == 1 &&
		len(project.ProjectList) > 0 && project.Limit == nil && project.Offset == nil {
		childID := project.Children[0]
	wrapLoop:
		for {
			child := builder.qry.Nodes[childID]
			if len(child.Children) != 1 || child.Limit != nil || child.Offset != nil {
				break
			}
			switch child.NodeType {
			case plan.Node_SORT, plan.Node_DISTINCT, plan.Node_PARTITION:
			case plan.Node_WINDOW:
				if len(child.WinSpecList) != 1 || child.WinSpecList[0].GetW() == nil ||
					child.WinSpecList[0].GetW().Name != "row_number" || len(child.FilterList) != 1 {
					break wrapLoop
				}
			default:
				break wrapLoop
			}
			childID = child.Children[0]
		}
		inner := builder.qry.Nodes[childID]
		if inner.NodeType == plan.Node_PROJECT && inner != project && len(inner.Children) == 1 &&
			len(inner.BindingTags) == 1 && len(inner.ProjectList) > 0 && inner.Limit == nil && inner.Offset == nil {
			agg := builder.qry.Nodes[inner.Children[0]]
			if agg.NodeType != plan.Node_AGG {
				return nil, false, nil
			}
			outerCol := project.ProjectList[0].GetCol()
			if len(agg.AggList) > 0 && len(agg.AggList) == len(subCtx.aggregates) &&
				len(agg.BindingTags) > 1 && agg.BindingTags[1] == subCtx.aggregateTag &&
				outerCol != nil && outerCol.RelPos == inner.BindingTags[0] && outerCol.ColPos == 0 {
				projectedAggregates := make([]*plan.Expr, len(agg.AggList))
				rawAggregates := make([]*plan.Expr, len(agg.AggList))
				for i, aggregate := range agg.AggList {
					fn := aggregate.GetF()
					if fn == nil || fn.Func == nil {
						return nil, false, nil
					}
					projectPos := int32(0)
					if i > 0 {
						projectPos = int32(len(project.ProjectList) + i - 1)
					}
					projected := GetColExpr(aggregate.Typ, project.BindingTags[0], projectPos)
					projected.Typ.NotNullable = false
					var err error
					projectedAggregates[i], err = builder.restoreAggregateEmptyResult(projected, aggregate, fn.Func.ObjName)
					if err != nil {
						return nil, false, err
					}
					rawAggregates[i] = GetColExpr(aggregate.Typ, subCtx.aggregateTag, int32(i))
				}
				postJoin, ok := replaceAggregateRefsForPostJoin(
					DeepCopyExpr(inner.ProjectList[0]), subCtx.aggregateTag, projectedAggregates)
				if ok {
					postJoin, stillCorrelated := decreaseDepth(postJoin)
					if !stillCorrelated {
						innerProject := append([]*plan.Expr(nil), inner.ProjectList...)
						innerProject[0] = rawAggregates[0]
						inner.ProjectList = append(innerProject, rawAggregates[1:]...)
						for i := 1; i < len(rawAggregates); i++ {
							project.ProjectList = append(project.ProjectList, GetColExpr(
								agg.AggList[i].Typ, inner.BindingTags[0], int32(len(innerProject)+i-1)))
						}
						return postJoin, true, nil
					}
				}
			}
		}
	}
	if project.NodeType == plan.Node_AGG {
		if len(project.BindingTags) < 2 || project.BindingTags[1] != subCtx.aggregateTag ||
			len(project.AggList) != 1 || len(subCtx.aggregates) != 1 || len(subCtx.results) != 1 {
			return nil, false, nil
		}
		aggregate := project.AggList[0]
		fn := aggregate.GetF()
		if fn == nil || fn.Func == nil {
			return nil, false, nil
		}
		projected := GetColExpr(aggregate.Typ, subCtx.aggregateTag, 0)
		projected.Typ.NotNullable = false
		postJoinProjection, err := builder.restoreAggregateEmptyResult(projected, aggregate, fn.Func.ObjName)
		return postJoinProjection, err == nil, err
	}
	if project.NodeType != plan.Node_PROJECT || len(project.Children) != 1 || len(project.BindingTags) != 1 ||
		len(project.ProjectList) == 0 || project.Limit != nil || project.Offset != nil || project.RankOption != nil {
		return nil, false, nil
	}
	agg := builder.qry.Nodes[project.Children[0]]
	if agg.NodeType != plan.Node_AGG || len(agg.BindingTags) < 2 || agg.BindingTags[1] != subCtx.aggregateTag ||
		len(agg.AggList) != len(subCtx.aggregates) {
		return nil, false, nil
	}
	projectTag := project.BindingTags[0]
	projectedAggregates := make([]*plan.Expr, len(agg.AggList))
	rawAggregates := make([]*plan.Expr, len(agg.AggList))
	firstAppendedPos := int32(len(project.ProjectList))
	for i, aggregate := range agg.AggList {
		fn := aggregate.GetF()
		if fn == nil || fn.Func == nil {
			return nil, false, nil
		}
		projectPos := int32(0)
		if i > 0 {
			projectPos = firstAppendedPos + int32(i-1)
		}
		rawAggregates[i] = GetColExpr(aggregate.Typ, subCtx.aggregateTag, int32(i))
		projected := GetColExpr(aggregate.Typ, projectTag, projectPos)
		projected.Typ.NotNullable = false
		var err error
		projectedAggregates[i], err = builder.restoreAggregateEmptyResult(projected, aggregate, fn.Func.ObjName)
		if err != nil {
			return nil, false, err
		}
	}
	postJoinProjection, ok := replaceAggregateRefsForPostJoin(
		DeepCopyExpr(project.ProjectList[0]), subCtx.aggregateTag, projectedAggregates)
	if !ok {
		return nil, false, nil
	}
	postJoinProjection, stillCorrelated := decreaseDepth(postJoinProjection)
	if stillCorrelated {
		return nil, false, nil
	}
	newProjectList := make([]*plan.Expr, len(project.ProjectList), len(project.ProjectList)+len(rawAggregates)-1)
	copy(newProjectList, project.ProjectList)
	newProjectList[0] = rawAggregates[0]
	newProjectList = append(newProjectList, rawAggregates[1:]...)
	project.ProjectList = newProjectList
	return postJoinProjection, true, nil
}
