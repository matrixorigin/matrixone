// Copyright 2024 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func (builder *QueryBuilder) markSinkProject(nodeID int32, step int32, colRefBool map[[2]int32]bool) {
	node := builder.qry.Nodes[nodeID]

	switch node.NodeType {
	case plan.Node_SINK_SCAN, plan.Node_RECURSIVE_SCAN, plan.Node_RECURSIVE_CTE:
		for _, i := range node.SourceStep {
			if i >= step {
				for _, expr := range node.ProjectList {
					colRefBool[[2]int32{i, expr.GetCol().ColPos}] = true
				}
			}
		}
	default:
		for i := range node.Children {
			builder.markSinkProject(node.Children[i], step, colRefBool)
		}
	}
}

func (builder *QueryBuilder) pruneUnneededColumns(nodeID int32, step int32, colRefCnt map[[2]int32]int, sinkColUsed map[[2]int32]bool) error {
	node := builder.qry.Nodes[nodeID]

	switch node.NodeType {
	case plan.Node_FUNCTION_SCAN:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		tag := node.BindingTags[0]

		for _, expr := range node.FilterList {
			increaseRefCnt(expr, -1, colRefCnt)
		}

		for i := range node.TableDef.Cols {
			if colRefCnt[[2]int32{tag, int32(i)}] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: tag,
				ColPos: int32(i),
			})
		}

		if len(node.OutputList) == 0 {
			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: tag,
				ColPos: 0,
			})
		}
		childId := node.Children[0]
		childNode := builder.qry.Nodes[childId]

		if childNode.NodeType == plan.Node_VALUE_SCAN {
			break
		}
		for _, expr := range node.TblFuncExprList {
			increaseRefCnt(expr, 1, colRefCnt)
		}
		err := builder.pruneUnneededColumns(childId, step, colRefCnt, sinkColUsed)

		if err != nil {
			return err
		}

		for _, expr := range node.TblFuncExprList {
			increaseRefCnt(expr, -1, colRefCnt)
		}

	case plan.Node_TABLE_SCAN, plan.Node_MATERIAL_SCAN, plan.Node_EXTERNAL_SCAN, plan.Node_SOURCE_SCAN:
		for _, expr := range node.FilterList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		for _, expr := range node.BlockFilterList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		for _, rfSpec := range node.RuntimeFilterProbeList {
			if rfSpec.Expr != nil {
				increaseRefCnt(rfSpec.Expr, 1, colRefCnt)
			}
		}

		tag := node.BindingTags[0]

		for _, expr := range node.FilterList {
			increaseRefCnt(expr, -1, colRefCnt)
		}

		for _, expr := range node.BlockFilterList {
			increaseRefCnt(expr, -1, colRefCnt)
		}

		for _, rfSpec := range node.RuntimeFilterProbeList {
			if rfSpec.Expr != nil {
				increaseRefCnt(rfSpec.Expr, -1, colRefCnt)
			}
		}

		for i := range node.TableDef.Cols {
			globalRef := [2]int32{tag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: tag,
				ColPos: int32(i),
			})
		}

		if len(node.OutputList) == 0 {
			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: tag,
				ColPos: 0,
			})
		}

	case plan.Node_INTERSECT, plan.Node_INTERSECT_ALL,
		plan.Node_UNION, plan.Node_UNION_ALL,
		plan.Node_MINUS, plan.Node_MINUS_ALL:

		leftID := node.Children[0]
		rightID := node.Children[1]
		for _, expr := range node.ProjectList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		rightNode := builder.qry.Nodes[rightID]
		if rightNode.NodeType == plan.Node_PROJECT {
			projectTag := rightNode.BindingTags[0]
			for i := range rightNode.ProjectList {
				colRefCnt[[2]int32{projectTag, int32(i)}] = 1
			}
		}

		err := builder.pruneUnneededColumns(leftID, step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		err = builder.pruneUnneededColumns(rightID, step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		childOutList := builder.qry.Nodes[leftID].OutputList
		for i, expr := range node.ProjectList {
			increaseRefCnt(expr, -1, colRefCnt)
			node.OutputList = append(node.OutputList, childOutList[i])
		}

	case plan.Node_JOIN:
		for _, expr := range node.OnList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		leftID := node.Children[0]
		err := builder.pruneUnneededColumns(leftID, step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		rightID := node.Children[1]
		err = builder.pruneUnneededColumns(rightID, step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		for _, expr := range node.OnList {
			increaseRefCnt(expr, -1, colRefCnt)
		}

		leftOutList := builder.qry.Nodes[leftID].OutputList
		for _, colRef := range leftOutList {
			globalRef := [2]int32{colRef.RelPos, colRef.ColPos}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, colRef)
		}

		if node.JoinType == plan.Node_MARK {
			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: node.BindingTags[0],
				ColPos: 0,
			})
		} else {
			rightOutList := builder.qry.Nodes[rightID].OutputList
			for _, colRef := range rightOutList {
				globalRef := [2]int32{colRef.RelPos, colRef.ColPos}
				if colRefCnt[globalRef] == 0 {
					continue
				}

				node.OutputList = append(node.OutputList, colRef)
			}
		}

		if len(node.OutputList) == 0 {
			node.OutputList = append(node.OutputList, leftOutList[0])
		}

	case plan.Node_AGG:
		increaseRefCntForExprList(node.GroupBy, 1, colRefCnt)
		increaseRefCntForExprList(node.AggList, 1, colRefCnt)

		err := builder.pruneUnneededColumns(node.Children[0], step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		groupTag := node.BindingTags[0]
		aggregateTag := node.BindingTags[1]
		groupSize := int32(len(node.GroupBy))

		for idx, expr := range node.GroupBy {
			increaseRefCnt(expr, -1, colRefCnt)
			globalRef := [2]int32{groupTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: groupTag,
				ColPos: int32(idx),
			})
		}

		for idx, expr := range node.AggList {
			increaseRefCnt(expr, -1, colRefCnt)
			globalRef := [2]int32{aggregateTag, int32(idx)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: aggregateTag,
				ColPos: int32(idx),
			})
		}

		if len(node.OutputList) == 0 {
			if groupSize > 0 {
				node.OutputList = append(node.OutputList, plan.ColRef{
					RelPos: groupTag,
					ColPos: 0,
				})
			} else {
				node.OutputList = append(node.OutputList, plan.ColRef{
					RelPos: aggregateTag,
					ColPos: 0,
				})
			}
		}

	case plan.Node_SAMPLE:
		groupTag := node.BindingTags[0]
		sampleTag := node.BindingTags[1]

		increaseRefCntForExprList(node.GroupBy, 1, colRefCnt)
		increaseRefCntForExprList(node.AggList, 1, colRefCnt)

		// the result order of sample will follow [group by columns, sample columns, other columns].
		// and the projection list needs to be based on the result order.
		err := builder.pruneUnneededColumns(node.Children[0], step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		// deal with group col and sample col.
		for i, expr := range node.GroupBy {
			increaseRefCnt(expr, -1, colRefCnt)

			globalRef := [2]int32{groupTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: groupTag,
				ColPos: int32(i),
			})
		}

		for i, expr := range node.AggList {
			increaseRefCnt(expr, -1, colRefCnt)

			globalRef := [2]int32{sampleTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: sampleTag,
				ColPos: int32(i),
			})
		}

		childOutList := builder.qry.Nodes[node.Children[0]].OutputList
		for _, colRef := range childOutList {
			globalRef := [2]int32{colRef.RelPos, colRef.ColPos}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, colRef)
		}

	case plan.Node_TIME_WINDOW:
		for _, expr := range node.AggList {
			increaseRefCnt(expr, 1, colRefCnt)
		}
		increaseRefCnt(node.OrderBy[0].Expr, 1, colRefCnt)

		err := builder.pruneUnneededColumns(node.Children[0], step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		timeTag := node.BindingTags[0]

		increaseRefCnt(node.OrderBy[0].Expr, -1, colRefCnt)

		idx := 0
		var wstart, wend *plan.Expr
		var i, j int
		for k, expr := range node.AggList {
			if e, ok := expr.Expr.(*plan.Expr_Col); ok {
				if e.Col.Name == TimeWindowStart {
					wstart = expr
					i = k
				}
				if e.Col.Name == TimeWindowEnd {
					wend = expr
					j = k
				}
				continue
			}
			increaseRefCnt(expr, -1, colRefCnt)

			globalRef := [2]int32{timeTag, int32(k)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: timeTag,
				ColPos: int32(idx),
			})
			idx++
		}

		if wstart != nil {
			increaseRefCnt(wstart, -1, colRefCnt)

			globalRef := [2]int32{timeTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				break
			}

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: timeTag,
				ColPos: int32(idx),
			})
			idx++
		}

		if wend != nil {
			increaseRefCnt(wend, -1, colRefCnt)

			globalRef := [2]int32{timeTag, int32(j)}
			if colRefCnt[globalRef] == 0 {
				break
			}

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: timeTag,
				ColPos: int32(idx),
			})
		}

	case plan.Node_WINDOW:
		for _, expr := range node.WinSpecList {
			increaseRefCnt(expr, 1, colRefCnt)
		}

		err := builder.pruneUnneededColumns(node.Children[0], step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		childOutList := builder.qry.Nodes[node.Children[0]].OutputList
		for _, colRef := range childOutList {
			globalRef := [2]int32{colRef.RelPos, colRef.ColPos}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, colRef)
		}

		windowTag := node.BindingTags[0]
		l := len(childOutList)

		for _, expr := range node.FilterList {
			builder.remapWindowClause(expr, windowTag, int32(l))
		}

		for i, expr := range node.WinSpecList {
			increaseRefCnt(expr, -1, colRefCnt)

			globalRef := [2]int32{windowTag, int32(node.GetWindowIdx())}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: windowTag,
				ColPos: int32(i),
			})
		}

	case plan.Node_FILL:

		//for _, expr := range node.AggList {
		//	increaseRefCnt(expr, 1, colRefCnt)
		//}

		err := builder.pruneUnneededColumns(node.Children[0], step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		childOutList := builder.qry.Nodes[node.Children[0]].OutputList
		for _, colRef := range childOutList {
			globalRef := [2]int32{colRef.RelPos, colRef.ColPos}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, colRef)
		}

		if len(node.OutputList) == 0 {
			node.OutputList = append(node.OutputList, childOutList[0])
		}

	case plan.Node_SORT, plan.Node_PARTITION:
		for _, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, 1, colRefCnt)
		}

		err := builder.pruneUnneededColumns(node.Children[0], step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		for _, orderBy := range node.OrderBy {
			increaseRefCnt(orderBy.Expr, -1, colRefCnt)
		}

		childOutList := builder.qry.Nodes[node.Children[0]].OutputList
		for _, colRef := range childOutList {
			globalRef := [2]int32{colRef.RelPos, colRef.ColPos}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, colRef)
		}

		if len(node.OutputList) == 0 {
			node.OutputList = append(node.OutputList, childOutList[0])
		}

	case plan.Node_FILTER:
		increaseRefCntForExprList(node.FilterList, 1, colRefCnt)

		err := builder.pruneUnneededColumns(node.Children[0], step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		increaseRefCntForExprList(node.FilterList, -1, colRefCnt)

		childOutList := builder.qry.Nodes[node.Children[0]].OutputList
		for _, colRef := range childOutList {
			globalRef := [2]int32{colRef.RelPos, colRef.ColPos}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, colRef)
		}

		if len(node.OutputList) == 0 && len(childOutList) > 0 {
			node.OutputList = append(node.OutputList, childOutList[0])
		}

	case plan.Node_SINK_SCAN, plan.Node_RECURSIVE_SCAN, plan.Node_RECURSIVE_CTE:
		tag := node.BindingTags[0]

		for i := range node.ProjectList {
			globalRef := [2]int32{tag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}
			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: tag,
				ColPos: int32(i),
			})

			for _, sourceStep := range node.SourceStep {
				if sourceStep >= step {
					continue
				}
				sinkColUsed[[2]int32{sourceStep, int32(i)}] = true
			}
		}

	case plan.Node_SINK:
		childNode := builder.qry.Nodes[node.Children[0]]
		resultTag := childNode.BindingTags[0]
		for i := range childNode.ProjectList {
			if sinkColUsed[[2]int32{step, int32(i)}] {
				colRefCnt[[2]int32{resultTag, int32(i)}] = 1
			}
		}

		err := builder.pruneUnneededColumns(node.Children[0], step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		for i := range node.ProjectList {
			if !sinkColUsed[[2]int32{step, int32(i)}] {
				continue
			}

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: resultTag,
				ColPos: int32(i),
			})
		}

	case plan.Node_PROJECT, plan.Node_MATERIAL:
		projectTag := node.BindingTags[0]

		var neededProj []int32

		for i, expr := range node.ProjectList {
			globalRef := [2]int32{projectTag, int32(i)}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			neededProj = append(neededProj, int32(i))
			increaseRefCnt(expr, 1, colRefCnt)
		}

		if len(neededProj) == 0 {
			increaseRefCnt(node.ProjectList[0], 1, colRefCnt)
			neededProj = append(neededProj, 0)
		}

		err := builder.pruneUnneededColumns(node.Children[0], step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		for _, needed := range neededProj {
			expr := node.ProjectList[needed]
			increaseRefCnt(expr, -1, colRefCnt)

			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: projectTag,
				ColPos: needed,
			})
		}

		if len(node.OutputList) == 0 {
			node.OutputList = append(node.OutputList, plan.ColRef{
				RelPos: projectTag,
				ColPos: 0,
			})
		}

	case plan.Node_VALUE_SCAN:
		node.NotCacheable = true
		// VALUE_SCAN always have one column now
		if node.TableDef != nil {
			tag := node.BindingTags[0]
			globalRef := [2]int32{tag, 0}

			for i := range node.TableDef.Cols {
				globalRef[1] = int32(i)
				if colRefCnt[globalRef] == 0 {
					continue
				}

				node.OutputList = append(node.OutputList, plan.ColRef{
					RelPos: tag,
					ColPos: int32(i),
				})
			}
		}

	case plan.Node_LOCK_OP:
		preNode := builder.qry.Nodes[node.Children[0]]
		pkexpr := &plan.Expr{
			Typ: *node.LockTargets[0].GetPrimaryColTyp(),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: preNode.BindingTags[0],
					ColPos: node.LockTargets[0].PrimaryColIdxInBat,
				},
			},
		}

		increaseRefCnt(pkexpr, 1, colRefCnt)

		err := builder.pruneUnneededColumns(node.Children[0], step, colRefCnt, sinkColUsed)
		if err != nil {
			return err
		}

		increaseRefCnt(pkexpr, -1, colRefCnt)

		childOutList := builder.qry.Nodes[node.Children[0]].OutputList
		for _, colRef := range childOutList {
			globalRef := [2]int32{colRef.RelPos, colRef.ColPos}
			if colRefCnt[globalRef] == 0 {
				continue
			}

			node.OutputList = append(node.OutputList, colRef)
		}

		if len(node.OutputList) == 0 {
			node.OutputList = append(node.OutputList, childOutList[0])
		}

	default:
		return moerr.NewInternalError(builder.GetContext(), "unsupport node type")
	}

	return nil
}
