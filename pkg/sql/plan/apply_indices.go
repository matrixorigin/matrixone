// Copyright 2023 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func (builder *QueryBuilder) applyIndices(nodeID int32) int32 {
	node := builder.qry.Nodes[nodeID]

	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		return builder.applyIndicesForPointSelect(nodeID, node)

	case plan.Node_JOIN:
		return builder.applyIndicesForJoin(nodeID, node)

	default:
		for i, childID := range node.Children {
			node.Children[i] = builder.applyIndices(childID)
		}

		return nodeID
	}
}

func (builder *QueryBuilder) applyIndicesForPointSelect(nodeID int32, node *plan.Node) int32 {
	if len(node.FilterList) == 0 || node.TableDef.Pkey == nil || len(node.TableDef.Indexes) == 0 {
		return nodeID
	}

	if node.Stats.Selectivity > 0.1 || node.Stats.Outcnt > InFilterCardLimit {
		return nodeID
	}

	col2filter := make(map[int32]int)
	for i, expr := range node.FilterList {
		fn, ok := expr.Expr.(*plan.Expr_F)
		if !ok {
			continue
		}

		if !IsEqualFunc(fn.F.Func.Obj) {
			continue
		}

		if _, ok := fn.F.Args[0].Expr.(*plan.Expr_C); ok {
			if _, ok := fn.F.Args[1].Expr.(*plan.Expr_Col); ok {
				fn.F.Args[0], fn.F.Args[1] = fn.F.Args[1], fn.F.Args[0]
			}
		}

		col, ok := fn.F.Args[0].Expr.(*plan.Expr_Col)
		if !ok {
			continue
		}

		if _, ok := fn.F.Args[1].Expr.(*plan.Expr_C); !ok {
			continue
		}

		col2filter[col.Col.ColPos] = i
	}

	filterOnPK := true
	for _, part := range node.TableDef.Pkey.Names {
		colIdx := node.TableDef.Name2ColIndex[part]
		_, ok := col2filter[colIdx]
		if !ok {
			filterOnPK = false
			break
		}
	}

	if filterOnPK {
		return nodeID
	}

	indexes := node.TableDef.Indexes
	sort.Slice(indexes, func(i, j int) bool {
		return indexes[i].Unique && !indexes[j].Unique
	})

	filterIdx := make([]int, 0, len(col2filter))
	for _, idxDef := range indexes {
		if !idxDef.TableExist {
			continue
		}

		numParts := len(idxDef.Parts)
		if !idxDef.Unique {
			numParts--
		}
		if numParts == 0 || numParts > len(col2filter) {
			continue
		}

		filterIdx = filterIdx[:0]
		for i := 0; i < numParts; i++ {
			colIdx := node.TableDef.Name2ColIndex[idxDef.Parts[i]]
			idx, ok := col2filter[colIdx]
			if !ok {
				break
			}

			filterIdx = append(filterIdx, idx)
		}

		if len(filterIdx) < numParts {
			continue
		}

		idxTag := builder.genNewTag()
		idxObjRef, idxTableDef := builder.compCtx.Resolve(node.ObjRef.SchemaName, idxDef.IndexTableName)

		builder.nameByColRef[[2]int32{idxTag, 0}] = idxTableDef.Name + "." + idxTableDef.Cols[0].Name
		builder.nameByColRef[[2]int32{idxTag, 1}] = idxTableDef.Name + "." + idxTableDef.Cols[1].Name

		var idxFilter *plan.Expr
		if numParts == 1 {
			idx := filterIdx[0]

			args := node.FilterList[idx].Expr.(*plan.Expr_F).F.Args
			col := args[0].Expr.(*plan.Expr_Col).Col
			col.RelPos = idxTag
			col.ColPos = 0
			col.Name = idxTableDef.Cols[0].Name

			if idxDef.Unique {
				idxFilter = node.FilterList[idx]
			} else {
				args[0].Typ = DeepCopyType(idxTableDef.Cols[0].Typ)
				args[1], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{args[1]})
				idxFilter, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "startswith", args)
			}

			node.FilterList = append(node.FilterList[:idx], node.FilterList[idx+1:]...)
		} else {
			serialArgs := make([]*plan.Expr, numParts)
			for i := range filterIdx {
				serialArgs[i] = node.FilterList[filterIdx[i]].Expr.(*plan.Expr_F).F.Args[1]
			}
			rightArg, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", serialArgs)

			funcName := "="
			if !idxDef.Unique {
				funcName = "startswith"
			}
			idxFilter, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{
				{
					Typ: DeepCopyType(idxTableDef.Cols[0].Typ),
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: idxTag,
							ColPos: 0,
						},
					},
				},
				rightArg,
			})

			hitFilterSet := make(map[int]emptyType)
			for i := range filterIdx {
				hitFilterSet[filterIdx[i]] = emptyStruct
			}

			newFilterList := make([]*plan.Expr, 0, len(node.FilterList)-numParts)
			for i, filter := range node.FilterList {
				if _, ok := hitFilterSet[i]; !ok {
					newFilterList = append(newFilterList, filter)
				}
			}

			node.FilterList = newFilterList
		}

		idxTableNodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			ObjRef:      idxObjRef,
			TableDef:    idxTableDef,
			FilterList:  []*plan.Expr{idxFilter},
			Limit:       node.Limit,
			Offset:      node.Offset,
			BindingTags: []int32{idxTag},
		}, builder.ctxByNode[nodeID])

		node.Limit, node.Offset = nil, nil

		pkIdx := node.TableDef.Name2ColIndex[node.TableDef.Pkey.PkeyColName]
		pkExpr := &plan.Expr{
			Typ: DeepCopyType(node.TableDef.Cols[pkIdx].Typ),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: node.BindingTags[0],
					ColPos: pkIdx,
				},
			},
		}

		joinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			pkExpr,
			{
				Typ: DeepCopyType(pkExpr.Typ),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag,
						ColPos: 1,
					},
				},
			},
		})
		joinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{nodeID, idxTableNodeID},
			OnList:   []*plan.Expr{joinCond},
		}, builder.ctxByNode[nodeID])

		ReCalcNodeStats(nodeID, builder, false, true, true)
		nodeID = joinNodeID

		break
	}

	return nodeID
}

func (builder *QueryBuilder) applyIndicesForJoin(nodeID int32, node *plan.Node) int32 {
	node.Children[1] = builder.applyIndices(node.Children[1])

	leftChild := builder.qry.Nodes[node.Children[0]]
	if leftChild.NodeType != plan.Node_TABLE_SCAN {
		node.Children[0] = builder.applyIndices(node.Children[0])
		return nodeID
	} else if leftChild.TableDef.Pkey == nil {
		return nodeID
	}

	newLeftChildID := builder.applyIndicesForPointSelect(node.Children[0], leftChild)
	if newLeftChildID != node.Children[0] {
		node.Children[0] = newLeftChildID
		return nodeID
	}

	rightChild := builder.qry.Nodes[node.Children[1]]

	if rightChild.Stats.Outcnt > InFilterCardLimit || rightChild.Stats.Outcnt > leftChild.Stats.Cost*0.1 {
		return nodeID
	}

	leftTags := make(map[int32]emptyType)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = emptyStruct
	}

	rightTags := make(map[int32]emptyType)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = emptyStruct
	}

	col2Cond := make(map[int32]int)
	for i, expr := range node.OnList {
		if !isEquiCond(expr, leftTags, rightTags) {
			continue
		}

		col, ok := expr.Expr.(*plan.Expr_F).F.Args[0].Expr.(*plan.Expr_Col)
		if !ok {
			continue
		}

		col2Cond[col.Col.ColPos] = i
	}

	joinOnPK := true
	for _, part := range leftChild.TableDef.Pkey.Names {
		colIdx := leftChild.TableDef.Name2ColIndex[part]
		_, ok := col2Cond[colIdx]
		if !ok {
			joinOnPK = false
			break
		}
	}

	if joinOnPK {
		return nodeID
	}

	indexes := leftChild.TableDef.Indexes
	condIdx := make([]int, 0, len(col2Cond))
	for _, idxDef := range indexes {
		if !idxDef.TableExist || !idxDef.Unique {
			continue
		}

		numParts := len(idxDef.Parts)
		if numParts > len(col2Cond) {
			continue
		}

		condIdx = condIdx[:0]
		for i := 0; i < numParts; i++ {
			colIdx := leftChild.TableDef.Name2ColIndex[idxDef.Parts[i]]
			idx, ok := col2Cond[colIdx]
			if !ok {
				break
			}

			condIdx = append(condIdx, idx)
		}

		if len(condIdx) < numParts {
			continue
		}

		idxTag := builder.genNewTag()
		idxObjRef, idxTableDef := builder.compCtx.Resolve(leftChild.ObjRef.SchemaName, idxDef.IndexTableName)

		builder.nameByColRef[[2]int32{idxTag, 0}] = idxTableDef.Name + "." + idxTableDef.Cols[0].Name
		builder.nameByColRef[[2]int32{idxTag, 1}] = idxTableDef.Name + "." + idxTableDef.Cols[1].Name

		idxTableNodeID := builder.appendNode(&plan.Node{
			NodeType:    plan.Node_TABLE_SCAN,
			ObjRef:      idxObjRef,
			TableDef:    idxTableDef,
			BindingTags: []int32{idxTag},
		}, builder.ctxByNode[nodeID])

		pkIdx := leftChild.TableDef.Name2ColIndex[leftChild.TableDef.Pkey.PkeyColName]
		pkExpr := &plan.Expr{
			Typ: DeepCopyType(leftChild.TableDef.Cols[pkIdx].Typ),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: leftChild.BindingTags[0],
					ColPos: pkIdx,
				},
			},
		}

		pkJoinCond, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
			pkExpr,
			{
				Typ: DeepCopyType(pkExpr.Typ),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag,
						ColPos: 1,
					},
				},
			},
		})

		var idxJoinCond *plan.Expr
		if numParts == 1 {
			idx := condIdx[0]

			args := node.OnList[idx].Expr.(*plan.Expr_F).F.Args
			col := args[0].Expr.(*plan.Expr_Col).Col
			col.RelPos = idxTag
			col.ColPos = 0
			col.Name = idxTableDef.Cols[0].Name

			idxJoinCond = node.OnList[idx]
			node.OnList[idx] = pkJoinCond
		} else {
			serialArgs := make([]*plan.Expr, numParts)
			for i := range condIdx {
				serialArgs[i] = node.OnList[condIdx[i]].Expr.(*plan.Expr_F).F.Args[1]
			}
			rightArg, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", serialArgs)

			idxJoinCond, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
				{
					Typ: DeepCopyType(idxTableDef.Cols[0].Typ),
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: idxTag,
							ColPos: 0,
						},
					},
				},
				rightArg,
			})

			hitFilterSet := make(map[int]emptyType)
			for i := range condIdx {
				hitFilterSet[condIdx[i]] = emptyStruct
			}

			newOnList := make([]*plan.Expr, 0, len(node.OnList)-numParts)
			for i, filter := range node.OnList {
				if _, ok := hitFilterSet[i]; !ok {
					newOnList = append(newOnList, filter)
				}
			}

			node.OnList = append(newOnList, pkJoinCond)
		}

		idxJoinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{idxTableNodeID, node.Children[1]},
			Limit:    node.Limit,
			Offset:   node.Offset,
			OnList:   []*plan.Expr{idxJoinCond},
		}, builder.ctxByNode[nodeID])

		node.Children[1] = idxJoinNodeID
		node.Limit, node.Offset = nil, nil

		ReCalcNodeStats(nodeID, builder, false, true, true)

		break
	}

	return nodeID
}
