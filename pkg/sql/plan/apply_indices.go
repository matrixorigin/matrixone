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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func isRuntimeConstExpr(expr *plan.Expr) bool {
	switch expr.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_P, *plan.Expr_V:
		return true

	case *plan.Expr_F:
		fn := expr.GetF()
		return fn.Func.ObjName == "cast" && isRuntimeConstExpr(fn.Args[0])

	default:
		return false
	}
}

func (builder *QueryBuilder) applyIndices(nodeID int32, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {
	node := builder.qry.Nodes[nodeID]

	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		return builder.applyIndicesForFilters(nodeID, node, colRefCnt, idxColMap)

	case plan.Node_JOIN:
		return builder.applyIndicesForJoins(nodeID, node, colRefCnt, idxColMap)

	default:
		for i, childID := range node.Children {
			node.Children[i] = builder.applyIndices(childID, colRefCnt, idxColMap)
		}

		replaceColumnsForNode(node, idxColMap)

		return nodeID
	}
}

func (builder *QueryBuilder) applyIndicesForFilters(nodeID int32, node *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {

	if len(node.FilterList) == 0 || node.TableDef.Pkey == nil || len(node.TableDef.Indexes) == 0 {
		return nodeID
	}
	// 1. Master Index Check
	{
		masterIndexes := make([]*plan.IndexDef, 0)
		for _, indexDef := range node.TableDef.Indexes {
			if !indexDef.Unique && catalog.IsMasterIndexAlgo(indexDef.IndexAlgo) {
				masterIndexes = append(masterIndexes, indexDef)
			}
		}

		if len(masterIndexes) == 0 {
			goto END0
		}

		for _, expr := range node.FilterList {
			fn := expr.GetF()
			if fn == nil {
				goto END0
			}

			switch fn.Func.ObjName {
			case "=":
				if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
					fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
				}

				if !isRuntimeConstExpr(fn.Args[1]) {
					goto END0
				}
			case "in", "between":

			default:
				goto END0
			}

			col := fn.Args[0].GetCol()
			if col == nil {
				goto END0
			}
		}

		for _, indexDef := range masterIndexes {
			isAllFilterColumnsIncluded := true
			for _, expr := range node.FilterList {
				fn := expr.GetF()
				col := fn.Args[0].GetCol()
				if !isKeyPresentInList(col.Name, indexDef.Parts) {
					isAllFilterColumnsIncluded = false
					break
				}
			}
			if isAllFilterColumnsIncluded {
				return builder.applyIndicesForFiltersMasterIndex(nodeID, node, colRefCnt, idxColMap)
			}
		}

	}
END0:
	// 2. Regular Index Check
	{
		return builder.applyIndicesForFiltersRegularIndex(nodeID, node, colRefCnt, idxColMap)
	}
}

func (builder *QueryBuilder) applyIndicesForFiltersRegularIndex(nodeID int32, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {
	if len(node.FilterList) == 0 || node.TableDef.Pkey == nil || len(node.TableDef.Indexes) == 0 {
		return nodeID
	}

	var pkPos int32 = -1
	if len(node.TableDef.Pkey.Names) == 1 {
		pkPos = node.TableDef.Name2ColIndex[node.TableDef.Pkey.Names[0]]
	}

	indexes := node.TableDef.Indexes
	sort.Slice(indexes, func(i, j int) bool {
		return (indexes[i].Unique && !indexes[j].Unique) || (indexes[i].Unique == indexes[j].Unique && len(indexes[i].Parts) > len(indexes[j].Parts))
	})

	// Apply unique/secondary indices if only indexed column is referenced

	{
		colPos := int32(-1)
		for _, expr := range node.FilterList {
			fn := expr.GetF()
			if fn == nil {
				goto END0
			}

			switch fn.Func.ObjName {
			case "=":
				if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
					fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
				}

				if !isRuntimeConstExpr(fn.Args[1]) {
					goto END0
				}

			case "in", "between":

			default:
				goto END0
			}

			col := fn.Args[0].GetCol()
			if col == nil {
				goto END0
			}

			if colPos != -1 && colPos != col.ColPos {
				goto END0
			}

			colPos = col.ColPos
		}

		if colPos == pkPos {
			return nodeID
		}

		for i := range node.TableDef.Cols {
			if i != int(colPos) && colRefCnt[[2]int32{node.BindingTags[0], int32(i)}] > 0 {
				goto END0
			}
		}

		colCnt := colRefCnt[[2]int32{node.BindingTags[0], colPos}] - len(node.FilterList)

		for _, idxDef := range indexes {
			if !idxDef.Unique && colCnt > 0 {
				goto END0
			}

			if !idxDef.TableExist {
				continue
			}

			numParts := len(idxDef.Parts)
			if !idxDef.Unique {
				numParts--
			}

			if numParts != 1 || node.TableDef.Name2ColIndex[idxDef.Parts[0]] != colPos {
				continue
			}

			idxTag := builder.genNewTag()
			idxObjRef, idxTableDef := builder.compCtx.Resolve(node.ObjRef.SchemaName, idxDef.IndexTableName)

			builder.nameByColRef[[2]int32{idxTag, 0}] = idxTableDef.Name + "." + idxTableDef.Cols[0].Name
			builder.nameByColRef[[2]int32{idxTag, 1}] = idxTableDef.Name + "." + idxTableDef.Cols[1].Name

			for i, expr := range node.FilterList {
				fn := expr.GetF()
				col := fn.Args[0].GetCol()
				col.RelPos = idxTag
				col.ColPos = 0

				idxColExpr := &plan.Expr{
					Typ: DeepCopyType(idxTableDef.Cols[0].Typ),
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: idxTag,
							ColPos: 0,
						},
					},
				}

				if !idxDef.Unique {
					origType := fn.Args[0].Typ
					fn.Args[0].Typ = DeepCopyType(idxTableDef.Cols[0].Typ)
					switch fn.Func.ObjName {
					case "=":
						fn.Args[1], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[1]})
						node.FilterList[i], _ = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "prefix_eq", fn.Args)

					case "between":
						fn.Args[1], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[1]})
						fn.Args[2], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[2]})
						node.FilterList[i], _ = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "prefix_between", fn.Args)

					case "in":
						fn.Args[1], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[1]})
						node.FilterList[i], _ = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "prefix_in", fn.Args)
					}

					idxColExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_extract", []*plan.Expr{
						idxColExpr,
						{
							Typ: &plan.Type{
								Id: int32(types.T_int64),
							},
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_I64Val{I64Val: 1},
								},
							},
						},
						{
							Typ: origType,
							Expr: &plan.Expr_T{
								T: &plan.TargetType{
									Typ: DeepCopyType(origType),
								},
							},
						},
					})
				}

				idxColMap[[2]int32{node.BindingTags[0], colPos}] = idxColExpr
			}

			idxTableNodeID := builder.appendNode(&plan.Node{
				NodeType:     plan.Node_TABLE_SCAN,
				TableDef:     idxTableDef,
				ObjRef:       idxObjRef,
				ParentObjRef: node.ObjRef,
				FilterList:   node.FilterList,
				Limit:        node.Limit,
				Offset:       node.Offset,
				BindingTags:  []int32{idxTag},
			}, builder.ctxByNode[nodeID])

			return idxTableNodeID
		}

	}

END0:
	if node.Stats.Selectivity > 0.05 || node.Stats.Outcnt > float64(GetInFilterCardLimit()) {
		return nodeID
	}

	// Apply unique/secondary indices for point select

	col2filter := make(map[int32]int)
	for i, expr := range node.FilterList {
		fn := expr.GetF()
		if fn == nil {
			continue
		}

		if fn.Func.ObjName != "=" {
			continue
		}

		if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
			fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
		}

		col := fn.Args[0].GetCol()
		if col == nil || !isRuntimeConstExpr(fn.Args[1]) {
			continue
		}

		col2filter[col.ColPos] = i
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

	filterIdx := make([]int, 0, len(col2filter))
	for _, idxDef := range indexes {
		if !idxDef.TableExist {
			continue
		}

		numParts := len(idxDef.Parts)
		numKeyParts := numParts
		if !idxDef.Unique {
			numKeyParts--
		}
		if numKeyParts == 0 {
			continue
		}

		usePartialIndex := false

		filterIdx = filterIdx[:0]
		for i := 0; i < numKeyParts; i++ {
			colIdx := node.TableDef.Name2ColIndex[idxDef.Parts[i]]
			idx, ok := col2filter[colIdx]
			if !ok {
				break
			}

			filterIdx = append(filterIdx, idx)

			filter := node.FilterList[idx]
			if filter.Selectivity <= 0.05 && node.Stats.TableCnt*filter.Selectivity <= float64(GetInFilterCardLimit()) {
				usePartialIndex = true
			}
		}

		if len(filterIdx) < numParts && (idxDef.Unique || !usePartialIndex) {
			continue
		}

		idxTag := builder.genNewTag()
		idxObjRef, idxTableDef := builder.compCtx.Resolve(node.ObjRef.SchemaName, idxDef.IndexTableName)

		builder.nameByColRef[[2]int32{idxTag, 0}] = idxTableDef.Name + "." + idxTableDef.Cols[0].Name
		builder.nameByColRef[[2]int32{idxTag, 1}] = idxTableDef.Name + "." + idxTableDef.Cols[1].Name

		var idxFilter *plan.Expr
		if numParts == 1 {
			idx := filterIdx[0]

			args := node.FilterList[idx].GetF().Args
			col := args[0].GetCol()
			col.RelPos = idxTag
			col.ColPos = 0
			col.Name = idxTableDef.Cols[0].Name

			idxFilter = node.FilterList[idx]
			node.FilterList = append(node.FilterList[:idx], node.FilterList[idx+1:]...)
		} else {
			serialArgs := make([]*plan.Expr, len(filterIdx))
			for i := range filterIdx {
				serialArgs[i] = node.FilterList[filterIdx[i]].GetF().Args[1]
			}
			rightArg, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", serialArgs)

			funcName := "="
			if len(filterIdx) < numParts {
				funcName = "prefix_eq"
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

			newFilterList := make([]*plan.Expr, 0, len(node.FilterList)-len(filterIdx))
			for i, filter := range node.FilterList {
				if _, ok := hitFilterSet[i]; !ok {
					newFilterList = append(newFilterList, filter)
				}
			}

			node.FilterList = newFilterList
		}

		calcScanStats(node, builder)

		idxTableNodeID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDef,
			ObjRef:       idxObjRef,
			ParentObjRef: DeepCopyObjectRef(node.ObjRef),
			FilterList:   []*plan.Expr{idxFilter},
			Limit:        node.Limit,
			Offset:       node.Offset,
			BindingTags:  []int32{idxTag},
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
			JoinType: plan.Node_SEMI,
			OnList:   []*plan.Expr{joinCond},
		}, builder.ctxByNode[nodeID])

		return joinNodeID
	}

	// Apply single-column unique/secondary indices for non-equi expression

	colPos2Idx := make(map[int32]int)

	for i, idxDef := range indexes {
		if !idxDef.TableExist {
			continue
		}

		numParts := len(idxDef.Parts)
		if !idxDef.Unique {
			numParts--
		}

		if numParts == 1 {
			colPos2Idx[node.TableDef.Name2ColIndex[idxDef.Parts[0]]] = i
		}
	}

	for i, expr := range node.FilterList {
		fn := expr.GetF()
		if fn == nil {
			continue
		}

		col := fn.Args[0].GetCol()
		if col == nil {
			continue
		}

		if col.ColPos == pkPos {
			return nodeID
		}

		switch fn.Func.ObjName {
		case "between", "in":

		default:
			continue
		}

		idxPos, ok := colPos2Idx[col.ColPos]
		if !ok {
			continue
		}

		idxTag := builder.genNewTag()
		idxDef := node.TableDef.Indexes[idxPos]
		idxObjRef, idxTableDef := builder.compCtx.Resolve(node.ObjRef.SchemaName, idxDef.IndexTableName)

		builder.nameByColRef[[2]int32{idxTag, 0}] = idxTableDef.Name + "." + idxTableDef.Cols[0].Name
		builder.nameByColRef[[2]int32{idxTag, 1}] = idxTableDef.Name + "." + idxTableDef.Cols[1].Name

		col.RelPos = idxTag
		col.ColPos = 0
		col.Name = idxTableDef.Cols[0].Name

		var idxFilter *plan.Expr
		if idxDef.Unique {
			idxFilter = expr
		} else {
			fn.Args[0].Typ = DeepCopyType(idxTableDef.Cols[0].Typ)

			switch fn.Func.ObjName {
			case "in":
				fn.Args[1], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[1]})
				idxFilter, _ = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "prefix_in", fn.Args)

			case "between":
				fn.Args[1], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[1]})
				fn.Args[2], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[2]})
				idxFilter, _ = bindFuncExprAndConstFold(builder.GetContext(), builder.compCtx.GetProcess(), "prefix_between", fn.Args)
			}
		}

		node.FilterList = append(node.FilterList[:i], node.FilterList[i+1:]...)
		calcScanStats(node, builder)

		idxTableNodeID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDef,
			ObjRef:       idxObjRef,
			ParentObjRef: DeepCopyObjectRef(node.ObjRef),
			FilterList:   []*plan.Expr{idxFilter},
			Limit:        node.Limit,
			Offset:       node.Offset,
			BindingTags:  []int32{idxTag},
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
			DeepCopyExpr(pkExpr),
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
			JoinType: plan.Node_SEMI,
			OnList:   []*plan.Expr{joinCond},
		}, builder.ctxByNode[nodeID])

		return joinNodeID
	}

	return nodeID
}

func (builder *QueryBuilder) applyIndicesForJoins(nodeID int32, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {
	node.Children[1] = builder.applyIndices(node.Children[1], colRefCnt, idxColMap)

	leftChild := builder.qry.Nodes[node.Children[0]]
	if leftChild.NodeType != plan.Node_TABLE_SCAN {
		node.Children[0] = builder.applyIndices(node.Children[0], colRefCnt, idxColMap)
		replaceColumnsForNode(node, idxColMap)
		return nodeID
	} else if leftChild.TableDef.Pkey == nil {
		replaceColumnsForNode(node, idxColMap)
		return nodeID
	}

	newLeftChildID := builder.applyIndicesForFiltersRegularIndex(node.Children[0], leftChild, colRefCnt, idxColMap)
	if newLeftChildID != node.Children[0] {
		node.Children[0] = newLeftChildID
		return nodeID
	}

	rightChild := builder.qry.Nodes[node.Children[1]]

	if rightChild.Stats.Outcnt > float64(GetInFilterCardLimit()) || rightChild.Stats.Outcnt > leftChild.Stats.Cost*0.1 {
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

		col := expr.GetF().Args[0].GetCol()
		if col == nil {
			continue
		}

		col2Cond[col.ColPos] = i
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
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDef,
			ObjRef:       idxObjRef,
			ParentObjRef: DeepCopyObjectRef(leftChild.ObjRef),
			BindingTags:  []int32{idxTag},
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

			args := node.OnList[idx].GetF().Args
			col := args[0].GetCol()
			col.RelPos = idxTag
			col.ColPos = 0
			col.Name = idxTableDef.Cols[0].Name

			idxJoinCond = node.OnList[idx]
			node.OnList[idx] = pkJoinCond
		} else {
			serialArgs := make([]*plan.Expr, numParts)
			for i := range condIdx {
				serialArgs[i] = node.OnList[condIdx[i]].GetF().Args[1]
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

		break
	}

	return nodeID
}
