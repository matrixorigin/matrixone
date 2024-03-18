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

	"github.com/matrixorigin/matrixone/pkg/catalog"
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
	for i, childID := range node.Children {
		node.Children[i] = builder.applyIndices(childID, colRefCnt, idxColMap)
	}
	replaceColumnsForNode(node, idxColMap)

	switch node.NodeType {
	case plan.Node_TABLE_SCAN:
		return builder.applyIndicesForFilters(nodeID, node, colRefCnt, idxColMap)

	case plan.Node_JOIN:
		return builder.applyIndicesForJoins(nodeID, node, colRefCnt, idxColMap)

	case plan.Node_SORT:
		return builder.applyIndicesForSort(nodeID, node, colRefCnt, idxColMap)

	}
	return nodeID
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
			case "between":
			case "in":

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
				return builder.applyIndicesForFiltersUsingMasterIndex(nodeID, node,
					colRefCnt, idxColMap, indexDef)
			}
		}

	}
END0:
	// 2. Regular Index Check
	{
		return builder.applyIndicesForFiltersRegularIndex(nodeID, node, colRefCnt, idxColMap)
	}
}

func (builder *QueryBuilder) applyIndicesForSort(nodeID int32, sortNode *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {

	// 1. Vector Index Check
	// Handle Queries like
	// SELECT id,embedding FROM tbl ORDER BY l2_distance(embedding, "[1,2,3]") LIMIT 10;
	{
		scanNode := builder.resolveTableScanWithIndexFromChildren(sortNode)

		// 1.a if there are no table scans with multi-table indexes, skip
		if scanNode == nil || sortNode == nil || len(sortNode.OrderBy) != 1 {
			goto END0
		}
		multiTableIndexes := make(map[string]*MultiTableIndex)
		for _, indexDef := range scanNode.TableDef.Indexes {
			if catalog.IsIvfIndexAlgo(indexDef.IndexAlgo) {
				if _, ok := multiTableIndexes[indexDef.IndexName]; !ok {
					multiTableIndexes[indexDef.IndexName] = &MultiTableIndex{
						IndexAlgo: catalog.ToLower(indexDef.IndexAlgo),
						IndexDefs: make(map[string]*plan.IndexDef),
					}
				}
				multiTableIndexes[indexDef.IndexName].IndexDefs[catalog.ToLower(indexDef.IndexAlgoTableType)] = indexDef
			}
		}
		if len(multiTableIndexes) == 0 {
			return nodeID
		}

		//1.b if sortNode has more than one order by, skip
		if len(sortNode.OrderBy) != 1 {
			goto END0
		}

		// 1.c if sortNode does not have a registered distance function, skip
		distFnExpr := sortNode.OrderBy[0].Expr.GetF()
		if distFnExpr == nil {
			goto END0
		}
		if _, ok := distFuncOpTypes[distFnExpr.Func.ObjName]; !ok {
			goto END0
		}

		// 1.d if the order by argument order is not of the form dist_func(col, const), swap and see
		// if that works. if not, skip
		if isRuntimeConstExpr(distFnExpr.Args[0]) && distFnExpr.Args[1].GetCol() != nil {
			distFnExpr.Args[0], distFnExpr.Args[1] = distFnExpr.Args[1], distFnExpr.Args[0]
		}
		if !isRuntimeConstExpr(distFnExpr.Args[1]) {
			goto END0
		}
		if distFnExpr.Args[0].GetCol() == nil {
			goto END0
		}
		// NOTE: here we assume the first argument is the column to order by
		colPosOrderBy := distFnExpr.Args[0].GetCol().ColPos

		// 1.d if the distance function in sortNode is not indexed for that column in any of the IVFFLAT index, skip
		distanceFunctionIndexed := false
		var multiTableIndexWithSortDistFn *MultiTableIndex
		for _, multiTableIndex := range multiTableIndexes {
			switch multiTableIndex.IndexAlgo {
			case catalog.MoIndexIvfFlatAlgo.ToString():
				storedParams, err := catalog.IndexParamsStringToMap(multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexAlgoParams)
				if err != nil {
					continue
				}
				storedOpType, ok := storedParams[catalog.IndexAlgoParamOpType]
				if !ok {
					continue
				}

				// if index is not the order by column, skip
				idxDef0 := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
				if scanNode.TableDef.Name2ColIndex[idxDef0.Parts[0]] != colPosOrderBy {
					continue
				}

				// if index is of the same distance function in order by, the index is valid
				if storedOpType == distFuncOpTypes[distFnExpr.Func.ObjName] {
					distanceFunctionIndexed = true
					multiTableIndexWithSortDistFn = multiTableIndex
				}
			}
			if distanceFunctionIndexed {
				break
			}
		}
		if !distanceFunctionIndexed {
			goto END0
		}

		return builder.applyIndicesForSortUsingVectorIndex(nodeID, sortNode, scanNode,
			colRefCnt, idxColMap, multiTableIndexWithSortDistFn, colPosOrderBy)
	}
END0:
	// 2. Regular Index Check
	{

	}

	return nodeID
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

		for _, idxDef := range indexes {
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

			idxColExpr := &plan.Expr{
				Typ: idxTableDef.Cols[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag,
						ColPos: 0,
					},
				},
			}

			if !idxDef.Unique {
				origType := node.TableDef.Cols[colPos].Typ
				idxColExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_extract", []*plan.Expr{
					idxColExpr,
					{
						Typ: plan.Type{
							Id: int32(types.T_int64),
						},
						Expr: &plan.Expr_Lit{
							Lit: &plan.Literal{
								Value: &plan.Literal_I64Val{I64Val: 0},
							},
						},
					},
					{
						Typ: origType,
						Expr: &plan.Expr_T{
							T: &plan.TargetType{},
						},
					},
				})
			}

			idxColMap[[2]int32{node.BindingTags[0], colPos}] = idxColExpr

			for i, expr := range node.FilterList {
				fn := expr.GetF()
				col := fn.Args[0].GetCol()
				col.RelPos = idxTag
				col.ColPos = 0

				if !idxDef.Unique {
					fn.Args[0].Typ = idxTableDef.Cols[0].Typ
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
				}
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
	if node.Stats.Selectivity > InFilterSelectivityLimit || node.Stats.Outcnt > float64(GetInFilterCardLimitOnPK(node.Stats.TableCnt)) {
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
			if filter.Selectivity <= InFilterSelectivityLimit && node.Stats.TableCnt*filter.Selectivity <= float64(GetInFilterCardLimitOnPK(node.Stats.TableCnt)) {
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
			idxFilter = DeepCopyExpr(node.FilterList[idx])
			args := idxFilter.GetF().Args
			col := args[0].GetCol()
			col.RelPos = idxTag
			col.ColPos = 0
			col.Name = idxTableDef.Cols[0].Name
		} else {
			serialArgs := make([]*plan.Expr, len(filterIdx))
			for i := range filterIdx {
				serialArgs[i] = DeepCopyExpr(node.FilterList[filterIdx[i]].GetF().Args[1])
			}
			rightArg, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", serialArgs)

			funcName := "="
			if len(filterIdx) < numParts {
				funcName = "prefix_eq"
			}
			idxFilter, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{
				{
					Typ: idxTableDef.Cols[0].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: idxTag,
							ColPos: 0,
						},
					},
				},
				rightArg,
			})
		}

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
			Typ: node.TableDef.Cols[pkIdx].Typ,
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
				Typ: pkExpr.Typ,
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
			JoinType: plan.Node_INDEX,
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

	for i := range node.FilterList {
		expr := DeepCopyExpr(node.FilterList[i])
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
			fn.Args[0].Typ = idxTableDef.Cols[0].Typ

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
			Typ: node.TableDef.Cols[pkIdx].Typ,
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
				Typ: pkExpr.Typ,
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
			JoinType: plan.Node_INDEX,
			OnList:   []*plan.Expr{joinCond},
		}, builder.ctxByNode[nodeID])

		return joinNodeID
	}

	return nodeID
}

func (builder *QueryBuilder) applyIndicesForJoins(nodeID int32, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {
	if node.JoinType == plan.Node_INDEX {
		return nodeID
	}

	leftChild := builder.qry.Nodes[node.Children[0]]
	if leftChild.NodeType != plan.Node_TABLE_SCAN || leftChild.TableDef.Pkey == nil {
		return nodeID
	}

	rightChild := builder.qry.Nodes[node.Children[1]]

	if rightChild.Stats.Outcnt > float64(GetInFilterCardLimitOnPK(leftChild.Stats.TableCnt)) || rightChild.Stats.Outcnt > leftChild.Stats.Cost*0.1 {
		return nodeID
	}

	leftTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[0]) {
		leftTags[tag] = true
	}

	rightTags := make(map[int32]bool)
	for _, tag := range builder.enumerateTags(node.Children[1]) {
		rightTags[tag] = true
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
		if !idxDef.TableExist {
			continue
		}

		numParts := len(idxDef.Parts)
		numKeyParts := numParts
		if !idxDef.Unique {
			numKeyParts--
		}
		if numKeyParts == 0 || numKeyParts > len(col2Cond) {
			continue
		}

		condIdx = condIdx[:0]
		for i := 0; i < numKeyParts; i++ {
			colIdx := leftChild.TableDef.Name2ColIndex[idxDef.Parts[i]]
			idx, ok := col2Cond[colIdx]
			if !ok {
				break
			}

			condIdx = append(condIdx, idx)
		}

		if len(condIdx) < numKeyParts {
			continue
		}

		idxTag := builder.genNewTag()
		idxObjRef, idxTableDef := builder.compCtx.Resolve(leftChild.ObjRef.SchemaName, idxDef.IndexTableName)

		builder.nameByColRef[[2]int32{idxTag, 0}] = idxTableDef.Name + "." + idxTableDef.Cols[0].Name
		builder.nameByColRef[[2]int32{idxTag, 1}] = idxTableDef.Name + "." + idxTableDef.Cols[1].Name

		rfTag := builder.genNewTag()

		var rfBuildExpr *plan.Expr
		if numParts == 1 {
			rfBuildExpr = &plan.Expr{
				Typ: idxTableDef.Cols[0].Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: -1,
						ColPos: 0,
					},
				},
			}
		} else {
			serialArgs := make([]*plan.Expr, len(condIdx))
			for i := range condIdx {
				serialArgs[i] = &plan.Expr{
					Typ: node.OnList[condIdx[i]].GetF().Args[1].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: -1,
							ColPos: int32(condIdx[i]),
						},
					},
				}
			}
			rfBuildExpr, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", serialArgs)
		}

		idxTableNodeID := builder.appendNode(&plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDef,
			ObjRef:       idxObjRef,
			ParentObjRef: DeepCopyObjectRef(leftChild.ObjRef),
			BindingTags:  []int32{idxTag},
			RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{
				{
					Tag:         rfTag,
					MatchPrefix: len(condIdx) < numParts,
					Expr: &plan.Expr{
						Typ: idxTableDef.Cols[0].Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: idxTag,
								ColPos: 0,
							},
						},
					},
				},
			},
		}, builder.ctxByNode[nodeID])

		node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, &plan.RuntimeFilterSpec{
			Tag:         rfTag,
			MatchPrefix: len(condIdx) < numParts,
			UpperLimit:  GetInFilterCardLimitOnPK(leftChild.Stats.TableCnt),
			Expr:        rfBuildExpr,
		})

		pkIdx := leftChild.TableDef.Name2ColIndex[leftChild.TableDef.Pkey.PkeyColName]
		pkExpr := &plan.Expr{
			Typ: leftChild.TableDef.Cols[pkIdx].Typ,
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
				Typ: pkExpr.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag,
						ColPos: 1,
					},
				},
			},
		})

		idxJoinNodeID := builder.appendNode(&plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{node.Children[0], idxTableNodeID},
			JoinType: plan.Node_INDEX,
			Limit:    leftChild.Limit,
			Offset:   leftChild.Offset,
			OnList:   []*plan.Expr{pkJoinCond},
		}, builder.ctxByNode[nodeID])

		leftChild.Limit, leftChild.Offset = nil, nil

		node.Children[0] = idxJoinNodeID

		break
	}

	return nodeID
}
