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
	"fmt"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func containsDynamicParam(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_P, *plan.Expr_V:
		return true
	case *plan.Expr_F:
		for _, subExpr := range exprImpl.F.Args {
			if containsDynamicParam(subExpr) {
				return true
			}
		}
	}
	return false
}

func isRuntimeConstExpr(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Lit, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Vec, *plan.Expr_T:
		return true

	case *plan.Expr_F:
		for _, subExpr := range exprImpl.F.Args {
			if !isRuntimeConstExpr(subExpr) {
				return false
			}
		}

		return true

	case *plan.Expr_List:
		for _, subExpr := range exprImpl.List.List {
			if !isRuntimeConstExpr(subExpr) {
				return false
			}
		}

		return true

	default:
		return false
	}
}

func (builder *QueryBuilder) applyIndices(nodeID int32, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {
	if builder.optimizerHints != nil && builder.optimizerHints.applyIndices != 0 {
		return nodeID
	}
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

	case plan.Node_PROJECT:
		//NOTE: This is the entry point for vector index rule on SORT NODE.
		return builder.applyIndicesForProject(nodeID, node, colRefCnt, idxColMap)

	}
	return nodeID
}

func (builder *QueryBuilder) applyIndicesForFilters(nodeID int32, node *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {

	if len(node.FilterList) == 0 || len(node.TableDef.Indexes) == 0 {
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
				return builder.applyIndicesForFiltersUsingMasterIndex(nodeID, node, indexDef)
			}
		}

	}
END0:
	// 2. Regular Index Check
	{
		return builder.applyIndicesForFiltersRegularIndex(nodeID, node, colRefCnt, idxColMap)
	}
}

func getColSeqFromColDef(tblCol *plan.ColDef) string {
	return fmt.Sprintf("%d", tblCol.GetSeqnum())
}

func (builder *QueryBuilder) applyIndicesForProject(nodeID int32, projNode *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {

	// 1. Vector Index Check
	// Handle Queries like
	// SELECT id,embedding FROM tbl ORDER BY l2_distance(embedding, "[1,2,3]") LIMIT 10;
	{
		sortNode := builder.resolveSortNode(projNode, 1)
		if sortNode == nil || len(sortNode.OrderBy) != 1 {
			goto END0
		}

		scanNode := builder.resolveScanNodeWithIndex(sortNode, 1)
		if scanNode == nil {
			goto END0
		}

		// 1.a if there are no table scans with multi-table indexes, skip
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

		// This is important to get consistent result.
		// HashMap can give you random order during iteration.
		var multiTableIndexKeys []string
		for key := range multiTableIndexes {
			multiTableIndexKeys = append(multiTableIndexKeys, key)
		}
		sort.Strings(multiTableIndexKeys)

		for _, multiTableIndexKey := range multiTableIndexKeys {
			multiTableIndex := multiTableIndexes[multiTableIndexKey]
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

		newSortNode := builder.applyIndicesForSortUsingVectorIndex(nodeID, projNode, sortNode, scanNode,
			colRefCnt, idxColMap, multiTableIndexWithSortDistFn, colPosOrderBy)

		// TODO: consult with nitao and aungr
		projNode.Children[0] = newSortNode
		replaceColumnsForNode(projNode, idxColMap)

		return newSortNode
	}
END0:
	// 2. Regular Index Check
	{

	}

	return nodeID
}

func (builder *QueryBuilder) applyIndicesForFiltersRegularIndex(nodeID int32, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {
	if len(node.FilterList) == 0 || len(node.TableDef.Indexes) == 0 {
		return nodeID
	}
	//----------------------------------------------------------------------
	//ts1 := node.GetScanTS()

	scanSnapshot := node.ScanSnapshot
	if scanSnapshot == nil {
		scanSnapshot = &Snapshot{}
	}
	//----------------------------------------------------------------------

	for i := range node.FilterList { // if already have filter on first pk column and have a good selectivity, no need to go index
		expr := node.FilterList[i]
		fn := expr.GetF()
		if fn == nil {
			continue
		}
		col := fn.Args[0].GetCol()
		if col == nil {
			continue
		}
		if GetSortOrder(node.TableDef, col.ColPos) == 0 && node.FilterList[i].Selectivity <= 0.001 {
			return node.NodeId
		}
	}

	var indexes []*IndexDef
	for i := range node.TableDef.Indexes {
		if node.TableDef.Indexes[i].TableExist {
			indexes = append(indexes, node.TableDef.Indexes[i])
		}
	}
	if len(indexes) == 0 {
		return nodeID
	}

	sort.Slice(indexes, func(i, j int) bool {
		return (indexes[i].Unique && !indexes[j].Unique) || (indexes[i].Unique == indexes[j].Unique && len(indexes[i].Parts) > len(indexes[j].Parts))
	})

	// Apply unique/secondary indices if only indexed column is referenced
	ret := builder.tryIndexOnlyScan(indexes, node, colRefCnt, idxColMap, scanSnapshot)
	if ret != -1 {
		return ret
	}

	if catalog.IsFakePkName(node.TableDef.Pkey.PkeyColName) {
		// for cluster by table, make it less prone to go index
		if node.Stats.Selectivity > 0.0001 || node.Stats.Outcnt > 1000 {
			return nodeID
		}
	}
	if node.Stats.Selectivity > InFilterSelectivityLimit || node.Stats.Outcnt > float64(GetInFilterCardLimitOnPK(builder.compCtx.GetProcess().GetService(), node.Stats.TableCnt)) {
		return nodeID
	}

	// Apply unique/secondary indices for point select
	idxToChoose, idxSel, filterIdx := builder.getMostSelectiveIndex(indexes, node)
	if idxToChoose != -1 {
		return builder.applyIndexForPointSelect(indexes[idxToChoose], node, filterIdx, idxSel, scanSnapshot)
	}

	return builder.applyIndicesForNonEquiCond(indexes, node, scanSnapshot)
}

func (builder *QueryBuilder) tryIndexOnlyScan(indexes []*IndexDef, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr, scanSnapshot *Snapshot) int32 {
	col2filter := make(map[int32]int)
	colPos := int32(-1)
	for i, expr := range node.FilterList {
		fn := expr.GetF()
		if fn == nil {
			return -1
		}

		switch fn.Func.ObjName {
		case "=":
			if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
				fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
			}

			col := fn.Args[0].GetCol()
			if col == nil || colPos != -1 || !isRuntimeConstExpr(fn.Args[1]) {
				return -1
			}

			col2filter[col.ColPos] = i

		case "in", "between":
			col := fn.Args[0].GetCol()
			if col == nil {
				return -1
			}

			if len(col2filter) > 0 || (colPos != -1 && colPos != col.ColPos) {
				return -1
			}

			colPos = col.ColPos

		default:
			return -1
		}
	}

	if colPos > -1 {
		for i := range node.TableDef.Cols {
			if i != int(colPos) && colRefCnt[[2]int32{node.BindingTags[0], int32(i)}] > 0 {
				return -1
			}
		}
	}

	hitFilterIdx := make([]int, 0, len(col2filter))
	missFilterIdx := make([]int, 0, len(node.FilterList))
	for _, idxDef := range indexes {
		numParts := len(idxDef.Parts)
		if idxDef.Unique {
			if len(col2filter) > 0 && len(col2filter) < numParts {
				continue
			}
			if colPos > -1 && numParts > 1 {
				continue
			}
		}

		numKeyParts := numParts
		if !idxDef.Unique {
			numKeyParts--
		}
		if numKeyParts == 0 {
			continue
		}

		if colPos != -1 {
			if node.TableDef.Name2ColIndex[idxDef.Parts[0]] != colPos {
				continue
			}
		} else {
			hitFilterIdx = hitFilterIdx[:0]
			missFilterIdx = missFilterIdx[:0]
			hitPrefix := true
			indexedCols := make(map[int32]bool)
			for i := 0; i < numKeyParts; i++ {
				colIdx := node.TableDef.Name2ColIndex[idxDef.Parts[i]]
				idx, ok := col2filter[colIdx]
				if ok {
					if hitPrefix {
						hitFilterIdx = append(hitFilterIdx, idx)
					} else {
						missFilterIdx = append(missFilterIdx, idx)
					}
				} else {
					hitPrefix = false
				}
				indexedCols[colIdx] = true
			}

			for i := range node.TableDef.Cols {
				if !indexedCols[int32(i)] && colRefCnt[[2]int32{node.BindingTags[0], int32(i)}] > 0 {
					hitFilterIdx = hitFilterIdx[:0]
					break
				}
			}

			if len(hitFilterIdx) == 0 || len(hitFilterIdx)+len(missFilterIdx) < len(node.FilterList) {
				continue
			}
		}

		idxTag := builder.genNewTag()
		idxObjRef, idxTableDef := builder.compCtx.Resolve(node.ObjRef.SchemaName, idxDef.IndexTableName, scanSnapshot)

		builder.addNameByColRef(idxTag, idxTableDef)

		idxColExpr := &plan.Expr{
			Typ: idxTableDef.Cols[0].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag,
					ColPos: 0,
				},
			},
		}

		if colPos != -1 { // a IN (1, 2, 3), a BETWEEN 1 AND 2
			if numParts > 1 {
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
					case "between":
						fn.Args[1], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[1]})
						fn.Args[2], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[2]})
						node.FilterList[i], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "prefix_between", fn.Args)

					case "in":
						fn.Args[1], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[1]})
						node.FilterList[i], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "prefix_in", fn.Args)
					}
				}
			}
		} else { // a = 1 AND b = 2 AND c = 3
			if numParts == 1 {
				idx := hitFilterIdx[0]
				idxFilter := node.FilterList[idx]
				args := idxFilter.GetF().Args
				col := args[0].GetCol()
				col.RelPos = idxTag
				oldColPos := col.ColPos
				col.ColPos = 0

				node.FilterList[idx] = idxFilter
				idxColMap[[2]int32{node.BindingTags[0], oldColPos}] = idxColExpr
			} else {
				for i := 0; i < numKeyParts; i++ {
					colIdx := node.TableDef.Name2ColIndex[idxDef.Parts[i]]
					origType := node.TableDef.Cols[colIdx].Typ
					mappedExpr, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_extract", []*plan.Expr{
						DeepCopyExpr(idxColExpr),
						{
							Typ: plan.Type{
								Id: int32(types.T_int64),
							},
							Expr: &plan.Expr_Lit{
								Lit: &plan.Literal{
									Value: &plan.Literal_I64Val{I64Val: int64(i)},
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

					idxColMap[[2]int32{node.BindingTags[0], colIdx}] = mappedExpr
				}

				compositeFilterSel := 1.0
				serialArgs := make([]*plan.Expr, len(hitFilterIdx))
				for i := range hitFilterIdx {
					filter := node.FilterList[hitFilterIdx[i]]
					serialArgs[i] = DeepCopyExpr(filter.GetF().Args[1])
					compositeFilterSel = compositeFilterSel * filter.Selectivity
				}
				rightArg, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", serialArgs)

				funcName := "="
				if len(hitFilterIdx) < numParts {
					funcName = "prefix_eq"
				}
				idxFilter, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), funcName, []*plan.Expr{
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
				idxFilter.Selectivity = compositeFilterSel

				newFilterList := make([]*plan.Expr, 0, len(missFilterIdx)+1)
				for _, idx := range missFilterIdx {
					newFilterList = append(newFilterList, replaceColumnsForExpr(node.FilterList[idx], idxColMap))
				}
				node.FilterList = append(newFilterList, idxFilter)
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
			ScanSnapshot: node.ScanSnapshot,
		}, builder.ctxByNode[node.NodeId])

		return idxTableNodeID
	}
	return -1
}

func (builder *QueryBuilder) applyIndicesForNonEquiCond(indexes []*IndexDef, node *plan.Node, scanSnapshot *Snapshot) int32 {
	// Apply single-column unique/secondary indices for non-equi expression
	colPos2Idx := make(map[int32]int)
	for i, idxDef := range indexes {
		numParts := len(idxDef.Parts)
		if !idxDef.Unique {
			numParts--
		}
		if idxDef.Unique && numParts == 1 {
			colPos2Idx[node.TableDef.Name2ColIndex[idxDef.Parts[0]]] = i
		} else if !idxDef.Unique && numParts >= 1 {
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
		idxDef := indexes[idxPos]
		idxObjRef, idxTableDef := builder.compCtx.Resolve(node.ObjRef.SchemaName, idxDef.IndexTableName, scanSnapshot)
		builder.addNameByColRef(idxTag, idxTableDef)

		col.RelPos = idxTag
		col.ColPos = 0

		var idxFilter *plan.Expr
		if idxDef.Unique {
			idxFilter = expr
		} else {
			fn.Args[0].Typ = idxTableDef.Cols[0].Typ

			switch fn.Func.ObjName {
			case "in":
				fn.Args[1], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[1]})
				idxFilter, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "prefix_in", fn.Args)

			case "between":
				fn.Args[1], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[1]})
				fn.Args[2], _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{fn.Args[2]})
				idxFilter, _ = BindFuncExprImplByPlanExpr(builder.GetContext(), "prefix_between", fn.Args)
			}
		}
		idxFilter.Selectivity = expr.Selectivity

		idxTableNode := &plan.Node{
			NodeType:     plan.Node_TABLE_SCAN,
			TableDef:     idxTableDef,
			ObjRef:       idxObjRef,
			ParentObjRef: DeepCopyObjectRef(node.ObjRef),
			FilterList:   []*plan.Expr{idxFilter},
			BindingTags:  []int32{idxTag},
			ScanSnapshot: node.ScanSnapshot,
		}
		idxTableNodeID := builder.appendNode(idxTableNode, builder.ctxByNode[node.NodeId])

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
		joinNode := &plan.Node{
			NodeType: plan.Node_JOIN,
			Children: []int32{node.NodeId, idxTableNodeID},
			JoinType: plan.Node_INDEX,
			OnList:   []*plan.Expr{joinCond},
		}
		joinNodeID := builder.appendNode(joinNode, builder.ctxByNode[node.NodeId])

		if len(node.FilterList) == 0 {
			idxTableNode.Limit, idxTableNode.Offset = node.Limit, node.Offset
		} else {
			joinNode.Limit, joinNode.Offset = node.Limit, node.Offset
		}
		node.Limit, node.Offset = nil, nil

		return joinNodeID
	}

	return node.NodeId
}

func (builder *QueryBuilder) applyIndexForPointSelect(idxDef *IndexDef, node *plan.Node, filterIdx []int, idxSel float64, scanSnapshot *Snapshot) int32 {

	numParts := len(idxDef.Parts)
	idxTag := builder.genNewTag()
	idxObjRef, idxTableDef := builder.compCtx.Resolve(node.ObjRef.SchemaName, idxDef.IndexTableName, scanSnapshot)
	builder.addNameByColRef(idxTag, idxTableDef)

	var idxFilter *plan.Expr
	if numParts == 1 {
		idx := filterIdx[0]
		idxFilter = DeepCopyExpr(node.FilterList[idx])
		args := idxFilter.GetF().Args
		col := args[0].GetCol()
		col.RelPos = idxTag
		col.ColPos = 0
	} else {

		serialArgs := make([]*plan.Expr, len(filterIdx))
		for i := range filterIdx {
			filter := node.FilterList[filterIdx[i]]
			serialArgs[i] = DeepCopyExpr(filter.GetF().Args[1])
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
		idxFilter.Selectivity = idxSel
	}

	idxTableNode := &plan.Node{
		NodeType:     plan.Node_TABLE_SCAN,
		TableDef:     idxTableDef,
		ObjRef:       idxObjRef,
		ParentObjRef: DeepCopyObjectRef(node.ObjRef),
		FilterList:   []*plan.Expr{idxFilter},
		BindingTags:  []int32{idxTag},
		ScanSnapshot: node.ScanSnapshot,
	}

	idxTableNodeID := builder.appendNode(idxTableNode, builder.ctxByNode[node.NodeId])

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

	joinNode := &plan.Node{
		NodeType: plan.Node_JOIN,
		Children: []int32{node.NodeId, idxTableNodeID},
		JoinType: plan.Node_INDEX,
		OnList:   []*plan.Expr{joinCond},
	}
	joinNodeID := builder.appendNode(joinNode, builder.ctxByNode[node.NodeId])

	if len(node.FilterList) == 0 {
		idxTableNode.Limit, idxTableNode.Offset = node.Limit, node.Offset
	} else {
		joinNode.Limit, joinNode.Offset = node.Limit, node.Offset
	}
	node.Limit, node.Offset = nil, nil

	return joinNodeID

}

func (builder *QueryBuilder) getMostSelectiveIndex(indexes []*IndexDef, node *plan.Node) (int, float64, []int) {
	currentSel := 1.0
	currentIdx := -1
	var savedFilterIdx []int

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

	firstPkColIdx := node.TableDef.Name2ColIndex[node.TableDef.Pkey.Names[0]]
	_, ok := col2filter[firstPkColIdx]
	if ok { //point select filter on first column of primary key, no need to go index
		return -1, 0, nil
	}

	filterIdx := make([]int, 0, len(col2filter))
	for i, idxDef := range indexes {
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
		for j := 0; j < numKeyParts; j++ {
			colIdx := node.TableDef.Name2ColIndex[idxDef.Parts[j]]
			idx, ok := col2filter[colIdx]
			if !ok {
				break
			}
			filterIdx = append(filterIdx, idx)
			filter := node.FilterList[idx]
			if filter.Selectivity <= InFilterSelectivityLimit && node.Stats.TableCnt*filter.Selectivity <= float64(GetInFilterCardLimitOnPK(builder.compCtx.GetProcess().GetService(), node.Stats.TableCnt)) {
				usePartialIndex = true
			}
		}

		if len(filterIdx) < numParts && (idxDef.Unique || !usePartialIndex) {
			continue
		}

		compositeFilterSel := 1.0
		for k := range filterIdx {
			compositeFilterSel *= node.FilterList[filterIdx[k]].Selectivity
		}
		if compositeFilterSel < currentSel {
			currentSel = compositeFilterSel
			currentIdx = i
			savedFilterIdx = filterIdx
		}
	}
	return currentIdx, currentSel, savedFilterIdx
}

func (builder *QueryBuilder) applyIndicesForJoins(nodeID int32, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {
	sid := builder.compCtx.GetProcess().GetService()

	if node.JoinType == plan.Node_INDEX {
		return nodeID
	}

	leftChild := builder.qry.Nodes[node.Children[0]]
	if leftChild.NodeType != plan.Node_TABLE_SCAN {
		return nodeID
	}

	//----------------------------------------------------------------------
	//ts2 := leftChild.GetScanTS()

	scanSnapshot := leftChild.ScanSnapshot
	if scanSnapshot == nil {
		scanSnapshot = &Snapshot{}
	}
	//----------------------------------------------------------------------

	rightChild := builder.qry.Nodes[node.Children[1]]

	if rightChild.Stats.Selectivity > 0.5 {
		return nodeID
	}

	if rightChild.Stats.Outcnt > float64(GetInFilterCardLimitOnPK(sid, leftChild.Stats.TableCnt)) || rightChild.Stats.Outcnt > leftChild.Stats.Cost*0.1 {
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
		//idxObjRef, idxTableDef := builder.compCtx.Resolve(leftChild.ObjRef.SchemaName, idxDef.IndexTableName, *ts)
		idxObjRef, idxTableDef := builder.compCtx.Resolve(leftChild.ObjRef.SchemaName, idxDef.IndexTableName, scanSnapshot)
		builder.addNameByColRef(idxTag, idxTableDef)

		rfTag := builder.genNewMsgTag()

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

		probeExpr := &plan.Expr{
			Typ: idxTableDef.Cols[0].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag,
					ColPos: 0,
				},
			},
		}
		idxTableNodeID := builder.appendNode(&plan.Node{
			NodeType:               plan.Node_TABLE_SCAN,
			TableDef:               idxTableDef,
			ObjRef:                 idxObjRef,
			ParentObjRef:           DeepCopyObjectRef(leftChild.ObjRef),
			BindingTags:            []int32{idxTag},
			ScanSnapshot:           leftChild.ScanSnapshot,
			RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{MakeRuntimeFilter(rfTag, len(condIdx) < numParts, 0, probeExpr)},
		}, builder.ctxByNode[nodeID])

		node.RuntimeFilterBuildList = append(node.RuntimeFilterBuildList, MakeRuntimeFilter(rfTag, len(condIdx) < numParts, GetInFilterCardLimitOnPK(sid, leftChild.Stats.TableCnt), rfBuildExpr))
		recalcStatsByRuntimeFilter(builder.qry.Nodes[idxTableNodeID], node, builder)

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
