// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// You replace Sort Node with a new Project Node
func (builder *QueryBuilder) applyIndicesForSort(nodeID int32, sortNode *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {

	// 1. Find Sort and TableScan nodes
	scanNode := builder.resolveTableScanWithIndexFromChildren(sortNode)

	// 2.a Skip condition: Only one order by column is allowed
	if scanNode == nil || sortNode == nil || len(sortNode.OrderBy) != 1 {
		return nodeID
	}

	// 2.b Skip condition: if there are no multi-table indexes
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

	{
		distFuncOpTypes := map[string]string{
			"l2_distance":     "vector_l2_ops",
			"cosine_distance": "vector_ip_ops",
			"inner_product":   "vector_cosine_ops",
		}
		colPosOrderBy := int32(-1)
		for _, expr := range sortNode.OrderBy {

			// 2.c Skip condition: if the distance function in l2_distance, cosine_distance or inner_product
			fn := expr.Expr.GetF()
			if fn == nil {
				goto END0
			}
			if _, ok := distFuncOpTypes[fn.Func.ObjName]; !ok {
				goto END0
			}

			// 2.d Skip condition: if the distance function is not indexed in any of the multi-table IVFFLAT indexes
			distanceFunctionIndexed := false
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
					if storedOpType == distFuncOpTypes[fn.Func.ObjName] {
						distanceFunctionIndexed = true
					}
				}
			}
			if !distanceFunctionIndexed {
				goto END0
			}

			{ // swap order if l2_distance(const, col) is provided by the user
				if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
					fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
				}

				if !isRuntimeConstExpr(fn.Args[1]) {
					goto END0
				}
			}

			// 2.e Skip condition: if order by function is not of the form distFn(col, const)
			if fn.Args[0].GetCol() == nil {
				goto END0
			}
			colPosOrderBy = fn.Args[0].GetCol().ColPos
		}

		// 3.a Check if all other columns are not referenced
		//TODO: need to understand this part for Aungr
		//for i := range scanNode.TableDef.Cols {
		//	if i != int(colPos) && colRefCnt[[2]int32{scanNode.BindingTags[0], int32(i)}] > 0 {
		//		goto END0
		//	}
		//}
		//colCnt := colRefCnt[[2]int32{scanNode.BindingTags[0], colPos}] - len(sortNode.OrderBy)

		for _, multiTableIndex := range multiTableIndexes {

			// 3.b Check if all index columns are referenced
			//if colCnt > 0 {
			//	goto END0
			//}

			switch multiTableIndex.IndexAlgo {
			case catalog.MoIndexIvfFlatAlgo.ToString():

				// 4.a Modify Order By to use the index
				for _, expr := range sortNode.OrderBy {

					// 4.a.1 Skip Condition.
					fn := expr.Expr.GetF()
					{
						storedParams, err := catalog.IndexParamsStringToMap(multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexAlgoParams)
						if err != nil {
							continue
						}
						storedOpType, ok := storedParams[catalog.IndexAlgoParamOpType]
						if !ok {
							continue
						}
						if storedOpType != distFuncOpTypes[fn.Func.ObjName] {
							continue
						}
					}

					// 4.a.2 Skip Condition.
					idxDef0 := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
					if scanNode.TableDef.Name2ColIndex[idxDef0.Parts[0]] != colPosOrderBy {
						continue
					}

					// 4.b Create idxTags, idxObjRefs and idxTableDefs
					var idxTags = make(map[string]int32)
					var idxObjRefs = make([]*ObjectRef, 3)
					var idxTableDefs = make([]*TableDef, 3)
					idxTags["meta1.scan"] = builder.genNewTag()
					idxTags["meta2.scan"] = builder.genNewTag()
					idxTags["centroids.scan"] = builder.genNewTag()
					idxTags["entries.scan"] = builder.genNewTag()
					idxObjRefs[0], idxTableDefs[0] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName)
					idxObjRefs[1], idxTableDefs[1] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName)
					idxObjRefs[2], idxTableDefs[2] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName)
					builder.nameByColRef[[2]int32{idxTags["meta1.scan"], 0}] = idxTableDefs[0].Name + "." + idxTableDefs[0].Cols[0].Name
					builder.nameByColRef[[2]int32{idxTags["meta1.scan"], 1}] = idxTableDefs[0].Name + "." + idxTableDefs[0].Cols[1].Name
					builder.nameByColRef[[2]int32{idxTags["meta2.scan"], 0}] = idxTableDefs[0].Name + "." + idxTableDefs[0].Cols[0].Name
					builder.nameByColRef[[2]int32{idxTags["meta2.scan"], 1}] = idxTableDefs[0].Name + "." + idxTableDefs[0].Cols[1].Name
					builder.nameByColRef[[2]int32{idxTags["centroids.scan"], 0}] = idxTableDefs[1].Name + "." + idxTableDefs[1].Cols[0].Name
					builder.nameByColRef[[2]int32{idxTags["centroids.scan"], 1}] = idxTableDefs[1].Name + "." + idxTableDefs[1].Cols[1].Name
					builder.nameByColRef[[2]int32{idxTags["centroids.scan"], 2}] = idxTableDefs[1].Name + "." + idxTableDefs[1].Cols[2].Name
					builder.nameByColRef[[2]int32{idxTags["entries.scan"], 0}] = idxTableDefs[2].Name + "." + idxTableDefs[2].Cols[0].Name
					builder.nameByColRef[[2]int32{idxTags["entries.scan"], 1}] = idxTableDefs[2].Name + "." + idxTableDefs[2].Cols[1].Name
					builder.nameByColRef[[2]int32{idxTags["entries.scan"], 2}] = idxTableDefs[2].Name + "." + idxTableDefs[2].Cols[2].Name

					// 4.c Create Centroids.Version == cast(MetaTable.Version)
					//     Order By L2 Distance(centroids,	input_literal) ASC limit @probe_limit //TODO: @probe_limit = 1
					metaForCurrVersion1, _ := makeMetaTblScanWhereKeyEqVersionAndCastVersion(builder, builder.ctxByNode[nodeID],
						idxTableDefs, idxObjRefs, idxTags, "meta1")
					centroidsForCurrVersion, _ := makeCentroidsCrossJoinMetaForCurrVersion(builder, builder.ctxByNode[nodeID],
						idxTableDefs, idxObjRefs, idxTags, metaForCurrVersion1, fn)

					// 4.d Create Entries.Version ==  cast(MetaTable.Version)
					metaForCurrVersion2, _ := makeMetaTblScanWhereKeyEqVersionAndCastVersion(builder, builder.ctxByNode[nodeID],
						idxTableDefs, idxObjRefs, idxTags, "meta2")
					entriesForCurrVersion, _ := makeEntriesCrossJoinMetaForCurrVersion(builder, builder.ctxByNode[nodeID],
						idxTableDefs, idxObjRefs, idxTags, metaForCurrVersion2)

					// 4.e Create JOIN entries and centroids on centroid_id_fk == centroid_id
					//     Project origin_pk
					entriesJoinCentroids := makeEntriesJoinCentroids(builder, builder.ctxByNode[nodeID], idxTableDefs, idxTags,
						entriesForCurrVersion, centroidsForCurrVersion)

					// 4.f Create entries JOIN tbl on entries.original_pk == tbl.pk
					//     Project tbl.*
					var pkPos int32 = -1
					if len(scanNode.TableDef.Pkey.Names) == 1 {
						pkPos = scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.Names[0]]
					}
					entriesOriginPkEqTblPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
						{
							Typ: DeepCopyType(idxTableDefs[2].Cols[2].Typ),
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: idxTags["centroid_entries.project"],
									ColPos: 0, // entries.origin_pk
								},
							},
						},
						{
							Typ: DeepCopyType(idxTableDefs[2].Cols[1].Typ),
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: scanNode.BindingTags[0],
									ColPos: pkPos, // tbl.pk
								},
							},
						},
					})
					entriesJoinTbl := builder.appendNode(&plan.Node{
						NodeType: plan.Node_JOIN,
						JoinType: plan.Node_SEMI,
						Children: []int32{entriesJoinCentroids, scanNode.NodeId},
						OnList:   []*Expr{entriesOriginPkEqTblPk},
					}, builder.ctxByNode[nodeID])

					var projectList []*Expr
					for i, colDef := range scanNode.TableDef.Cols {
						idxColExpr := &plan.Expr{
							Typ: DeepCopyType(colDef.Typ),
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: scanNode.BindingTags[0],
									ColPos: int32(i),
								},
							},
						}
						projectList = append(projectList, idxColExpr)
					}

					idxTags["tbl_entries.project"] = builder.genNewTag()
					projectTbl := builder.appendNode(&plan.Node{
						NodeType:    plan.Node_PROJECT,
						Children:    []int32{entriesJoinTbl},
						ProjectList: projectList,
						BindingTags: []int32{idxTags["tbl_entries.project"]},
					}, builder.ctxByNode[nodeID])

					for i, colDef := range scanNode.TableDef.Cols {
						idxColExpr := &plan.Expr{
							Typ: DeepCopyType(colDef.Typ),
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: idxTags["tbl_entries.project"],
									ColPos: int32(i),
								},
							},
						}
						idxColMap[[2]int32{scanNode.BindingTags[0], int32(i)}] = idxColExpr
					}

					return projectTbl
				}

			}
		}

	}
END0:
	return nodeID
}

func makeEntriesJoinCentroids(builder *QueryBuilder, bindCtx *BindContext, idxTableDefs []*TableDef, idxTags map[string]int32, entriesForCurrVersion int32, centroidsForCurrVersion int32) int32 {
	centroidIdEqEntriesCentroidId, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: DeepCopyType(idxTableDefs[1].Cols[1].Typ),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["centroids.project"],
					ColPos: 1, // centroids.__mo_index_centroid_id
				},
			},
		},
		{
			Typ: DeepCopyType(idxTableDefs[2].Cols[1].Typ),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["entries.project"],
					ColPos: 1, // entries.__mo_index_centroid_fk_id
				},
			},
		},
	})

	// 1. Create JOIN entries and centroids on centroid_id_fk == centroid_id
	joinEntriesAndCentroids := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_SEMI,
		Children: []int32{entriesForCurrVersion, centroidsForCurrVersion},
		OnList:   []*Expr{centroidIdEqEntriesCentroidId},
	}, bindCtx)

	// 2. Project entries.origin_pk
	idxTags["centroid_entries.project"] = builder.genNewTag()
	projectCols := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{joinEntriesAndCentroids},
		ProjectList: []*Expr{
			{
				Typ: DeepCopyType(idxTableDefs[2].Cols[2].Typ),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTags["entries.project"], // entriesForCurrVersion
						ColPos: 2,                          // entries.pk
					},
				},
			},
		},
		BindingTags: []int32{idxTags["centroid_entries.project"]},
	}, bindCtx)
	return projectCols
}

func makeCentroidsTblOrderByL2Distance(builder *QueryBuilder, bindCtx *BindContext, fn *plan.Function,
	idxTableDefs []*TableDef, idxObjRefs []*ObjectRef,
	centroidsTblWithCurrVerId int32, distFn string, idxTag int32) (int32, error) {

	// 1.a Project centroids.__mo_index_centroid_id
	centroidIdProj := &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: 1,
			},
		},
	}

	// 1.b Project l2_distance(centroids, input_literal)
	l2DistanceProj, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "l2_distance", []*plan.Expr{
		{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: 2,
				},
			},
		},
		fn.Args[1],
	})

	// 2.a OrderBy
	//orderBy := []*plan.OrderBySpec{
	//	{
	//		Flag: plan.OrderBySpec_ASC,
	//	},
	//}

	// 2.b Limit
	//TODO: modify
	//limit := makePlan2Int64ConstExprWithType(1)

	// 3. Create "order by l2_distance(centroids, input_literal) asc limit @probe_limit"
	l2DistanceOrderBy := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT,
		//TableDef:    idxTableDefs[1],
		//ObjRef:      idxObjRefs[1],
		ProjectList: []*Expr{centroidIdProj, l2DistanceProj},
		//Limit:       limit,
		//OrderBy:     orderBy,
		Children: []int32{centroidsTblWithCurrVerId},
	}, bindCtx)

	return l2DistanceOrderBy, nil
}

func (builder *QueryBuilder) resolveSortAndTableScanForVectorIndex(node *plan.Node) (*plan.Node, *plan.Node) {
	if node.NodeType == plan.Node_SORT {
		if node.OrderBy != nil {
			sortNode := node
			tableScanNode := builder.resolveTableScanWithIndexFromChildren(node)
			if tableScanNode != nil {
				return sortNode, tableScanNode
			}
		}
	}

	for _, childID := range node.Children {
		if sortNode, tableScanNode := builder.resolveSortAndTableScanForVectorIndex(builder.qry.Nodes[childID]); sortNode != nil && tableScanNode != nil {
			return sortNode, tableScanNode
		}
	}
	return nil, nil
}

func (builder *QueryBuilder) resolveTableScanWithIndexFromChildren(node *plan.Node) *plan.Node {
	if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.Indexes != nil {
		return node
	}

	for _, childID := range node.Children {
		if n := builder.resolveTableScanWithIndexFromChildren(builder.qry.Nodes[childID]); n != nil {
			return n
		}
	}
	return nil
}
