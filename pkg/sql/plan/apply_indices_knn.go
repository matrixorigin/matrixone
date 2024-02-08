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

func (builder *QueryBuilder) applyIndicesKNN(nodeID int32, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {
	node := builder.qry.Nodes[nodeID]

	switch node.NodeType {
	case plan.Node_SORT:
		return builder.handleSort(nodeID, node, colRefCnt, idxColMap)

	default:
		for i, childID := range node.Children {
			node.Children[i] = builder.applyIndicesKNN(childID, colRefCnt, idxColMap)
		}

		//TODO: watch out for this part.
		replaceColumnsForNode(node, idxColMap)

		return nodeID
	}
}

func (builder *QueryBuilder) handleSort(nodeID int32, sortNode *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {

	// 1.a Skip condition one: if there are multiple order by columns
	if len(sortNode.OrderBy) != 1 {
		//TODO: this needs to be modified.
		// There could be another column in order by which is reference by regular secondary index.
		return nodeID
	}

	scanNode := builder.resolveTableScanWithIndexFromChildren(sortNode)
	if scanNode == nil {
		return nodeID
	}
	indexes := scanNode.TableDef.Indexes

	// 1.b Skip condition two: if there are no multi-table indexes
	multiTableIndexes := make(map[string]*MultiTableIndex)
	for _, indexDef := range indexes {
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

	allowedFunctions := map[string]bool{
		"l2_distance":     true,
		"cosine_distance": true,
		"inner_product":   true,
	}

	{
		// 1.b Skip condition two: if there are no multi-table indexes
		if len(multiTableIndexes) == 0 {
			goto END0
		}

		// 1.c Skip condition three: if order by column is not an allowed function with one constant
		colPos := int32(-1)
		for _, expr := range sortNode.OrderBy {
			fn := expr.Expr.GetF()

			if fn == nil || !allowedFunctions[fn.Func.ObjName] {
				goto END0
			}

			{ // 2.a swap order if l2_distance(const, col) is used
				if isRuntimeConstExpr(fn.Args[0]) && fn.Args[1].GetCol() != nil {
					fn.Args[0], fn.Args[1] = fn.Args[1], fn.Args[0]
				}

				if !isRuntimeConstExpr(fn.Args[1]) {
					goto END0
				}
			}

			col := fn.Args[0].GetCol()
			if col == nil {
				goto END0
			}
			colPos = col.ColPos
		}

		// 2.a Check if all other columns are not referenced
		//TODO: need to understand this part for Aungr
		//for i := range scanNode.TableDef.Cols {
		//	if i != int(colPos) && colRefCnt[[2]int32{scanNode.BindingTags[0], int32(i)}] > 0 {
		//		goto END0
		//	}
		//}
		//colCnt := colRefCnt[[2]int32{scanNode.BindingTags[0], colPos}] - len(sortNode.OrderBy)

		// 2.b Apply Index
		for _, multiTableIndex := range multiTableIndexes {

			// 2.b.1 Check if all index columns are referenced
			//if colCnt > 0 {
			//	goto END0
			//}

			switch multiTableIndex.IndexAlgo {
			case catalog.MoIndexIvfFlatAlgo.ToString():

				// 2.b.2 Check if index is single column and only column required in the final output
				idxDef0 := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
				if scanNode.TableDef.Name2ColIndex[idxDef0.Parts[0]] != colPos {
					continue
				}

				// 2.b.3 Create idxTags, idxObjRefs and idxTableDefs
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

				// 2.b.4 Register all the columns in the index
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

				// 2.b.5 Create Centroids.Version == cast(MetaTable.Version)
				metaForCurrVersion1, _ := makeMetaTblScanWhereKeyEqVersionAndCastVersion(builder, builder.ctxByNode[nodeID],
					idxTableDefs, idxObjRefs, idxTags, "meta1")
				centroidsForCurrVersion, _ := makeCentroidsCrossJoinMetaForCurrVersion(builder, builder.ctxByNode[nodeID],
					idxTableDefs, idxObjRefs, idxTags, metaForCurrVersion1)

				// 2.b.6 Create Entries.Version ==  cast(MetaTable.Version)
				metaForCurrVersion2, _ := makeMetaTblScanWhereKeyEqVersionAndCastVersion(builder, builder.ctxByNode[nodeID],
					idxTableDefs, idxObjRefs, idxTags, "meta2")
				entriesForCurrVersion, _ := makeEntriesCrossJoinMetaForCurrVersion(builder, builder.ctxByNode[nodeID],
					idxTableDefs, idxObjRefs, idxTags, metaForCurrVersion2)

				for _, expr := range sortNode.OrderBy {

					//l2DistanceOrderBy, _ := makeCentroidsTblOrderByL2Distance(builder, builder.ctxByNode[nodeID], fn,
					//	idxTableDefs, idxObjRefs, centroidsTblWithCurrVerId, fn.Func.ObjName, idxTag)

					centroidIdEqEntriesCentroidId, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
						{
							Typ: DeepCopyType(idxTableDefs[1].Cols[1].Typ),
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: idxTags["centroids.project"], // centroidsForCurrVersion
									ColPos: 1,                            // centroids.centroid_id
								},
							},
						},
						{
							Typ: DeepCopyType(idxTableDefs[2].Cols[1].Typ),
							Expr: &plan.Expr_Col{
								Col: &plan.ColRef{
									RelPos: idxTags["entries.project"], // entriesForCurrVersion
									ColPos: 1,                          // entries.centroid_id_fk
								},
							},
						},
					})

					joinEntriesAndCentroids := builder.appendNode(&plan.Node{
						NodeType: plan.Node_JOIN,
						JoinType: plan.Node_INNER,
						Children: []int32{entriesForCurrVersion, centroidsForCurrVersion},
						OnList:   []*Expr{centroidIdEqEntriesCentroidId},
					}, builder.ctxByNode[nodeID])

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
					}, builder.ctxByNode[nodeID])

					fn := expr.Expr.GetF()
					col := fn.Args[0].GetCol()
					col.RelPos = idxTags["centroid_entries.project"] //TODO: watch out for this part.
					col.ColPos = 0

					idxColExpr := &plan.Expr{
						Typ: DeepCopyType(idxTableDefs[0].Cols[1].Typ),
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: idxTags["centroid_entries.project"],
								ColPos: 0,
							},
						},
					}
					idxColMap[[2]int32{scanNode.BindingTags[0], colPos}] = idxColExpr
					return projectCols

				}
			}
		}

	}
END0:
	return nodeID
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
		{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: 2,
				},
			},
		},
		//fn.Args[1],
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
