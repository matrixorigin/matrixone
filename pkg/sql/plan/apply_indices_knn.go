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
		return builder.applyIndicesForKNN(nodeID, node, colRefCnt, idxColMap)

	default:
		for i, childID := range node.Children {
			node.Children[i] = builder.applyIndicesKNN(childID, colRefCnt, idxColMap)
		}

		replaceColumnsForNode(node, idxColMap)

		return nodeID
	}
}

func (builder *QueryBuilder) applyIndicesForKNN(nodeID int32, node *plan.Node, colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr) int32 {

	// 1.a Skip condition one: if there are multiple order by columns
	if len(node.OrderBy) != 1 {
		//TODO: this needs to be modified.
		// There could be another column in order by which is reference by regular secondary index.
		return nodeID
	}

	// 1.b Skip condition two: if there are no multi-table indexes
	multiTableIndexes := make(map[string]*MultiTableIndex)
	for _, indexDef := range node.TableDef.Indexes {
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
		for _, expr := range node.OrderBy {
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
		for i := range node.TableDef.Cols {
			if i != int(colPos) && colRefCnt[[2]int32{node.BindingTags[0], int32(i)}] > 0 {
				goto END0
			}
		}
		colCnt := colRefCnt[[2]int32{node.BindingTags[0], colPos}] - len(node.OrderBy)

		// 2.b Apply Index
		for _, multiTableIndex := range multiTableIndexes {

			// 2.b.1 Check if all index columns are referenced
			if colCnt > 0 {
				goto END0
			}

			switch multiTableIndex.IndexAlgo {
			case catalog.MoIndexIvfFlatAlgo.ToString():

				// 2.b.2 Check if index is single column and only column required in the final output
				idxDef0 := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata]
				if node.TableDef.Name2ColIndex[idxDef0.Parts[0]] != colPos {
					continue
				}

				idxTag := builder.genNewTag()

				// 2.b.3 Create idxObjRefs and idxTableDefs
				var idxObjRefs = make([]*ObjectRef, 3)
				var idxTableDefs = make([]*TableDef, 3)
				idxObjRefs[0], idxTableDefs[0] = builder.compCtx.Resolve(node.ObjRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName)
				idxObjRefs[1], idxTableDefs[1] = builder.compCtx.Resolve(node.ObjRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName)
				idxObjRefs[2], idxTableDefs[2] = builder.compCtx.Resolve(node.ObjRef.SchemaName, multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName)

				// 2.b.4 Create cast(MetaTable.Version)
				metaTblWithCurrVerId, _ := makeMetaTblScanWhereKeyEqVersion(builder, builder.ctxByNode[nodeID], idxTableDefs, idxObjRefs)

				// 2.b.5 Create Centroids.Version == cast(MetaTable.Version)
				centroidsTblWithCurrVerId, _ := makeCrossJoinCentroidsMetaForCurrVersion(builder, builder.ctxByNode[nodeID], idxTableDefs, idxObjRefs, metaTblWithCurrVerId)

				for _, expr := range node.OrderBy {
					fn := expr.Expr.GetF()
					col := fn.Args[0].GetCol()
					col.RelPos = idxTag
					col.ColPos = 0

					l2DistanceOrderBy, _ := makeCentroidsTblOrderByL2Distance(builder, builder.ctxByNode[nodeID], fn,
						idxTableDefs, idxObjRefs, centroidsTblWithCurrVerId, fn.Func.ObjName)

					entriesForCurrVersion, _ := makeCrossJoinEntriesMetaForCurrVersion(builder, builder.ctxByNode[nodeID],
						idxTableDefs, idxObjRefs, metaTblWithCurrVerId)

					entriesTblInCentroidsId := builder.appendNode(&plan.Node{
						NodeType: plan.Node_JOIN,
						JoinType: plan.Node_SEMI,
						Children: []int32{entriesForCurrVersion, l2DistanceOrderBy},
						Limit:    node.Limit,
						//Offset:      node.Offset, //TODO: check with someone.
						//BindingTags: []int32{idxTag},//TODO: check with someone.
						ProjectList: []*Expr{
							{
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{
										RelPos: 0, // entriesForCurrVersion
										ColPos: 2, // entries.pk
									},
								},
							},
						},
					}, builder.ctxByNode[nodeID])

					return entriesTblInCentroidsId

				}
			}
		}

	}
END0:
	return nodeID
}

func makeCentroidsTblOrderByL2Distance(builder *QueryBuilder, bindCtx *BindContext, fn *plan.Function,
	idxTableDefs []*TableDef, idxObjRefs []*ObjectRef,
	centroidsTblWithCurrVerId int32, distFn string) (int32, error) {

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
	l2DistanceProj, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), distFn, []*plan.Expr{
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
	orderBy := []*plan.OrderBySpec{
		{
			Flag: plan.OrderBySpec_ASC,
		},
	}

	// 2.b Limit
	//TODO: modify
	limit := makePlan2Int64ConstExprWithType(1)

	// 3. Create "order by l2_distance(centroids, input_literal) asc limit @probe_limit"
	l2DistanceOrderBy := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		TableDef:    idxTableDefs[1],
		ObjRef:      idxObjRefs[1],
		ProjectList: []*Expr{centroidIdProj, l2DistanceProj},
		Limit:       limit,
		OrderBy:     orderBy,
		Children:    []int32{centroidsTblWithCurrVerId},
	}, bindCtx)

	return l2DistanceOrderBy, nil
}

func makeCrossJoinEntriesMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext, indexTableDefs []*TableDef, idxRefs []*ObjectRef, metaTableScanId int32) (int32, error) {
	entriesScanId := makeEntriesTblScan(builder, bindCtx, indexTableDefs, idxRefs)

	// 0: entries.version
	// 1: entries.centroid_id
	// 2: entries.pk
	// 3: meta.value i.e, current version
	joinProjections := getProjectionByLastNode(builder, entriesScanId)[:3]
	joinProjections = append(joinProjections, &plan.Expr{
		Typ: makePlan2Type(&bigIntType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: 0,
			},
		},
	})

	//TODO: Add NormalizeL2
	// TODO: Check for condition that the index available is for vector_l2_ops

	joinMetaAndCentroidsId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		JoinType:    plan.Node_SINGLE,
		Children:    []int32{entriesScanId, metaTableScanId},
		ProjectList: joinProjections,
	}, bindCtx)

	prevProjections := getProjectionByLastNode(builder, joinMetaAndCentroidsId)
	whereCentroidVersionEqCurrVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		prevProjections[0],
		prevProjections[3],
	})
	if err != nil {
		return -1, err
	}
	filterCentroidsForCurrVersionId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_FILTER,
		Children:    []int32{joinMetaAndCentroidsId},
		FilterList:  []*Expr{whereCentroidVersionEqCurrVersion},
		ProjectList: prevProjections[:3],
	}, bindCtx)
	return filterCentroidsForCurrVersionId, nil
}

func makeEntriesTblScan(builder *QueryBuilder, bindCtx *BindContext, indexTableDefs []*TableDef, idxRefs []*ObjectRef) int32 {
	scanNodeProjections := make([]*Expr, len(indexTableDefs[1].Cols))
	for colIdx, column := range indexTableDefs[2].Cols {
		scanNodeProjections[colIdx] = &plan.Expr{
			Typ: column.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					ColPos: int32(colIdx),
					Name:   column.Name,
				},
			},
		}
	}
	centroidsScanId := builder.appendNode(&Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      idxRefs[2],
		TableDef:    indexTableDefs[2],
		ProjectList: scanNodeProjections,
	}, bindCtx)
	return centroidsScanId
}
