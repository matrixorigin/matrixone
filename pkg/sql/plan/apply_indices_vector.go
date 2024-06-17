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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var (
	/*
		   ### Common Mistakes and Troubleshooting Tips:
			1. If you use 2 Project's : one col[i] and other l2_distance(col[i]), make sure that l2_distance gets the Deep copy
					   of the col[i].
			2. If a plan doesn't work, try using idxColMap and early return to see if the plan works on each stage.
			3. Feel free to check out the builder.Query.Nodes to see if the plan is being built correctly.


			### NOTES:
			1. INDEX JOIN Limit Rules:
			2. Nodes that require BindingTags: TableScan, Project
	*/
	distFuncOpTypes = map[string]string{
		"l2_distance":     "vector_l2_ops",
		"inner_product":   "vector_ip_ops",
		"cosine_distance": "vector_cosine_ops",
	}
	distFuncInternalDistFunc = map[string]string{
		"l2_distance":     "l2_distance_sq",
		"inner_product":   "spherical_distance",
		"cosine_distance": "spherical_distance",
	}
	textType = types.T_text.ToType() // return type of @probe_limit
)

// You replace Sort Node with a new Project Node
func (builder *QueryBuilder) applyIndicesForSortUsingVectorIndex(nodeID int32, projNode, sortNode, scanNode *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr, multiTableIndexWithSortDistFn *MultiTableIndex,
	colPosOrderBy int32) int32 {

	var pkPos = scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName] //TODO: watch out.

	distFnExpr := sortNode.OrderBy[0].Expr.GetF()
	sortDirection := sortNode.OrderBy[0].Flag // For the most part, it is ASC

	// 1.a if any of the other columns in the table are referenced, skip
	//for i := range scanNode.TableDef.Cols {
	//	if i != int(colPosOrderBy) && colRefCnt[[2]int32{scanNode.BindingTags[0], int32(i)}] > 0 {
	//		goto END0 //TODO: need to understand this part for Aungr
	//	}
	//}
	//TODO: selectivity rule.

	// 1.b Check the order by column has refCount > len(sortNode.OrderBy)
	//colCntOrderBy := colRefCnt[[2]int32{scanNode.BindingTags[0], colPosOrderBy}] - len(sortNode.OrderBy)
	//if colCntOrderBy > 0 {
	//	//goto END0 //TODO: need to understand this part for Aungr
	//}

	// 2.a  idxTags, idxObjRefs and idxTableDefs
	var idxTags = make(map[string]int32)
	var idxObjRefs = make([]*ObjectRef, 3)
	var idxTableDefs = make([]*TableDef, 3)
	idxTags["meta.scan"] = builder.genNewTag()
	idxTags["centroids.scan"] = builder.genNewTag()
	idxTags["entries.scan"] = builder.genNewTag()
	// TODO: plan node should hold snapshot info and account info
	//idxObjRefs[0], idxTableDefs[0] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndexWithSortDistFn.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName, *scanNode.ScanTS)
	//idxObjRefs[1], idxTableDefs[1] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndexWithSortDistFn.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName, *scanNode.ScanTS)
	//idxObjRefs[2], idxTableDefs[2] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndexWithSortDistFn.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName, *scanNode.ScanTS)

	scanSnapshot := scanNode.ScanSnapshot
	if scanSnapshot == nil {
		scanSnapshot = &Snapshot{}
	}

	idxObjRefs[0], idxTableDefs[0] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndexWithSortDistFn.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName, *scanSnapshot)
	idxObjRefs[1], idxTableDefs[1] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndexWithSortDistFn.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName, *scanSnapshot)
	idxObjRefs[2], idxTableDefs[2] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndexWithSortDistFn.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName, *scanSnapshot)

	builder.addNameByColRef(idxTags["meta.scan"], idxTableDefs[0])
	builder.addNameByColRef(idxTags["centroids.scan"], idxTableDefs[1])
	builder.addNameByColRef(idxTags["entries.scan"], idxTableDefs[2])

	// 2.b Create Centroids.Version == cast(MetaTable.Version)
	//     Order By L2 Distance(centroids,	input_literal) ASC limit @probe_limit
	metaForCurrVersion1, castMetaValueColToBigInt, _ := makeMetaTblScanWhereKeyEqVersionAndCastVersion(builder, builder.ctxByNode[nodeID],
		idxTableDefs, idxObjRefs, idxTags, "meta")
	centroidsForCurrVersionAndProbeLimit, _ := makeCentroidsSingleJoinMetaOnCurrVersionOrderByL2Dist(builder,
		builder.ctxByNode[nodeID], idxTableDefs, idxObjRefs, idxTags, metaForCurrVersion1, distFnExpr, sortDirection, castMetaValueColToBigInt)

	// 2.c Create Entries Node
	entriesTblScan, _ := makeEntriesTblScan(builder, builder.ctxByNode[nodeID], idxTableDefs, idxObjRefs, idxTags)

	// 2.d Create JOIN entries and centroids on
	// entries.centroid_id_fk == centroids.centroid_id AND entries.version == centroids.version
	entriesJoinCentroids := makeEntriesCrossJoinCentroidsOnCentroidId(builder, builder.ctxByNode[nodeID],
		idxTableDefs, idxTags,
		entriesTblScan, centroidsForCurrVersionAndProbeLimit)

	// If scan node has no filter condition, then 2 fast path's can be taken.
	// Path 1: Only use Index Table if Projection Columns are present in Index Table or Constants.
	// Path 2: May be use INDEX JOIN (not working yet)
	if scanNode.FilterList == nil {
		// 3.a Sort By entries by l2_distance(vector_col, literal) ASC limit original_limit
		sortTblByL2Distance := makeEntriesOrderByL2Distance(builder, builder.ctxByNode[nodeID], distFnExpr, entriesJoinCentroids, sortDirection, idxTableDefs, idxTags,
			sortNode)

		// Plan 1: Index-Table only Plan
		{

			// 3.a.1 Check if all the columns in the projection are present in Index Table or Constants.
			useIndexTablesOnly := true
			for _, projExp := range projNode.ProjectList {
				if isRuntimeConstExpr(projExp) {
					continue
				}

				if projExp.GetCol() != nil {
					if projExp.GetCol().ColPos == pkPos {
						continue
					}
					if projExp.GetCol().ColPos == colPosOrderBy {
						continue
					}
				}
				useIndexTablesOnly = false
				break
			}

			// 3.a.2 If all the columns in the projection are present in Index Table or Constants, then use Index Tables only.
			if useIndexTablesOnly {
				idxColMap[[2]int32{scanNode.BindingTags[0], pkPos}] = &plan.Expr{
					Typ: idxTableDefs[2].Cols[2].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: idxTags["entries.scan"],
							ColPos: 2, // entries.pk
						},
					},
				}
				idxColMap[[2]int32{scanNode.BindingTags[0], colPosOrderBy}] = &plan.Expr{
					Typ: idxTableDefs[2].Cols[3].Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: idxTags["entries.scan"],
							ColPos: 3, // entries.entry
						},
					},
				}

				return sortTblByL2Distance
			}
		}

		//// Plan 2: Create tbl "INDEX JOIN" entries on entries.original_pk == tbl.pk
		//{
		//	// 3.b.1 Create Table "INDEX JOIN" entries on entries.original_pk == tbl.pk. This should only work
		//	// when we don't have any filter condition on the scan node.
		//	projectTbl := makeTblIndexJoinEntriesCentroidOnPK(builder, builder.ctxByNode[nodeID],
		//		idxTableDefs, idxTags,
		//		scanNode, sortTblByL2Distance, pkPos, sortNode)
		//
		//	return projectTbl
		//}

	}

	// Path 3: Generic Plan which works for all cases.
	{

		// 1. Do Entries INNER JOIN Centroids
		tlbJoinEntries := makeTblInnerJoinEntriesCentroidOnPK(builder, builder.ctxByNode[nodeID],
			idxTableDefs, idxTags,
			scanNode, entriesJoinCentroids, pkPos)

		// 2. Do Sort by L2 Distance
		sortTblByL2Distance := makeInnerJoinOrderByL2Distance(builder, builder.ctxByNode[nodeID],
			distFnExpr, tlbJoinEntries, sortDirection, idxTableDefs, idxTags, sortNode)

		return sortTblByL2Distance
	}
}

func makeMetaTblScanWhereKeyEqVersionAndCastVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTags map[string]int32, prefix string) (int32, *Expr, error) {

	// 1. Scan <key, value> from meta table
	metaTableScanId, scanCols, _ := makeHiddenTblScanWithBindingTag(builder, bindCtx, indexTableDefs[0], idxRefs[0], idxTags[prefix+".scan"])

	// 2. WHERE key = 'version'
	whereKeyEqVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		scanCols[0], // key
		MakePlan2StringConstExprWithType("version"), // "version"
	})
	if err != nil {
		return -1, nil, err
	}
	metaScanNode := builder.qry.Nodes[metaTableScanId]
	metaScanNode.FilterList = []*Expr{whereKeyEqVersion}

	// 3. Project "value column" as BigInt
	castMetaValueColToBigInt, err := makePlan2CastExpr(builder.GetContext(), scanCols[1], makePlan2Type(&bigIntType))
	if err != nil {
		return -1, nil, err
	}

	return metaTableScanId, castMetaValueColToBigInt, nil
}

func makeCentroidsSingleJoinMetaOnCurrVersionOrderByL2Dist(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTags map[string]int32,
	metaTableScanId int32, distFnExpr *plan.Function, sortDirection plan.OrderBySpec_OrderByFlag, castMetaValueColToBigInt *Expr) (int32, error) {

	// 1. Scan <version, centroid_id, centroid> from centroids table
	centroidsScanId, scanCols, _ := makeHiddenTblScanWithBindingTag(builder, bindCtx, indexTableDefs[1], idxRefs[1],
		idxTags["centroids.scan"])

	//2. JOIN centroids and meta on version
	joinCond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		scanCols[0],              // centroids.version
		castMetaValueColToBigInt, // cast(meta.value as BIGINT)
	})
	if err != nil {
		return -1, err
	}
	joinMetaAndCentroidsId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Children: []int32{centroidsScanId, metaTableScanId},
		OnList:   []*Expr{joinCond},
	}, bindCtx)

	// 3. Build Projection for l2_distance(centroid, literal)
	centroidsCol := &plan.Expr{
		Typ: indexTableDefs[1].Cols[2].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: idxTags["centroids.scan"],
				ColPos: 2,
			},
		},
	}
	//normalizeL2Lit, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "normalize_l2", []*plan.Expr{
	//	distFnExpr.Args[1],
	//})
	distFnName := distFuncInternalDistFunc[distFnExpr.Func.ObjName]
	l2DistanceLitCol, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), distFnName, []*plan.Expr{
		centroidsCol,       // centroid
		distFnExpr.Args[1], // lit
	})

	// 4. Sort by l2_distance(centroid, literal) limit @probe_limit
	// 4.1 @probe_limit is a system variable
	probeLimitValueExpr := &plan.Expr{
		Typ: makePlan2Type(&textType), // T_text
		Expr: &plan.Expr_V{
			V: &plan.VarRef{
				Name:   "probe_limit",
				Global: false,
				System: false,
			},
		},
	}

	//4.2 ISNULL(@var)
	arg0, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "isnull", []*plan.Expr{
		probeLimitValueExpr,
	})
	if err != nil {
		return -1, err
	}

	// 4.3 CAST( 1 AS BIGINT)
	arg1 := makePlan2Uint64ConstExprWithType(1)

	// 4.4 CAST(@var AS BIGINT)
	targetType := types.T_uint64.ToType()
	planTargetType := makePlan2Type(&targetType)
	arg2, err := appendCastBeforeExpr(builder.GetContext(), probeLimitValueExpr, planTargetType)
	if err != nil {
		return -1, err
	}

	ifNullLimitExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "case", []*plan.Expr{
		arg0,
		arg1,
		arg2,
	})
	if err != nil {
		return -1, err
	}

	sortCentroidsByL2DistanceId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{joinMetaAndCentroidsId},
		Limit:    ifNullLimitExpr,
		OrderBy: []*OrderBySpec{
			{
				Expr: l2DistanceLitCol,
				Flag: sortDirection,
			},
		},
	}, bindCtx)

	return sortCentroidsByL2DistanceId, nil
}

func makeEntriesTblScan(builder *QueryBuilder, bindCtx *BindContext, indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTags map[string]int32) (int32, error) {

	// 1. Scan <version, centroid_id_fk, origin_pk, embedding> from entries table
	entriesScanId, _, _ := makeHiddenTblScanWithBindingTag(builder, bindCtx, indexTableDefs[2], idxRefs[2],
		idxTags["entries.scan"])

	return entriesScanId, nil
}

func makeEntriesCrossJoinCentroidsOnCentroidId(builder *QueryBuilder, bindCtx *BindContext, idxTableDefs []*TableDef,
	idxTags map[string]int32, entries int32, centroidsForCurrVersion int32) int32 {

	centroidVersionEqEntriesVersion, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: idxTableDefs[2].Cols[0].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["entries.scan"],
					ColPos: 0, // entries.__mo_version
				},
			},
		},
		{
			Typ: idxTableDefs[1].Cols[0].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["centroids.scan"],
					ColPos: 0, // centroids.__mo_version
				},
			},
		},
	})

	entriesCentroidIdEqCentroidId, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: idxTableDefs[2].Cols[1].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["entries.scan"],
					ColPos: 1, // entries.__mo_index_centroid_fk_id
				},
			},
		},
		{
			Typ: idxTableDefs[1].Cols[1].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["centroids.scan"],
					ColPos: 1, // centroids.__mo_index_centroid_id
				},
			},
		},
	})

	var onList = []*Expr{entriesCentroidIdEqCentroidId, centroidVersionEqEntriesVersion}
	// Create JOIN entries and centroids
	// ON
	// - centroids.centroid_id == entries.centroid_id_fk AND
	// - centroids.version == entries.version
	joinEntriesAndCentroids := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_SEMI,
		Children: []int32{entries, centroidsForCurrVersion},
		OnList:   onList,
	}, bindCtx)

	return joinEntriesAndCentroids
}

//TODO: fix it later
//func makeTblIndexJoinEntriesCentroidOnPK(builder *QueryBuilder, bindCtx *BindContext,
//	idxTableDefs []*TableDef, idxTags map[string]int32,
//	scanNode *plan.Node, entriesJoinCentroids int32, pkPos int32,
//	sortNode *plan.Node) int32 {
//
//	entriesOriginPkEqTblPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
//
//		{
//			Typ: idxTableDefs[2].Cols[2].Typ,
//			Expr: &plan.Expr_Col{
//				Col: &plan.ColRef{
//					RelPos: scanNode.BindingTags[0],
//					ColPos: pkPos, // tbl.pk
//				},
//			},
//		},
//		{
//			Typ: idxTableDefs[2].Cols[2].Typ,
//			Expr: &plan.Expr_Col{
//				Col: &plan.ColRef{
//					RelPos: idxTags["entries.scan"],
//					ColPos: 2, // entries.origin_pk
//				},
//			},
//		},
//	})
//	entriesJoinTbl := builder.appendNode(&plan.Node{
//		NodeType: plan.Node_JOIN,
//		JoinType: plan.Node_INDEX,
//		Children: []int32{scanNode.NodeId, entriesJoinCentroids},
//		OnList:   []*Expr{entriesOriginPkEqTblPk},
//	}, bindCtx)
//
//	return entriesJoinTbl
//}

func makeTblInnerJoinEntriesCentroidOnPK(builder *QueryBuilder, bindCtx *BindContext,
	idxTableDefs []*TableDef, idxTags map[string]int32,
	scanNode *plan.Node, entriesJoinCentroids int32, pkPos int32) int32 {

	entriesOriginPkEqTblPk, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{

		{
			Typ: idxTableDefs[2].Cols[2].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: scanNode.BindingTags[0],
					ColPos: pkPos, // tbl.pk
				},
			},
		},
		{
			Typ: idxTableDefs[2].Cols[2].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["entries.scan"],
					ColPos: 2, // entries.origin_pk
				},
			},
		},
	})
	entriesJoinTbl := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Children: []int32{scanNode.NodeId, entriesJoinCentroids},
		OnList:   []*Expr{entriesOriginPkEqTblPk},
	}, bindCtx)

	return entriesJoinTbl
}

func makeEntriesOrderByL2Distance(builder *QueryBuilder, bindCtx *BindContext,
	fn *plan.Function, entriesJoinCentroids int32,
	sortDirection plan.OrderBySpec_OrderByFlag,
	idxTableDefs []*TableDef, idxTags map[string]int32,
	sortNode *plan.Node) int32 {

	distFnName := distFuncInternalDistFunc[fn.Func.ObjName]
	l2DistanceColLit, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), distFnName, []*plan.Expr{
		{
			Typ: idxTableDefs[2].Cols[3].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["entries.scan"],
					ColPos: 3, // entries.entry
				},
			},
		},
		fn.Args[1], // lit
	})
	sortTblByL2Distance := builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{entriesJoinCentroids},
		Limit:    DeepCopyExpr(sortNode.Limit),
		Offset:   DeepCopyExpr(sortNode.Offset),
		OrderBy: []*OrderBySpec{
			{
				Expr: l2DistanceColLit,
				Flag: sortDirection,
			},
		},
	}, bindCtx)
	return sortTblByL2Distance
}

func makeInnerJoinOrderByL2Distance(builder *QueryBuilder, bindCtx *BindContext,
	fn *plan.Function, tlbJoinEntries int32,
	sortDirection plan.OrderBySpec_OrderByFlag,
	idxTableDefs []*TableDef, idxTags map[string]int32,
	sortNode *plan.Node) int32 {

	distFnName := distFuncInternalDistFunc[fn.Func.ObjName]
	l2DistanceColLit, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), distFnName, []*plan.Expr{
		{
			Typ: idxTableDefs[2].Cols[3].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["entries.scan"],
					ColPos: 3, // entries.entry
				},
			},
		},
		fn.Args[1], // lit
	})
	sortTblByL2Distance := builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{tlbJoinEntries},
		Limit:    DeepCopyExpr(sortNode.Limit),
		Offset:   DeepCopyExpr(sortNode.Offset),
		OrderBy: []*OrderBySpec{
			{
				Expr: l2DistanceColLit,
				Flag: sortDirection,
			},
		},
	}, bindCtx)
	return sortTblByL2Distance
}

func makeHiddenTblScanWithBindingTag(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDef *TableDef, idxObjRef *ObjectRef, idxTag int32) (int32, []*Expr, *Node) {

	// 1. Create Scan
	scanId := builder.appendNode(&Node{
		NodeType:    plan.Node_TABLE_SCAN,
		TableDef:    indexTableDef,
		ObjRef:      idxObjRef,
		BindingTags: []int32{idxTag},
	}, bindCtx)

	// 2. Create Scan Cols
	scanCols := make([]*Expr, len(indexTableDef.Cols))
	for colIdx, column := range indexTableDef.Cols {
		scanCols[colIdx] = &plan.Expr{
			Typ: column.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag,
					ColPos: int32(colIdx),
					Name:   column.Name,
				},
			},
		}
	}
	return scanId, scanCols, nil
}

func (builder *QueryBuilder) resolveScanNodeWithIndex(node *plan.Node, depth int32) *plan.Node {
	if depth == 0 {
		if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.Indexes != nil {
			return node
		}
		return nil
	}

	if node.NodeType == plan.Node_SORT && len(node.Children) == 1 {
		return builder.resolveScanNodeWithIndex(builder.qry.Nodes[node.Children[0]], depth-1)
	}

	return nil
}

func (builder *QueryBuilder) resolveSortNode(node *plan.Node, depth int32) *plan.Node {
	if depth == 0 {
		if node.NodeType == plan.Node_SORT {
			return node
		}
		return nil
	}

	if node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 {
		return builder.resolveSortNode(builder.qry.Nodes[node.Children[0]], depth-1)
	}

	return nil
}
