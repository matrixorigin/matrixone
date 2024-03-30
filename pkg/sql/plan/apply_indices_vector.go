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
	distFuncOpTypes = map[string]string{
		"l2_distance":     "vector_l2_ops",
		"cosine_distance": "vector_ip_ops",
		"inner_product":   "vector_cosine_ops",
	}
	textType = types.T_text.ToType() // return type of @probe_limit
)

// You replace Sort Node with a new Project Node
func (builder *QueryBuilder) applyIndicesForSortUsingVectorIndex(nodeID int32, sortNode, scanNode *plan.Node,
	colRefCnt map[[2]int32]int, idxColMap map[[2]int32]*plan.Expr, multiTableIndexWithSortDistFn *MultiTableIndex,
	colPosOrderBy int32) int32 {

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
	idxObjRefs[0], idxTableDefs[0] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndexWithSortDistFn.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Metadata].IndexTableName)
	idxObjRefs[1], idxTableDefs[1] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndexWithSortDistFn.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Centroids].IndexTableName)
	idxObjRefs[2], idxTableDefs[2] = builder.compCtx.Resolve(scanNode.ObjRef.SchemaName, multiTableIndexWithSortDistFn.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexTableName)
	builder.nameByColRef[[2]int32{idxTags["meta.scan"], 0}] = idxTableDefs[0].Name + "." + idxTableDefs[0].Cols[0].Name
	builder.nameByColRef[[2]int32{idxTags["meta.scan"], 1}] = idxTableDefs[0].Name + "." + idxTableDefs[0].Cols[1].Name
	builder.nameByColRef[[2]int32{idxTags["centroids.scan"], 0}] = idxTableDefs[1].Name + "." + idxTableDefs[1].Cols[0].Name
	builder.nameByColRef[[2]int32{idxTags["centroids.scan"], 1}] = idxTableDefs[1].Name + "." + idxTableDefs[1].Cols[1].Name
	builder.nameByColRef[[2]int32{idxTags["centroids.scan"], 2}] = idxTableDefs[1].Name + "." + idxTableDefs[1].Cols[2].Name
	builder.nameByColRef[[2]int32{idxTags["entries.scan"], 0}] = idxTableDefs[2].Name + "." + idxTableDefs[2].Cols[0].Name
	builder.nameByColRef[[2]int32{idxTags["entries.scan"], 1}] = idxTableDefs[2].Name + "." + idxTableDefs[2].Cols[1].Name
	builder.nameByColRef[[2]int32{idxTags["entries.scan"], 2}] = idxTableDefs[2].Name + "." + idxTableDefs[2].Cols[2].Name
	builder.nameByColRef[[2]int32{idxTags["entries.scan"], 3}] = idxTableDefs[2].Name + "." + idxTableDefs[2].Cols[3].Name

	// 2.b Create Centroids.Version == cast(MetaTable.Version)
	//     Order By L2 Distance(centroids,	input_literal) ASC limit @probe_limit
	metaForCurrVersion1, castMetaValueColToBigInt, _ := makeMetaTblScanWhereKeyEqVersionAndCastVersion(builder, builder.ctxByNode[nodeID],
		idxTableDefs, idxObjRefs, idxTags, "meta")
	centroidsForCurrVersionAndProbeLimit, _ := makeCentroidsSingleJoinMetaOnCurrVersionOrderByL2DistNormalizeL2(builder,
		builder.ctxByNode[nodeID], idxTableDefs, idxObjRefs, idxTags, metaForCurrVersion1, distFnExpr, sortDirection, castMetaValueColToBigInt)

	// 2.c Create Entries Node
	entriesTblScan, _ := makeEntriesTblScan(builder, builder.ctxByNode[nodeID], idxTableDefs, idxObjRefs, idxTags)

	// 2.d Create JOIN entries and centroids on
	// entries.centroid_id_fk == centroids.centroid_id AND entries.version == centroids.version
	entriesJoinCentroids := makeEntriesCrossJoinCentroidsOnCentroidId(builder, builder.ctxByNode[nodeID],
		idxTableDefs, idxTags,
		entriesTblScan, centroidsForCurrVersionAndProbeLimit)

	// 2.f Sort By entries by l2_distance(vector_col, normalize_l2(literal)) ASC limit original_limit
	sortTblByL2Distance := makeEntriesOrderByL2DistNormalizeL2(builder, builder.ctxByNode[nodeID], sortNode,
		distFnExpr, entriesJoinCentroids, sortDirection, idxTableDefs, idxTags)
	var pkPos = scanNode.TableDef.Name2ColIndex[scanNode.TableDef.Pkey.PkeyColName] //TODO: watch out.

	onlyUseIndexTables := true
	if onlyUseIndexTables {

		//TODO: this is a temporary change.
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

	// 2.e Create entries JOIN tbl on entries.original_pk == tbl.pk
	projectTbl := makeTblCrossJoinEntriesCentroidOnPK(builder, builder.ctxByNode[nodeID],
		idxTableDefs, idxTags,
		scanNode, sortTblByL2Distance, pkPos)

	return projectTbl

}

func (builder *QueryBuilder) resolveTableScanWithIndexFromChildren(node *plan.Node) *plan.Node {
	if !(node.NodeType == plan.Node_SORT || node.NodeType == plan.Node_TABLE_SCAN) {
		return nil
	}
	if node.NodeType == plan.Node_SORT && len(node.Children) == 1 {
		if n := builder.resolveTableScanWithIndexFromChildren(builder.qry.Nodes[node.Children[0]]); n != nil {
			return n
		}
	}

	if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef.Indexes != nil {
		return node
	}

	return nil
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

func makeCentroidsSingleJoinMetaOnCurrVersionOrderByL2DistNormalizeL2(builder *QueryBuilder, bindCtx *BindContext,
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

	// 3. Build Projection for l2_distance(centroid, normalize_l2(literal))
	centroidsCol := &plan.Expr{
		Typ: indexTableDefs[1].Cols[2].Typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: idxTags["centroids.scan"],
				ColPos: 2,
			},
		},
	}
	normalizeL2Lit, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "normalize_l2", []*plan.Expr{
		distFnExpr.Args[1],
	})
	distFnName := distFnExpr.Func.ObjName
	l2DistanceLitNormalizeL2Col, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), distFnName, []*plan.Expr{
		centroidsCol,   // centroid
		normalizeL2Lit, // normalize_l2(literal)
	})

	// 4. Sort by l2_distance(centroid, normalize_l2(literal)) limit @probe_limit
	// 4.1 @probe_limit is a system variable
	probeLimitValueExpr := &plan.Expr{
		Typ: *makePlan2Type(&textType), // T_text
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
	arg1 := makePlan2Int64ConstExprWithType(1)

	// 4.4 CAST(@var AS BIGINT)
	targetType := types.T_int64.ToType()
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
				Expr: l2DistanceLitNormalizeL2Col,
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

func makeEntriesCrossJoinCentroidsOnCentroidId(builder *QueryBuilder, bindCtx *BindContext, idxTableDefs []*TableDef, idxTags map[string]int32, entries int32, centroidsForCurrVersion int32) int32 {
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

	var onList []*Expr
	var joinType int32 = 3
	//TODO: arjun. remove this part
	if joinType == 1 {
		andEq, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "and", []*Expr{
			entriesCentroidIdEqCentroidId,
			centroidVersionEqEntriesVersion,
		})
		onList = []*Expr{andEq}
	} else if joinType == 2 {
		onList = []*Expr{entriesCentroidIdEqCentroidId, centroidVersionEqEntriesVersion}
	} else if joinType == 3 {
		onList = []*Expr{entriesCentroidIdEqCentroidId}
	}

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

func makeTblCrossJoinEntriesCentroidOnPK(builder *QueryBuilder, bindCtx *BindContext,
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
		JoinType: plan.Node_INDEX,
		Children: []int32{scanNode.NodeId, entriesJoinCentroids},
		OnList:   []*Expr{entriesOriginPkEqTblPk},
	}, bindCtx)

	return entriesJoinTbl
}

func makeEntriesOrderByL2DistNormalizeL2(builder *QueryBuilder, bindCtx *BindContext,
	sortNode *plan.Node, fn *plan.Function, entriesJoinCentroids int32,
	sortDirection plan.OrderBySpec_OrderByFlag,
	idxTableDefs []*TableDef, idxTags map[string]int32) int32 {

	distFnName := fn.Func.ObjName
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
