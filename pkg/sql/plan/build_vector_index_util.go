// Copyright 2023 Matrix Origin
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
	bigIntType  = types.T_int64.ToType()
	varCharType = types.T_varchar.ToType()

	opTypeToDistanceFunc = map[string]string{
		"vector_l2_ops":     "l2_distance",
		"vector_ip_ops":     "inner_product",
		"vector_cosine_ops": "cosine_distance",
	}
)

func makeIvfFlatIndexTblScan(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTableId int32) (int32, []*Expr) {
	scanNodeProjections := make([]*Expr, len(indexTableDefs[idxTableId].Cols))
	for colIdx, column := range indexTableDefs[idxTableId].Cols {
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
		ObjRef:      idxRefs[idxTableId],
		TableDef:    indexTableDefs[idxTableId],
		ProjectList: scanNodeProjections,
	}, bindCtx)
	return centroidsScanId, scanNodeProjections
}

func makeMetaTblScanWhereKeyEqVersion(builder *QueryBuilder, bindCtx *BindContext, indexTableDefs []*TableDef, idxRefs []*ObjectRef) (int32, error) {
	metaTableScanId, scanCols := makeIvfFlatIndexTblScan(builder, bindCtx, indexTableDefs, idxRefs, 0)

	whereKeyEqVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		DeepCopyExpr(scanCols[0]),
		MakePlan2StringConstExprWithType("version"),
	})
	if err != nil {
		return -1, err
	}
	builder.qry.Nodes[metaTableScanId].FilterList = []*Expr{whereKeyEqVersion}
	return metaTableScanId, nil
}

func makeTableProjectionIncludingNormalizeL2(builder *QueryBuilder, bindCtx *BindContext, tableScanId int32,
	tableDef *TableDef,
	typeOriginPk *Type, posOriginPk int,
	typeOriginVecColumn *Type, posOriginVecColumn int) (int32, error) {

	normalizeL2, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "normalize_l2", []*Expr{
		{ // tbl.embedding
			Typ: *typeOriginVecColumn,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(posOriginVecColumn),
					Name:   tableDef.Cols[posOriginVecColumn].Name,
				},
			},
		},
	})
	if err != nil {
		return -1, err
	}

	// id, embedding, normalize_l2(embedding)
	tableProjectId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{tableScanId},

		ProjectList: []*Expr{

			{ // tbl.pk
				Typ: *typeOriginPk,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(posOriginPk),
						Name:   tableDef.Cols[posOriginPk].Name,
					},
				},
			},

			{ // tbl.embedding
				Typ: *typeOriginVecColumn,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(posOriginVecColumn),
						Name:   tableDef.Cols[posOriginVecColumn].Name,
					},
				},
			},

			// tbl.normalize_l2(embedding)
			normalizeL2,
		},
	}, bindCtx)
	return tableProjectId, nil
}

func makeCrossJoinCentroidsMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext, indexTableDefs []*TableDef, idxRefs []*ObjectRef, metaTableScanId int32) (int32, error) {
	centroidsScanId, _ := makeIvfFlatIndexTblScan(builder, bindCtx, indexTableDefs, idxRefs, 1)

	metaProjection := getProjectionByLastNode(builder, metaTableScanId)
	metaProjectValueCol := DeepCopyExpr(metaProjection[1])
	metaProjectValueCol.Expr.(*plan.Expr_Col).Col.RelPos = 1
	prevMetaScanCastValAsBigInt, err := makePlan2CastExpr(builder.GetContext(), metaProjectValueCol, makePlan2Type(&bigIntType))
	if err != nil {
		return -1, err
	}
	// 0: centroids.version
	// 1: centroids.centroid_id
	// 2: centroids.centroid
	prevCentroidScanProjection := getProjectionByLastNode(builder, centroidsScanId)[:3]
	whereCentroidVersionEqCurrVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		prevCentroidScanProjection[0],
		prevMetaScanCastValAsBigInt,
	})
	if err != nil {
		return -1, err
	}

	joinMetaAndCentroidsId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		JoinType:    plan.Node_INNER,
		Children:    []int32{centroidsScanId, metaTableScanId},
		ProjectList: prevCentroidScanProjection,
		OnList:      []*Expr{whereCentroidVersionEqCurrVersion},
	}, bindCtx)

	return joinMetaAndCentroidsId, nil
}

func makeCrossJoinTblAndCentroids(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef,
	leftChildTblId int32, rightChildCentroidsId int32,
	typeOriginPk *Type, posOriginPk int,
	typeOriginVecColumn *Type, posOriginVecColumn int) int32 {

	crossJoinTblAndCentroidsId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER, // since there is no OnList, it is a cross join
		Children: []int32{leftChildTblId, rightChildCentroidsId},
		ProjectList: []*Expr{
			{
				// centroids.version
				Typ: *makePlan2Type(&bigIntType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 0,
						Name:   catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
					},
				},
			},
			{ // centroids.centroid_id
				Typ: *makePlan2Type(&bigIntType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 1,
						Name:   catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
					},
				},
			},
			{ // tbl.pk
				Typ: *typeOriginPk,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 0,
						Name:   tableDef.Cols[posOriginPk].Name,
					},
				},
			},
			{ // tbl.embedding
				Typ: *typeOriginVecColumn,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 1,
						Name:   tableDef.Cols[posOriginVecColumn].Name,
					},
				},
			},
			{ // centroids.centroid
				Typ: *typeOriginVecColumn,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 2,
						Name:   catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
					},
				},
			},
			{ // tbl.normalize_l2(embedding)
				Typ: *typeOriginVecColumn,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: 2,
					},
				},
			},
		},
	}, bindCtx)

	return crossJoinTblAndCentroidsId
}

func makeMinCentroidIdAndCpKey(builder *QueryBuilder, bindCtx *BindContext,
	crossJoinTblAndCentroidsID int32, multiTableIndex *MultiTableIndex) (int32, error) {

	lastNodeProjections := getProjectionByLastNode(builder, crossJoinTblAndCentroidsID)
	centroidsVersion := lastNodeProjections[0]
	centroidsId := lastNodeProjections[1]
	tblPk := lastNodeProjections[2]
	centroidsCentroid := lastNodeProjections[4]
	tblNormalizeL2Embedding := lastNodeProjections[5]

	// 1.a Group By
	groupByList := []*plan.Expr{
		DeepCopyExpr(centroidsVersion), // centroids.version
		DeepCopyExpr(tblPk),            // tbl.pk
	}

	// 1.b Agg Functions
	entriesParams := multiTableIndex.IndexDefs[catalog.SystemSI_IVFFLAT_TblType_Entries].IndexAlgoParams
	paramMap, err := catalog.IndexParamsStringToMap(entriesParams)
	if err != nil {
		return -1, err
	}
	vectorOps := paramMap[catalog.IndexAlgoParamOpType]
	distFn := opTypeToDistanceFunc[vectorOps]
	l2Distance, err := BindFuncExprImplByPlanExpr(builder.GetContext(), distFn, []*plan.Expr{
		DeepCopyExpr(centroidsCentroid),       // centroids.centroid
		DeepCopyExpr(tblNormalizeL2Embedding), // tbl.normalize_l2(embedding)
	})
	if err != nil {
		return -1, err
	}

	serialL2DistanceCentroidId, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", []*plan.Expr{
		l2Distance,
		DeepCopyExpr(centroidsId), // centroids.centroid_id
	})
	if err != nil {
		return -1, err
	}

	minSerialFullL2DistanceAndCentroidId, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "min", []*plan.Expr{
		serialL2DistanceCentroidId,
	})
	if err != nil {
		return -1, err
	}

	aggList := []*plan.Expr{
		minSerialFullL2DistanceAndCentroidId,
	}

	// 1.c Project List
	centroidsIdOfMinimumL2Distance, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_extract", []*plan.Expr{
		{
			Typ: makePlan2TypeValue(&varCharType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -2,                          // -1 is group by, -2 is agg function
					ColPos: int32(0 + len(groupByList)), // agg function is the one after `group by`
				},
			},
		},
		makePlan2Int64ConstExprWithType(1),
		{
			Typ: makePlan2TypeValue(&bigIntType),
			Expr: &plan.Expr_T{
				T: &plan.TargetType{},
			},
		},
	})
	if err != nil {
		return -1, err
	}

	centroidsVersionProj := DeepCopyExpr(centroidsVersion)
	centroidsVersionProj.Expr.(*plan.Expr_Col).Col.RelPos = -1
	centroidsVersionProj.Expr.(*plan.Expr_Col).Col.ColPos = 0

	tblPkProj := DeepCopyExpr(tblPk)
	tblPkProj.Expr.(*plan.Expr_Col).Col.RelPos = -1
	tblPkProj.Expr.(*plan.Expr_Col).Col.ColPos = 1

	// 1.d Create a new AGG node
	// NOTE: Don't add
	// serial(centroidsVersionProj, "centroidsIdOfMinimumL2Distance", tblPkProj) in here as you will be computing
	// the same value multiple times. Instead, add serial(...) in the next PROJECT node.
	projectionList := []*plan.Expr{
		centroidsVersionProj,           // centroids.version
		centroidsIdOfMinimumL2Distance, // centroids.centroid_id
		tblPkProj,                      // tbl.pk
	}

	newNodeID := builder.appendNode(
		&plan.Node{
			NodeType:    plan.Node_AGG,
			Children:    []int32{crossJoinTblAndCentroidsID},
			ProjectList: projectionList,
			AggList:     aggList,
			GroupBy:     groupByList,
		},
		bindCtx)

	// 2.a Project List
	lastProjections := getProjectionByLastNode(builder, newNodeID)

	// 2.b Create a serial(...) expression
	cpKey, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{
		DeepCopyExpr(lastProjections[0]),
		DeepCopyExpr(lastProjections[1]),
		DeepCopyExpr(lastProjections[2]),
	})
	if err != nil {
		return -1, err
	}

	// 2.c Create a new PROJECT node
	project := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{newNodeID},
		ProjectList: []*Expr{
			lastProjections[0],
			lastProjections[1],
			lastProjections[2],
			cpKey,
		},
	}, bindCtx)

	return project, nil
}

func makeFinalProjectWithTblEmbedding(builder *QueryBuilder, bindCtx *BindContext,
	lastNodeId, minCentroidIdNode int32,
	tableDef *TableDef,
	typeOriginPk *Type, posOriginPk int,
	typeOriginVecColumn *Type, posOriginVecColumn int) (int32, error) {

	condExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{ // tbl.pk
			Typ: *DeepCopyType(typeOriginPk),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(posOriginPk),
					Name:   tableDef.Cols[posOriginPk].Name,
				},
			},
		},
		{ // join.pk
			Typ: *DeepCopyType(typeOriginPk),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 1,
					ColPos: 2,
					Name:   tableDef.Cols[posOriginPk].Name,
				},
			},
		},
	})
	if err != nil {
		return -1, err
	}

	// 0: centroids.version,
	// 1: centroids.centroid_id,
	// 2: tbl.pk,
	// 3: tbl.embedding,
	var rProjections = getProjectionByLastNode(builder, minCentroidIdNode)

	rCentroidsVersion := DeepCopyExpr(rProjections[0])
	rCentroidsCentroidId := DeepCopyExpr(rProjections[1])
	rTblPk := DeepCopyExpr(rProjections[2])
	rCpKey := DeepCopyExpr(rProjections[3])

	rCentroidsVersion.Expr.(*plan.Expr_Col).Col.RelPos = 1
	rCentroidsCentroidId.Expr.(*plan.Expr_Col).Col.RelPos = 1
	rTblPk.Expr.(*plan.Expr_Col).Col.RelPos = 1
	rCpKey.Expr.(*plan.Expr_Col).Col.RelPos = 1

	finalProjectId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
		Children: []int32{lastNodeId, minCentroidIdNode},
		// version, centroid_id, pk, serial(version,pk)
		ProjectList: []*Expr{
			DeepCopyExpr(rCentroidsVersion),
			DeepCopyExpr(rCentroidsCentroidId),
			DeepCopyExpr(rTblPk),
			{ // tbl.pk
				Typ: *typeOriginVecColumn,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(posOriginVecColumn),
						Name:   tableDef.Cols[posOriginVecColumn].Name,
					},
				},
			},
			rCpKey,
		},
		OnList: []*Expr{condExpr},
	}, bindCtx)

	return finalProjectId, nil
}
