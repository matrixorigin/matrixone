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

func makeTableProjection(builder *QueryBuilder, bindCtx *BindContext, tableScanId int32,
	tableDef *TableDef, typeOriginPk *Type, posOriginPk int,
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

func minCentroidIdGroupByPK(builder *QueryBuilder, bindCtx *BindContext, crossJoinTblAndCentroidsID int32) (int32, error) {

	lastNodeProjections := getProjectionByLastNode(builder, crossJoinTblAndCentroidsID)
	// 1. Group By
	groupByList := []*plan.Expr{
		DeepCopyExpr(lastNodeProjections[0]), // centroids.version
		DeepCopyExpr(lastNodeProjections[2]), // tbl.pk
		DeepCopyExpr(lastNodeProjections[3]), // tbl.embedding
	}

	// 2. Agg Functions
	//TODO: modify this part to support multiple distance functions
	l2Distance, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "l2_distance", []*plan.Expr{
		DeepCopyExpr(lastNodeProjections[4]), // centroids.centroid
		DeepCopyExpr(lastNodeProjections[5]), // tbl.normalize_l2(embedding)
	})
	if err != nil {
		return -1, err
	}

	serialL2DistanceCentroidId, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_full", []*plan.Expr{
		l2Distance,
		DeepCopyExpr(lastNodeProjections[1]), // centroids.centroid_id
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

	// 3. Project List
	minL2DistanceCentroidId, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial_extract", []*plan.Expr{
		{
			Typ: makePlan2TypeValue(&varCharType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: -2,                          // -1 is group by, -2 is agg function
					ColPos: int32(0 + len(groupByList)), //TODO: verify
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

	centroidsVersion := DeepCopyExpr(lastNodeProjections[0])
	centroidsVersion.Expr.(*plan.Expr_Col).Col.RelPos = -1
	centroidsVersion.Expr.(*plan.Expr_Col).Col.ColPos = 0

	tblPk := DeepCopyExpr(lastNodeProjections[2])
	tblPk.Expr.(*plan.Expr_Col).Col.RelPos = -1
	tblPk.Expr.(*plan.Expr_Col).Col.ColPos = 1

	tblEmbedding := DeepCopyExpr(lastNodeProjections[3])
	tblEmbedding.Expr.(*plan.Expr_Col).Col.RelPos = -1
	tblEmbedding.Expr.(*plan.Expr_Col).Col.ColPos = 2

	projectionList := []*plan.Expr{
		centroidsVersion, // centroids.version
		minL2DistanceCentroidId,
		tblPk,        // tbl.pk
		tblEmbedding, // tbl.embedding
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

	return newNodeID, nil
}

func makeFinalProjectWithCPAndOptionalRowId(builder *QueryBuilder, bindCtx *BindContext,
	crossJoinTblAndCentroidsID int32) (int32, error) {

	// 0: centroids.version,
	// 1: centroids.centroid_id,
	// 2: tbl.pk,
	// 3: tbl.embedding,
	var joinProjections = getProjectionByLastNode(builder, crossJoinTblAndCentroidsID)

	cpKeyCol, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{
		DeepCopyExpr(joinProjections[0]),
		DeepCopyExpr(joinProjections[1]),
		DeepCopyExpr(joinProjections[2]),
	})
	if err != nil {
		return -1, err
	}

	finalProjectId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{crossJoinTblAndCentroidsID},
		// version, centroid_id, pk, embedding, serial(version,pk)
		ProjectList: []*Expr{joinProjections[0], joinProjections[1], joinProjections[2], joinProjections[3], cpKeyCol},
	}, bindCtx)

	return finalProjectId, nil
}
