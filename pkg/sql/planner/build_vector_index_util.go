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

package planner

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var (
	bigIntType = types.T_int64.ToType()
	//varCharType = types.T_varchar.ToType()
	//
	//opTypeToDistanceFunc = map[string]string{
	//	"vector_l2_ops":     "l2_distance",
	//	"vector_ip_ops":     "inner_product",
	//	"vector_cosine_ops": "cosine_distance",
	//}
)

func makeIvfFlatIndexTblScan(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*plan.TableDef, idxRefs []*plan.ObjectRef, idxTableId int32) (int32, []*plan.Expr) {
	scanNodeProjections := make([]*plan.Expr, len(indexTableDefs[idxTableId].Cols))
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
	centroidsScanId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_TABLE_SCAN,
		ObjRef:      idxRefs[idxTableId],
		TableDef:    indexTableDefs[idxTableId],
		ProjectList: scanNodeProjections,
	}, bindCtx)
	return centroidsScanId, scanNodeProjections
}

func makeMetaTblScanWhereKeyEqVersion(builder *QueryBuilder, bindCtx *BindContext, indexTableDefs []*plan.TableDef, idxRefs []*plan.ObjectRef) (int32, error) {
	metaTableScanId, scanCols := makeIvfFlatIndexTblScan(builder, bindCtx, indexTableDefs, idxRefs, 0)

	whereKeyEqVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
		plan.DeepCopyExpr(scanCols[0]),
		MakePlan2StringConstExprWithType("version"),
	})
	if err != nil {
		return -1, err
	}
	builder.qry.Nodes[metaTableScanId].FilterList = []*plan.Expr{whereKeyEqVersion}
	return metaTableScanId, nil
}

func makeCrossJoinCentroidsMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*plan.TableDef, idxRefs []*plan.ObjectRef, metaTableScanId int32) (int32, error) {
	centroidsScanId, _ := makeIvfFlatIndexTblScan(builder, bindCtx, indexTableDefs, idxRefs, 1)

	metaProjection := getProjectionByLastNode(builder, metaTableScanId)
	metaProjectValueCol := plan.DeepCopyExpr(metaProjection[1])
	metaProjectValueCol.Expr.(*plan.Expr_Col).Col.RelPos = 1
	prevMetaScanCastValAsBigInt, err := makePlan2CastExpr(builder.GetContext(), metaProjectValueCol, makePlan2Type(&bigIntType))
	if err != nil {
		return -1, err
	}
	// 0: centroids.version
	// 1: centroids.centroid_id
	// 2: centroids.centroid
	prevCentroidScanProjection := getProjectionByLastNode(builder, centroidsScanId)[:3]
	whereCentroidVersionEqCurrVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*plan.Expr{
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
		OnList:      []*plan.Expr{whereCentroidVersionEqCurrVersion},
	}, bindCtx)

	return joinMetaAndCentroidsId, nil
}

func makeTblCrossJoinL2Centroids(builder *QueryBuilder, bindCtx *BindContext, tableDef *plan.TableDef, lastNodeId int32, currVersionCentroids int32, typeOriginPk plan.Type, posOriginPk int, typeOriginVecColumn plan.Type, posOriginVecColumn int, optype string) int32 {
	joinTblAndCentroidsUsingCrossL2Join := builder.appendNode(&plan.Node{
		NodeType:     plan.Node_JOIN,
		JoinType:     plan.Node_L2,
		ExtraOptions: optype,
		Children:     []int32{lastNodeId, currVersionCentroids},
		ProjectList: []*plan.Expr{
			{ // centroids.version
				Typ: makePlan2TypeValue(&bigIntType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 0,
						Name:   catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
					},
				},
			},
			{ // centroids.centroid_id
				Typ: makePlan2TypeValue(&bigIntType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 1,
						Name:   catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
					},
				},
			},
			{ // tbl.pk
				Typ: *plan.DeepCopyType(&typeOriginPk),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(posOriginPk),
						Name:   tableDef.Cols[posOriginPk].Name,
					},
				},
			},
			{ // tbl.embedding
				Typ: *plan.DeepCopyType(&typeOriginVecColumn),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(posOriginVecColumn),
						Name:   tableDef.Cols[posOriginVecColumn].Name,
					},
				},
			},
		},
		OnList: []*plan.Expr{
			{ // centroids.centroid
				Typ: *plan.DeepCopyType(&typeOriginVecColumn),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: 2,
						Name:   catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
					},
				},
			},
			{ // tbl.embedding
				Typ: *plan.DeepCopyType(&typeOriginVecColumn),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(posOriginVecColumn),
						Name:   tableDef.Cols[posOriginVecColumn].Name,
					},
				},
			},
		},
	}, bindCtx)
	return joinTblAndCentroidsUsingCrossL2Join
}

func makeFinalProject(builder *QueryBuilder, bindCtx *BindContext, joinTblAndCentroidsUsingCrossL2Join int32) (int32, error) {
	var finalProjections = getProjectionByLastNode(builder, joinTblAndCentroidsUsingCrossL2Join)

	centroidsVersion := plan.DeepCopyExpr(finalProjections[0])
	centroidsId := plan.DeepCopyExpr(finalProjections[1])
	tblPk := plan.DeepCopyExpr(finalProjections[2])
	tblEmbedding := plan.DeepCopyExpr(finalProjections[3])
	cpKey, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{
		plan.DeepCopyExpr(finalProjections[0]),
		plan.DeepCopyExpr(finalProjections[1]),
		plan.DeepCopyExpr(finalProjections[2]),
	})
	if err != nil {
		return -1, err
	}

	projectWithCpKey := builder.appendNode(
		&plan.Node{
			NodeType:    plan.Node_PROJECT,
			Children:    []int32{joinTblAndCentroidsUsingCrossL2Join},
			ProjectList: []*plan.Expr{centroidsVersion, centroidsId, tblPk, tblEmbedding, cpKey},
		},
		bindCtx)
	return projectWithCpKey, nil
}
