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
	bigIntType = types.T_int64.ToType()
)

func makeMetaTblScanWhereKeyEqVersion(builder *QueryBuilder, bindCtx *BindContext, indexTableDefs []*TableDef, idxRefs []*ObjectRef) (int32, error) {
	var metaTableScanId int32

	scanNodeProjections := make([]*Expr, len(indexTableDefs[0].Cols))
	for colIdx, column := range indexTableDefs[0].Cols {
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
	metaTableScanId = builder.appendNode(&Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      idxRefs[0],
		TableDef:    indexTableDefs[0],
		ProjectList: scanNodeProjections,
	}, bindCtx)

	prevProjection := getProjectionByLastNode(builder, metaTableScanId)
	whereKeyEqVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		prevProjection[0],
		MakePlan2StringConstExprWithType("version"),
	})
	if err != nil {
		return -1, err
	}
	metaTableScanId = builder.appendNode(&Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{metaTableScanId},
		FilterList: []*Expr{whereKeyEqVersion},
	}, bindCtx)

	prevProjection = getProjectionByLastNode(builder, metaTableScanId)
	castValueColToBigInt, err := makePlan2CastExpr(builder.GetContext(), prevProjection[1], makePlan2Type(&bigIntType))
	if err != nil {
		return -1, err
	}
	metaTableScanId = builder.appendNode(&Node{
		NodeType:    plan.Node_PROJECT,
		Stats:       &plan.Stats{},
		Children:    []int32{metaTableScanId},
		ProjectList: []*Expr{castValueColToBigInt},
	}, bindCtx)

	return metaTableScanId, nil
}

func makeCentroidsTblScan(builder *QueryBuilder, bindCtx *BindContext, indexTableDefs []*TableDef, idxRefs []*ObjectRef) int32 {
	scanNodeProjections := make([]*Expr, len(indexTableDefs[1].Cols))
	for colIdx, column := range indexTableDefs[1].Cols {
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
		ObjRef:      idxRefs[1],
		TableDef:    indexTableDefs[1],
		ProjectList: scanNodeProjections,
	}, bindCtx)
	return centroidsScanId
}

func makeCrossJoinCentroidsMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext, indexTableDefs []*TableDef, idxRefs []*ObjectRef, metaTableScanId int32) (int32, error) {
	centroidsScanId := makeCentroidsTblScan(builder, bindCtx, indexTableDefs, idxRefs)

	// 0: centroids.version
	// 1: centroids.centroid_id
	// 2: centroids.centroid
	// 3: meta.value i.e, current version
	joinProjections := getProjectionByLastNode(builder, centroidsScanId)[:3]
	joinProjections = append(joinProjections, &plan.Expr{
		Typ: *makePlan2Type(&bigIntType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 1,
				ColPos: 0,
			},
		},
	})

	joinMetaAndCentroidsId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		JoinType:    plan.Node_SINGLE,
		Children:    []int32{centroidsScanId, metaTableScanId},
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

func makeCrossJoinTblAndCentroids(builder *QueryBuilder, bindCtx *BindContext, tableDef *TableDef,
	leftChildTblId int32, rightChildCentroidsId int32,
	typeOriginPk *Type, posOriginPk int,
	typeOriginVecColumn *Type, posOriginVecColumn int) int32 {

	crossJoinTblAndCentroidsId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_INNER,
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
						ColPos: int32(posOriginPk),
						Name:   tableDef.Cols[posOriginPk].Name,
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
		},
	}, bindCtx)

	return crossJoinTblAndCentroidsId
}

func partitionByWindowAndFilterByRowNum(builder *QueryBuilder, bindCtx *BindContext, crossJoinTblAndCentroidsID int32) (int32, error) {
	// 5. partition by tbl.pk
	projections := getProjectionByLastNode(builder, crossJoinTblAndCentroidsID)
	lastTag := builder.genNewTag()
	partitionBySortKeyId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PARTITION,
		Children:    []int32{crossJoinTblAndCentroidsID},
		ProjectList: projections,
		OrderBy: []*OrderBySpec{
			{
				Flag: plan.OrderBySpec_INTERNAL,
				Expr: projections[2], // tbl.pk
			},
		},
		BindingTags: []int32{lastTag},
	}, bindCtx)

	// 6. Window operation
	// Window Function: row_number();
	// Partition By: tbl.pk;
	// Order By: l2_distance(tbl.embedding, centroids.centroid)
	projections = getProjectionByLastNode(builder, partitionBySortKeyId)
	l2Distance, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "l2_distance", []*Expr{projections[3], projections[4]})
	if err != nil {
		return -1, err
	}
	rowNumber, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "row_number", []*Expr{})
	if err != nil {
		return -1, err
	}
	winSpec := &plan.Expr{
		Typ: *makePlan2Type(&bigIntType),
		Expr: &plan.Expr_W{
			W: &plan.WindowSpec{
				WindowFunc:  rowNumber,
				PartitionBy: []*Expr{projections[2]}, // tbl.pk
				OrderBy: []*OrderBySpec{
					{
						Flag: plan.OrderBySpec_ASC,
						Expr: l2Distance,
					},
				},
				Frame: &plan.FrameClause{
					Type: plan.FrameClause_RANGE,
					Start: &plan.FrameBound{
						Type:      plan.FrameBound_PRECEDING,
						UnBounded: true,
					},
					End: &plan.FrameBound{
						Type:      plan.FrameBound_CURRENT_ROW,
						UnBounded: false,
					},
				},
				Name: "row_number",
			},
		},
	}
	rowNumberCol := &Expr{
		Typ: *makePlan2Type(&bigIntType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				// For WindowNode：
				// Rel = -1 means this column from window partition
				RelPos: -1,
				ColPos: 5, // {version, centroid_id, org_pk, cp_col, row_id, row_number}
			},
		},
	}
	windowId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_WINDOW,
		Children:    []int32{partitionBySortKeyId},
		BindingTags: []int32{lastTag},
		WindowIdx:   int32(0),
		WinSpecList: []*Expr{winSpec},
		ProjectList: []*Expr{projections[0], projections[1], projections[2], rowNumberCol},
	}, bindCtx)

	// 7. Filter records where row_number() = 1
	projections = getProjectionByLastNode(builder, windowId)
	whereRowNumberEqOne, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		projections[3],
		MakePlan2Int64ConstExprWithType(1),
	})
	if err != nil {
		return -1, err
	}

	filterId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_FILTER,
		Children:    []int32{windowId},
		FilterList:  []*Expr{whereRowNumberEqOne},
		ProjectList: []*Expr{projections[0], projections[1], projections[2]},
	}, bindCtx)
	return filterId, nil
}

func makeFinalProjectWithCPAndOptionalRowId(builder *QueryBuilder, bindCtx *BindContext,
	crossJoinTblAndCentroidsID int32, lastNodeId int32, isUpdate bool) (int32, error) {

	// 0: centroids.version,
	// 1: centroids.centroid_id,
	// 2: tbl.pk,
	// 3: entries.row_id (if update)
	var joinProjections = getProjectionByLastNode(builder, crossJoinTblAndCentroidsID)
	if isUpdate {
		lastProjection := builder.qry.Nodes[lastNodeId].ProjectList
		originRowIdIdx := len(lastProjection) - 1
		joinProjections = append(joinProjections, &plan.Expr{
			Typ: lastProjection[originRowIdIdx].Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: 0,
					ColPos: int32(originRowIdIdx),
					Name:   catalog.Row_ID,
				},
			},
		})
	}

	cpKeyCol, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "serial", []*plan.Expr{joinProjections[0], joinProjections[2]})
	if err != nil {
		return -1, err
	}

	finalProjectId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_PROJECT,
		Children: []int32{crossJoinTblAndCentroidsID},
		// version, centroid_id, pk, serial(version,pk)
		ProjectList: []*Expr{joinProjections[0], joinProjections[1], joinProjections[2], cpKeyCol},
	}, bindCtx)

	return finalProjectId, nil
}
