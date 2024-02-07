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

import "github.com/matrixorigin/matrixone/pkg/pb/plan"

func makeMetaTblScanWhereKeyEqVersionAndCastVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTag0 int32) (int32, error) {
	var metaTableScanId int32

	scanNodeProjections := make([]*Expr, len(indexTableDefs[0].Cols))
	for colIdx, column := range indexTableDefs[0].Cols {
		scanNodeProjections[colIdx] = &plan.Expr{
			Typ: column.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag0,
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
		BindingTags: []int32{idxTag0},
		ProjectList: scanNodeProjections,
	}, bindCtx)

	whereKeyEqVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag0,
					ColPos: 0,
				},
			},
		},
		MakePlan2StringConstExprWithType("version"),
	})
	if err != nil {
		return -1, err
	}
	metaTableScanId = builder.appendNode(&Node{
		NodeType:    plan.Node_FILTER,
		Children:    []int32{metaTableScanId},
		FilterList:  []*Expr{whereKeyEqVersion},
		ProjectList: scanNodeProjections,
		BindingTags: []int32{idxTag0}, /// may not be necessary
	}, bindCtx)

	castValueColToBigInt, err := makePlan2CastExpr(builder.GetContext(),
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag0,
					ColPos: 1,
				},
			},
		}, makePlan2Type(&bigIntType))
	if err != nil {
		return -1, err
	}
	metaTableScanId = builder.appendNode(&Node{
		NodeType:    plan.Node_PROJECT,
		Stats:       &plan.Stats{},
		Children:    []int32{metaTableScanId},
		ProjectList: []*Expr{castValueColToBigInt},
		BindingTags: []int32{idxTag0},
	}, bindCtx)

	return metaTableScanId, nil
}

func makeCentroidsTblScanWithBindingTag(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTag1 int32) int32 {

	scanNodeProjections := make([]*Expr, len(indexTableDefs[1].Cols))
	for colIdx, column := range indexTableDefs[1].Cols {
		scanNodeProjections[colIdx] = &plan.Expr{
			Typ: column.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag1,
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
		BindingTags: []int32{idxTag1},
	}, bindCtx)
	return centroidsScanId
}

func makeCentroidsCrossJoinMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef,
	metaTableScanId int32, idxTag0, idxTag1 int32) (int32, error) {
	centroidsScanId := makeCentroidsTblScanWithBindingTag(builder, bindCtx, indexTableDefs, idxRefs, idxTag1)

	// 0: centroids.version
	// 1: centroids.centroid_id
	// 2: centroids.centroid
	// 3: meta.value i.e, current version
	centroidsTblProjection := []*plan.Expr{
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag1,
					ColPos: 0,
				},
			},
		},
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag1,
					ColPos: 1,
				},
			},
		},
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag1,
					ColPos: 2,
				},
			},
		},
	}
	joinProjection := append(centroidsTblProjection, &plan.Expr{
		Typ: makePlan2Type(&bigIntType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: idxTag0,
				ColPos: 0,
			},
		},
	})

	joinMetaAndCentroidsId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		JoinType:    plan.Node_SINGLE,
		Children:    []int32{centroidsScanId, metaTableScanId},
		BindingTags: []int32{idxTag0, idxTag1},
		ProjectList: joinProjection,
	}, bindCtx)

	whereCentroidVersionEqCurrVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag1,
					ColPos: 0,
				},
			},
		},
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag0,
					ColPos: 0,
				},
			},
		},
	})
	if err != nil {
		return -1, err
	}
	filterCentroidsForCurrVersionId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_FILTER,
		Children:    []int32{joinMetaAndCentroidsId},
		FilterList:  []*Expr{whereCentroidVersionEqCurrVersion},
		BindingTags: []int32{idxTag0, idxTag1},
		ProjectList: []*Expr{
			&plan.Expr{
				Typ: makePlan2Type(&bigIntType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag1,
						ColPos: 0,
					},
				},
			},
			&plan.Expr{
				Typ: makePlan2Type(&bigIntType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag1,
						ColPos: 1,
					},
				},
			},
			&plan.Expr{
				Typ: makePlan2Type(&bigIntType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag1,
						ColPos: 2,
					},
				},
			},
		},
	}, bindCtx)
	return filterCentroidsForCurrVersionId, nil
}

func makeEntriesTblScanWithBindingTag(builder *QueryBuilder, bindCtx *BindContext, indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTag int32) int32 {
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
		BindingTags: []int32{idxTag},
	}, bindCtx)
	return centroidsScanId
}

func makeEntriesCrossJoinMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef,
	metaTableScanId int32, idxTag0, idxTag2 int32) (int32, error) {
	entriesScanId := makeEntriesTblScanWithBindingTag(builder, bindCtx, indexTableDefs, idxRefs, idxTag2)

	// 0: entries.version
	// 1: entries.centroid_id
	// 2: entries.pk
	// 3: meta.value i.e, current version
	entriesProjection := []*plan.Expr{
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag2,
					ColPos: 0,
				},
			},
		},
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag2,
					ColPos: 1,
				},
			},
		},
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag2,
					ColPos: 2,
				},
			},
		},
	}
	joinProjections := append(entriesProjection, &plan.Expr{
		Typ: makePlan2Type(&bigIntType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: idxTag0,
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
		BindingTags: []int32{idxTag0, idxTag2},
	}, bindCtx)

	whereCentroidVersionEqCurrVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag2,
					ColPos: 0,
				},
			},
		},
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag0,
					ColPos: 0,
				},
			},
		},
	})
	if err != nil {
		return -1, err
	}
	filterCentroidsForCurrVersionId := builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{joinMetaAndCentroidsId},
		FilterList: []*Expr{whereCentroidVersionEqCurrVersion},
		ProjectList: []*Expr{
			&plan.Expr{
				Typ: makePlan2Type(&bigIntType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag2,
						ColPos: 0,
					},
				},
			},
			&plan.Expr{
				Typ: makePlan2Type(&bigIntType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag2,
						ColPos: 1,
					},
				},
			},
			&plan.Expr{
				Typ: makePlan2Type(&bigIntType),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTag2,
						ColPos: 2,
					},
				},
			},
		},
		BindingTags: []int32{idxTag0, idxTag2},
	}, bindCtx)
	return filterCentroidsForCurrVersionId, nil
}
