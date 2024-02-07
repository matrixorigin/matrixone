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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

var (
	varcharType = types.T_varchar.ToType()
)

func makeMetaTblScanWhereKeyEqVersionAndCastVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTag0 int32) (int32, *Expr, error) {

	metaTableScanId, scanProj := makeMetaTblScanWithBindingTag(builder, bindCtx, indexTableDefs, idxRefs, idxTag0)

	// TODO:
	// Getting
	// Filter Cond: (__mo_index_secondary_018d8167-af7f-7005-b02b-8a6fe6b31be5.__mo_index_key = cast('version' AS BIGINT))
	// instead of
	// Filter Cond: (__mo_index_secondary_018d8167-af7f-7005-b02b-8a6fe6b31be5.__mo_index_key = 'version')
	whereKeyEqVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		scanProj[0],
		MakePlan2StringConstExprWithType("version"),
	})
	if err != nil {
		return -1, nil, err
	}
	metaTableScanId = builder.appendNode(&Node{
		NodeType:    plan.Node_FILTER,
		Children:    []int32{metaTableScanId},
		FilterList:  []*Expr{whereKeyEqVersion},
		BindingTags: []int32{idxTag0}, /// may not be necessary
	}, bindCtx)

	metaTableScanId = builder.appendNode(&Node{
		NodeType:    plan.Node_PROJECT,
		Stats:       &plan.Stats{},
		Children:    []int32{metaTableScanId},
		ProjectList: []*Expr{scanProj[1]},
		BindingTags: []int32{idxTag0},
	}, bindCtx)

	castValueColToBigInt, err := makePlan2CastExpr(builder.GetContext(), scanProj[1], makePlan2Type(&bigIntType))
	if err != nil {
		return -1, nil, err
	}

	return metaTableScanId, castValueColToBigInt, nil
}

func makeCentroidsCrossJoinMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef,
	metaTableScanId int32, idxTag0, idxTag1 int32, castVersionToBigInt *Expr) (int32, error) {
	centroidsScanId, scanProj := makeCentroidsTblScanWithBindingTag(builder, bindCtx, indexTableDefs, idxRefs, idxTag1)

	// 0: centroids.version
	// 1: centroids.centroid_id
	// 2: centroids.centroid
	// 3: meta.value i.e, current version
	centroidsTblProjection := []*plan.Expr{
		scanProj[0],
		scanProj[1],
		scanProj[2],
	}
	joinProjections := append(centroidsTblProjection, &plan.Expr{
		Typ: makePlan2Type(&varcharType),
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: idxTag0,
				ColPos: 1,
			},
		},
	})

	joinMetaAndCentroidsId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		JoinType:    plan.Node_SINGLE,
		Children:    []int32{centroidsScanId, metaTableScanId},
		BindingTags: []int32{idxTag0, idxTag1},
		ProjectList: joinProjections,
	}, bindCtx)

	whereCentroidVersionEqCurrVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		scanProj[0],
		castVersionToBigInt,
	})
	if err != nil {
		return -1, err
	}
	filterCentroidsForCurrVersionId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_FILTER,
		Children:    []int32{joinMetaAndCentroidsId},
		FilterList:  []*Expr{whereCentroidVersionEqCurrVersion},
		BindingTags: []int32{idxTag0, idxTag1},
		//ProjectList: []*Expr{
		//	&plan.Expr{
		//		Typ: makePlan2Type(&bigIntType),
		//		Expr: &plan.Expr_Col{
		//			Col: &plan.ColRef{
		//				RelPos: idxTag1,
		//				ColPos: 0,
		//			},
		//		},
		//	},
		//	&plan.Expr{
		//		Typ: makePlan2Type(&bigIntType),
		//		Expr: &plan.Expr_Col{
		//			Col: &plan.ColRef{
		//				RelPos: idxTag1,
		//				ColPos: 1,
		//			},
		//		},
		//	},
		//	&plan.Expr{
		//		Typ: makePlan2Type(&bigIntType),
		//		Expr: &plan.Expr_Col{
		//			Col: &plan.ColRef{
		//				RelPos: idxTag1,
		//				ColPos: 2,
		//			},
		//		},
		//	},
		//},
	}, bindCtx)
	return filterCentroidsForCurrVersionId, nil
}

func makeEntriesCrossJoinMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef,
	metaTableScanId int32, idxTag0, idxTag2 int32, castVersionToBigInt *Expr) (int32, error) {
	entriesScanId, scanProj := makeEntriesTblScanWithBindingTag(builder, bindCtx, indexTableDefs, idxRefs, idxTag2)

	// 0: entries.version
	// 1: entries.centroid_id
	// 2: entries.pk
	// 3: meta.value i.e, current version
	entriesProjection := []*plan.Expr{
		scanProj[0],
		scanProj[1],
		scanProj[2],
	}
	joinProjections := append(entriesProjection,
		&plan.Expr{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag0,
					ColPos: 1,
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

	//return joinMetaAndCentroidsId, nil

	whereCentroidVersionEqCurrVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		scanProj[0],
		castVersionToBigInt,
	})
	if err != nil {
		return -1, err
	}
	filterCentroidsForCurrVersionId := builder.appendNode(&plan.Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{joinMetaAndCentroidsId},
		FilterList: []*Expr{whereCentroidVersionEqCurrVersion},
		//ProjectList: []*Expr{
		//	&plan.Expr{
		//		Typ: makePlan2Type(&bigIntType),
		//		Expr: &plan.Expr_Col{
		//			Col: &plan.ColRef{
		//				RelPos: idxTag2,
		//				ColPos: 0,
		//			},
		//		},
		//	},
		//	&plan.Expr{
		//		Typ: makePlan2Type(&bigIntType),
		//		Expr: &plan.Expr_Col{
		//			Col: &plan.ColRef{
		//				RelPos: idxTag2,
		//				ColPos: 1,
		//			},
		//		},
		//	},
		//	&plan.Expr{
		//		Typ: makePlan2Type(&bigIntType),
		//		Expr: &plan.Expr_Col{
		//			Col: &plan.ColRef{
		//				RelPos: idxTag2,
		//				ColPos: 2,
		//			},
		//		},
		//	},
		//},
		BindingTags: []int32{idxTag0, idxTag2},
	}, bindCtx)
	return filterCentroidsForCurrVersionId, nil
}
