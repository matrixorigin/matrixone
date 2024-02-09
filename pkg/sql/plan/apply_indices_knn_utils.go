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
	float64Type = types.T_float64.ToType()
)

func makeMetaTblScanWhereKeyEqVersionAndCastVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTags map[string]int32, prefix string) (int32, error) {

	// 1. Scan key, value, row_id from meta table
	metaTableScanId, scanCols, _ := makeHiddenTblScanWithBindingTag(builder, bindCtx, indexTableDefs[0], idxRefs[0], idxTags[prefix+".scan"])

	// 2. Filter key == "version"
	whereKeyEqVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		scanCols[0], MakePlan2StringConstExprWithType("version")})
	if err != nil {
		return -1, err
	}
	metaFilterId := builder.appendNode(&Node{
		NodeType:   plan.Node_FILTER,
		Children:   []int32{metaTableScanId},
		FilterList: []*Expr{whereKeyEqVersion},
	}, bindCtx)

	// 3. Project value column as BigInt
	idxTags[prefix+".project"] = builder.genNewTag()
	castMetaValueColToBigInt, err := makePlan2CastExpr(builder.GetContext(), scanCols[1], makePlan2Type(&bigIntType))
	metaProjectId := builder.appendNode(&Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{metaFilterId},
		ProjectList: []*plan.Expr{castMetaValueColToBigInt},
		BindingTags: []int32{idxTags[prefix+".project"]},
	}, bindCtx)

	return metaProjectId, nil
}

func makeCentroidsCrossJoinMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTags map[string]int32,
	metaTableScanId int32, fn *plan.Function) (int32, error) {

	// 1. Scan version, centroid_id, centroid from centroids table
	centroidsScanId, scanCols, _ := makeHiddenTblScanWithBindingTag(builder, bindCtx, indexTableDefs[1], idxRefs[1],
		idxTags["centroids.scan"])

	//2. JOIN centroids and meta on version + Project version, centroid_id, centroid
	joinCond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		scanCols[0],
		{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["meta1.project"],
					ColPos: 0,
				},
			},
		},
	})
	if err != nil {
		return -1, err
	}
	joinMetaAndCentroidsId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_SINGLE,
		Children: []int32{centroidsScanId, metaTableScanId},
		OnList:   []*Expr{joinCond},
	}, bindCtx)

	// 3. Project version, centroid_id, centroid, l2_distance(literal, normalize_l2(col))
	NormalizeL2Col, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), "normalize_l2", []*plan.Expr{
		{
			Typ: DeepCopyType(indexTableDefs[1].Cols[2].Typ),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["centroids.scan"],
					ColPos: 2,
				},
			},
		},
	})
	distFnName := fn.Func.ObjName
	l2DistanceLitNormalizeL2Col, _ := BindFuncExprImplByPlanExpr(builder.GetContext(), distFnName, []*plan.Expr{
		NormalizeL2Col, // normalize_l2(col)
		fn.Args[1],     // literal
	})
	idxTags["centroids.project"] = builder.genNewTag()
	projectCols := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{joinMetaAndCentroidsId},
		ProjectList: []*Expr{scanCols[0], scanCols[1], scanCols[2], l2DistanceLitNormalizeL2Col},
		BindingTags: []int32{idxTags["centroids.project"]},
	}, bindCtx)

	// 4. Sort by l2_distance(literal, normalize_l2(col)) limit 1
	sortByL2DistanceId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_SORT,
		Children: []int32{projectCols},
		Limit:    makePlan2Int64ConstExprWithType(1), //TODO: need to fix.
		OrderBy: []*OrderBySpec{
			{
				Expr: &plan.Expr{
					Typ: makePlan2Type(&float64Type),
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: idxTags["centroids.project"],
							ColPos: 3,
						},
					},
				},
				Flag: plan.OrderBySpec_ASC, //TODO: need to fix.
			},
		},
	}, bindCtx)

	return sortByL2DistanceId, nil
}

// TODO: add LIMIT and OFFSET
func makeEntriesCrossJoinMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTags map[string]int32,
	metaTableScanId int32) (int32, error) {

	// 1. Scan version, centroid_id_fk, origin_pk from entries table
	entriesScanId, scanCols, _ := makeHiddenTblScanWithBindingTag(builder, bindCtx, indexTableDefs[2], idxRefs[2],
		idxTags["entries.scan"])

	// 2. JOIN entries and meta on version + Project version, centroid_id_fk, origin_pk
	joinCond, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		scanCols[0],
		{
			Typ: makePlan2Type(&bigIntType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["meta2.project"],
					ColPos: 0,
				},
			},
		},
	})
	if err != nil {
		return -1, err
	}
	joinMetaAndEntriesId := builder.appendNode(&plan.Node{
		NodeType: plan.Node_JOIN,
		JoinType: plan.Node_SINGLE,
		Children: []int32{entriesScanId, metaTableScanId},
		OnList:   []*Expr{joinCond},
	}, bindCtx)

	// 3. Project version, centroid_id_fk, origin_pk, meta.value
	idxTags["entries.project"] = builder.genNewTag()
	projectCols := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Children:    []int32{joinMetaAndEntriesId},
		ProjectList: []*Expr{scanCols[0], scanCols[1], scanCols[2]},
		BindingTags: []int32{idxTags["entries.project"]},
	}, bindCtx)

	return projectCols, nil
	//whereCentroidVersionEqCurrVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
	//	scanCols[0],
	//	castMetaValueColToBigInt,
	//})
	//if err != nil {
	//	return -1, err
	//}
	//filterCentroidsForCurrVersionId := builder.appendNode(&plan.Node{
	//	NodeType:   plan.Node_FILTER,
	//	Children:   []int32{joinMetaAndEntriesId},
	//	FilterList: []*Expr{whereCentroidVersionEqCurrVersion},
	//	//ProjectList: []*Expr{
	//	//	&plan.Expr{
	//	//		Typ: makePlan2Type(&bigIntType),
	//	//		Expr: &plan.Expr_Col{
	//	//			Col: &plan.ColRef{
	//	//				RelPos: idxTag2,
	//	//				ColPos: 0,
	//	//			},
	//	//		},
	//	//	},
	//	//	&plan.Expr{
	//	//		Typ: makePlan2Type(&bigIntType),
	//	//		Expr: &plan.Expr_Col{
	//	//			Col: &plan.ColRef{
	//	//				RelPos: idxTag2,
	//	//				ColPos: 1,
	//	//			},
	//	//		},
	//	//	},
	//	//	&plan.Expr{
	//	//		Typ: makePlan2Type(&bigIntType),
	//	//		Expr: &plan.Expr_Col{
	//	//			Col: &plan.ColRef{
	//	//				RelPos: idxTag2,
	//	//				ColPos: 2,
	//	//			},
	//	//		},
	//	//	},
	//	//},
	//	BindingTags: []int32{idxTags[0], idxTags[2]},
	//}, bindCtx)
	//return filterCentroidsForCurrVersionId, nil
}
