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
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTags map[string]int32) (int32, error) {

	// 1. Scan key, value, row_id from meta table
	metaTableScanId, _, node := makeHiddenTblScanWithBindingTag(builder, bindCtx, indexTableDefs[0], idxRefs[0], idxTags["meta"])

	// 2. Filter key == "version"
	idxTags["meta.filter"] = builder.genNewTag()
	whereKeyEqVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Typ: DeepCopyType(indexTableDefs[0].Cols[0].Typ),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["meta"],
					ColPos: 0,
				},
			},
		},
		MakePlan2StringConstExprWithType("version"),
	})
	if err != nil {
		return -1, err
	}
	metaFilterId := builder.appendNode(&Node{
		NodeType:     plan.Node_FILTER,
		ParentObjRef: DeepCopyObjectRef(node.ObjRef),
		Children:     []int32{metaTableScanId},
		FilterList:   []*Expr{whereKeyEqVersion},
		BindingTags:  []int32{idxTags["meta.filter"]},
	}, bindCtx)

	// 3. Project value
	idxTags["meta.project"] = builder.genNewTag()
	metaProjectId := builder.appendNode(&Node{
		NodeType:     plan.Node_PROJECT,
		Stats:        &plan.Stats{},
		ParentObjRef: DeepCopyObjectRef(node.ObjRef),
		Children:     []int32{metaFilterId},
		ProjectList: []*Expr{
			{
				Typ: DeepCopyType(indexTableDefs[0].Cols[1].Typ),
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: idxTags["meta"],
						ColPos: 1,
					},
				},
			},
		},
		BindingTags: []int32{idxTags["meta.project"]},
	}, bindCtx)

	return metaProjectId, nil
}

func makeCentroidsCrossJoinMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef,
	metaTableScanId int32, idxTags map[string]int32, castMetaValueColToBigInt *Expr) (int32, error) {

	// 1. Scan version, centroid_id, centroid from centroids table
	centroidsScanId, scanProj, _ := makeHiddenTblScanWithBindingTag(builder, bindCtx, indexTableDefs[1], idxRefs[1], idxTags["centroids"])

	//2. JOIN centroids and meta on version
	idxTags["centroids.join"] = builder.genNewTag()
	joinMetaAndCentroidsId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_JOIN,
		JoinType:    plan.Node_SINGLE,
		Stats:       &plan.Stats{},
		Children:    []int32{centroidsScanId, metaTableScanId},
		BindingTags: []int32{idxTags["centroids.join"]},
	}, bindCtx)

	// 3. Project version, centroid_id, centroid, meta.value
	idxTags["centroids.project1"] = builder.genNewTag()
	joinProjections := []*plan.Expr{
		scanProj[0], //centroids.version
		scanProj[1], //centroids.centroid_id
		scanProj[2], //centroids.centroid
		{
			Typ: makePlan2Type(&varcharType),
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["meta.project"], //meta.value i.e, current version
					ColPos: 0,
				},
			},
		},
	}
	projectCols := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Stats:       &plan.Stats{},
		Children:    []int32{joinMetaAndCentroidsId},
		ProjectList: joinProjections,
		BindingTags: []int32{idxTags["centroids.project1"]},
	}, bindCtx)

	// 4. Filter centroids for current version
	idxTags["centroids.filter"] = builder.genNewTag()
	whereCentroidVersionEqCurrVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		{
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTags["centroids.project1"],
					ColPos: 0,
				},
			},
		},
		castMetaValueColToBigInt,
	})
	if err != nil {
		return -1, err
	}
	filterCentroidsForCurrVersionId := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_FILTER,
		Children:    []int32{projectCols},
		FilterList:  []*Expr{whereCentroidVersionEqCurrVersion},
		BindingTags: []int32{idxTags["centroids.filter"]},
		Stats:       &plan.Stats{},
	}, bindCtx)

	// 5. Project version, centroid_id, centroid
	idxTags["centroids.project2"] = builder.genNewTag()
	projectCentroidsTable := builder.appendNode(&plan.Node{
		NodeType:    plan.Node_PROJECT,
		Stats:       &plan.Stats{},
		Children:    []int32{filterCentroidsForCurrVersionId},
		ProjectList: joinProjections[:3],
		BindingTags: []int32{idxTags["centroids.project2"]},
	}, bindCtx)

	return projectCentroidsTable, nil
}

func makeEntriesCrossJoinMetaForCurrVersion(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef,
	metaTableScanId int32, idxTags []int32, castMetaValueColToBigInt *Expr) (int32, error) {
	entriesScanId, scanProj, _ := makeHiddenTblScanWithBindingTag(builder, bindCtx, indexTableDefs[2], idxRefs[2], idxTags[2])

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
					RelPos: idxTags[0],
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
		BindingTags: []int32{idxTags[0], idxTags[2]},
	}, bindCtx)

	//return joinMetaAndCentroidsId, nil

	whereCentroidVersionEqCurrVersion, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*Expr{
		scanProj[0],
		castMetaValueColToBigInt,
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
		BindingTags: []int32{idxTags[0], idxTags[2]},
	}, bindCtx)
	return filterCentroidsForCurrVersionId, nil
}
