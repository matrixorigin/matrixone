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

func makeMetaTblScanWithBindingTag(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTag0 int32) (int32, []*Expr) {
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
	metaTableScanId := builder.appendNode(&Node{
		NodeType:    plan.Node_TABLE_SCAN,
		Stats:       &plan.Stats{},
		ObjRef:      idxRefs[0],
		TableDef:    indexTableDefs[0],
		BindingTags: []int32{idxTag0},
		ProjectList: scanNodeProjections,
	}, bindCtx)
	return metaTableScanId, scanNodeProjections
}

func makeCentroidsTblScanWithBindingTag(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTag1 int32) (int32, []*Expr) {

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
	return centroidsScanId, scanNodeProjections
}

func makeEntriesTblScanWithBindingTag(builder *QueryBuilder, bindCtx *BindContext,
	indexTableDefs []*TableDef, idxRefs []*ObjectRef, idxTag2 int32) (int32, []*Expr) {
	scanNodeProjections := make([]*Expr, len(indexTableDefs[1].Cols))
	for colIdx, column := range indexTableDefs[2].Cols {
		scanNodeProjections[colIdx] = &plan.Expr{
			Typ: column.Typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: idxTag2,
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
		BindingTags: []int32{idxTag2},
	}, bindCtx)
	return centroidsScanId, scanNodeProjections
}
