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
