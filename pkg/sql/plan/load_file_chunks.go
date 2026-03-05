// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var loadFileChunksColDefs []*plan.ColDef

func init() {
	i64Typ := types.T_int64.ToType()
	blobTyp := types.T_blob.ToType()
	loadFileChunksColDefs = []*plan.ColDef{
		{
			Name: "chunk_id",
			Typ:  makePlan2Type(&i64Typ),
		},
		{
			Name: "offset",
			Typ:  makePlan2Type(&i64Typ),
		},
		{
			Name: "data",
			Typ:  makePlan2Type(&blobTyp),
		},
	}
}

func (builder *QueryBuilder) buildLoadFileChunks(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) int32 {
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "load_file_chunks",
			},
			Cols: loadFileChunksColDefs,
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx)
}
