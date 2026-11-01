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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const indexFileChunksFuncName = "index_file_chunks"

var indexFileChunksColDefs []*plan.ColDef

func init() {
	idTyp := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	i64Typ := types.T_int64.ToType()
	blobTyp := types.T_blob.ToType()
	indexFileChunksColDefs = []*plan.ColDef{
		{Name: catalog.IndexStorage_TblCol_Index_Id, Typ: makePlan2Type(&idTyp)},
		{Name: catalog.IndexStorage_TblCol_Chunk_Id, Typ: makePlan2Type(&i64Typ)},
		{Name: catalog.IndexStorage_TblCol_Data, Typ: makePlan2Type(&blobTyp)},
		{Name: catalog.IndexStorage_TblCol_Tag, Typ: makePlan2Type(&i64Typ)},
	}
}

// buildIndexFileChunks binds index_file_chunks(source, first chunk, chunk count), which
// only internal SQL may call.
func (builder *QueryBuilder) buildIndexFileChunks(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if !defines.IsInternalExecutor(builder.GetContext()) {
		return 0, moerr.NewNotSupportedf(builder.GetContext(), "table function '%s' not supported", indexFileChunksFuncName)
	}
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc:   &plan.TableFunction{Name: indexFileChunksFuncName},
			Cols:      indexFileChunksColDefs,
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), nil
}
