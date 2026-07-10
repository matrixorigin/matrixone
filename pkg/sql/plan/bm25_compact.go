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

// bm25CompactColDefs — the compact table function returns a single status row:
// merged_subs, the number of tag=0 sub-indexes the merge produced. Callers
// (ALTER … REINDEX … BM25 MERGE / idxcron) run it for its side effect and
// discard the row.
func bm25CompactColDefs() []*plan.ColDef {
	tp := types.New(types.T_int64, 0, 0)
	return []*plan.ColDef{{
		Name: "merged_subs",
		Typ:  plan.Type{Id: int32(tp.Oid), NotNullable: true},
	}}
}

// buildBm25Compact builds a FUNCTION_SCAN node for the standalone
// `bm25_compact(db, store, meta, capacity)` compaction table function — no
// driving table, four varchar args (passed as-is, no leading param strip).
func (builder *QueryBuilder) buildBm25Compact(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) int32 {
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: "bm25_compact",
			},
			Cols: bm25CompactColDefs(),
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx)
}
