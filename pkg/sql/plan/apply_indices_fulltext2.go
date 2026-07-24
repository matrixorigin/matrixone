// Copyright 2026 Matrix Origin
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

// fulltext2-specific pieces of the MATCH query rewrite. fulltext2 reuses the
// classic MATCH(col) AGAINST(...) surface (fulltext_match), so it is resolved by
// findMatchFullTextIndex and chained by applyJoinFullTextIndices exactly like a
// classic fulltext index — the only difference is the per-match TVF: this file's
// buildFulltext2SearchTableFunc emits a fulltext2_search TVF (WAND positional
// engine) instead of fulltext_index_scan. Both emit (doc_id, score), so the
// downstream join/sort/limit is unchanged.
package plan

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const fulltext2_search_func_name = "fulltext2_search"

// buildFulltext2SearchCfg validates a MATCH against a fulltext2 index and returns
// the fulltext2_search config JSON ({db,index,metadata[,parser]}). It resolves the
// storage/metadata hidden tables and rejects NL/BOOLEAN modes on a POSITION_FREE
// index (which has no positions — only IN BM25 MODE bag-of-words is valid).
func (builder *QueryBuilder) buildFulltext2SearchCfg(scanNode *plan.Node, idxdef *plan.IndexDef, mode int64) (string, error) {
	storeTbl, metaTbl, ok := builder.findFulltext2IndexTables(scanNode, idxdef)
	if !ok {
		return "", moerr.NewInternalErrorf(builder.GetContext(),
			"fulltext2 index %q: storage/metadata tables not found (index may be partially materialized); reindex required",
			idxdef.IndexName)
	}

	// A POSITION_FREE index stores no positions, so NL / BOOLEAN (and the bare default,
	// which is NL semantics) phrase matching cannot work — only IN BM25 MODE (bag-of-words)
	// is valid. Reject the others up front with a clear message instead of silently
	// returning wrong/empty results.
	if mode != int64(tree.FULLTEXT_BM25) && fulltext2PositionFreeFromParams(idxdef.IndexAlgoParams) {
		return "", moerr.NewInvalidInputf(builder.GetContext(),
			"fulltext2 index %q is POSITION_FREE (bag-of-words only): query it with MATCH(...) AGAINST(... IN BM25 MODE); it has no positions for natural-language / boolean phrase matching",
			idxdef.IndexName)
	}

	cfgMap := map[string]string{
		"db":       scanNode.ObjRef.SchemaName,
		"index":    storeTbl,
		"metadata": metaTbl,
	}
	if parser := fulltext2ParserFromParams(idxdef.IndexAlgoParams); parser != "" {
		cfgMap["parser"] = parser
	}
	cfgBytes, err := json.Marshal(cfgMap)
	if err != nil {
		return "", err
	}
	return string(cfgBytes), nil
}

// buildFulltext2SearchNode builds the fulltext2_search FUNCTION_SCAN node for a MATCH
// resolved to a fulltext2 index. exprs are the TVF args [cfg(const), pattern, mode];
// pattern is passed as an expression, so a prepared-statement '?' parameter is bound
// and evaluated at execution time (the TVF reads it per row). The node emits the same
// (doc_id, score) shape as fulltext_index_scan, so the downstream join/sort is shared.
func (builder *QueryBuilder) buildFulltext2SearchNode(ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	colDefs := DeepCopyColDefList(ftIndexColdefs)
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: fulltext2_search_func_name,
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		TblFuncExprList: exprs,
		Children:        children,
	}
	return builder.appendNode(node, ctx), nil
}

// fulltext2ParserFromParams extracts the "parser" field from an index's
// IndexAlgoParams JSON (empty → default parser at the TVF).
func fulltext2ParserFromParams(params string) string {
	if len(params) == 0 {
		return ""
	}
	var p struct {
		Parser string `json:"parser"`
	}
	if err := json.Unmarshal([]byte(params), &p); err != nil {
		return ""
	}
	return p.Parser
}

// fulltext2PositionFreeFromParams reports whether the index was built POSITION_FREE
// (position_free=="true" in IndexAlgoParams). Such an index has no positional payload,
// so only IN BM25 MODE (bag-of-words) can query it.
func fulltext2PositionFreeFromParams(params string) bool {
	if len(params) == 0 {
		return false
	}
	var p struct {
		PositionFree string `json:"position_free"`
	}
	if err := json.Unmarshal([]byte(params), &p); err != nil {
		return false
	}
	return p.PositionFree == "true"
}

// findFulltext2IndexTables resolves the storage + metadata hidden tables of a
// fulltext2 index (the two sibling defs sharing the IndexName).
func (builder *QueryBuilder) findFulltext2IndexTables(scanNode *plan.Node, idxdef *plan.IndexDef) (storeTbl, metaTbl string, ok bool) {
	if scanNode == nil || scanNode.TableDef == nil || idxdef == nil {
		return "", "", false
	}
	for _, idx := range scanNode.TableDef.Indexes {
		if idx == nil || idx.IndexName != idxdef.IndexName {
			continue
		}
		switch idx.IndexAlgoTableType {
		case catalog.FullText2Index_TblType_Storage:
			storeTbl = idx.IndexTableName
		case catalog.FullText2Index_TblType_Metadata:
			metaTbl = idx.IndexTableName
		}
	}
	return storeTbl, metaTbl, storeTbl != "" && metaTbl != ""
}
