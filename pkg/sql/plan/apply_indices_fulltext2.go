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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	fulltext2engine "github.com/matrixorigin/matrixone/pkg/fulltext2"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func fulltext2ConjunctiveCandidateLimitEligible(match *plan.Expr, idxdef *plan.IndexDef) bool {
	if match == nil || idxdef == nil || !catalog.IsFullText2IndexAlgo(idxdef.IndexAlgo) {
		return false
	}
	fn := match.GetF()
	if fn == nil || len(fn.Args) < 2 {
		return false
	}
	pattern, mode := fn.Args[0].GetLit(), fn.Args[1].GetLit()
	if pattern == nil || pattern.Isnull || mode == nil || mode.Isnull ||
		mode.GetI64Val() != int64(tree.FULLTEXT_BOOLEAN) {
		return false
	}
	eligible, err := fulltext2engine.IsConjunctiveTermQuery(
		[]byte(pattern.GetSval()),
		fulltext2ParserFromParams(idxdef.IndexAlgoParams),
	)
	return err == nil && eligible
}

// fulltext2PeelablePkColName returns the pk column name ONLY when the pk's type is one the
// fulltext2 in-index predicate evaluator can actually compare — the integer family (incl.
// BIT) and varchar/char, i.e. exactly isComparableIncludeType's set. For any other pk type
// (uuid, decimal, date, datetime, ...) it returns "", so buildFilterPredicateJSON leaves a
// pk predicate RESIDUAL on the base scan instead of peeling one the TVF would reject at
// runtime ("col -1 type not comparable"). This keeps the planner peel and the executor eval
// in agreement (the peel resolves the pk sentinel by name with no type gate of its own).
func fulltext2PeelablePkColName(scanNode *plan.Node) string {
	if scanNode.TableDef == nil || scanNode.TableDef.Pkey == nil {
		return ""
	}
	pkColName := scanNode.TableDef.Pkey.PkeyColName
	if pkColName == "" {
		return ""
	}
	for _, c := range scanNode.TableDef.Cols {
		if c.Name != pkColName {
			continue
		}
		switch types.T(c.Typ.Id) {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64,
			types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64,
			types.T_bit, types.T_varchar, types.T_char:
			return pkColName
		}
		return ""
	}
	return ""
}

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

	cfgMap := map[string]any{
		"db":       scanNode.ObjRef.SchemaName,
		"index":    storeTbl,
		"metadata": metaTbl,
	}
	if parser := fulltext2ParserFromParams(idxdef.IndexAlgoParams); parser != "" {
		cfgMap["parser"] = parser
	}
	// Carry the INCLUDE column NAMES so the engine can map a covering query's requested
	// columns (by name) to each result's positional Include values (search_cache
	// fillIncludeResult). Harmless for a non-covering query (the TVF requests nothing).
	if incCols := indexDefIncludedColumnsBestEffort(idxdef); len(incCols) > 0 {
		cfgMap["include_columns"] = incCols
	}
	cfgBytes, err := json.Marshal(cfgMap)
	if err != nil {
		return "", err
	}
	return string(cfgBytes), nil
}

// ft2SearchBaseColDefs returns the fulltext2_search TVF's built-in output coldefs: the same
// (pk, relevance) shape and types as classic fulltext's ftIndexColdefs, but RENAMED to the
// reserved __mo_ft_* aliases. The covered path emits INCLUDE columns as sibling outputs and
// the runtime (fulltext2_search.go start()) classifies the output batch BY NAME, so the pk/
// score outputs must use names no user INCLUDE column can equal — otherwise an INCLUDE column
// named "doc_id"/"score" collides and is misrouted (silent wrong output / shuffle panic).
// This is also what decouples fulltext2 from classic fulltext's coldef list (it only borrowed
// the shape). Kept as a copy-then-rename so the classic types (T_any pk / T_float32 score)
// stay the single source of truth.
func ft2SearchBaseColDefs() []*plan.ColDef {
	cds := DeepCopyColDefList(ftIndexColdefs)
	cds[0].Name = catalog.FullText2Search_OutCol_DocId
	cds[1].Name = catalog.FullText2Search_OutCol_Score
	return cds
}

// buildFulltext2SearchNode builds the fulltext2_search FUNCTION_SCAN node for a MATCH
// resolved to a fulltext2 index. exprs are the TVF args [cfg(const), pattern, mode];
// pattern is passed as an expression, so a prepared-statement '?' parameter is bound
// and evaluated at execution time (the TVF reads it per row). The node emits the reserved
// (__mo_ft_doc_id, __mo_ft_score) shape, so the downstream join/sort binds by position.
func (builder *QueryBuilder) buildFulltext2SearchNode(ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	colDefs := ft2SearchBaseColDefs()
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

// buildFulltext2SearchNodeCovered builds the fulltext2_search FUNCTION_SCAN node for the
// COVERED fast path: its output coldefs are [doc_id, score] PLUS one column per INCLUDE
// column (at positions 2+), so a fully-covered projection reads pk/score/include straight
// from the TVF with NO base-table JOIN. includeCols are the INCLUDE column NAMES in the
// SAME order as the cfg's include_columns (= the order decodeInclude returns), so output
// col 2+j maps to the j-th include value. Each include coldef takes the base column's real
// type (looked up from scanNode.TableDef.Cols by name). Everything else matches the plain
// buildFulltext2SearchNode (used by the JOIN path).
func (builder *QueryBuilder) buildFulltext2SearchNodeCovered(ctx *BindContext, exprs []*plan.Expr, children []int32, scanNode *plan.Node, includeCols []string) (int32, error) {
	colDefs := ft2SearchBaseColDefs()
	for _, name := range includeCols {
		colTyp, ok := baseColTypeByName(scanNode, name)
		if !ok {
			return -1, moerr.NewInternalErrorf(builder.GetContext(),
				"fulltext2 covered path: INCLUDE column %q not found on base table %q", name, scanNode.TableDef.Name)
		}
		colDefs = append(colDefs, &plan.ColDef{Name: name, Typ: colTyp})
	}
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

// baseColTypeByName returns the plan type of the named column on scanNode's base table
// (case-insensitive), and whether it was found.
func baseColTypeByName(scanNode *plan.Node, name string) (plan.Type, bool) {
	if scanNode == nil || scanNode.TableDef == nil {
		return plan.Type{}, false
	}
	for _, c := range scanNode.TableDef.Cols {
		if strings.EqualFold(c.Name, name) {
			return c.Typ, true
		}
	}
	return plan.Type{}, false
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
