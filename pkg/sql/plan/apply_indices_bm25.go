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

// This file holds the bm25-specific pieces of the MATCH/BM25 query rewrite. bm25 has
// its OWN query verb — BM25(col) AGAINST('query') — which binds to the distinct
// bm25_match function (NOT fulltext_match), so the planner disambiguates it from a
// classic MATCH by function name. The shared join/sort/limit-pushdown machinery lives
// in apply_indices_fulltext.go and drives BOTH: its collectors resolve bm25_match via
// findMatchBm25Index (here) alongside fulltext_match, and its one join loop dispatches
// the per-match TVF by index algo — buildBm25SearchTableFunc (here) for bm25,
// fulltext_index_scan for classic — into a single join with one combined score sort.
// So a query mixing BM25() and MATCH() is served by one pass. This file therefore only
// contains what is genuinely bm25-specific: the bm25_search TVF builder, its hidden-
// table resolver, and the bm25 index finder.
package plan

import (
	"encoding/json"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const bm25_search_func_name = "bm25_search"

// buildBm25SearchTableFunc builds the bm25_search TVF AST for a BM25(...) match
// resolved to a bm25 ranked-retrieval index. Args: [param="", cfg{db,index,metadata}
// JSON, pattern]. The TVF tokenizes the pattern as bag-of-words and answers a BM25
// top-K walk, emitting (doc_id, score) — the same shape fulltext_index_scan emits, so
// the downstream join/projection/limit is unchanged.
func (builder *QueryBuilder) buildBm25SearchTableFunc(scanNode *plan.Node, idxdef *plan.IndexDef, pattern string, aliasName string) (*tree.AliasedTableExpr, error) {
	// bm25 is a position-free bag-of-words BM25 index with a single, mode-free query
	// surface: BM25(col) AGAINST('query') always answers ranked bag-of-words top-K.
	// (Boolean / phrase / query-expansion — which need term positions — are simply
	// not expressible via BM25(); use a classic fulltext MATCH index for those.)
	storeTbl, metaTbl, ok := builder.findBm25IndexTables(scanNode, idxdef)
	if !ok {
		return nil, moerr.NewInternalErrorf(builder.GetContext(),
			"bm25 index %q: storage/metadata tables not found (index may be partially materialized); reindex required",
			idxdef.IndexName)
	}

	// json.Marshal so a schema name containing a double-quote/backslash is escaped
	// (the search side sonic.Unmarshal's it).
	cfgBytes, err := json.Marshal(map[string]string{
		"db":       scanNode.ObjRef.SchemaName,
		"index":    storeTbl,
		"metadata": metaTbl,
	})
	if err != nil {
		return nil, err
	}
	cfg := string(cfgBytes)

	bm25Func := tree.NewCStr(bm25_search_func_name, 1)
	var exprs tree.Exprs
	exprs = append(exprs, tree.NewNumVal[string]("", "", false, tree.P_char))
	exprs = append(exprs, tree.NewNumVal[string](cfg, cfg, false, tree.P_char))
	exprs = append(exprs, tree.NewNumVal[string](pattern, pattern, false, tree.P_char))
	name := tree.NewUnresolvedName(bm25Func)

	return &tree.AliasedTableExpr{
		Expr: &tree.TableFunction{
			Func: &tree.FuncExpr{
				Func:     tree.FuncName2ResolvableFunctionReference(name),
				FuncName: bm25Func,
				Exprs:    exprs,
				Type:     tree.FUNC_TYPE_TABLE,
			},
		},
		As: tree.AliasClause{Alias: tree.Identifier(aliasName)},
	}, nil
}

// findBm25IndexTables resolves the storage + metadata hidden tables of a bm25
// index — the two sibling defs sharing the storage def's IndexName. ok is false
// if either is missing (partial/restored catalog).
func (builder *QueryBuilder) findBm25IndexTables(scanNode *plan.Node, idxdef *plan.IndexDef) (storeTbl string, metaTbl string, ok bool) {
	if scanNode == nil || scanNode.TableDef == nil || idxdef == nil {
		return "", "", false
	}
	for _, idx := range scanNode.TableDef.Indexes {
		if idx == nil || idx.IndexName != idxdef.IndexName {
			continue
		}
		switch idx.IndexAlgoTableType {
		case catalog.Bm25Index_TblType_Storage:
			storeTbl = idx.IndexTableName
		case catalog.Bm25Index_TblType_Metadata:
			metaTbl = idx.IndexTableName
		}
	}
	return storeTbl, metaTbl, storeTbl != "" && metaTbl != ""
}

// findMatchBm25Index resolves a bm25_match(...) function — the bound form of
// BM25(col) AGAINST('q') — to a bm25 storage index def on the scan's columns.
// Because BM25() binds to its own function (never fulltext_match), the function name
// alone disambiguates it from a classic MATCH; no mode gating is needed (BM25 has no
// modes) and a coexisting classic fulltext index on the same column is irrelevant here.
func (builder *QueryBuilder) findMatchBm25Index(fn *plan.Function, scanNode *plan.Node) *plan.IndexDef {
	if fn == nil || scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return nil
	}
	if len(fn.Args) < 3 || fn.Args[0].GetLit() == nil || fn.Args[1].GetLit() == nil {
		return nil
	}
	if scanNode.TableDef.Pkey == nil || scanNode.TableDef.Pkey.PkeyColName == "" {
		return nil
	}

	scanTag := scanNode.BindingTags[0]
	argColNames := make([]string, 0, len(fn.Args)-2)
	for j := 2; j < len(fn.Args); j++ {
		col := fn.Args[j].GetCol()
		if col == nil || col.RelPos != scanTag {
			return nil
		}

		colName := col.Name
		if colName == "" {
			if col.ColPos < 0 || int(col.ColPos) >= len(scanNode.TableDef.Cols) {
				return nil
			}
			colName = scanNode.TableDef.Cols[col.ColPos].Name
		}
		argColNames = append(argColNames, colName)
	}

	nargs := len(fn.Args) - 2
	for _, idx := range scanNode.TableDef.Indexes {
		if idx == nil || !idx.TableExist {
			continue
		}
		// Match the bm25 storage def as the single representative — the metadata
		// sibling is resolved later in buildBm25SearchTableFunc.
		if idx.GetIndexAlgo() != catalog.MoIndexBm25Algo.ToString() ||
			idx.IndexAlgoTableType != catalog.Bm25Index_TblType_Storage {
			continue
		}
		if len(idx.Parts) != nargs {
			continue
		}

		nfound := 0
		for _, p := range idx.Parts {
			partName := catalog.ResolveAlias(p)
			for _, colName := range argColNames {
				if strings.EqualFold(partName, colName) || strings.EqualFold(p, colName) {
					// found
					nfound++
					break
				}
			}
		}

		if nfound == nargs && nfound == len(idx.Parts) {
			return idx
		}
	}
	return nil
}
