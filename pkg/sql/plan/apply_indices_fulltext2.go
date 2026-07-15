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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const fulltext2_search_func_name = "fulltext2_search"

// buildFulltext2SearchTableFunc builds the fulltext2_search TVF AST for a MATCH
// resolved to a fulltext2 index. Args: [param="", cfg{db,index,metadata} JSON,
// pattern, mode]. The TVF loads the index segments, runs the WAND positional
// query (NL phrase or boolean per mode), and emits (doc_id, score).
func (builder *QueryBuilder) buildFulltext2SearchTableFunc(scanNode *plan.Node, idxdef *plan.IndexDef, pattern string, mode int64, aliasName string) (*tree.AliasedTableExpr, error) {
	storeTbl, metaTbl, ok := builder.findFulltext2IndexTables(scanNode, idxdef)
	if !ok {
		return nil, moerr.NewInternalErrorf(builder.GetContext(),
			"fulltext2 index %q: storage/metadata tables not found (index may be partially materialized); reindex required",
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
		return nil, err
	}
	cfg := string(cfgBytes)

	ftFunc := tree.NewCStr(fulltext2_search_func_name, 1)
	var exprs tree.Exprs
	exprs = append(exprs, tree.NewNumVal[string]("", "", false, tree.P_char))
	exprs = append(exprs, tree.NewNumVal[string](cfg, cfg, false, tree.P_char))
	exprs = append(exprs, tree.NewNumVal[string](pattern, pattern, false, tree.P_char))
	exprs = append(exprs, tree.NewNumVal[int64](mode, strconv.FormatInt(mode, 10), false, tree.P_int64))
	name := tree.NewUnresolvedName(ftFunc)

	return &tree.AliasedTableExpr{
		Expr: &tree.TableFunction{
			Func: &tree.FuncExpr{
				Func:     tree.FuncName2ResolvableFunctionReference(name),
				FuncName: ftFunc,
				Exprs:    exprs,
				Type:     tree.FUNC_TYPE_TABLE,
			},
		},
		As: tree.AliasClause{Alias: tree.Identifier(aliasName)},
	}, nil
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
