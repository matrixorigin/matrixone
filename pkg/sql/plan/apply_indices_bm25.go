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

package plan

import (
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const bm25_search_func_name = "bm25_search"

// buildBm25SearchTableFunc builds the bm25_search TVF AST for a MATCH resolved
// to a bm25 ranked-retrieval index. Args: [param="", cfg{db,index,metadata}
// JSON, pattern]. The TVF tokenizes the pattern as bag-of-words and answers a
// BM25 top-K walk, emitting (doc_id, score) — the same shape fulltext_index_scan
// emits, so the downstream join/projection/limit is unchanged.
func (builder *QueryBuilder) buildBm25SearchTableFunc(scanNode *plan.Node, idxdef *plan.IndexDef, pattern string, mode int64, aliasName string) (*tree.AliasedTableExpr, error) {
	// bm25 is a position-free bag-of-words BM25 index: it implements only ranked
	// retrieval (DEFAULT / NATURAL LANGUAGE / RETRIEVAL). Boolean (+/-/~/phrase)
	// and query-expansion need term positions bm25 does not store — reject them
	// with a clear message rather than silently returning bag-of-words results.
	switch mode {
	case int64(tree.FULLTEXT_DEFAULT), int64(tree.FULLTEXT_NL):
		// ranked retrieval — supported (both tokenize the pattern as bag-of-words)
	default:
		return nil, moerr.NewNotSupported(builder.GetContext(),
			"a bm25 index only supports ranked retrieval (default or natural language mode); boolean and query-expansion need a classic fulltext index")
	}

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
