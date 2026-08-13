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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestFulltext2ParserFromParams(t *testing.T) {
	require.Equal(t, "", fulltext2ParserFromParams(""))
	require.Equal(t, "ngram", fulltext2ParserFromParams(`{"parser":"ngram"}`))
	require.Equal(t, "gojieba", fulltext2ParserFromParams(`{"parser":"gojieba","position_free":"true"}`))
	require.Equal(t, "", fulltext2ParserFromParams(`{no json`)) // malformed → default
	require.Equal(t, "", fulltext2ParserFromParams(`{"other":"x"}`))
}

func TestFulltext2PositionFreeFromParams(t *testing.T) {
	require.False(t, fulltext2PositionFreeFromParams(""))
	require.True(t, fulltext2PositionFreeFromParams(`{"position_free":"true"}`))
	require.False(t, fulltext2PositionFreeFromParams(`{"position_free":"false"}`))
	require.False(t, fulltext2PositionFreeFromParams(`{bad json`))
	require.False(t, fulltext2PositionFreeFromParams(`{"parser":"ngram"}`))
}

func ft2ScanNode(indexName, storeTbl, metaTbl string) *plan.Node {
	return &plan.Node{
		ObjRef: &plan.ObjectRef{SchemaName: "db"},
		TableDef: &plan.TableDef{
			Indexes: []*plan.IndexDef{
				{IndexName: indexName, IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: storeTbl},
				{IndexName: indexName, IndexAlgoTableType: catalog.FullText2Index_TblType_Metadata, IndexTableName: metaTbl},
			},
		},
	}
}

func TestFindFulltext2IndexTables(t *testing.T) {
	b := &QueryBuilder{}
	node := ft2ScanNode("ft2idx", "__store", "__meta")
	idxdef := &plan.IndexDef{IndexName: "ft2idx"}

	store, meta, ok := b.findFulltext2IndexTables(node, idxdef)
	require.True(t, ok)
	require.Equal(t, "__store", store)
	require.Equal(t, "__meta", meta)

	// nil guards.
	_, _, ok = b.findFulltext2IndexTables(nil, idxdef)
	require.False(t, ok)
	_, _, ok = b.findFulltext2IndexTables(node, nil)
	require.False(t, ok)

	// only the storage sibling present → incomplete → ok=false.
	partial := &plan.Node{TableDef: &plan.TableDef{Indexes: []*plan.IndexDef{
		{IndexName: "ft2idx", IndexAlgoTableType: catalog.FullText2Index_TblType_Storage, IndexTableName: "__store"},
	}}}
	_, _, ok = b.findFulltext2IndexTables(partial, idxdef)
	require.False(t, ok)
}

func TestBuildFulltext2SearchCfg(t *testing.T) {
	b := NewQueryBuilder(plan.Query_SELECT, NewMockCompilerContext(true), false, true)
	node := ft2ScanNode("ft2idx", "__store", "__meta")

	// positional index, NL mode → cfg JSON with parser.
	idxdef := &plan.IndexDef{IndexName: "ft2idx", IndexAlgoParams: `{"parser":"ngram"}`}
	cfg, err := b.buildFulltext2SearchCfg(node, idxdef, int64(tree.FULLTEXT_NL))
	require.NoError(t, err)
	var m map[string]string
	require.NoError(t, json.Unmarshal([]byte(cfg), &m))
	require.Equal(t, "db", m["db"])
	require.Equal(t, "__store", m["index"])
	require.Equal(t, "__meta", m["metadata"])
	require.Equal(t, "ngram", m["parser"])

	// tables not found → error.
	_, err = b.buildFulltext2SearchCfg(node, &plan.IndexDef{IndexName: "missing"}, int64(tree.FULLTEXT_NL))
	require.ErrorContains(t, err, "not found")

	// POSITION_FREE index + non-BM25 mode → rejected.
	pf := &plan.IndexDef{IndexName: "ft2idx", IndexAlgoParams: `{"position_free":"true"}`}
	_, err = b.buildFulltext2SearchCfg(node, pf, int64(tree.FULLTEXT_NL))
	require.ErrorContains(t, err, "POSITION_FREE")

	// POSITION_FREE index + BM25 mode → allowed.
	cfg, err = b.buildFulltext2SearchCfg(node, pf, int64(tree.FULLTEXT_BM25))
	require.NoError(t, err)
	require.Contains(t, cfg, "__store")
}
