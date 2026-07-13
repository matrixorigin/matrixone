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

// CPU-side unit coverage for the bm25 catalog hooks (ParamsFromTree, SyncDescriptor,
// ValidQuantization, HiddenTableTypes). These pure functions are otherwise only
// exercised by the integration BVTs, which do not feed Go per-package coverage.
package runtime

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestParamsFromTree_DefaultParser(t *testing.T) {
	res, err := CatalogHooks{}.ParamsFromTree(&tree.Index{IndexOption: &tree.IndexOption{}})
	require.NoError(t, err)
	require.Equal(t, DefaultParser, res["parser"])
}

func TestParamsFromTree_ExplicitAndFlags(t *testing.T) {
	res, err := CatalogHooks{}.ParamsFromTree(&tree.Index{IndexOption: &tree.IndexOption{
		ParserName:       "default",
		Async:            true,
		AutoUpdate:       true,
		Day:              3,
		Hour:             4,
		Second:           30,
		MaxIndexCapacity: 100000,
	}})
	require.NoError(t, err)
	require.Equal(t, "default", res["parser"])
	require.Equal(t, "true", res[catalog.Async])
	require.Equal(t, "true", res[catalog.AutoUpdate])
	require.Equal(t, "3", res[catalog.Day])
	require.Equal(t, "4", res[catalog.Hour])
	require.Equal(t, "30", res[catalog.Second])
	require.Equal(t, "100000", res[catalog.IndexAlgoParamMaxIndexCapacity])
}

func TestParamsFromTree_UnsupportedParser(t *testing.T) {
	// bm25 only tokenizes with jieba, so only gojieba/default are accepted; ngram (and
	// any other name) is rejected rather than silently tokenized as jieba.
	for _, p := range []string{"bogus", "ngram"} {
		_, err := CatalogHooks{}.ParamsFromTree(&tree.Index{IndexOption: &tree.IndexOption{ParserName: p}})
		require.Error(t, err, "parser %q must be rejected", p)
	}
}

func TestExperimentalFlag(t *testing.T) {
	require.Equal(t, "experimental_bm25_index", CatalogHooks{}.ExperimentalFlag())
	require.Equal(t, Bm25IndexFlag, CatalogHooks{}.ExperimentalFlag())
}

func TestParamsFromTree_OmitsUnsetFlags(t *testing.T) {
	res, err := CatalogHooks{}.ParamsFromTree(&tree.Index{IndexOption: &tree.IndexOption{ParserName: "gojieba"}})
	require.NoError(t, err)
	// zero-valued cadence flags must not appear (so idxcron treats them as unset)
	require.NotContains(t, res, catalog.Async)
	require.NotContains(t, res, catalog.Day)
	require.NotContains(t, res, catalog.Second)
	require.NotContains(t, res, catalog.IndexAlgoParamMaxIndexCapacity)
}

func TestValidQuantization(t *testing.T) {
	require.NoError(t, CatalogHooks{}.ValidQuantization("", ""))
	require.Error(t, CatalogHooks{}.ValidQuantization("int8", ""), "bm25 has no quantization")
}

func TestSyncDescriptor(t *testing.T) {
	d := CatalogHooks{}.SyncDescriptor()
	require.True(t, d.UsesCDC)
	require.True(t, d.AlwaysAsync)
	require.Equal(t, "MERGE", d.IdxcronReindexOption)
	require.Equal(t, "BM25", d.IdxcronAlgoToken)
	require.False(t, d.IdxcronListsAware)
}

func TestHiddenTableTypes(t *testing.T) {
	got := CatalogHooks{}.HiddenTableTypes()
	require.ElementsMatch(t, []string{
		catalog.Bm25Index_TblType_Storage,
		catalog.Bm25Index_TblType_Metadata,
	}, got)
}
