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

package runtime

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestFullTextHiddenTableTypes(t *testing.T) {
	got := CatalogHooks{}.HiddenTableTypes()
	require.Len(t, got, 1)
	require.Equal(t, catalog.FullTextIndex_TblType, got[0])
}

func TestFullTextShouldTruncateHiddenTable(t *testing.T) {
	require.True(t, CatalogHooks{}.ShouldTruncateHiddenTable(catalog.FullTextIndex_TblType))
	require.True(t, CatalogHooks{}.ShouldTruncateHiddenTable("anything"))
}

func TestFullTextAlterTableCloneBehavior(t *testing.T) {
	// Fulltext returns the zero value — its single hidden table is
	// empty at CREATE-INDEX time and async-skip happens at the index
	// level via SyncDescriptor.
	b := CatalogHooks{}.AlterTableCloneBehavior()
	require.Empty(t, b.DeleteBeforeClone)
	require.Empty(t, b.SkipWhenAsync)
	require.False(t, b.ContainsDelete(catalog.FullTextIndex_TblType))
	require.False(t, b.ContainsSkipWhenAsync(catalog.FullTextIndex_TblType))
	// fulltext rebuilds its hidden table via CDC when async, so the whole index
	// is skipped on async clone (not per hidden table).
	require.True(t, b.SkipWholeIndex)

	// RestoreBehavior deletes the fulltext hidden table before the clone
	// re-supplies it (CreateTable populates it inline for a sync index and the
	// block-level clone appends).
	require.Equal(t, CatalogHooks{}.HiddenTableTypes(),
		CatalogHooks{}.RestoreBehavior().DeleteBeforeClone)
}

func TestFullTextDefaultOptions(t *testing.T) {
	require.Nil(t, CatalogHooks{}.DefaultOptions())
}

func TestFullTextExperimentalFlag(t *testing.T) {
	require.Equal(t, "", CatalogHooks{}.ExperimentalFlag())
}

func TestFullTextSupportedOpTypes(t *testing.T) {
	require.Nil(t, CatalogHooks{}.SupportedOpTypes())
}

func TestFullTextSyncDescriptor(t *testing.T) {
	d := CatalogHooks{}.SyncDescriptor()
	require.True(t, d.UsesCDC)
	require.Equal(t, int8(0), d.SinkerType) // SinkerType_IndexSync == 0
	require.Equal(t, "", d.IdxcronAction)   // no scheduled rebuild
}

func TestFullTextAlwaysAsync(t *testing.T) {
	h := CatalogHooks{}
	// Only the retrieval parser is unconditionally async (WAND binary index
	// maintained via CDC); ngram/default/empty stay sync (param-gated).
	require.True(t, h.AlwaysAsync(`{"parser":"retrieval"}`))
	require.False(t, h.AlwaysAsync(`{"parser":"ngram"}`))
	require.False(t, h.AlwaysAsync(`{"parser":"default"}`))
	require.False(t, h.AlwaysAsync(`{}`))
	require.False(t, h.AlwaysAsync(""))
	// async-ness of a non-retrieval index still comes from the `async` param,
	// not AlwaysAsync.
	require.False(t, h.AlwaysAsync(`{"parser":"ngram","async":"true"}`))
}

func TestFullTextParamsFromTree_Rejected(t *testing.T) {
	// Fulltext parses to *tree.FullTextIndex, not *tree.Index, so the
	// generic ParamsFromTree hook is unreachable; it returns an error.
	_, err := CatalogHooks{}.ParamsFromTree(&tree.Index{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "FullTextIndex")
}
