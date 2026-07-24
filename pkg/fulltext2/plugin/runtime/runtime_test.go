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
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/stretchr/testify/require"
)

// TestCatalogHooks exercises every static accessor on CatalogHooks so the
// fulltext2 runtime metadata contract is pinned (storage+metadata hidden-table
// layout, always-async CDC maintenance, experimental gating, no vector/op/PK
// constraints, and the *tree.Index-parse NotSupported guard).
func TestCatalogHooks(t *testing.T) {
	// Interface compliance is asserted in the source; re-assign here too.
	var _ catalogplugin.Hooks = CatalogHooks{}
	h := CatalogHooks{}

	// HiddenTableTypes — [Storage, Metadata].
	require.Equal(t, []string{
		catalog.FullText2Index_TblType_Storage,
		catalog.FullText2Index_TblType_Metadata,
	}, h.HiddenTableTypes())

	// ShouldTruncateHiddenTable — always true for any input.
	require.True(t, h.ShouldTruncateHiddenTable(""))
	require.True(t, h.ShouldTruncateHiddenTable(catalog.FullText2Index_TblType_Storage))
	require.True(t, h.ShouldTruncateHiddenTable("anything"))

	// AlterTableCloneBehavior — whole index skipped on clone.
	require.True(t, h.AlterTableCloneBehavior().SkipWholeIndex)

	// RestoreBehavior — zero value (no delete-before-clone).
	require.Equal(t, catalogplugin.RestoreBehavior{}, h.RestoreBehavior())

	// Nil / empty accessors.
	require.Nil(t, h.BuildSessionVars())
	require.Nil(t, h.DefaultOptions())
	require.Nil(t, h.SupportedVectorTypes())
	require.Nil(t, h.SupportedOpTypes())
	require.Nil(t, h.SupportedIncludeColumnTypes())
	require.Nil(t, h.SupportedPrimaryKeyTypes())

	// ExperimentalFlag — the experimental_fulltext2_index gate.
	require.Equal(t, Fulltext2IndexFlag, h.ExperimentalFlag())
	require.Equal(t, "experimental_fulltext2_index", h.ExperimentalFlag())

	// ValidQuantization — no quantization, never errors.
	require.NoError(t, h.ValidQuantization("", ""))
	require.NoError(t, h.ValidQuantization("foo", "bar"))

	// ParamsFromTree — always NotSupported (fulltext2 parses *tree.FullTextIndex).
	params, err := h.ParamsFromTree(nil)
	require.Nil(t, params)
	require.Error(t, err)

	// SyncDescriptor — always-async CDC + idxcron MERGE.
	sd := h.SyncDescriptor()
	require.True(t, sd.UsesCDC)
	require.True(t, sd.AlwaysAsync)
	require.Equal(t, catalogplugin.SinkerType_IndexSync, sd.SinkerType)
	require.Equal(t, "fulltext2_reindex", sd.IdxcronAction)
	require.Equal(t, "FULLTEXT2", sd.IdxcronAlgoToken)
	require.Equal(t, "MERGE", sd.IdxcronReindexOption)
}
