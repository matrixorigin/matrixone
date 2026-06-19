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
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func TestHnswHiddenTableTypes(t *testing.T) {
	got := CatalogHooks{}.HiddenTableTypes()
	require.Len(t, got, 2)
	require.Contains(t, got, catalog.Hnsw_TblType_Metadata)
	require.Contains(t, got, catalog.Hnsw_TblType_Storage)
}

func TestHnswShouldTruncateHiddenTable(t *testing.T) {
	require.True(t, CatalogHooks{}.ShouldTruncateHiddenTable("anything"))
}

func TestHnswAlterTableCloneBehavior(t *testing.T) {
	// HNSW returns the zero value — its hidden tables are empty at
	// CREATE-INDEX time (data lands via sync CROSS APPLY hnsw_create
	// or async CDC), and the whole-index async-skip happens at the
	// SyncDescriptor level (AlwaysAsync=true).
	b := CatalogHooks{}.AlterTableCloneBehavior()
	require.Empty(t, b.DeleteBeforeClone)
	require.Empty(t, b.SkipWhenAsync)
	require.False(t, b.ContainsDelete(catalog.Hnsw_TblType_Metadata))
	require.False(t, b.ContainsDelete(catalog.Hnsw_TblType_Storage))
	require.False(t, b.ContainsSkipWhenAsync(catalog.Hnsw_TblType_Metadata))
	require.False(t, b.ContainsSkipWhenAsync(catalog.Hnsw_TblType_Storage))
	// HNSW is AlwaysAsync and rebuilds via CDC, so the whole index is skipped
	// on async clone (not per hidden table).
	require.True(t, b.SkipWholeIndex)

	// RestoreBehavior is the zero value today — restore rebuilds the index, no
	// hidden table is restored directly.
	require.Empty(t, CatalogHooks{}.RestoreBehavior().DeleteBeforeClone)
}

func TestHnswDefaultOptions(t *testing.T) {
	got := CatalogHooks{}.DefaultOptions()
	require.Equal(t, metric.OpType_L2Distance, got[catalog.IndexAlgoParamOpType])
}

func TestHnswExperimentalFlag(t *testing.T) {
	require.Equal(t, "experimental_hnsw_index", CatalogHooks{}.ExperimentalFlag())
	require.Equal(t, "experimental_hnsw_index", HnswIndexFlag)
}

func TestHnswSupportedOpTypes(t *testing.T) {
	got := CatalogHooks{}.SupportedOpTypes()
	require.NotEmpty(t, got)
	for k := range metric.OpTypeToUsearchMetric {
		require.Contains(t, got, k)
	}
}

func TestHnswSyncDescriptor(t *testing.T) {
	d := CatalogHooks{}.SyncDescriptor()
	require.True(t, d.UsesCDC)
	require.True(t, CatalogHooks{}.AlwaysAsync(""))
}

func TestHnswParamsFromTree_Defaults(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{}}
	got, err := CatalogHooks{}.ParamsFromTree(idx)
	require.NoError(t, err)
	require.Equal(t, metric.OpType_L2Distance, got[catalog.IndexAlgoParamOpType])
	require.NotContains(t, got, catalog.HnswM)
	require.NotContains(t, got, catalog.HnswEfConstruction)
}

func TestHnswParamsFromTree_AllOptions(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{
		AlgoParamM:            16,
		HnswEfConstruction:    200,
		HnswEfSearch:          64,
		AlgoParamVectorOpType: metric.OpType_CosineDistance,
		Async:                 true,
	}}
	got, err := CatalogHooks{}.ParamsFromTree(idx)
	require.NoError(t, err)
	require.Equal(t, "16", got[catalog.HnswM])
	require.Equal(t, "200", got[catalog.HnswEfConstruction])
	require.Equal(t, "64", got[catalog.HnswEfSearch])
	require.Equal(t, metric.OpType_CosineDistance, got[catalog.IndexAlgoParamOpType])
	require.Equal(t, "true", got[catalog.Async])
}

func TestHnswParamsFromTree_NegativeM(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{AlgoParamM: -1}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "M")
}

func TestHnswParamsFromTree_NegativeEfConstruction(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{HnswEfConstruction: -1}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ef_construction")
}

func TestHnswParamsFromTree_NegativeEfSearch(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{HnswEfSearch: -1}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ef_search")
}

func TestHnswParamsFromTree_InvalidOpType(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{AlgoParamVectorOpType: "not_real"}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid op_type")
}
