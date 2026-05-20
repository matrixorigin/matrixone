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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/stretchr/testify/require"
)

func TestIvfpqHiddenTableTypes(t *testing.T) {
	got := CatalogHooks{}.HiddenTableTypes()
	require.Len(t, got, 2)
	require.Contains(t, got, catalog.Ivfpq_TblType_Metadata)
	require.Contains(t, got, catalog.Ivfpq_TblType_Storage)
}

func TestIvfpqShouldTruncateHiddenTable(t *testing.T) {
	h := CatalogHooks{}
	require.True(t, h.ShouldTruncateHiddenTable(catalog.Ivfpq_TblType_Metadata))
	require.True(t, h.ShouldTruncateHiddenTable(catalog.Ivfpq_TblType_Storage))
	require.True(t, h.ShouldTruncateHiddenTable("anything"))
}

func TestIvfpqAlterTableCloneBehavior(t *testing.T) {
	// IVF-PQ returns the zero value — hidden tables are empty at
	// CREATE-INDEX time and async-skip happens at the index level.
	b := CatalogHooks{}.AlterTableCloneBehavior()
	require.Empty(t, b.DeleteBeforeClone)
	require.Empty(t, b.SkipWhenAsync)
	require.False(t, b.ContainsDelete(catalog.Ivfpq_TblType_Metadata))
	require.False(t, b.ContainsDelete(catalog.Ivfpq_TblType_Storage))
	require.False(t, b.ContainsSkipWhenAsync(catalog.Ivfpq_TblType_Metadata))
	require.False(t, b.ContainsSkipWhenAsync(catalog.Ivfpq_TblType_Storage))
}

func TestIvfpqDefaultOptions(t *testing.T) {
	got := CatalogHooks{}.DefaultOptions()
	require.Equal(t, metric.OpType_L2Distance, got[catalog.IndexAlgoParamOpType])
	require.Equal(t, metric.Quantization_F32_Str, got[catalog.Quantization])
	require.Equal(t, vectorindex.DistributionMode_SINGLE_GPU_Str, got[catalog.DistributionMode])
}

func TestIvfpqExperimentalFlag(t *testing.T) {
	require.Equal(t, "experimental_ivfpq_index", CatalogHooks{}.ExperimentalFlag())
	require.Equal(t, "experimental_ivfpq_index", IvfpqIndexFlag)
}

func TestIvfpqSupportedOpTypes(t *testing.T) {
	got := CatalogHooks{}.SupportedOpTypes()
	require.NotEmpty(t, got)
	for k := range metric.OpTypeToUsearchMetric {
		require.Contains(t, got, k)
	}
}

func TestIvfpqSyncDescriptor(t *testing.T) {
	// IVF-PQ: AlwaysAsync CDC + idxcron periodic rebuild
	// (Phase 4 / 3.9 wiring). Mirrors CAGRA.
	d := CatalogHooks{}.SyncDescriptor()
	require.True(t, d.UsesCDC)
	require.True(t, d.AlwaysAsync)
	require.Equal(t, catalogplugin.SinkerType_IndexSync, d.SinkerType)
	require.Equal(t, "ivfpq_reindex", d.IdxcronAction)
	require.Equal(t, "IVFPQ", d.IdxcronAlgoToken)
	require.False(t, d.IdxcronListsAware)
}

func TestIvfpqParamsFromTree_Defaults(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{}}
	got, err := CatalogHooks{}.ParamsFromTree(idx)
	require.NoError(t, err)
	// No list/m/bits, defaults applied for op_type/quantization/distribution_mode.
	require.NotContains(t, got, catalog.IndexAlgoParamLists)
	require.NotContains(t, got, catalog.HnswM)
	require.NotContains(t, got, catalog.BitsPerCode)
	require.Equal(t, metric.OpType_L2Distance, got[catalog.IndexAlgoParamOpType])
	require.Equal(t, metric.Quantization_F32_Str, got[catalog.Quantization])
	require.Equal(t, vectorindex.DistributionMode_SINGLE_GPU_Str, got[catalog.DistributionMode])
}

func TestIvfpqParamsFromTree_AllOptions(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{
		AlgoParamList:         128,
		AlgoParamM:            16,
		BitsPerCode:           8,
		AlgoParamVectorOpType: metric.OpType_CosineDistance,
		Quantization:          metric.Quantization_INT8_Str,
		DistributionMode:      vectorindex.DistributionMode_SINGLE_GPU_Str,
	}}
	got, err := CatalogHooks{}.ParamsFromTree(idx)
	require.NoError(t, err)
	require.Equal(t, "128", got[catalog.IndexAlgoParamLists])
	require.Equal(t, "16", got[catalog.HnswM])
	require.Equal(t, "8", got[catalog.BitsPerCode])
	require.Equal(t, metric.OpType_CosineDistance, got[catalog.IndexAlgoParamOpType])
	require.Equal(t, metric.Quantization_INT8_Str, got[catalog.Quantization])
}

func TestIvfpqParamsFromTree_InvalidOpType(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{
		AlgoParamVectorOpType: "not_a_real_op_type",
	}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid op_type")
}

func TestIvfpqParamsFromTree_InvalidQuantization(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{
		Quantization: "not_real",
	}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "quantization is invalid")
}

func TestIvfpqParamsFromTree_InvalidDistributionMode(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{
		DistributionMode: "not_real",
	}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "distribution_mode is invalid")
}

func TestIvfpqParamsFromTree_IncludeColumns(t *testing.T) {
	col1 := tree.NewUnresolvedColName("price")
	col2 := tree.NewUnresolvedColName("name")
	empty := tree.NewUnresolvedColName("")
	idx := &tree.Index{IndexOption: &tree.IndexOption{
		IncludeColumns: []*tree.UnresolvedName{col1, empty, col2},
	}}
	got, err := CatalogHooks{}.ParamsFromTree(idx)
	require.NoError(t, err)
	require.Equal(t, "price,name", got[catalog.IncludedColumns])
}

func TestIvfpqJoinIncludeColumns(t *testing.T) {
	require.Equal(t, "", joinIncludeColumns(nil))
	require.Equal(t, "", joinIncludeColumns([]*tree.UnresolvedName{}))
	cols := []*tree.UnresolvedName{
		tree.NewUnresolvedColName("a"),
		tree.NewUnresolvedColName(""),
		tree.NewUnresolvedColName("b"),
	}
	require.Equal(t, "a,b", joinIncludeColumns(cols))
}
