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

func TestCagraHiddenTableTypes(t *testing.T) {
	got := CatalogHooks{}.HiddenTableTypes()
	require.Len(t, got, 2)
	require.Contains(t, got, catalog.Cagra_TblType_Metadata)
	require.Contains(t, got, catalog.Cagra_TblType_Storage)
}

func TestCagraShouldTruncateHiddenTable(t *testing.T) {
	require.True(t, CatalogHooks{}.ShouldTruncateHiddenTable("anything"))
}

func TestCagraAlterTableCloneBehavior(t *testing.T) {
	// CAGRA returns the zero value — hidden tables are empty at
	// CREATE-INDEX time and async-skip happens at the index level.
	b := CatalogHooks{}.AlterTableCloneBehavior()
	require.Empty(t, b.DeleteBeforeClone)
	require.Empty(t, b.SkipWhenAsync)
	require.False(t, b.ContainsDelete(catalog.Cagra_TblType_Metadata))
	require.False(t, b.ContainsDelete(catalog.Cagra_TblType_Storage))
	require.False(t, b.ContainsSkipWhenAsync(catalog.Cagra_TblType_Metadata))
	require.False(t, b.ContainsSkipWhenAsync(catalog.Cagra_TblType_Storage))
}

func TestCagraDefaultOptions(t *testing.T) {
	got := CatalogHooks{}.DefaultOptions()
	require.Equal(t, metric.OpType_L2Distance, got[catalog.IndexAlgoParamOpType])
	require.Equal(t, metric.Quantization_F32_Str, got[catalog.Quantization])
	require.Equal(t, vectorindex.DistributionMode_SINGLE_GPU_Str, got[catalog.DistributionMode])
}

func TestCagraExperimentalFlag(t *testing.T) {
	require.Equal(t, "experimental_cagra_index", CatalogHooks{}.ExperimentalFlag())
	require.Equal(t, "experimental_cagra_index", CagraIndexFlag)
}

func TestCagraSupportedOpTypes(t *testing.T) {
	got := CatalogHooks{}.SupportedOpTypes()
	require.NotEmpty(t, got)
	for k := range metric.OpTypeToUsearchMetric {
		require.Contains(t, got, k)
	}
}

func TestCagraSyncDescriptor(t *testing.T) {
	// CAGRA: AlwaysAsync CDC + idxcron periodic rebuild
	// (Phase 3.7 / 3.9 wiring).
	d := CatalogHooks{}.SyncDescriptor()
	require.True(t, d.UsesCDC)
	require.True(t, d.AlwaysAsync)
	require.Equal(t, catalogplugin.SinkerType_IndexSync, d.SinkerType)
	require.Equal(t, "cagra_reindex", d.IdxcronAction)
	require.Equal(t, "CAGRA", d.IdxcronAlgoToken)
	require.False(t, d.IdxcronListsAware, "cuvs has no nlist heuristic")
}

func TestCagraParamsFromTree_Defaults(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{}}
	got, err := CatalogHooks{}.ParamsFromTree(idx)
	require.NoError(t, err)
	require.Equal(t, metric.OpType_L2Distance, got[catalog.IndexAlgoParamOpType])
	require.Equal(t, metric.Quantization_F32_Str, got[catalog.Quantization])
	require.Equal(t, vectorindex.DistributionMode_SINGLE_GPU_Str, got[catalog.DistributionMode])
}

func TestCagraParamsFromTree_AllOptions(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{
		IntermediateGraphDegree: 128,
		GraphDegree:             64,
		ITopkSize:               32,
		AlgoParamVectorOpType:   metric.OpType_CosineDistance,
		Quantization:            metric.Quantization_INT8_Str,
		Async:                   true,
	}}
	got, err := CatalogHooks{}.ParamsFromTree(idx)
	require.NoError(t, err)
	require.Equal(t, "128", got[catalog.IntermediateGraphDegree])
	require.Equal(t, "64", got[catalog.GraphDegree])
	require.Equal(t, "32", got[catalog.ITopkSize])
	require.Equal(t, metric.OpType_CosineDistance, got[catalog.IndexAlgoParamOpType])
	require.Equal(t, metric.Quantization_INT8_Str, got[catalog.Quantization])
	require.Equal(t, "true", got[catalog.Async])
}

func TestCagraParamsFromTree_NegativeIntermediate(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{IntermediateGraphDegree: -1}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "intermediate_graph_degree")
}

func TestCagraParamsFromTree_NegativeGraphDegree(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{GraphDegree: -1}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "graph_degree")
}

func TestCagraParamsFromTree_NegativeITopkSize(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{ITopkSize: -1}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "itopk_size")
}

func TestCagraParamsFromTree_InvalidOpType(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{AlgoParamVectorOpType: "not_real"}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid op_type")
}

func TestCagraParamsFromTree_InvalidQuantization(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{Quantization: "not_real"}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "quantization is invalid")
}

func TestCagraParamsFromTree_InvalidDistributionMode(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{DistributionMode: "not_real"}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "distribution_mode is invalid")
}

func TestCagraParamsFromTree_IncludeColumns(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{
		IncludeColumns: []*tree.UnresolvedName{
			tree.NewUnresolvedColName("price"),
			tree.NewUnresolvedColName("name"),
		},
	}}
	got, err := CatalogHooks{}.ParamsFromTree(idx)
	require.NoError(t, err)
	require.Equal(t, "price,name", got[catalog.IncludedColumns])
}

func TestCagraJoinIncludeColumns(t *testing.T) {
	require.Equal(t, "", joinIncludeColumns(nil))
	require.Equal(t, "", joinIncludeColumns([]*tree.UnresolvedName{}))
	cols := []*tree.UnresolvedName{
		tree.NewUnresolvedColName("a"),
		tree.NewUnresolvedColName(""),
		tree.NewUnresolvedColName("b"),
	}
	require.Equal(t, "a,b", joinIncludeColumns(cols))
}
