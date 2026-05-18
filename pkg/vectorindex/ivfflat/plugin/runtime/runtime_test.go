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

func TestIvfflatHiddenTableTypes(t *testing.T) {
	got := CatalogHooks{}.HiddenTableTypes()
	require.Len(t, got, 3)
	require.Contains(t, got, catalog.SystemSI_IVFFLAT_TblType_Metadata)
	require.Contains(t, got, catalog.SystemSI_IVFFLAT_TblType_Centroids)
	require.Contains(t, got, catalog.SystemSI_IVFFLAT_TblType_Entries)
}

func TestIvfflatShouldTruncateHiddenTable(t *testing.T) {
	h := CatalogHooks{}
	require.True(t, h.ShouldTruncateHiddenTable(catalog.SystemSI_IVFFLAT_TblType_Entries))
	require.False(t, h.ShouldTruncateHiddenTable(catalog.SystemSI_IVFFLAT_TblType_Metadata))
	require.False(t, h.ShouldTruncateHiddenTable(catalog.SystemSI_IVFFLAT_TblType_Centroids))
}

func TestIvfflatDefaultOptions(t *testing.T) {
	got := CatalogHooks{}.DefaultOptions()
	require.Equal(t, "1", got[catalog.IndexAlgoParamLists])
	require.Equal(t, metric.OpType_L2Distance, got[catalog.IndexAlgoParamOpType])
}

func TestIvfflatExperimentalFlag(t *testing.T) {
	// IVF-FLAT is NOT gated.
	require.Equal(t, "", CatalogHooks{}.ExperimentalFlag())
}

func TestIvfflatSupportedOpTypes(t *testing.T) {
	got := CatalogHooks{}.SupportedOpTypes()
	require.NotEmpty(t, got)
	for k := range metric.OpTypeToIvfMetric {
		require.Contains(t, got, k)
	}
}

func TestIvfflatSyncDescriptor(t *testing.T) {
	d := CatalogHooks{}.SyncDescriptor()
	require.True(t, d.UsesCDC)
	require.False(t, d.AlwaysAsync)
	require.Equal(t, actionIvfflatReindex, d.IdxcronAction)
	require.Equal(t, "ivf_threads_search", d.IdxcronFrontendProbeVar)
}

func TestIvfflatParamsFromTree_DefaultsListsOmitted(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{}}
	got, err := CatalogHooks{}.ParamsFromTree(idx)
	require.NoError(t, err)
	require.Equal(t, "1", got[catalog.IndexAlgoParamLists])
	require.Equal(t, metric.OpType_L2Distance, got[catalog.IndexAlgoParamOpType])
}

func TestIvfflatParamsFromTree_AllOptions(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{
		AlgoParamList:         32,
		AlgoParamVectorOpType: metric.OpType_CosineDistance,
		Async:                 true,
		AutoUpdate:            true,
		Day:                   3,
		Hour:                  2,
	}}
	got, err := CatalogHooks{}.ParamsFromTree(idx)
	require.NoError(t, err)
	require.Equal(t, "32", got[catalog.IndexAlgoParamLists])
	require.Equal(t, metric.OpType_CosineDistance, got[catalog.IndexAlgoParamOpType])
	require.Equal(t, "true", got[catalog.Async])
	require.Equal(t, "true", got[catalog.AutoUpdate])
	require.Equal(t, "3", got[catalog.Day])
	require.Equal(t, "2", got[catalog.Hour])
}

func TestIvfflatParamsFromTree_NegativeList(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{AlgoParamList: -1}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "list")
}

func TestIvfflatParamsFromTree_InvalidOpType(t *testing.T) {
	idx := &tree.Index{IndexOption: &tree.IndexOption{AlgoParamVectorOpType: "not_real"}}
	_, err := CatalogHooks{}.ParamsFromTree(idx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid op_type")
}
