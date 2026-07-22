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

func TestIvfflatAlterTableCloneBehavior(t *testing.T) {
	b := CatalogHooks{}.AlterTableCloneBehavior()

	// All three hidden tables must be DELETE'd before clone — the
	// temp table's CREATE INDEX already seeded each with a row
	// (version=0 metadata, an initial centroid, bootstrapped entries),
	// and the clone copies source rows on top, so the seed has to go
	// first or every hidden table ends up duplicated.
	require.True(t, b.ContainsDelete(catalog.SystemSI_IVFFLAT_TblType_Metadata))
	require.True(t, b.ContainsDelete(catalog.SystemSI_IVFFLAT_TblType_Centroids))
	require.True(t, b.ContainsDelete(catalog.SystemSI_IVFFLAT_TblType_Entries))
	require.False(t, b.ContainsDelete("unknown_table_type"))

	// Only entries is skipped when the index is async — CDC rebuilds
	// entries from ts=0 on the new table. Metadata + centroids still
	// have to be cloned so the sinker has a k-means model to write
	// against.
	require.False(t, b.ContainsSkipWhenAsync(catalog.SystemSI_IVFFLAT_TblType_Metadata))
	require.False(t, b.ContainsSkipWhenAsync(catalog.SystemSI_IVFFLAT_TblType_Centroids))
	require.True(t, b.ContainsSkipWhenAsync(catalog.SystemSI_IVFFLAT_TblType_Entries))
	require.False(t, b.ContainsSkipWhenAsync("unknown_table_type"))
	// IVF-FLAT must NOT skip the whole index on async clone: its metadata +
	// centroids are cloned (only entries are CDC-rebuilt), via the per-hidden-
	// table policy above. This is the bug the explicit flag fixes — inferring
	// the skip from UsesCDC would drop the cloned k-means model.
	require.False(t, b.SkipWholeIndex)

	// RestoreBehavior deletes all three hidden tables before the clone re-supplies
	// them (CreateTable seeds them non-empty and the block-level clone appends).
	require.Equal(t, CatalogHooks{}.HiddenTableTypes(),
		CatalogHooks{}.RestoreBehavior().DeleteBeforeClone)
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

func TestIvfflatParamsFromTree_Quantization(t *testing.T) {
	for _, q := range []string{"int8", "uint8", "float16", "bf16", "float32", "INT8", "Bf16", "UINT8"} {
		idx := &tree.Index{IndexOption: &tree.IndexOption{Quantization: q}}
		got, err := CatalogHooks{}.ParamsFromTree(idx)
		require.NoErrorf(t, err, "quantization %q", q)
		require.Equalf(t, catalog.ToLower(q), got[catalog.Quantization], "stored quantization %q", q)
	}
	// omitted -> not present in params
	idx := &tree.Index{IndexOption: &tree.IndexOption{}}
	got, err := CatalogHooks{}.ParamsFromTree(idx)
	require.NoError(t, err)
	_, present := got[catalog.Quantization]
	require.False(t, present)
}

func TestIvfflatParamsFromTree_InvalidQuantization(t *testing.T) {
	for _, q := range []string{"float64", "f16", "garbage"} {
		idx := &tree.Index{IndexOption: &tree.IndexOption{Quantization: q}}
		_, err := CatalogHooks{}.ParamsFromTree(idx)
		require.Errorf(t, err, "quantization %q should be rejected", q)
		require.Contains(t, err.Error(), "unsupported quantization")
	}
}

// TestIvfflatValidQuantization exercises the (quant, op) catalog hook. The affine
// int8/uint8 scalar quantizer only preserves L2 geometry, so those quantizations
// are gated to op_type 'vector_l2_ops'; the lossless-ordering f32/f16/bf16 casts
// keep every op type.
func TestIvfflatValidQuantization(t *testing.T) {
	h := CatalogHooks{}
	// empty quantization is always allowed
	require.NoError(t, h.ValidQuantization("", metric.OpType_InnerProduct))

	// f32/f16/bf16 keep every op type
	for _, q := range []string{"float32", "float16", "bf16"} {
		for _, op := range []string{metric.OpType_L2Distance, metric.OpType_InnerProduct, metric.OpType_CosineDistance} {
			require.NoErrorf(t, h.ValidQuantization(q, op), "quant=%s op=%s", q, op)
		}
	}

	// int8/uint8 are valid only with L2 (case-insensitive); IP/cosine rejected
	for _, q := range []string{"int8", "uint8", "INT8", "UInt8"} {
		require.NoErrorf(t, h.ValidQuantization(q, metric.OpType_L2Distance), "quant=%s L2", q)
		err := h.ValidQuantization(q, metric.OpType_InnerProduct)
		require.Errorf(t, err, "quant=%s IP must be rejected", q)
		require.Contains(t, err.Error(), "only supported with L2")
		require.Errorf(t, h.ValidQuantization(q, metric.OpType_CosineDistance), "quant=%s cosine must be rejected", q)
	}

	// unknown quantization is rejected regardless of op
	require.Error(t, h.ValidQuantization("float64", metric.OpType_L2Distance))
	require.Error(t, h.ValidQuantization("garbage", metric.OpType_L2Distance))

	// L1 must be rejected for int8/uint8. The gate used to be a DENYLIST naming
	// only IP/cosine, so L1 was accepted and search then divided the quantized
	// score by QuantMul^2 — the squared-L2 inverse. L1 scales linearly, so its
	// inverse is 1/QuantMul: ordering stayed monotonic (hiding the bug) while
	// returned distances were off by a factor of QuantMul.
	for _, q := range []string{"int8", "uint8"} {
		err := h.ValidQuantization(q, metric.OpType_L1Distance)
		require.Errorf(t, err, "quant=%s L1 must be rejected", q)
		require.Contains(t, err.Error(), "only supported with L2")
	}

	// L2sq is the other metric whose inverse IS QuantMul^2, so it stays allowed.
	for _, q := range []string{"int8", "uint8"} {
		require.NoErrorf(t, h.ValidQuantization(q, metric.OpType_L2sqDistance), "quant=%s L2sq", q)
	}

	// An op_type this gate has never seen must be REJECTED, not silently given
	// the L2 rescale. This is what makes it an allowlist: adding a metric to
	// pkg/vectorindex/metric without working out its inverse scale fails here
	// rather than returning wrong distances.
	for _, q := range []string{"int8", "uint8"} {
		require.Errorf(t, h.ValidQuantization(q, "vector_future_ops"),
			"quant=%s unknown op must be rejected", q)
	}
}
