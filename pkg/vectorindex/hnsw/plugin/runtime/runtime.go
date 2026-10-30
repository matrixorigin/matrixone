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

// Package runtime holds the HNSW algorithm's catalog-side metadata.
// See pkg/vectorindex/ivfpq/plugin/runtime for the canonical template.
package runtime

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
)

var _ catalogplugin.Hooks = CatalogHooks{}

type CatalogHooks struct{}

func (CatalogHooks) HiddenTableTypes() []string {
	return []string{
		catalog.Hnsw_TblType_Metadata,
		catalog.Hnsw_TblType_Storage,
		catalog.Hnsw_TblType_Dict,
		catalog.IndexIgnored_TblType,
	}
}

// ShouldTruncateHiddenTable — HNSW has no preserved-across-truncate
// state; both hidden tables are derived from source rows and must reset.
func (CatalogHooks) ShouldTruncateHiddenTable(_ string) bool { return true }

// AlterTableCloneBehavior — HNSW leaves both hidden tables empty at
// CREATE-INDEX time (data lands later via sync CROSS APPLY hnsw_create
// or async CDC), so no DELETE before clone is needed. The "skip clone
// when async" decision happens at the whole-index level
// (SyncDescriptor) rather than per table, so SkipWhenAsync stays empty
// here.
func (CatalogHooks) AlterTableCloneBehavior() catalogplugin.AlterTableCloneBehavior {
	return catalogplugin.AlterTableCloneBehavior{SkipWholeIndex: true}
}

// RestoreBehavior — HNSW's hidden tables (usearch Storage + Metadata) are keyed,
// so the restore's block-level clone overwrites the CreateTable seed rather than
// appending — nothing needs delete-before-clone (empty DeleteBeforeClone). The
// model is rebuilt post-clone by the compile hook's RestoreInitSQL (ALTER …
// REINDEX … hnsw FORCE_SYNC), run by the CDC's first iteration.
func (CatalogHooks) RestoreBehavior() catalogplugin.RestoreBehavior {
	return catalogplugin.RestoreBehavior{}
}

// BuildSessionVars returns nil — HNSW captures no session vars into
// algo_params, keeping its algo_params byte-compatible with pre-session_vars
// indexes. The index-defining max_index_capacity rides a flat algo_params key
// written by ParamsFromTree only when explicitly set in CREATE INDEX.
func (CatalogHooks) BuildSessionVars() []string {
	return nil
}

// HnswIndexFlag is the experimental-feature flag gating HNSW DDL. Single
// source of truth; both the catalog gate (pkg/sql/compile/util.go via
// ExperimentalFlag) and the per-plugin HandleCreateIndex gate reference
// this constant.
const HnswIndexFlag = "experimental_hnsw_index"

// ExperimentalFlag: HNSW DDL is gated by HnswIndexFlag.
func (CatalogHooks) ExperimentalFlag() string { return HnswIndexFlag }

// SupportedVectorTypes: HNSW (usearch) indexes f32, f64, f16, bf16, int8 and uint8 vectors.
func (CatalogHooks) SupportedVectorTypes() []types.T {
	return []types.T{types.T_array_float32, types.T_array_float64, types.T_array_float16,
		types.T_array_bf16, types.T_array_int8, types.T_array_uint8}
}

func (CatalogHooks) IsVectorIndex() bool { return true }

// SupportedPrimaryKeyTypes: requires an int64 primary key.
func (CatalogHooks) SupportedPrimaryKeyTypes() []types.T { return []types.T{types.T_int64} }

// ValidQuantization gates the (quantization, op_type) pair for HNSW: the value must be
// an element type usearch stores (float32, float16, bf16, float64, int8, uint8), and
// int8/uint8 are L2-only: their trained affine map q(x)=x*mul+add preserves L2 ordering
// (the offset cancels in a difference) but not inner product or cosine. quant=="" =>
// the base column type (valid); op=="" => value rule only.
func (CatalogHooks) ValidQuantization(quant, op string) error {
	switch strings.ToLower(quant) {
	case "", metric.Quantization_F32_Str, metric.Quantization_F16_Str, metric.Quantization_BF16_Str,
		metric.Quantization_F64_Str:
		return nil
	case metric.Quantization_INT8_Str, metric.Quantization_UINT8_Str:
		switch strings.ToLower(op) {
		case metric.OpType_InnerProduct, metric.OpType_CosineDistance:
			return moerr.NewNotSupportedNoCtxf(
				"hnsw quantization %q is only supported with L2 (op_type 'vector_l2_ops'); the int8/uint8 affine quantizer does not preserve inner-product / cosine geometry", quant)
		}
		return nil
	default:
		return moerr.NewNotSupportedNoCtxf(
			"hnsw quantization %q (supported: float32, float16, bf16, float64, int8, uint8)", quant)
	}
}

// NeedsDict reports whether an index of this QUANTIZATION has a dict table holding the
// range it maps vectors with.
func NeedsDict(quant string) bool {
	switch strings.ToLower(quant) {
	case metric.Quantization_INT8_Str, metric.Quantization_UINT8_Str:
		return true
	}
	return false
}

// CheckQuantizationBase rejects a QUANTIZATION wider than the base column element type.
func CheckQuantizationBase(quant string, base types.T) error {
	if strings.ToLower(quant) == metric.Quantization_F64_Str {
		if base != types.T_array_float64 {
			return moerr.NewNotSupportedNoCtxf(
				"HNSW QUANTIZATION '%s' (8 bytes/element) cannot upcast base column %s (%d bytes/element); use a quantization of equal or smaller width, or omit it to keep the base type",
				quant, base.String(), types.Type{Oid: base}.GetArrayElementSize())
		}
		return nil
	}
	return quantizer.CheckNoUpcast("HNSW", quant, base)
}

// SupportedIncludeColumnTypes: this index has no INCLUDE-column support.
func (CatalogHooks) SupportedIncludeColumnTypes() []types.T { return nil }

func (CatalogHooks) SupportedOpTypes() map[string]string {
	out := make(map[string]string, len(metric.OpTypeToUsearchMetric))
	for k, v := range metric.OpTypeToUsearchMetric {
		out[k] = fmt.Sprint(v)
	}
	return out
}

// SyncDescriptor: HNSW is always async via ISCP CDC (matches the legacy
// hardcoded behaviour in pkg/sql/compile/iscp_util.go:68-71). No idxcron
// task.
func (CatalogHooks) SyncDescriptor() catalogplugin.SyncDescriptor {
	return catalogplugin.SyncDescriptor{
		UsesCDC:     true,
		SinkerType:  catalogplugin.SinkerType_IndexSync,
		AlwaysAsync: true,
	}
}

// DefaultMaxIndexCapacity is the hnsw_max_index_capacity session-var default: 0 sizes
// each model from the host memory budget.
const DefaultMaxIndexCapacity = int64(0)

// ParamsFromTree is lifted verbatim from catalog.indexParamsToMap's
// INDEX_TYPE_HNSW case (pkg/catalog/secondary_index_utils.go:341-375).
func (CatalogHooks) ParamsFromTree(idx *tree.Index) (map[string]string, error) {
	res := make(map[string]string)

	if idx.IndexOption.AlgoParamM < 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid M. hnsw.M must be > 0")
	}
	if idx.IndexOption.HnswEfConstruction < 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid ef_construction. hnsw.ef_construction must be > 0")
	}
	if idx.IndexOption.HnswEfSearch < 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid ef_search. hnsw.ef_search must be > 0")
	}

	if idx.IndexOption.AlgoParamM > 0 {
		res[catalog.HnswM] = strconv.FormatInt(idx.IndexOption.AlgoParamM, 10)
	}
	if idx.IndexOption.HnswEfConstruction > 0 {
		res[catalog.HnswEfConstruction] = strconv.FormatInt(idx.IndexOption.HnswEfConstruction, 10)
	}
	if idx.IndexOption.HnswEfSearch > 0 {
		res[catalog.HnswEfSearch] = strconv.FormatInt(idx.IndexOption.HnswEfSearch, 10)
	}

	if len(idx.IndexOption.AlgoParamVectorOpType) > 0 {
		opType := catalog.ToLower(idx.IndexOption.AlgoParamVectorOpType)
		if _, ok := metric.OpTypeToUsearchMetric[opType]; !ok {
			return nil, moerr.NewInvalidInputNoCtxf("invalid op_type. '%s'", opType)
		}
		res[catalog.IndexAlgoParamOpType] = idx.IndexOption.AlgoParamVectorOpType
	} else {
		res[catalog.IndexAlgoParamOpType] = metric.OpType_L2Distance
	}

	if idx.IndexOption.Async {
		res[catalog.Async] = "true"
	}

	if idx.IndexOption.MaxIndexCapacity > 0 {
		res[catalog.IndexAlgoParamMaxIndexCapacity] = strconv.FormatInt(idx.IndexOption.MaxIndexCapacity, 10)
	}

	if idx.IndexOption.MaxBadRecordsSet {
		res[catalog.IndexAlgoParamMaxBadRecords] = strconv.FormatInt(idx.IndexOption.MaxBadRecords, 10)
	}

	if len(idx.IndexOption.Quantization) > 0 {
		quantization := catalog.ToLower(idx.IndexOption.Quantization)
		if err := (CatalogHooks{}).ValidQuantization(quantization, res[catalog.IndexAlgoParamOpType]); err != nil {
			return nil, err
		}
		res[catalog.Quantization] = quantization
	}
	return res, nil
}
