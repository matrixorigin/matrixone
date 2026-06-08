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

// Package runtime holds the IVF-PQ algorithm's catalog-side metadata:
// hidden-table types, parameter schema, op-type set, default options.
//
// What lives here (and what doesn't):
//   - Algorithm-metadata constants and table-driven facts (op_type set,
//     supported quantizations, default param values).
//   - The implementation of plugin/catalog.Hooks.
//   - Pure CREATE-INDEX param parsing (no QueryBuilder / *Scope state).
//
// Anything that drives a SQL pipeline stage — CREATE/ALTER/DROP execution,
// plan-tree construction, query rewriting — belongs in compile/ or plan/
// next door, not here. The rule is: lifted code follows the pkg/sql/<layer>
// it came from; runtime/ is only for facts that aren't tied to a layer.
//
// Lifted from the IVF-PQ case of catalog.indexParamsToMap
// (pkg/catalog/secondary_index_utils.go:434-477).
package runtime

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// actionIvfpqReindex mirrors idxcron.Action_*. Inlined here to avoid
// importing pkg/vectorindex/idxcron (would create a cycle).
const actionIvfpqReindex = "ivfpq_reindex"

// Compile-time interface check.
var _ catalogplugin.Hooks = CatalogHooks{}

// CatalogHooks implements plugin/catalog.Hooks for IVF-PQ.
//
// All four methods are required by the framework — see
// pkg/indexplugin/catalog/hooks.go for the contract. The compile-
// time interface check below catches missing methods.
type CatalogHooks struct{}

// HiddenTableTypes lists the IndexAlgoTableType strings this algorithm
// uses for its hidden tables. The order does not matter; downstream code
// keys into the map by name. IVF-PQ needs two: metadata + storage.
// IVF-FLAT (for contrast) returns three: metadata, centroids, entries.
func (CatalogHooks) HiddenTableTypes() []string {
	return []string{
		catalog.Ivfpq_TblType_Metadata,
		catalog.Ivfpq_TblType_Storage,
	}
}

// ShouldTruncateHiddenTable — IVF-PQ has no preserved-across-truncate
// state; both hidden tables are derived from source rows and must reset.
func (CatalogHooks) ShouldTruncateHiddenTable(_ string) bool { return true }

// AlterTableCloneBehavior — IVF-PQ leaves both hidden tables empty at
// CREATE-INDEX time. Mirrors HNSW.
func (CatalogHooks) AlterTableCloneBehavior() catalogplugin.AlterTableCloneBehavior {
	return catalogplugin.AlterTableCloneBehavior{SkipWholeIndex: true}
}

// RestoreBehavior — IVF-PQ's prebuilt model lives in the Storage (tag=0 blob) +
// Metadata hidden tables; a future RestoreDirectly={Storage,Metadata} would let
// restore load that model instead of rebuilding via async CDC. Returns the zero
// value today: restore rebuilds via CDC like normal DML.
func (CatalogHooks) RestoreBehavior() catalogplugin.RestoreBehavior {
	return catalogplugin.RestoreBehavior{}
}

// DefaultOptions is the params map produced when CREATE INDEX is issued
// without a WITH(...) clause. Return nil if your algorithm requires
// explicit options. Keys come from pkg/catalog (IndexAlgoParamOpType etc.).
func (CatalogHooks) DefaultOptions() map[string]string {
	return map[string]string{
		catalog.IndexAlgoParamOpType: metric.OpType_L2Distance,
		catalog.Quantization:         metric.Quantization_F32_Str,
		catalog.DistributionMode:     vectorindex.DistributionMode_SINGLE_GPU_Str,
	}
}

// IvfpqIndexFlag is the experimental-feature flag gating IVF-PQ DDL.
// Single source of truth; both the catalog gate (pkg/sql/compile/util.go
// via ExperimentalFlag) and the per-plugin HandleCreateIndex gate
// reference this constant.
const IvfpqIndexFlag = "experimental_ivfpq_index"

// ExperimentalFlag: IVF-PQ DDL is gated by IvfpqIndexFlag.
func (CatalogHooks) ExperimentalFlag() string { return IvfpqIndexFlag }

// SupportedOpTypes maps the SQL-visible op_type strings (e.g.
// "vector_l2_ops") to a stable internal identifier. Used by plan-side
// op_type validation when matching an ORDER BY distance function against
// the index's declared op_type.
// SupportedVectorTypes: IVF-PQ (cuvs) indexes f32 vectors only.
func (CatalogHooks) SupportedVectorTypes() []types.T { return []types.T{types.T_array_float32} }

// SupportedPrimaryKeyTypes: requires an int64 primary key.
func (CatalogHooks) SupportedPrimaryKeyTypes() []types.T { return []types.T{types.T_int64} }

// SupportedIncludeColumnTypes: cuvs INCLUDE (pre-filter) columns accept
// int32/int64/float32/float64 scalars.
func (CatalogHooks) SupportedIncludeColumnTypes() []types.T {
	return []types.T{types.T_int32, types.T_int64, types.T_float32, types.T_float64}
}

func (CatalogHooks) SupportedOpTypes() map[string]string {
	out := make(map[string]string, len(metric.OpTypeToUsearchMetric))
	for k, v := range metric.OpTypeToUsearchMetric {
		out[k] = fmt.Sprint(v)
	}
	return out
}

// SyncDescriptor: IVF-PQ mirrors CAGRA — always async via ISCP CDC
// (event-level deltas) AND participates in idxcron (periodic model
// rebuild). See pkg/vectorindex/cagra/plugin/runtime/runtime.go's
// SyncDescriptor for the full rationale.
func (CatalogHooks) SyncDescriptor() catalogplugin.SyncDescriptor {
	return catalogplugin.SyncDescriptor{
		UsesCDC:           true,
		SinkerType:        catalogplugin.SinkerType_IndexSync,
		AlwaysAsync:       true,
		IdxcronAction:     actionIvfpqReindex,
		IdxcronAlgoToken:  "IVFPQ",
		IdxcronListsAware: false,
	}
}

// ParamsFromTree is lifted verbatim from catalog.indexParamsToMap's
// INDEX_TYPE_IVFPQ case (pkg/catalog/secondary_index_utils.go:434-477).
func (CatalogHooks) ParamsFromTree(idx *tree.Index) (map[string]string, error) {
	res := make(map[string]string)

	if idx.IndexOption.AlgoParamList <= 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid lists. lists must be > 0 for IVFPQ")
	}
	res[catalog.IndexAlgoParamLists] = strconv.FormatInt(idx.IndexOption.AlgoParamList, 10)
	if idx.IndexOption.AlgoParamM > 0 {
		res[catalog.HnswM] = strconv.FormatInt(idx.IndexOption.AlgoParamM, 10)
	}
	if idx.IndexOption.BitsPerCode > 0 {
		res[catalog.BitsPerCode] = strconv.FormatInt(idx.IndexOption.BitsPerCode, 10)
	}

	if len(idx.IndexOption.AlgoParamVectorOpType) > 0 {
		opType := catalog.ToLower(idx.IndexOption.AlgoParamVectorOpType)
		if _, ok := metric.OpTypeToUsearchMetric[opType]; !ok {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid op_type. '%s'", opType))
		}
		res[catalog.IndexAlgoParamOpType] = idx.IndexOption.AlgoParamVectorOpType
	} else {
		res[catalog.IndexAlgoParamOpType] = metric.OpType_L2Distance
	}

	if idx.IndexOption.Async {
		res[catalog.Async] = "true"
	}

	if len(idx.IndexOption.Quantization) > 0 {
		quantize := catalog.ToLower(idx.IndexOption.Quantization)
		if !metric.ValidQuantization(quantize) {
			return nil, moerr.NewInternalErrorNoCtx("invalid quantization. quantization is invalid. f32, f16, int8, uint8")
		}
		res[catalog.Quantization] = quantize
	} else {
		res[catalog.Quantization] = metric.Quantization_F32_Str
	}

	if len(idx.IndexOption.DistributionMode) > 0 {
		mode := catalog.ToLower(idx.IndexOption.DistributionMode)
		if !vectorindex.ValidDistributionMode(mode) {
			return nil, moerr.NewInternalErrorNoCtx("invalid distribution_mode. distribution_mode is invalid. single, sharded, replicated")
		}
		res[catalog.DistributionMode] = mode
	} else {
		res[catalog.DistributionMode] = vectorindex.DistributionMode_SINGLE_GPU_Str
	}

	if joined := joinIncludeColumns(idx.IndexOption.IncludeColumns); len(joined) > 0 {
		res[catalog.IncludedColumns] = joined
	}

	// Idxcron cadence knobs — see CAGRA's runtime.go for the rationale.
	if idx.IndexOption.AutoUpdate {
		res[catalog.AutoUpdate] = "true"
	}
	if idx.IndexOption.Day > 0 {
		res[catalog.Day] = strconv.FormatInt(idx.IndexOption.Day, 10)
	}
	if idx.IndexOption.Hour > 0 {
		res[catalog.Hour] = strconv.FormatInt(idx.IndexOption.Hour, 10)
	}

	return res, nil
}

// joinIncludeColumns mirrors catalog.joinIncludeColumns
// (pkg/catalog/secondary_index_utils.go:275) which is unexported.
func joinIncludeColumns(cols []*tree.UnresolvedName) string {
	if len(cols) == 0 {
		return ""
	}
	names := make([]string, 0, len(cols))
	for _, c := range cols {
		name := c.ColName()
		if name == "" {
			continue
		}
		names = append(names, name)
	}
	return strings.Join(names, ",")
}
