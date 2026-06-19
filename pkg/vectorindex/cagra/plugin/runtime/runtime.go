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

// Package runtime holds the CAGRA algorithm's catalog-side metadata.
// See pkg/vectorindex/ivfpq/plugin/runtime for the canonical template.
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

// actionCagraReindex mirrors idxcron.Action_*. Inlined here to avoid
// importing pkg/vectorindex/idxcron — that import would pull in
// pkg/indexplugin which transitively reaches us, creating a cycle.
const actionCagraReindex = "cagra_reindex"

// Compile-time interface check.
var _ catalogplugin.Hooks = CatalogHooks{}

// CatalogHooks implements plugin/catalog.Hooks for CAGRA.
type CatalogHooks struct{}

func (CatalogHooks) HiddenTableTypes() []string {
	return []string{
		catalog.Cagra_TblType_Metadata,
		catalog.Cagra_TblType_Storage,
	}
}

// ShouldTruncateHiddenTable — CAGRA has no preserved-across-truncate
// state; both hidden tables are derived from source rows and must reset.
func (CatalogHooks) ShouldTruncateHiddenTable(_ string) bool { return true }

// AlterTableCloneBehavior — CAGRA leaves both hidden tables empty at
// CREATE-INDEX time. Mirrors HNSW.
func (CatalogHooks) AlterTableCloneBehavior() catalogplugin.AlterTableCloneBehavior {
	return catalogplugin.AlterTableCloneBehavior{SkipWholeIndex: true}
}

// RestoreBehavior — CAGRA's hidden tables (Storage tag=0 model blob + Metadata)
// are keyed by index_id, so the restore's block-level clone overwrites the
// CreateTable seed rather than appending — nothing needs delete-before-clone
// (empty DeleteBeforeClone). The model is rebuilt post-clone by the compile
// hook's RestoreInitSQL (ALTER … REINDEX … cagra FORCE_SYNC), run by the CDC's
// first iteration.
func (CatalogHooks) RestoreBehavior() catalogplugin.RestoreBehavior {
	return catalogplugin.RestoreBehavior{}
}

// BuildSessionVars are the environmental/perf vars captured into
// algo_params.session_vars at CREATE INDEX (cagra_* threads, lower_case for
// name resolution). CAGRA does NOT train k-means; its index-defining
// max_index_capacity rides a flat algo_params key written by ParamsFromTree
// only when explicitly set in CREATE INDEX. The experimental flag is NOT
// captured: the background reindex (ProcessInitSQL, IsFrontend=false) skips the
// experimental gate, so its create-time value is never consulted.
func (CatalogHooks) BuildSessionVars() []string {
	return []string{
		"cagra_threads_build",
		"lower_case_table_names",
	}
}

func (CatalogHooks) DefaultOptions() map[string]string {
	return map[string]string{
		catalog.IndexAlgoParamOpType: metric.OpType_L2Distance,
		catalog.Quantization:         metric.Quantization_F32_Str,
		catalog.DistributionMode:     vectorindex.DistributionMode_SINGLE_GPU_Str,
	}
}

// CagraIndexFlag is the experimental-feature flag gating CAGRA DDL.
// Single source of truth; both the catalog gate (pkg/sql/compile/util.go
// via ExperimentalFlag) and the per-plugin HandleCreateIndex gate
// reference this constant.
const CagraIndexFlag = "experimental_cagra_index"

// ExperimentalFlag: CAGRA DDL is gated by CagraIndexFlag.
func (CatalogHooks) ExperimentalFlag() string { return CagraIndexFlag }

// SupportedVectorTypes: CAGRA (cuvs) accepts f32 and f16 base columns. f16 is
// stored natively as half, or downcast-quantized to int8/uint8 via QUANTIZATION.
// int8/uint8 base columns are unsupported (the CDC overflow brute force is f32/f16-only).
func (CatalogHooks) SupportedVectorTypes() []types.T {
	return []types.T{types.T_array_float32, types.T_array_float16}
}

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

// SyncDescriptor: CAGRA is always async via ISCP CDC (event-level
// deltas) AND participates in idxcron (periodic model rebuild). The
// initial cagra_create build runs via the CDC pipeline's InitSQL on
// first iteration; subsequent source-table mutations stream in as
// tag=1 events (see pkg/vectorindex/cagra/sync.go). When the user
// sets auto_update=true, the idxcron task fires on the configured
// day/hour cadence to refresh tag=0.
//
// IdxcronListsAware=false: CAGRA has no nlist / training-sample
// concept, so the executor's checkIndexUpdatable skips the IVF-FLAT
// heuristic and just enforces "lastUpdateAt + interval < now".
func (CatalogHooks) SyncDescriptor() catalogplugin.SyncDescriptor {
	return catalogplugin.SyncDescriptor{
		UsesCDC:           true,
		SinkerType:        catalogplugin.SinkerType_IndexSync,
		IdxcronAction:     actionCagraReindex,
		IdxcronAlgoToken:  "CAGRA",
		IdxcronListsAware: false,
	}
}

// AlwaysAsync — CAGRA is unconditionally async (maintained via CDC).
func (CatalogHooks) AlwaysAsync(string) bool { return true }

// DefaultMaxIndexCapacity mirrors the cagra_max_index_capacity session-var
// default; the build path (cagra_create) uses it when the flat algo_params key
// is absent (a legacy index). 0 means "auto-detect from source row count".
const DefaultMaxIndexCapacity = int64(0)

// ParamsFromTree is lifted verbatim from catalog.indexParamsToMap's
// INDEX_TYPE_CAGRA case (pkg/catalog/secondary_index_utils.go:376-433).
func (CatalogHooks) ParamsFromTree(idx *tree.Index) (map[string]string, error) {
	res := make(map[string]string)

	if idx.IndexOption.IntermediateGraphDegree < 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid intermediate_graph_degree. cagra.intermediate_graph_degree must be > 0")
	}
	if idx.IndexOption.GraphDegree < 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid graph_degree. cagra.graph_degree must be > 0")
	}
	if idx.IndexOption.ITopkSize < 0 {
		return nil, moerr.NewInternalErrorNoCtx("invalid itopk_size. cagra.itopk_size must be > 0")
	}

	if idx.IndexOption.IntermediateGraphDegree > 0 {
		res[catalog.IntermediateGraphDegree] = strconv.FormatInt(idx.IndexOption.IntermediateGraphDegree, 10)
	}
	if idx.IndexOption.GraphDegree > 0 {
		res[catalog.GraphDegree] = strconv.FormatInt(idx.IndexOption.GraphDegree, 10)
	}
	if idx.IndexOption.ITopkSize > 0 {
		res[catalog.ITopkSize] = strconv.FormatInt(idx.IndexOption.ITopkSize, 10)
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

	// Idxcron cadence knobs — read fresh by the executor on every cron
	// tick (executor.go:443+), so users can ALTER these later without
	// re-registering the task. auto_update=false (default) leaves the
	// cron firing as a no-op skip.
	if idx.IndexOption.AutoUpdate {
		res[catalog.AutoUpdate] = "true"
	}
	if idx.IndexOption.Day > 0 {
		res[catalog.Day] = strconv.FormatInt(idx.IndexOption.Day, 10)
	}
	if idx.IndexOption.Hour > 0 {
		res[catalog.Hour] = strconv.FormatInt(idx.IndexOption.Hour, 10)
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

	if idx.IndexOption.MaxIndexCapacity > 0 {
		res[catalog.IndexAlgoParamMaxIndexCapacity] = strconv.FormatInt(idx.IndexOption.MaxIndexCapacity, 10)
	}
	return res, nil
}

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
