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

// Package runtime holds IVF-FLAT's catalog-side metadata: hidden-table
// types, parameter schema, op-type set, default options, sync descriptor.
// See pkg/vectorindex/ivfpq/plugin/runtime for the canonical template.
//
// Lifted from the IVF-FLAT case of catalog.indexParamsToMap
// (pkg/catalog/secondary_index_utils.go:304-340).
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
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/quantizer"
)

// Kept local so the plugin package stays independent of the cron executor,
// whose tests import the IVF-FLAT idxcron hook directly.
const actionIvfflatReindex = "ivfflat_reindex"

// Compile-time interface check.
var _ catalogplugin.Hooks = CatalogHooks{}

// CatalogHooks implements plugin/catalog.Hooks for IVF-FLAT.
type CatalogHooks struct{}

// HiddenTableTypes — IVF-FLAT uses three hidden tables (metadata,
// centroids, entries) vs two for HNSW/CAGRA/IVF-PQ. Order is irrelevant;
// downstream code keys into the map by name.
func (CatalogHooks) HiddenTableTypes() []string {
	return []string{
		catalog.SystemSI_IVFFLAT_TblType_Metadata,
		catalog.SystemSI_IVFFLAT_TblType_Centroids,
		catalog.SystemSI_IVFFLAT_TblType_Entries,
	}
}

// ShouldTruncateHiddenTable — only entries are reset; metadata and
// centroids preserve the k-means model across TRUNCATE so a subsequent
// ALTER REINDEX is cheap.
func (CatalogHooks) ShouldTruncateHiddenTable(algoTableType string) bool {
	return algoTableType == catalog.SystemSI_IVFFLAT_TblType_Entries
}

// AlterTableCloneBehavior — IVF-FLAT seeds all three hidden tables
// during CREATE INDEX on the temp table (a "version=0" metadata row,
// an initial centroid, and the bootstrapped entries), so the clone
// loop in cloneUnaffectedIndex must DELETE every target table before
// copying source rows or each hidden table ends up with the seed
// duplicated.
//
// Additionally, when the index is async its entries table is rebuilt
// from ts=0 by the ISCP CDC pipeline on the new table once the index
// re-registers. Cloning entries AND letting CDC rebuild them produces
// duplicates, so SkipWhenAsync names entries — but only entries:
// metadata + centroids still need to be cloned so the CDC sinker has
// a k-means model to write against.
func (CatalogHooks) AlterTableCloneBehavior() catalogplugin.AlterTableCloneBehavior {
	return catalogplugin.AlterTableCloneBehavior{
		DeleteBeforeClone: []string{
			catalog.SystemSI_IVFFLAT_TblType_Metadata,
			catalog.SystemSI_IVFFLAT_TblType_Centroids,
			catalog.SystemSI_IVFFLAT_TblType_Entries,
		},
		SkipWhenAsync: []string{
			catalog.SystemSI_IVFFLAT_TblType_Entries,
		},
	}
}

// RestoreBehavior — IVF-FLAT seeds all three hidden tables non-empty at
// CREATE-INDEX (a version=0 metadata row, an initial centroid, bootstrap
// entries), and the restore's block-level clone APPENDS. So every hidden table
// must be emptied with DELETE … WHERE TRUE before the clone re-supplies the
// snapshot's metadata/centroids/entries — DeleteBeforeClone is the full
// hidden-table set.
func (h CatalogHooks) RestoreBehavior() catalogplugin.RestoreBehavior {
	return catalogplugin.RestoreBehavior{DeleteBeforeClone: h.HiddenTableTypes()}
}

// BuildSessionVars returns nil — IVF-FLAT captures no session vars into
// algo_params, keeping its algo_params byte-compatible with pre-session_vars
// indexes. Index-defining knobs (k-means) ride flat algo_params keys written by
// ParamsFromTree only when explicitly set in CREATE INDEX.
func (CatalogHooks) BuildSessionVars() []string {
	return nil
}

// DefaultOptions mirrors the IVF-FLAT case of indexParamsToMap when the
// statement carries no WITH(...) clause: lists=1, op_type=l2.
func (CatalogHooks) DefaultOptions() map[string]string {
	return map[string]string{
		catalog.IndexAlgoParamLists:  "1",
		catalog.IndexAlgoParamOpType: metric.OpType_L2Distance,
	}
}

// ExperimentalFlag — IVF-FLAT is NOT gated. The legacy handler on main
// (Scope.handleVectorIvfFlatIndex) never checked `experimental_ivf_index`
// at DDL time, so returning "" here preserves that behavior: the gate at
// pkg/sql/compile/util.go skips when the flag is empty. The variable
// itself still exists and flows through IdxcronMetadata for downstream
// consumers.
func (CatalogHooks) ExperimentalFlag() string { return "" }

// SupportedOpTypes returns IVF-FLAT's metric registry. IVF uses a
// distinct metric table from HNSW/USearch (OpTypeToIvfMetric).
// SupportedVectorTypes: IVF-FLAT indexes all vector element types. Entries are
// stored in their own (narrow) type; centroids are f32 (decoupled). kmeans runs
// in f32, narrow distances go through the float32 bridge / narrow kernels.
func (CatalogHooks) SupportedVectorTypes() []types.T {
	return []types.T{
		types.T_array_float32, types.T_array_float64,
		types.T_array_bf16, types.T_array_float16, types.T_array_int8, types.T_array_uint8,
	}
}

// SupportedPrimaryKeyTypes: IVF-FLAT imposes no PK-type constraint — the
// primary key may be any type. nil = "no constraint".
func (CatalogHooks) SupportedPrimaryKeyTypes() []types.T { return nil }

// ValidQuantization gates the quantization value for IVF-FLAT: it must name a
// narrow vector type IVF-FLAT supports (float32/float16/bf16/int8/uint8, via
// quantizer.ToVectorType). IVF-FLAT re-ranks in the QUANTIZED domain from the
// stored narrow entries (it has no source vectors to re-rank from), so the
// stored geometry must be preserved by the quantizer. The affine int8/uint8
// scalar quantizer q(x)=a*x+b preserves L2 ordering (up to the a^2 scale that
// search.go rescales away) but NOT inner-product or cosine geometry — the
// a*b*(sum x + sum y) cross term makes <q(x),q(y)> depend on each vector's sum.
// So int8/uint8 are gated to L2 op_type only, matching CAGRA/IVF-PQ; f32/f16/bf16
// (lossless-ordering casts) keep all op_types. One home for CREATE (plan/schema)
// and REINDEX (compile/ValidateReindexParams).
func (CatalogHooks) ValidQuantization(quant, op string) error {
	if quant == "" {
		return nil
	}
	vt, ok := quantizer.ToVectorType(quant)
	if !ok {
		return moerr.NewNotSupportedNoCtxf(
			"ivfflat quantization %q (supported: float32, float16, bf16, int8, uint8)", quant)
	}
	if vt == types.T_array_int8 || vt == types.T_array_uint8 {
		// ALLOWLIST, not a denylist. Rejecting only IP/cosine let L1 through,
		// and search then divides every quantized score by QuantMul^2 — correct
		// for squared L2 (||q(a)-q(b)||^2 = mul^2*||a-b||^2) but wrong for L1,
		// which scales linearly (|q(a)-q(b)| = mul*|a-b|, so the inverse is
		// 1/mul). With [min,max]=[0,1] a true L1 distance near 0.5 was reported
		// near 0.5/255: ordering stayed monotonic, so it looked fine, while
		// returned distances and any range predicate in source units were
		// silently wrong. Fail at DDL instead — and any op_type added later is
		// rejected until someone works out its inverse scale, rather than
		// inheriting the L2 one by default.
		switch strings.ToLower(strings.TrimSpace(op)) {
		case "":
			// op_type not specified. On CREATE the caller defaults it to L2; on
			// ALTER ... REINDEX the params carry only what is being changed, so
			// an empty op means "unchanged" and the existing one was already
			// validated when the index was created. Rejecting it here broke
			// `alter ... reindex` that only changes quantization
			// (TestIvfflatValidateReindexParams_Quantization).
		case metric.OpType_L2Distance, metric.OpType_L2sqDistance:
		default:
			return moerr.NewNotSupportedNoCtxf(
				"ivfflat quantization %q is only supported with L2 (op_type 'vector_l2_ops' or 'vector_l2sq_ops'); the int8/uint8 affine quantizer preserves L2 geometry only, and the quantized-score rescale assumes it", quant)
		}
	}
	return nil
}

// SupportedIncludeColumnTypes: this index has no INCLUDE-column support.
func (CatalogHooks) SupportedIncludeColumnTypes() []types.T { return nil }

func (CatalogHooks) SupportedOpTypes() map[string]string {
	out := make(map[string]string, len(metric.OpTypeToIvfMetric))
	for k, v := range metric.OpTypeToIvfMetric {
		out[k] = fmt.Sprint(v)
	}
	return out
}

// SyncDescriptor — IVF-FLAT uses BOTH:
//   - ISCP CDC (event-driven; only when the index is async, per
//     IndexAlgoParams), and
//   - idxcron `ivfflat_reindex` (scheduled rebuild — re-cluster centroids
//     and rebuild entries on the configured cadence).
//
// AlwaysAsync=false: CDC participation depends on the `async` param.
// Matches the legacy inline behaviour in pkg/sql/compile/iscp_util.go.
func (CatalogHooks) SyncDescriptor() catalogplugin.SyncDescriptor {
	return catalogplugin.SyncDescriptor{
		UsesCDC:       true,
		SinkerType:    catalogplugin.SinkerType_IndexSync,
		AlwaysAsync:   false,
		IdxcronAction: actionIvfflatReindex,
		// IdxcronAlgoToken is the keyword the cron executor splices into
		// the ALTER ... REINDEX SQL. IdxcronListsAware=true keeps the
		// IVF-FLAT nlist / kmeans-train-percent heuristic in
		// checkIndexUpdatable.
		IdxcronAlgoToken:  "IVFFLAT",
		IdxcronListsAware: true,
	}
}

// Build-param defaults mirror the frontend session-var defaults; the build
// path (ivf_create) uses them when the flat algo_params key is absent (a
// legacy index created before the param was promoted).
const (
	DefaultKmeansTrainPercent = float64(10)
	DefaultKmeansMaxIteration = int64(20)
)

// ParamsFromTree is lifted verbatim from catalog.indexParamsToMap's
// INDEX_TYPE_IVFFLAT case (pkg/catalog/secondary_index_utils.go:304-340).
func (CatalogHooks) ParamsFromTree(idx *tree.Index) (map[string]string, error) {
	res := make(map[string]string)

	if idx.IndexOption.AlgoParamList == 0 {
		// Parser blocks list=0 explicitly; reaching here means the user
		// omitted the option. Default to lists=1.
		res[catalog.IndexAlgoParamLists] = strconv.FormatInt(1, 10)
	} else if idx.IndexOption.AlgoParamList > 0 {
		res[catalog.IndexAlgoParamLists] = strconv.FormatInt(idx.IndexOption.AlgoParamList, 10)
	} else {
		return nil, moerr.NewInternalErrorNoCtx("invalid list. list must be > 0")
	}

	if len(idx.IndexOption.AlgoParamVectorOpType) > 0 {
		opType := catalog.ToLower(idx.IndexOption.AlgoParamVectorOpType)
		if _, ok := metric.OpTypeToIvfMetric[opType]; !ok {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid op_type: '%s'", opType))
		}
		res[catalog.IndexAlgoParamOpType] = idx.IndexOption.AlgoParamVectorOpType
	} else {
		res[catalog.IndexAlgoParamOpType] = metric.OpType_L2Distance
	}

	if idx.IndexOption.Async {
		res[catalog.Async] = "true"
	}
	if idx.IndexOption.AutoUpdate {
		res[catalog.AutoUpdate] = "true"
	}
	if idx.IndexOption.Day > 0 {
		res[catalog.Day] = strconv.FormatInt(idx.IndexOption.Day, 10)
	}
	if idx.IndexOption.Hour > 0 {
		res[catalog.Hour] = strconv.FormatInt(idx.IndexOption.Hour, 10)
	}
	if idx.IndexOption.Second > 0 {
		res[catalog.Second] = strconv.FormatInt(idx.IndexOption.Second, 10)
	}

	if idx.IndexOption.KmeansTrainPercent > 0 {
		res[catalog.IndexAlgoParamKmeansTrainPercent] = strconv.FormatInt(idx.IndexOption.KmeansTrainPercent, 10)
	}
	if idx.IndexOption.KmeansMaxIteration > 0 {
		res[catalog.IndexAlgoParamKmeansMaxIteration] = strconv.FormatInt(idx.IndexOption.KmeansMaxIteration, 10)
	}

	// QUANTIZATION stores the ivfflat ENTRIES in a narrow type (float16/int8);
	// the base column and f32 centroids are unchanged. Persist it in algo_params
	// so the entries build (compile) and the search can read it back. Only the
	// predefined names that map to a MO narrow vector type are accepted.
	if q := idx.IndexOption.Quantization; q != "" {
		if _, ok := quantizer.ToVectorType(q); !ok {
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"ivfflat: unsupported quantization '%s' (supported: 'float32', 'float16', 'bf16', 'int8', 'uint8')", q))
		}
		res[catalog.Quantization] = catalog.ToLower(q)
	}
	return res, nil
}
