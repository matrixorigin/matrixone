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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/vectorindex/plugin/catalog"
)

// actionIvfflatReindex mirrors idxcron.Action_Ivfflat_Reindex. Inlined
// here so this package doesn't import pkg/vectorindex/idxcron — which
// would create an import cycle in tests via testengine → pkg/sql/plan →
// this plugin.
//
// Stays in lock-step with pkg/vectorindex/idxcron/executor.go:56.
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

// DefaultOptions mirrors the IVF-FLAT case of indexParamsToMap when the
// statement carries no WITH(...) clause: lists=1, op_type=l2.
func (CatalogHooks) DefaultOptions() map[string]string {
	return map[string]string{
		catalog.IndexAlgoParamLists:  "1",
		catalog.IndexAlgoParamOpType: metric.OpType_L2Distance,
	}
}

// ExperimentalFlag — IVF-FLAT is gated by `experimental_ivf_index` (note:
// `_ivf_index`, not `_ivfflat_index` — the variable name predates the
// IVF-PQ split).
func (CatalogHooks) ExperimentalFlag() string { return "experimental_ivf_index" }

// SupportedOpTypes returns IVF-FLAT's metric registry. IVF uses a
// distinct metric table from HNSW/USearch (OpTypeToIvfMetric).
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
	}
}

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
	return res, nil
}
