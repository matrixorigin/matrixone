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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

var _ catalogplugin.Hooks = CatalogHooks{}

type CatalogHooks struct{}

func (CatalogHooks) HiddenTableTypes() []string {
	return []string{
		catalog.Hnsw_TblType_Metadata,
		catalog.Hnsw_TblType_Storage,
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
	return catalogplugin.AlterTableCloneBehavior{}
}

func (CatalogHooks) DefaultOptions() map[string]string {
	return map[string]string{
		catalog.IndexAlgoParamOpType: metric.OpType_L2Distance,
	}
}

// HnswIndexFlag is the experimental-feature flag gating HNSW DDL. Single
// source of truth; both the catalog gate (pkg/sql/compile/util.go via
// ExperimentalFlag) and the per-plugin HandleCreateIndex gate reference
// this constant.
const HnswIndexFlag = "experimental_hnsw_index"

// ExperimentalFlag: HNSW DDL is gated by HnswIndexFlag.
func (CatalogHooks) ExperimentalFlag() string { return HnswIndexFlag }

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
			return nil, moerr.NewInternalErrorNoCtx(fmt.Sprintf("invalid op_type. '%s'", opType))
		}
		res[catalog.IndexAlgoParamOpType] = idx.IndexOption.AlgoParamVectorOpType
	} else {
		res[catalog.IndexAlgoParamOpType] = metric.OpType_L2Distance
	}

	if idx.IndexOption.Async {
		res[catalog.Async] = "true"
	}
	return res, nil
}
