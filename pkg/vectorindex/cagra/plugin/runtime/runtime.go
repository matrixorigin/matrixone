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
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	catalogplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/catalog"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

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
	return catalogplugin.AlterTableCloneBehavior{}
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

func (CatalogHooks) SupportedOpTypes() map[string]string {
	out := make(map[string]string, len(metric.OpTypeToUsearchMetric))
	for k, v := range metric.OpTypeToUsearchMetric {
		out[k] = fmt.Sprint(v)
	}
	return out
}

// SyncDescriptor: CAGRA does not participate in ISCP CDC or idxcron
// today — its hidden tables are rebuilt synchronously inside
// HandleCreateIndex / HandleReindex, not maintained out-of-band. Flip
// UsesCDC to true (with SinkerType: SinkerType_IndexSync, AlwaysAsync
// per-param or always) when the actual CDC pipeline lands for CAGRA.
// Likewise, set IdxcronAction once Action_Cagra_Reindex lands in
// pkg/vectorindex/idxcron/executor.go.
func (CatalogHooks) SyncDescriptor() catalogplugin.SyncDescriptor {
	return catalogplugin.SyncDescriptor{}
}

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
