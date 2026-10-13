// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// newGpuAlgoVectorJoinCtx extends the vector-join mock with the session variables
// the ivfpq and cagra rewrites resolve. Without them prepare*IndexContext bails out
// early and produces no search at all, which would make this regression pass
// vacuously for those two algorithms.
func newGpuAlgoVectorJoinCtx(t testing.TB) *customMockCompilerContext {
	base := newVectorJoinMockCtx(t)
	inner := base.resolveVarFunc
	base.resolveVarFunc = func(varName string, isSystem, isGlobal bool) (interface{}, error) {
		switch varName {
		case "ivfpq_threads_search", "cagra_threads_search":
			return int64(4), nil
		case "ivfpq_batch_window", "cagra_batch_window":
			return int64(1000), nil
		case "gpu_multi_simulation":
			return int64(0), nil
		default:
			return inner(varName, isSystem, isGlobal)
		}
	}
	return base
}

// newVectorJoinTwoTableIndex builds the metadata+storage index pair that hnsw,
// ivfpq and cagra all take. Only the algorithm name and the two table-type
// constants differ between them.
func newVectorJoinTwoTableIndex(algo, metaType, storageType string) *MultiTableIndex {
	idxAlgoParams := `{"op_type": "` + metric.DistFuncOpTypes["l2_distance"] + `"}`
	def := func(tblType, tblName string) *plan.IndexDef {
		return &plan.IndexDef{
			IndexName:          "idx_v",
			IndexAlgo:          algo,
			IndexAlgoTableType: tblType,
			IndexTableName:     tblName,
			Parts:              []string{"v"},
			IndexAlgoParams:    idxAlgoParams,
		}
	}
	return &MultiTableIndex{
		IndexAlgo: algo,
		IndexDefs: map[string]*plan.IndexDef{
			metaType:    def(metaType, "idx_meta"),
			storageType: def(storageType, "idx_storage"),
		},
	}
}

// TestProviderChildSearchRunsPerProviderRow pins the provider-child shape of an
// hnsw search: the query vector comes from the joined provider row, so the
// INDEX_SEARCH_SCAN runs under a CROSS APPLY of the provider and carries the
// semantic k and the post-filter flag; execution sizes the candidate budget per
// row (#26869). Only hnsw and ivfflat consume vecCtx.vecArgExpr; ivfpq and cagra
// decline the rewrite (TestProviderChildShapeUnsupportedByIvfpqAndCagra).
func TestProviderChildSearchRunsPerProviderRow(t *testing.T) {
	const literalK = uint64(7)
	for _, lim := range []struct {
		name  string
		limit *plan.Expr
	}{
		{name: "literal k", limit: makePlan2Uint64ConstExprWithType(literalK)},
		{name: "prepared LIMIT ?", limit: &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_uint64)},
			Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
		}},
	} {
		t.Run("hnsw/"+lim.name, func(t *testing.T) {
			tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
				joinType:              plan.Node_INNER,
				providerSingle:        true,
				providerVectorNotNull: true,
			})
			// A residual filter on the base scan makes the search post-filtered.
			mainScan := tc.builder.qry.Nodes[tc.mainScanNodeID]
			mainScan.FilterList = append(mainScan.FilterList,
				newVectorJoinEqFilter(mainScan.BindingTags[0], 0))
			tc.builder.compCtx = newGpuAlgoVectorJoinCtx(t)

			vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
			require.NotNil(t, vecCtx, "test setup: the provider-child context must build")
			vecCtx.sortNode.Limit = DeepCopyExpr(lim.limit)
			vecCtx.limit = DeepCopyExpr(lim.limit)

			_, err := tc.builder.applyIndicesForSortUsingHnsw(tc.projNodeID, vecCtx, newVectorJoinHnswIndex(), nil)
			require.NoError(t, err)

			apply := findFirstNodeByType(tc.builder, plan.Node_APPLY)
			require.NotNil(t, apply, "a provider row drives the search through CROSS APPLY")
			require.Equal(t, plan.Node_CROSSAPPLY, apply.ApplyType)
			require.Len(t, apply.Children, 2)
			require.Equal(t, tc.providerNodeID, apply.Children[0])
			search := tc.builder.qry.Nodes[apply.Children[1]]
			require.Equal(t, plan.Node_INDEX_SEARCH_SCAN, search.NodeType)
			require.Empty(t, search.Children)
			require.True(t, search.IndexSearchScan.PostFilterOverFetch)
			require.Equal(t, lim.limit.GetLit().GetU64Val(), search.IndexSearchScan.CandidateLimit.GetLit().GetU64Val())
			if lim.limit.GetP() != nil {
				require.NotNil(t, search.IndexSearchScan.CandidateLimit.GetP(), "a prepared k is bound at execution")
			}
		})
	}
}

// TestProviderChildShapeUnsupportedByIvfpqAndCagra pins the reason the regression
// above covers only hnsw. Neither ivfpq nor cagra consumes vecCtx.vecArgExpr, so
// neither can be driven by a joined provider row and neither ever produces a search
// with a child. If that changes, this test fails and whoever adds the support has to
// confirm the node.Limit transport still holds for the newly reachable remote path.
func TestProviderChildShapeUnsupportedByIvfpqAndCagra(t *testing.T) {
	for _, algo := range []struct {
		name  string
		index func() *MultiTableIndex
		apply func(*QueryBuilder, int32, *vectorSortContext, *MultiTableIndex) (int32, error)
	}{
		{
			name: "ivfpq",
			index: func() *MultiTableIndex {
				return newVectorJoinTwoTableIndex(catalog.MoIndexIvfpqAlgo.ToString(),
					catalog.Ivfpq_TblType_Metadata, catalog.Ivfpq_TblType_Storage)
			},
			apply: func(b *QueryBuilder, id int32, vc *vectorSortContext, m *MultiTableIndex) (int32, error) {
				return b.applyIndicesForSortUsingIvfpq(id, vc, m, nil)
			},
		},
		{
			name: "cagra",
			index: func() *MultiTableIndex {
				return newVectorJoinTwoTableIndex(catalog.MoIndexCagraAlgo.ToString(),
					catalog.Cagra_TblType_Metadata, catalog.Cagra_TblType_Storage)
			},
			apply: func(b *QueryBuilder, id int32, vc *vectorSortContext, m *MultiTableIndex) (int32, error) {
				return b.applyIndicesForSortUsingCagra(id, vc, m, nil)
			},
		},
	} {
		t.Run(algo.name, func(t *testing.T) {
			tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
				joinType:              plan.Node_INNER,
				providerSingle:        true,
				providerVectorNotNull: true,
			})
			tc.builder.compCtx = newGpuAlgoVectorJoinCtx(t)

			vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
			require.NotNil(t, vecCtx, "the shared context still builds; only the rewrite declines")
			require.NotNil(t, vecCtx.vecArgExpr, "the provider supplies the query vector")

			newNodeID, err := algo.apply(tc.builder, tc.projNodeID, vecCtx, algo.index())
			require.NoError(t, err)
			require.Equal(t, tc.projNodeID, newNodeID, "the plan must be left untouched")
			require.Nil(t, findFirstNodeByType(tc.builder, plan.Node_FUNCTION_SCAN),
				"%s does not consume vecArgExpr, so it cannot serve a provider-child search", algo.name)
		})
	}
}
