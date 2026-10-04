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
	"github.com/matrixorigin/matrixone/pkg/vectorindex/overfetch"
)

// newGpuAlgoVectorJoinCtx extends the vector-join mock with the session variables
// the ivfpq and cagra rewrites resolve. Without them prepare*IndexContext bails out
// early and produces no search at all, which would make this regression pass
// vacuously for those two algorithms.
func newGpuAlgoVectorJoinCtx() *customMockCompilerContext {
	base := newVectorJoinMockCtx()
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

// TestProviderChildSearchAlwaysCarriesNodeLimit is the rolling-upgrade regression.
//
// Only hnsw is exercised here, and that is a finding rather than a shortcut: of the
// three algorithms the review named, hnsw is the only one that can take this path at
// all. The provider-child shape requires the rewrite to consume vecCtx.vecArgExpr --
// the query vector supplied by the joined provider row -- and only hnsw and ivfflat
// do. ivfpq and cagra look for a literal query vector, do not find one, and decline
// the rewrite entirely (pinned by TestProviderChildShapeUnsupportedByIvfpqAndCagra),
// so their FUNCTION_SCAN is always childless and compileTableFunction builds it a
// local scope. They carry the budget on node.Limit regardless, which costs nothing
// and keeps them correct if provider-child support is ever added.
//
// This is the shape that can reach a pre-change CN: a vector provider child gives
// the FUNCTION_SCAN a child, so compileTableFunction attaches the search operator to
// the already-compiled child scopes rather than building a fresh one, and those
// scopes may be Remote. A new coordinator can therefore ship this operator to an old
// executor mid-upgrade.
//
// That executor reads arg.Limit and nothing else — it has no IndexReaderParam
// handling at all — and evalLimitExpression turns a nil into a single candidate. So
// for every algorithm that takes this path, the emitted FUNCTION_SCAN must carry a
// non-nil node.Limit holding the OVER-FETCHED budget, whether k is a literal or a
// bound parameter. A nil here is the silent under-return this guards.
func TestProviderChildSearchAlwaysCarriesNodeLimit(t *testing.T) {
	algos := []struct {
		name  string
		index func() *MultiTableIndex
		apply func(*QueryBuilder, int32, *vectorSortContext, *MultiTableIndex) (int32, error)
	}{
		{
			name:  "hnsw",
			index: func() *MultiTableIndex { return newVectorJoinHnswIndex() },
			apply: func(b *QueryBuilder, id int32, vc *vectorSortContext, m *MultiTableIndex) (int32, error) {
				return b.applyIndicesForSortUsingHnsw(id, vc, m, nil)
			},
		},
	}

	const literalK = uint64(7)
	limits := []struct {
		name     string
		limit    *plan.Expr
		wantLit  bool // a literal k folds to the k' constant at plan time
		wantFold uint64
	}{
		{
			name:     "literal k",
			limit:    makePlan2Uint64ConstExprWithType(literalK),
			wantLit:  true,
			wantFold: overfetch.PostFilterLimit(literalK),
		},
		{
			name: "prepared LIMIT ?",
			limit: &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_uint64)},
				Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}},
			},
			wantLit: false,
		},
	}

	for _, algo := range algos {
		for _, lim := range limits {
			t.Run(algo.name+"/"+lim.name, func(t *testing.T) {
				tc := newVectorJoinPlanCase(t, vectorJoinPlanOptions{
					joinType:              plan.Node_INNER,
					providerSingle:        true,
					providerVectorNotNull: true,
				})

				// A residual filter on the base scan is what makes the search
				// post-filtered, and therefore what requires the over-fetch.
				mainScan := tc.builder.qry.Nodes[tc.mainScanNodeID]
				mainScan.FilterList = append(mainScan.FilterList,
					newVectorJoinEqFilter(mainScan.BindingTags[0], 0))

				tc.builder.compCtx = newGpuAlgoVectorJoinCtx()

				vecCtx := tc.builder.buildVectorSortContextThroughJoin(tc.projNode)
				require.NotNil(t, vecCtx, "test setup: the provider-child context must build")
				vecCtx.sortNode.Limit = DeepCopyExpr(lim.limit)
				vecCtx.limit = DeepCopyExpr(lim.limit)

				_, err := algo.apply(tc.builder, tc.projNodeID, vecCtx, algo.index())
				require.NoError(t, err)

				funcScan := findFirstNodeByType(tc.builder, plan.Node_FUNCTION_SCAN)
				require.NotNil(t, funcScan, "the rewrite must have produced a search")

				// The precondition that makes this the dangerous shape: the search
				// has a child, so its operator rides the child's (possibly Remote)
				// scopes instead of a locally built one.
				require.NotEmpty(t, funcScan.Children,
					"test setup: this must be the provider-child shape that can be shipped to a remote CN")
				require.Equal(t, []int32{tc.providerNodeID}, funcScan.Children)

				require.NotNil(t, funcScan.Limit,
					"a pre-change CN reads node.Limit alone; nil makes it fetch one candidate and under-return")

				if lim.wantLit {
					require.NotNil(t, funcScan.Limit.GetLit(),
						"a literal k must fold to the over-fetched constant")
					require.Equal(t, lim.wantFold, funcScan.Limit.GetLit().GetU64Val(),
						"the plan-level top must truncate at k', not at k")
				} else {
					require.Nil(t, funcScan.Limit.GetLit(),
						"a bound parameter must stay an expression resolved at EXECUTE")
				}
			})
		}
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
			tc.builder.compCtx = newGpuAlgoVectorJoinCtx()

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
