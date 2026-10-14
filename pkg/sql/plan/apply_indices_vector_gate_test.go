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

package plan

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestVectorIndexSupportsContext(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, newFullTextJoinMockCompilerContext(t), false, true)
	ctx := NewBindContext(builder, nil)
	scanTag := builder.genNewBindTag()
	tableDef := makeFullTextJoinTestTableDef("ft", true)
	match := makeFullTextMatchExpr("hello", 0, tableDef, scanTag, []int32{2, 3})
	scanID := builder.appendNode(makeFullTextJoinTestScan(tableDef, scanTag, []*planpb.Expr{match}), ctx)
	matchScan := builder.qry.Nodes[scanID]
	plainScan := builder.qry.Nodes[builder.appendNode(makeFullTextJoinTestScan(tableDef, builder.genNewBindTag(), nil), ctx)]

	ivfflat := catalog.MoIndexIvfFlatAlgo.ToString()
	hnsw := catalog.MoIndexHnswAlgo.ToString()
	cagra := catalog.MoIndexCagraAlgo.ToString()
	for _, c := range []struct {
		name string
		ctx  *vectorSortContext
		algo string
		want bool
	}{
		{"no context", nil, hnsw, true},
		{"plain scan", &vectorSortContext{scanNode: plainScan}, hnsw, true},
		{"MATCH filter, cagra", &vectorSortContext{scanNode: matchScan}, cagra, false},
		{"MATCH filter, hnsw", &vectorSortContext{scanNode: matchScan}, hnsw, true},
		{"MATCH filter, ivfflat", &vectorSortContext{scanNode: matchScan}, ivfflat, true},
		{"membership, cagra", &vectorSortContext{scanNode: plainScan, hasMembership: true}, cagra, false},
		{"membership, hnsw", &vectorSortContext{scanNode: plainScan, hasMembership: true}, hnsw, true},
		{"membership, ivfflat", &vectorSortContext{scanNode: plainScan, hasMembership: true}, ivfflat, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, builder.vectorIndexSupportsContext(c.ctx, c.algo))
		})
	}
}

// A MATCH only in the projection of the Top-K keeps every vector index off the scan.
func TestVectorIndexSupportsContextProjectedMatch(t *testing.T) {
	builder, scanID, projID := buildWrappedMatchGuardPlan(t, true)
	scan, proj := builder.qry.Nodes[scanID], builder.qry.Nodes[projID]
	for _, algo := range []string{catalog.MoIndexIvfFlatAlgo.ToString(), catalog.MoIndexHnswAlgo.ToString()} {
		require.False(t, builder.vectorIndexSupportsContext(&vectorSortContext{scanNode: scan, projNode: proj}, algo), algo)
		require.False(t, builder.vectorIndexSupportsContext(&vectorSortContext{scanNode: scan, childNode: proj}, algo), algo)
	}
	plainProj := &planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{scanID}}
	require.True(t, builder.vectorIndexSupportsContext(&vectorSortContext{scanNode: scan, projNode: plainProj}, catalog.MoIndexHnswAlgo.ToString()))
}

func TestCheckVectorSearchMode(t *testing.T) {
	ctx := context.Background()
	ivfflat := catalog.MoIndexIvfFlatAlgo.ToString()
	for _, algo := range []string{
		catalog.MoIndexCagraAlgo.ToString(), catalog.MoIndexIvfpqAlgo.ToString(),
	} {
		// cagra and ivfpq evaluate INCLUDE and primary-key filters in the search but
		// apply no membership key set: pre, iterative and auto apply.
		for _, mode := range []string{"pre", "iterative", "auto", "post", "force", ""} {
			require.NoError(t, checkVectorSearchMode(ctx, &vectorSortContext{rankOption: &planpb.RankOption{Mode: mode}}, algo), "%s %s", algo, mode)
		}
		require.NoError(t, checkVectorSearchMode(ctx, &vectorSortContext{}, algo))
		require.NoError(t, checkVectorSearchMode(ctx, nil, algo))
	}
	for _, mode := range []string{"pre", "auto", "iterative"} {
		require.NoError(t, checkVectorSearchMode(ctx, &vectorSortContext{rankOption: &planpb.RankOption{Mode: mode}}, ivfflat))
		require.NoError(t, checkVectorSearchMode(ctx, &vectorSortContext{rankOption: &planpb.RankOption{Mode: mode}}, catalog.MoIndexHnswAlgo.ToString()))
	}

	// mode=include is ivfflat only
	include := &vectorSortContext{rankOption: &planpb.RankOption{Mode: "include"}}
	require.NoError(t, checkVectorSearchMode(ctx, include, ivfflat))
	for _, algo := range []string{
		catalog.MoIndexHnswAlgo.ToString(), catalog.MoIndexCagraAlgo.ToString(), catalog.MoIndexIvfpqAlgo.ToString(),
	} {
		err := checkVectorSearchMode(ctx, include, algo)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported), algo)
		require.ErrorContains(t, err, "mode=include on index type "+algo)
	}
}

func TestIvfIterativeMode(t *testing.T) {
	require.True(t, ivfIterativeMode("iterative"))
	require.True(t, ivfIterativeMode("include"))
	require.False(t, ivfIterativeMode("pre"))
	require.False(t, ivfIterativeMode(""))
}

func TestHasMembershipVectorIndex(t *testing.T) {
	def := func(algos ...string) *planpb.TableDef {
		td := &planpb.TableDef{}
		for _, a := range algos {
			td.Indexes = append(td.Indexes, &planpb.IndexDef{IndexAlgo: a})
		}
		return td
	}
	require.False(t, hasMembershipVectorIndex(nil))
	require.False(t, hasMembershipVectorIndex(def()))
	require.False(t, hasMembershipVectorIndex(def(catalog.MoIndexCagraAlgo.ToString(), catalog.MoIndexIvfpqAlgo.ToString())))
	require.True(t, hasMembershipVectorIndex(def(catalog.MoIndexHnswAlgo.ToString())))
	require.True(t, hasMembershipVectorIndex(def(catalog.MoIndexCagraAlgo.ToString(), catalog.MoIndexIvfFlatAlgo.ToString())))
}

func TestCheckVectorSearchModeProbeKnobs(t *testing.T) {
	ctx := context.Background()
	opt := func(o *planpb.RankOption) *vectorSortContext { return &vectorSortContext{rankOption: o} }
	for _, algo := range []string{catalog.MoIndexHnswAlgo.ToString(), catalog.MoIndexCagraAlgo.ToString()} {
		err := checkVectorSearchMode(ctx, opt(&planpb.RankOption{Nprobe: 4}), algo)
		require.ErrorContains(t, err, "nprobe on index type "+algo)
		err = checkVectorSearchMode(ctx, opt(&planpb.RankOption{MaxNprobe: 4}), algo)
		require.ErrorContains(t, err, "max_nprobe on index type "+algo)
		require.NoError(t, checkVectorSearchMode(ctx, opt(&planpb.RankOption{Itopk: 4, MaxItopk: 8}), algo))
	}
	for _, algo := range []string{catalog.MoIndexIvfFlatAlgo.ToString(), catalog.MoIndexIvfpqAlgo.ToString()} {
		require.NoError(t, checkVectorSearchMode(ctx, opt(&planpb.RankOption{Itopk: 4, MaxItopk: 8, Nprobe: 2, MaxNprobe: 16}), algo))
	}
}

func TestCheckVectorSearchModeInternalTopKLimit(t *testing.T) {
	ctx := context.Background()
	opt := func(o *planpb.RankOption) *vectorSortContext { return &vectorSortContext{rankOption: o} }
	cagra := catalog.MoIndexCagraAlgo.ToString()
	require.NoError(t, checkVectorSearchMode(ctx, opt(&planpb.RankOption{Itopk: 1024, MaxItopk: 1024}), cagra))
	err := checkVectorSearchMode(ctx, opt(&planpb.RankOption{Itopk: 1025}), cagra)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "%v", err)
	require.ErrorContains(t, err, "itopk 1025 exceeds the cagra limit 1024")
	err = checkVectorSearchMode(ctx, opt(&planpb.RankOption{Mode: "iterative", MaxItopk: 2000}), cagra)
	require.ErrorContains(t, err, "max_itopk 2000 exceeds the cagra limit 1024")
	require.NoError(t, checkVectorSearchMode(ctx, opt(&planpb.RankOption{Itopk: 20000, MaxItopk: 20000}), catalog.MoIndexHnswAlgo.ToString()))
}

func TestSetIndexSearchScanKnobs(t *testing.T) {
	spec := &planpb.IndexSearchScan{}
	setIndexSearchScanKnobs(spec, &vectorSortContext{})
	require.Zero(t, spec.InternalTopk)
	setIndexSearchScanKnobs(spec, &vectorSortContext{rankOption: &planpb.RankOption{Itopk: 200, MaxItopk: 5000, Nprobe: 8, MaxNprobe: 64}})
	require.Equal(t, uint64(200), spec.InternalTopk)
	require.Equal(t, uint64(5000), spec.MaxInternalTopk)
	require.Equal(t, uint64(64), spec.MaxNprobe)
	setIndexSearchScanKnobs(nil, &vectorSortContext{rankOption: &planpb.RankOption{Itopk: 1}})
}
