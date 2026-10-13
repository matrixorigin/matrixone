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

package explain

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	ft2plan "github.com/matrixorigin/matrixone/pkg/fulltext2/plugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestIndexSearchScanInfoIsTypedAndVisible(t *testing.T) {
	node := &plan.Node{
		NodeType: plan.Node_INDEX_SEARCH_SCAN,
		Stats:    &plan.Stats{},
		IndexSearchScan: &plan.IndexSearchScan{
			Index:            &plan.IndexDef{IndexName: "idx_v", IndexAlgo: "ivfflat"},
			DistanceFunction: "l2_distance",
			CandidateLimit:   plan2.MakePlan2Uint64ConstExprWithType(12),
			AlgoOptions:      []byte(`{"initial_probe_count":4,"bucket_expand_step":8,"threads_search":3}`),
			PreFilters: []*plan.Expr{{
				Typ:  plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}},
			}},
		},
	}
	info, err := (&NodeDescribeImpl{Node: node}).GetExtraInfo(context.Background(), &ExplainOptions{})
	require.NoError(t, err)
	require.Len(t, info, 1)
	require.Contains(t, info[0], "Vector Index: idx_v")
	require.Contains(t, info[0], "Metric: l2_distance")
	require.Contains(t, info[0], "Candidate Limit: 12")
	require.Contains(t, info[0], "NProbe: 4")
	require.Contains(t, info[0], "Index Filter: true")
	require.NotContains(t, info[0], "Estimated Scan Rows")
	desc := &NodeDescribeImpl{Node: node}
	compact := "Vector Index: idx_v, Metric: l2_distance, Candidate Limit: 12, NProbe: 4, Index Filter: true"
	require.Equal(t, compact, info[0])
	filters := node.IndexSearchScan.PreFilters
	node.IndexSearchScan.PreFilters = nil
	nilOptions, err := desc.GetIndexSearchScanInfo(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, "Vector Index: idx_v, Metric: l2_distance, Candidate Limit: 12, NProbe: 4", nilOptions)
	node.IndexSearchScan.PreFilters = filters
	verbose, err := desc.GetIndexSearchScanInfo(t.Context(), &ExplainOptions{Verbose: true})
	require.NoError(t, err)
	require.Equal(t, "Vector Index: idx_v, Metric: l2_distance, Candidate Limit: 12, NProbe: 4, BucketExpandStep: 8, ThreadsSearch: 3, Index Filter: true", verbose)
	again, err := desc.GetIndexSearchScanInfo(t.Context(), &ExplainOptions{})
	require.NoError(t, err)
	require.Equal(t, compact, again)

	node.IndexSearchScan.ScanWork = &plan.IndexSearchScanWork{Rows: 100, Blocks: 2, VectorBytesPerRow: 128, Objects: 2}
	node.Stats.Dop = 2
	basic, err := (&NodeDescribeImpl{Node: node}).GetNodeBasicInfo(context.Background(), &ExplainOptions{})
	require.NoError(t, err)
	require.Contains(t, basic, "Estimated Scan Rows: 100, Blocks: 2, Vector Bytes/Row: 128, Objects: 2, Planned DOP: 2")
}

func fulltextSearchScanNode(t *testing.T) *plan.Node {
	t.Helper()
	data, err := ft2plan.EncodeScanOptions(ft2plan.ScanOptions{Config: "{}"})
	require.NoError(t, err)
	return &plan.Node{
		NodeType: plan.Node_INDEX_SEARCH_SCAN,
		Stats:    &plan.Stats{Sql: "SELECT doc_id FROM idx"},
		TableDef: &plan.TableDef{Cols: []*plan.ColDef{{Name: "doc_id"}, {Name: "score"}}},
		IndexSearchScan: &plan.IndexSearchScan{
			Index:       &plan.IndexDef{IndexName: "ft_body", IndexAlgo: "fulltext2"},
			AlgoOptions: data,
		},
	}
}

// A fulltext index search scan is labelled by its family, has no metric, and
// shows its candidate limit only when one is pushed.
func TestFulltextIndexSearchScanExplain(t *testing.T) {
	ctx := context.Background()
	node := fulltextSearchScanNode(t)
	desc := &NodeDescribeImpl{Node: node}

	basic, err := desc.GetNodeBasicInfo(ctx, &ExplainOptions{Format: EXPLAIN_FORMAT_TEXT})
	require.NoError(t, err)
	require.Contains(t, basic, "Fulltext Index Scan on ft_body")
	require.NotContains(t, basic, "[")

	info, err := desc.GetExtraInfo(ctx, &ExplainOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"Fulltext Index: ft_body"}, info)
	compact, err := desc.GetIndexSearchScanInfo(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, "Fulltext Index: ft_body", compact)

	node.IndexSearchScan.CandidateLimit = plan2.MakePlan2Uint64ConstExprWithType(15)
	info, err = desc.GetExtraInfo(ctx, &ExplainOptions{Verbose: true})
	require.NoError(t, err)
	require.Contains(t, info, "Sql: SELECT doc_id FROM idx")
	require.Contains(t, info, "Fulltext Index: ft_body, Candidate Limit: 15, Match Mode: 0")
	info, err = desc.GetExtraInfo(ctx, &ExplainOptions{})
	require.NoError(t, err)
	require.Equal(t, []string{"Fulltext Index: ft_body, Candidate Limit: 15"}, info)

	require.Equal(t, "Vector Index Scan", IndexSearchScanLabel(&plan.IndexSearchScan{
		Index: &plan.IndexDef{IndexAlgo: "not_registered"},
	}))
	require.Equal(t, "Vector Index Scan", IndexSearchScanLabel(nil))
}

func TestFulltextIndexSearchScanMarshal(t *testing.T) {
	ctx := context.Background()
	m := NewMarshalNodeImpl(fulltextSearchScanNode(t))
	name, err := m.GetNodeName(ctx)
	require.NoError(t, err)
	require.Equal(t, "Fulltext Index Scan", name)
	title, err := m.GetNodeTitle(ctx, &ExplainOptions{})
	require.NoError(t, err)
	require.Equal(t, "Fulltext Index Scan[ft_body]", title)
	labels, err := m.GetNodeLabels(ctx, &ExplainOptions{})
	require.NoError(t, err)
	require.Equal(t, "ft_body", labels[0].Value)

	broken := NewMarshalNodeImpl(&plan.Node{NodeType: plan.Node_INDEX_SEARCH_SCAN, Stats: &plan.Stats{}})
	_, err = broken.GetNodeTitle(ctx, &ExplainOptions{})
	require.Error(t, err)
	_, err = broken.GetNodeLabels(ctx, &ExplainOptions{})
	require.Error(t, err)
}

func TestHnswDetailedSettingsAndCompactCompatibility(t *testing.T) {
	node := &plan.Node{NodeId: 0, NodeType: plan.Node_INDEX_SEARCH_SCAN, IndexSearchScan: &plan.IndexSearchScan{Index: &plan.IndexDef{IndexName: "hx", IndexAlgo: "hnsw"}, DistanceFunction: "l2_distance"}}
	for _, threads := range []string{"1", "8"} {
		node.IndexSearchScan.AlgoOptions = []byte(`{"threads_search":` + threads + `}`)
		got, err := sqlJSONIndexSearchExpressions(t.Context(), node.IndexSearchScan, &ExplainOptions{})
		require.NoError(t, err)
		require.Contains(t, got, "ThreadsSearch: "+threads)
		desc := &NodeDescribeImpl{Node: node}
		text, err := desc.GetIndexSearchScanInfo(t.Context(), &ExplainOptions{Verbose: true})
		require.NoError(t, err)
		require.Equal(t, "Vector Index: hx, Metric: l2_distance, ThreadsSearch: "+threads, text)
		text, err = desc.GetIndexSearchScanInfo(t.Context(), &ExplainOptions{})
		require.NoError(t, err)
		require.Equal(t, "Vector Index: hx, Metric: l2_distance", text)
	}
	node.IndexSearchScan.AlgoOptions = []byte(`{`)
	_, err := sqlJSONIndexSearchExpressions(t.Context(), node.IndexSearchScan, &ExplainOptions{})
	require.Error(t, err)
	_, err = (&NodeDescribeImpl{Node: node}).GetIndexSearchScanInfo(t.Context(), &ExplainOptions{Verbose: true})
	require.Error(t, err)
	text, err := (&NodeDescribeImpl{Node: node}).GetIndexSearchScanInfo(t.Context(), &ExplainOptions{})
	require.NoError(t, err)
	require.Equal(t, "Vector Index: hx, Metric: l2_distance", text)
}
