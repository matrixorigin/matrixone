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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

func TestExplainVerboseBoundsIvfSearchRoundsAndAlwaysSummarizes(t *testing.T) {
	query := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{{NodeId: 0, NodeType: plan.Node_PROJECT}},
	}
	offset := uint64(0)
	for round, count := range []uint64{1, 2, 4, 8, 16, 32, 1} {
		query.BackgroundQueries = append(query.BackgroundQueries,
			vectorindex.EncodeIvfSearchRoundDiagnostic(vectorindex.IvfSearchRoundDiagnostic{
				Round:        uint64(round + 1),
				BucketOffset: offset,
				BucketCount:  count,
				RowLimit:     2,
				OutputRows:   map[bool]uint64{true: 2}[round == 6],
				Exhausted:    round == 6,
			}))
		offset += count
	}

	buffer := NewExplainDataBuffer()
	err := NewExplainQueryImpl(query).ExplainPlan(context.Background(), buffer, &ExplainOptions{
		Verbose: true,
		Analyze: true,
		Format:  EXPLAIN_FORMAT_TEXT,
	})
	require.NoError(t, err)
	text := buffer.ToString()
	for _, round := range []int{1, 2, 3, 6, 7} {
		require.Contains(t, text, fmt.Sprintf("Vector Index Search Round %d:", round))
	}
	for _, round := range []int{4, 5} {
		require.NotContains(t, text, fmt.Sprintf("Vector Index Search Round %d:", round))
	}
	require.Contains(t, text, "Vector Index Search Rounds: skipped 2 middle round(s)")
	require.Contains(t, text, "Vector Index Search Summary: search_count=1 round_count=7 buckets_searched=64")
	require.Contains(t, text, "bucket_windows=0:1, 1:3, 3:7, ...(2 skipped)..., 31:63, 63:64")
	require.Contains(t, text, "empty_rounds=6")
}

func TestExplainAnalyzeIvfSearchSummaryWithoutVerboseRoundExpansion(t *testing.T) {
	query := &plan.Query{
		Steps: []int32{0},
		Nodes: []*plan.Node{{NodeId: 0, NodeType: plan.Node_PROJECT}},
		BackgroundQueries: []*plan.Query{
			vectorindex.EncodeIvfSearchRoundDiagnostic(vectorindex.IvfSearchRoundDiagnostic{
				Round: 1, BucketCount: 1, RowLimit: 2,
			}),
			vectorindex.EncodeIvfSearchRoundDiagnostic(vectorindex.IvfSearchRoundDiagnostic{
				Round: 2, BucketOffset: 1, BucketCount: 2, RowLimit: 2, OutputRows: 2, Exhausted: true,
			}),
		},
	}

	buffer := NewExplainDataBuffer()
	require.NoError(t, NewExplainQueryImpl(query).ExplainPlan(
		context.Background(), buffer, &ExplainOptions{Analyze: true, Format: EXPLAIN_FORMAT_TEXT}))
	text := buffer.ToString()
	require.NotContains(t, text, "Vector Index Search Round 1:")
	require.Contains(t, text, "Vector Index Search Summary: search_count=1 round_count=2 buckets_searched=3")
}

func TestSplitIvfSearchDiagnosticsPreservesOrdinaryBackgroundPlans(t *testing.T) {
	ordinary := &plan.Query{Headings: []string{"ordinary"}}
	diagnostic := vectorindex.EncodeIvfSearchRoundDiagnostic(vectorindex.IvfSearchRoundDiagnostic{
		Round: 1, BucketCount: 1, RowLimit: 2,
	})
	malformed := &plan.Query{Headings: []string{"__mo_ivf_search_round_v1", "malformed"}}

	rounds, remaining := splitIvfSearchDiagnostics([]*plan.Query{ordinary, diagnostic, malformed})
	require.Equal(t, []vectorindex.IvfSearchRoundDiagnostic{{
		Round: 1, BucketCount: 1, RowLimit: 2,
	}}, rounds)
	require.Equal(t, []*plan.Query{ordinary, malformed}, remaining)
}
