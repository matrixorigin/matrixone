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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

// Shuffle joins publish expression-less runtime-filter specs as PASS markers.
// They are transport control messages, not predicates. Stats are deliberately
// varied here because optimizer hints may replace them after the markers have
// already been generated.
func TestRuntimeFilterProbeExplain(t *testing.T) {
	tests := []struct {
		name  string
		stats *plan.Stats
		specs []*plan.RuntimeFilterSpec
		want  string
	}{
		{
			name:  "pass marker without stats",
			specs: []*plan.RuntimeFilterSpec{{Tag: 1}},
		},
		{
			name:  "nil and pass marker without hashmap stats",
			stats: &plan.Stats{},
			specs: []*plan.RuntimeFilterSpec{nil, {Tag: 1}},
		},
		{
			name:  "mixed markers and predicates",
			stats: &plan.Stats{HashmapStats: &plan.HashMapStats{}},
			specs: []*plan.RuntimeFilterSpec{
				{Tag: 1},
				{Tag: 2, Expr: runtimeFilterTestColumn("probe_id"), MatchPrefix: true},
				{Tag: 3},
				{Tag: 4, Expr: runtimeFilterTestColumn("probe_id_2")},
			},
			want: "Runtime Filter Probe: probe_id Match Prefix, probe_id_2",
		},
		{
			name: "ordinary predicate without stats",
			specs: []*plan.RuntimeFilterSpec{
				{Tag: 1, Expr: runtimeFilterTestColumn("probe_id")},
			},
			want: "Runtime Filter Probe: probe_id",
		},
		{
			name: "shuffle pass marker",
			stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
				Shuffle: true,
			}},
			specs: []*plan.RuntimeFilterSpec{{Tag: 1}},
		},
		{
			name: "explicit predicate survives stale shuffle stats",
			stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
				Shuffle: true,
			}},
			specs: []*plan.RuntimeFilterSpec{
				{Tag: 1, Expr: runtimeFilterTestColumn("probe_id")},
			},
			want: "Runtime Filter Probe: probe_id",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := &plan.Node{
				NodeType:               plan.Node_JOIN,
				Stats:                  test.stats,
				RuntimeFilterProbeList: test.specs,
			}
			got, err := NewNodeDescriptionImpl(node).GetRuntimeFilteProbeInfo(
				context.Background(),
				NewExplainDefaultOptions(),
			)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestRuntimeFilterBuildExplain(t *testing.T) {
	tests := []struct {
		name  string
		stats *plan.Stats
		specs []*plan.RuntimeFilterSpec
		want  string
	}{
		{
			name:  "pass marker without stats",
			specs: []*plan.RuntimeFilterSpec{nil, {Tag: 1}},
		},
		{
			name:  "build expression and legacy fallback",
			stats: &plan.Stats{},
			specs: []*plan.RuntimeFilterSpec{
				{Tag: 1},
				{
					Tag:       2,
					Expr:      runtimeFilterTestColumn("ignored_probe_key"),
					BuildExpr: runtimeFilterTestColumn("build_id"),
				},
				{Tag: 3, Expr: runtimeFilterTestColumn("legacy_build_id")},
			},
			want: "Runtime Filter Build: build_id, legacy_build_id",
		},
		{
			name: "shuffle pass marker",
			stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
				Shuffle: true,
			}},
			specs: []*plan.RuntimeFilterSpec{{Tag: 1}},
		},
		{
			name: "explicit predicate survives stale shuffle stats",
			stats: &plan.Stats{HashmapStats: &plan.HashMapStats{
				Shuffle: true,
			}},
			specs: []*plan.RuntimeFilterSpec{
				{Tag: 1, BuildExpr: runtimeFilterTestColumn("build_id")},
			},
			want: "Runtime Filter Build: build_id",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			node := &plan.Node{
				NodeType:               plan.Node_JOIN,
				Stats:                  test.stats,
				RuntimeFilterBuildList: test.specs,
			}
			got, err := NewNodeDescriptionImpl(node).GetRuntimeFilterBuildInfo(
				context.Background(),
				NewExplainDefaultOptions(),
			)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestRuntimeFilterPassMarkersDoNotCreateExtraInfoLabels(t *testing.T) {
	node := &plan.Node{
		NodeType:               plan.Node_JOIN,
		Stats:                  &plan.Stats{},
		RuntimeFilterProbeList: []*plan.RuntimeFilterSpec{{Tag: 1}},
		RuntimeFilterBuildList: []*plan.RuntimeFilterSpec{{Tag: 1}},
	}

	got, err := NewNodeDescriptionImpl(node).GetExtraInfo(
		context.Background(),
		NewExplainDefaultOptions(),
	)
	require.NoError(t, err)
	require.Equal(t, []string{"Join Type: INNER"}, got)
}

func runtimeFilterTestColumn(name string) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{Name: name}},
	}
}
