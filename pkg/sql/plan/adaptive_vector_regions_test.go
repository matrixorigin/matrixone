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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestForceMultipleAdaptiveVectorRegions(t *testing.T) {
	makeSort := func(child int32) *planpb.Node {
		return &planpb.Node{
			NodeType:   planpb.Node_SORT,
			Children:   []int32{child},
			Limit:      makePlan2Int64ConstExprWithType(10),
			RankOption: &planpb.RankOption{Mode: "auto"},
		}
	}
	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{
		{NodeType: planpb.Node_TABLE_SCAN},
		makeSort(0),
		{NodeType: planpb.Node_TABLE_SCAN},
		makeSort(2),
		{NodeType: planpb.Node_JOIN, Children: []int32{1, 3}},
	}}}

	builder.forceMultipleAdaptiveVectorRegions(4)
	require.Equal(t, "force", builder.qry.Nodes[1].RankOption.Mode)
	require.Equal(t, "force", builder.qry.Nodes[3].RankOption.Mode)
}

func TestKeepSingleAdaptiveVectorRegion(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{
		{NodeType: planpb.Node_TABLE_SCAN},
		{
			NodeType:   planpb.Node_SORT,
			Children:   []int32{0},
			Limit:      makePlan2Int64ConstExprWithType(10),
			RankOption: &planpb.RankOption{Mode: "auto"},
		},
	}}}

	builder.forceMultipleAdaptiveVectorRegions(1)
	require.Equal(t, "auto", builder.qry.Nodes[1].RankOption.Mode)
}
