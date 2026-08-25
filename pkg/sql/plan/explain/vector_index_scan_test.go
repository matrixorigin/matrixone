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
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestVectorIndexScanInfoIsTypedAndVisible(t *testing.T) {
	node := &plan.Node{
		NodeType: plan.Node_VECTOR_INDEX_SCAN,
		Stats:    &plan.Stats{},
		VectorIndexScan: &plan.VectorIndexScan{
			Index:             &plan.IndexDef{IndexName: "idx_v", IndexAlgo: "ivfflat"},
			DistanceFunction:  "l2_distance",
			CandidateLimit:    plan2.MakePlan2Uint64ConstExprWithType(12),
			InitialProbeCount: 4,
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
}
