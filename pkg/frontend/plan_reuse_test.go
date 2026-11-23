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

package frontend

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestPlanReuseSeparatesExistingOwners(t *testing.T) {
	query := func(q *plan.Query) *plan.Plan { return &plan.Plan{Plan: &plan.Plan_Query{Query: q}} }
	for _, tc := range []struct {
		name       string
		plan       *plan.Plan
		generation planReuseDecision
		logical    bool
		physical   bool
	}{
		{name: "nil", logical: true, physical: true},
		{name: "nonquery", plan: &plan.Plan{}, generation: planReuseDecision{true, true}, logical: true, physical: true},
		{name: "empty query", plan: query(nil), logical: true, physical: true},
		{name: "ordinary", plan: query(&plan.Query{}), generation: planReuseDecision{true, true}, logical: true, physical: true},
		{name: "FK action", plan: query(&plan.Query{HasForeignKeyAction: true}), generation: planReuseDecision{true, true}},
		{name: "view UDF metadata", plan: query(&plan.Query{ViewMetadataDependsOnUdf: true}), generation: planReuseDecision{true, true}, physical: true},
		{name: "node flag is not a template rebuild policy", plan: query(&plan.Query{Nodes: []*plan.Node{{NodeType: plan.Node_VALUE_SCAN, NotCacheable: true}}}), generation: planReuseDecision{true, true}, logical: true, physical: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.generation, planReuse(false, tc.plan, ordinaryPlanGeneration))
			require.Equal(t, planReuseDecision{valid: true, reusable: tc.logical}, planReuse(false, tc.plan, preparedLogicalPlan))
			require.Equal(t, planReuseDecision{valid: true, reusable: tc.physical}, planReuse(false, tc.plan, preparedPhysicalCompile))
		})
	}
	// Invalid generations must not acquire AST ownership or invalidate a live
	// generation as though they were a valid execution-dependent replacement.
	malformed := &plan.Plan{Plan: (*plan.Plan_Query)(nil)}
	require.Equal(t, planReuseDecision{}, planReuse(false, malformed, ordinaryPlanGeneration))
	require.Equal(t, planReuseDecision{valid: true}, planReuse(true, nil, preparedLogicalPlan))
	require.Equal(t, planReuseDecision{}, planReuse(false, query(&plan.Query{}), planReuseLayer(255)))
}
