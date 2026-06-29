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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCanPullupDeepCorrelatedPredicates(t *testing.T) {
	for _, tc := range []struct {
		typ  plan.SubqueryRef_Type
		want bool
	}{
		{typ: plan.SubqueryRef_EXISTS, want: true},
		{typ: plan.SubqueryRef_NOT_EXISTS, want: true},
		{typ: plan.SubqueryRef_IN, want: true},
		{typ: plan.SubqueryRef_NOT_IN, want: true},
		{typ: plan.SubqueryRef_ANY, want: true},
		{typ: plan.SubqueryRef_ALL, want: true},
		{typ: plan.SubqueryRef_SCALAR, want: false},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			require.Equal(t, tc.want, canPullupDeepCorrelatedPredicates(tc.typ))
		})
	}
}

func TestHasInnerColumnInDeepCorrelatedFilters(t *testing.T) {
	const (
		subID    int32 = 0
		innerTag int32 = 1
		outerTag int32 = 2
	)

	builder := &QueryBuilder{
		qry: &plan.Query{
			Nodes: []*plan.Node{
				{
					NodeId:      subID,
					NodeType:    plan.Node_TABLE_SCAN,
					BindingTags: []int32{innerTag},
				},
			},
		},
	}

	require.False(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, nil))
	require.False(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, []*plan.Expr{}))
	require.True(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, []*plan.Expr{
		newFlattenSubqueryTestColExpr(innerTag),
	}))
	require.False(t, builder.hasInnerColumnInDeepCorrelatedFilters(subID, []*plan.Expr{
		newFlattenSubqueryTestColExpr(outerTag),
	}))
}

func newFlattenSubqueryTestColExpr(tag int32) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: tag,
			},
		},
	}
}
