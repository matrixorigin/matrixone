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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type viewDefTestCtx struct{}

func (viewDefTestCtx) GetContext() context.Context { return context.Background() }
func (viewDefTestCtx) ResolveVariable(string, bool, bool) (interface{}, error) {
	return nil, nil
}

func ftQuery(fnName string) *plan.Query {
	return &plan.Query{
		Steps: []int32{1},
		Nodes: []*plan.Node{
			{NodeId: 0, NodeType: plan.Node_TABLE_SCAN},
			{NodeId: 1, NodeType: plan.Node_PROJECT, Children: []int32{0},
				FilterList: []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: fnName},
				}}}},
			},
		},
	}
}

// TestValidateViewDefinition_RejectsUnservedMatch: a MATCH() no FULLTEXT index can serve
// survives optimization as one of two unevaluable placeholders. fulltext has no
// brute-force fallback, so persisting such a view yields an object that can never be
// queried -- and via ALTER / CREATE OR REPLACE it destroys a working definition (#27027).
func TestValidateViewDefinition_RejectsUnservedMatch(t *testing.T) {
	for _, fnName := range []string{"fulltext_match", "fulltext_match_score"} {
		t.Run(fnName, func(t *testing.T) {
			err := Hooks{}.ValidateViewDefinition(viewDefTestCtx{}, ftQuery(fnName))
			require.Error(t, err)
			require.Contains(t, err.Error(), "Can't find FULLTEXT index matching the column list")
		})
	}
}

// TestValidateViewDefinition_AllowsServedAndUnrelated: once the rewrite has replaced the
// placeholder with an index scan nothing is left to refuse, and a view with no MATCH at all
// is never this plugin's business.
func TestValidateViewDefinition_AllowsServedAndUnrelated(t *testing.T) {
	require.NoError(t, Hooks{}.ValidateViewDefinition(viewDefTestCtx{}, nil))
	require.NoError(t, Hooks{}.ValidateViewDefinition(viewDefTestCtx{}, &plan.Query{}))

	rewritten := &plan.Query{
		Steps: []int32{2},
		Nodes: []*plan.Node{
			{NodeId: 0, NodeType: plan.Node_TABLE_SCAN},
			{NodeId: 1, NodeType: plan.Node_FUNCTION_SCAN},
			{NodeId: 2, NodeType: plan.Node_JOIN, Children: []int32{0, 1}},
		},
	}
	require.NoError(t, Hooks{}.ValidateViewDefinition(viewDefTestCtx{}, rewritten))

	require.NoError(t, Hooks{}.ValidateViewDefinition(viewDefTestCtx{}, ftQuery("l2_distance")))
}
