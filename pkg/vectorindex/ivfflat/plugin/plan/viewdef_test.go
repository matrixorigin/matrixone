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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type vecViewCtx struct{}

func (vecViewCtx) GetContext() context.Context { return context.Background() }

// GetProcess returns nil: these tests plan without a service runtime, and the capability
// gates read that as "not activated" and build the legacy shape.
func (vecViewCtx) GetProcess() *process.Process { return nil }

func (vecViewCtx) ResolveVariable(string, bool, bool) (interface{}, error) {
	return nil, nil
}

// TestValidateViewDefinition_NeverRefuses: a vector index is an optimization, not a
// precondition. A view whose plan misses the index still runs as a brute-force scan and
// sort, so view DDL must not reject it -- rejecting would break ordinary working views.
func TestValidateViewDefinition_NeverRefuses(t *testing.T) {
	query := &plan.Query{
		Steps: []int32{1},
		Nodes: []*plan.Node{
			{NodeId: 0, NodeType: plan.Node_TABLE_SCAN},
			{NodeId: 1, NodeType: plan.Node_SORT, Children: []int32{0},
				OrderBy: []*plan.OrderBySpec{{Expr: &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: "l2_distance"},
				}}}}},
			},
		},
	}
	require.NoError(t, Hooks{}.ValidateViewDefinition(vecViewCtx{}, query))
	require.NoError(t, Hooks{}.ValidateViewDefinition(vecViewCtx{}, nil))
	require.Nil(t, Hooks{}.ValidateViewDefinition(vecViewCtx{}, &plan.Query{}))
}
