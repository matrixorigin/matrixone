package plan

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type vecViewCtx struct{}

func (vecViewCtx) GetContext() context.Context { return context.Background() }
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
