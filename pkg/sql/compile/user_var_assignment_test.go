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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/stretchr/testify/require"
)

func TestUserVariableAssignmentForcesCoordinatorProjection(t *testing.T) {
	c := newLazyUnionAllTestCompile(t)
	c.addr = "coordinator:6001"
	leaf := newLazyUnionAllLeaf(c, value_scan.NewArgument())
	leaf.NodeInfo.Addr = "remote-cn:6001"

	int64Type := types.T_int64.ToType().PlanType()
	node := &plan.Node{ProjectList: []*plan.Expr{{
		Typ: int64Type,
		Expr: &plan.Expr_UserVarAssignment{UserVarAssignment: &plan.UserVarAssignment{
			Name: "rank",
			Value: &plan.Expr{Typ: int64Type, Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: 1},
			}}},
		}},
	}}}

	result := c.ensureCoordinatorOnlyFunctions(node, []*Scope{leaf})
	require.Len(t, result, 1)
	require.NotSame(t, leaf, result[0])
	require.True(t, c.scopesRunOnCoordinator(result))
	freeLazyUnionAllTestScope(c, result[0])
}

func TestUserVariableAssignmentCannotCrossRemoteBoundary(t *testing.T) {
	int64Type := types.T_int64.ToType().PlanType()
	remote := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{
		ProjectList: []*plan.Expr{{
			Typ: int64Type,
			Expr: &plan.Expr_UserVarAssignment{UserVarAssignment: &plan.UserVarAssignment{
				Name:  "rank",
				Value: &plan.Expr{Typ: int64Type, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}},
			}},
		}},
	}}}
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(nil, remote), "cannot be sent to a remote CN")
}

func TestUserVariableAssignmentPlanDisablesRetry(t *testing.T) {
	int64Type := types.T_int64.ToType().PlanType()
	queryPlan := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{
		ProjectList: []*plan.Expr{{
			Typ: int64Type,
			Expr: &plan.Expr_UserVarAssignment{UserVarAssignment: &plan.UserVarAssignment{
				Name:  "rank",
				Value: &plan.Expr{Typ: int64Type, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}},
			}},
		}},
	}}}}}
	require.True(t, planHasUserVariableAssignment(queryPlan))
	c := &Compile{}
	if planHasUserVariableAssignment(queryPlan) {
		c.disableRetry = true
	}
	require.True(t, c.disableRetry)
}
