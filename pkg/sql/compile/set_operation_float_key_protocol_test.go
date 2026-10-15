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
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSetOperationFloatKeyProtocolPlacementSendReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "set_operation_float_key", []*planpb.Expr{plan2.GetColExpr(types.T_float64.ToType().PlanType(), 0, 0)})
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.SetOperationFloatKeys)
	require.Equal(t, defines.MORPCVersion116, remoteExpressionProtocolVersion(features))
	qry := &planpb.Query{Nodes: []*planpb.Node{{PhysicalEqualityKeyList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	place := func(version int64, want plan2.ExecType) {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		require.Equal(t, want, c.execType)
	}
	place(defines.MORPCVersion115, plan2.ExecTypeAP_ONECN)
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 116")
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, features), "versioned remote destination")
	place(defines.MORPCVersion116, plan2.ExecTypeAP_MULTICN)
	_, err = encodeRemoteScope(scope, c.proc)
	require.NoError(t, err)
	client.version = defines.MORPCVersion115
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 116", "send checks changed actual destination")
	client.customResponse = true
	client.response = &query.Response{}
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 116")
	require.Equal(t, client.calls, client.releases)
	p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	data, err := p.Marshal()
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, version := range []int64{defines.MORPCVersion115, defines.MORPCVersion116} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		validationErr := validateRemoteExpressionPipelineProtocol(c.proc, p)
		_, decodeErr := decodeScope(data, c.proc, true, nil)
		if version == defines.MORPCVersion115 {
			require.ErrorContains(t, validationErr, "version 116")
			require.ErrorContains(t, decodeErr, "version 116")
		} else {
			require.NoError(t, validationErr)
			require.NoError(t, decodeErr)
		}
	}
}

// Exercise the real compiler producer: the key is hidden in SetOp.KeyExprs or
// Group.GroupBy, rather than an artificial visible projection.
func TestCompiledSetOperationFloatKeyProtocol(t *testing.T) {
	for _, kind := range []planpb.Node_NodeType{planpb.Node_INTERSECT_ALL, planpb.Node_UNION} {
		t.Run(kind.String(), func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			c.anal = &AnalyzeModule{curNodeIdx: 1, isFirst: true}
			c.execType = plan2.ExecTypeAP_ONECN
			c.isPrepare = true
			visible := plan2.GetColExpr(types.T_float64.ToType().PlanType(), 0, 0)
			key, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "set_operation_float_key", []*planpb.Expr{plan2.DeepCopyExpr(visible)})
			require.NoError(t, err)
			node := &planpb.Node{NodeType: kind, ProjectList: []*planpb.Expr{visible}, PhysicalEqualityKeyList: []*planpb.Expr{key}}
			input := func() *Scope {
				return &Scope{Magic: Normal, NodeInfo: engine.Node{Mcpu: 1}, Proc: c.proc.NewNoContextChildProc(0), RootOp: projection.NewArgument()}
			}
			left, right := []*Scope{input()}, []*Scope{input()}
			var result []*Scope
			if kind == planpb.Node_UNION {
				result = c.compileUnion(node, left, right)
			} else {
				result = c.compileMinusAndIntersect(node, left, right, kind)
			}
			t.Cleanup(func() {
				for _, scope := range result {
					scope.FreeOperator(c)
					scope.release()
				}
				c.proc.Free()
			})
			require.Len(t, result, 1)
			scope := result[0]
			if kind == planpb.Node_UNION {
				require.Len(t, scope.RootOp.(*group.Group).GroupBy, 2)
			} else {
				require.Len(t, scope.RootOp.(*intersectall.IntersectAll).KeyExprs, 1)
			}
			scope.Magic = Remote
			scope.NodeInfo.Id = "old-worker"
			scope.NodeInfo.Addr = "remote:6001"
			client.version = defines.MORPCVersion115
			_, err = encodeRemoteScope(scope, c.proc)
			require.ErrorContains(t, err, "version 116")
			require.Positive(t, client.calls, "actual stage must probe its destination")
			client.version = defines.MORPCVersion116
			data, err := encodeRemoteScope(scope, c.proc)
			require.NoError(t, err)
			var wire pipeline.Pipeline
			require.NoError(t, wire.Unmarshal(data))
			features, err := planpb.RequiredRemoteExpressionFeatures(&wire)
			require.NoError(t, err)
			require.True(t, features.SetOperationFloatKeys)
			require.Equal(t, client.calls, client.releases)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion115)
			_, err = decodeScope(data, c.proc, true, nil)
			require.ErrorContains(t, err, "version 116")
		})
	}
}
