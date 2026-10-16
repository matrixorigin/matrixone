// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestTableCollationProtocolPlacementSendReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	arg := &planpb.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: ""}}}}
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "internal_table_collation", []*planpb.Expr{arg})
	require.NoError(t, err)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.TableCollationFunction)
	require.Equal(t, defines.MORPCVersion118, remoteExpressionProtocolVersion(features))
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, version := range []int64{113, 114, 115, 116, 117, 118, 117} {
		client.version = version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{scope.NodeInfo}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		_, err := encodeRemoteScope(scope, c.proc)
		if version < 118 {
			require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
			require.ErrorContains(t, err, "version 118")
		} else {
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			require.NoError(t, err)
		}
	}
	require.ErrorContains(t, validateRemoteExpressionDestination(c.proc, nil, features), "versioned remote destination")
	require.ErrorContains(t, validateRemoteExpressionDestination(nil, &pipeline.Pipeline{Node: &pipeline.NodeInfo{}}, features), "version 118")
	client.customResponse = true
	client.response = &query.Response{}
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 118", "an unknown destination fails closed")
	require.Equal(t, client.calls, client.releases)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	for _, nested := range []bool{false, true} {
		p := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{Op: int32(vm.Projection), ProjectList: []*planpb.Expr{expr}}}}
		if nested {
			p = &pipeline.Pipeline{Children: []*pipeline.Pipeline{p}}
		}
		data, err := p.Marshal()
		require.NoError(t, err)
		require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(nil, p), "version 118")
		for _, version := range []any{nil, "118", int64(113), int64(114), int64(115), int64(116), int64(117), int64(118)} {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			decoded, err := decodeScope(data, c.proc, true, nil)
			if decoded != nil {
				decoded.release()
			}
			if version == int64(118) {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "version 118")
				require.Nil(t, decoded, "reject before allocating operator owners")
			}
		}
	}
}
