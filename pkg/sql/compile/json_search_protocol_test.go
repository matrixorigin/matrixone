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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func bindJSONSearchProtocolExpr(t *testing.T, c *Compile) *planpb.Expr {
	t.Helper()
	args := make([]*planpb.Expr, 3)
	for i := range args {
		args[i] = &planpb.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: int32(i)}}}
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(c.proc.Ctx, "json_search", args)
	require.NoError(t, err)
	return expr
}

// Component tests exercise real plan/stream owners with simulated peers; they
// do not claim old-binary or mixed-cluster execution.
func TestJSONSearchRemoteProtocolFloor(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	expr := bindJSONSearchProtocolExpr(t, c)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	require.True(t, features.JSONSearch)
	require.Equal(t, defines.MORPCVersion136, remoteExpressionProtocolVersion(features))
	features.ODKUStatementResult = true
	require.Equal(t, defines.MORPCVersion136, remoteExpressionProtocolVersion(features))
}

func TestJSONSearchProtocolPlacementSendReceive(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	expr := bindJSONSearchProtocolExpr(t, c)
	features, err := planpb.RequiredRemoteExpressionFeatures(expr)
	require.NoError(t, err)
	qry := &planpb.Query{Nodes: []*planpb.Node{{ProjectList: []*planpb.Expr{expr}}}, Steps: []int32{0}}
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*planpb.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, tc := range []struct {
		version int64
		want    plan2.ExecType
	}{
		{0, plan2.ExecTypeAP_ONECN},
		{defines.MORPCVersion134, plan2.ExecTypeAP_ONECN},
		{defines.MORPCVersion136 - 1, plan2.ExecTypeAP_ONECN},
		{defines.MORPCVersion136, plan2.ExecTypeAP_MULTICN},
	} {
		client.version = tc.version
		c.execType = plan2.ExecTypeAP_MULTICN
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001", Mcpu: 4}}
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		require.Equal(t, tc.want, c.execType)
	}
	client.version = defines.MORPCVersion134
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 136")
	client.version = defines.MORPCVersion136
	minimum := int64(0)
	data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &minimum)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion136, minimum)
	client.version = defines.MORPCVersion134
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 136", "destination can change after placement")
	client.customResponse = true
	client.response = &query.Response{}
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "version 136")
	require.Equal(t, client.calls, client.releases)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	wire := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*planpb.Expr{expr}}}}
	for _, version := range []int64{0, defines.MORPCVersion134, defines.MORPCVersion136 - 1, defines.MORPCVersion136} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		err = validateRemoteExpressionPipelineProtocol(c.proc, wire)
		_, decodeErr := decodeScope(data, c.proc, true, nil)
		if version < defines.MORPCVersion136 {
			require.ErrorContains(t, err, "version 136")
			require.ErrorContains(t, decodeErr, "version 136")
			require.ErrorContains(t, validateJSONStringConsumerProtocol(c.proc, qry), "version 136")
		} else {
			require.NoError(t, err)
			require.NoError(t, decodeErr)
			require.NoError(t, validateJSONStringConsumerProtocol(c.proc, qry))
		}
	}
	decoded := new(pipeline.Pipeline)
	require.NoError(t, decoded.Unmarshal(data))
	decodedFeatures, err := pipelineExpressionFeatures(decoded)
	require.NoError(t, err)
	require.True(t, decodedFeatures.JSONSearch)
	require.True(t, features.JSONSearch)
}

func TestJSONSearchRechecksActualStream(t *testing.T) {
	for _, version := range []int64{0, defines.MORPCVersion134, defines.MORPCVersion136 - 1, defines.MORPCVersion136} {
		stream := &fakeStreamSender{}
		replies := make(chan morpc.Message, 1)
		replies <- &pipeline.Message{Id: 0, Cmd: pipeline.Method_PipelineProtocolCheck, Sid: pipeline.Status_Last, ProtocolVersion: version}
		sender := &messageSenderOnClient{ctx: context.Background(), streamSender: stream, receiveCh: replies}
		err := sender.confirmProtocolOnStream(defines.MORPCVersion136)
		if version < defines.MORPCVersion136 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, defines.MORPCVersion136, stream.sent[0].(*pipeline.Message).ProtocolVersion)
	}
}
