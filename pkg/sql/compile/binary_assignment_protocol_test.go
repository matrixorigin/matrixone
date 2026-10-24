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
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice/cnclient"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func binaryAssignmentProtocolExpr() *plan.Expr {
	typ := plan.Type{Id: int32(types.T_binary), Width: 4}
	return &plan.Expr{Typ: typ, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.CAST_ASSIGN, 0), ObjName: "cast_assign"}, Args: []*plan.Expr{{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}, {Typ: typ, Expr: &plan.Expr_T{T: &plan.TargetType{}}}}}}}
}

func TestBinaryAssignmentPlacementSenderReceiver(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
	expr := binaryAssignmentProtocolExpr()
	qry := &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{expr}}}, Steps: []int32{0}}
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	c.execType = plan2.ExecTypeAP_MULTICN
	client.version = defines.MORPCVersion125
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	client.version = defines.MORPCVersion126
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	c.execType = plan2.ExecTypeAP_MULTICN
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	client.version = defines.MORPCVersion127
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	c.execType = plan2.ExecTypeAP_MULTICN
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	c.execType = plan2.ExecTypeAP_MULTICN
	client.version = defines.MORPCVersion128
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	client.version = defines.MORPCVersion129
	c.cnList = engine.Nodes{{Id: "json-worker", Addr: "remote:6001"}}
	c.execType = plan2.ExecTypeAP_MULTICN
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
	client.version = defines.MORPCVersion130
	c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
	c.execType = plan2.ExecTypeAP_MULTICN
	require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
	require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
	op := projection.NewArgument()
	defer op.Release()
	op.ProjectList = []*plan.Expr{expr}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	client.version = defines.MORPCVersion125
	_, err := encodeRemoteScope(scope, c.proc)
	require.Error(t, err)
	client.version = defines.MORPCVersion126
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "MORPC version 130")
	client.version = defines.MORPCVersion127
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "MORPC version 130")
	client.version = defines.MORPCVersion128
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "MORPC version 130")
	client.version = defines.MORPCVersion129
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "MORPC version 130")
	client.version = defines.MORPCVersion130
	minimum := int64(0)
	data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &minimum)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion130, minimum)
	var decoded pipeline.Pipeline
	require.NoError(t, decoded.Unmarshal(data))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion125)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded), "protocol version 130")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded), "protocol version 130")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded), "protocol version 130")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion128)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded), "protocol version 130")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion127)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded), "protocol version 130")
	scope.NodeInfo.Addr = "stale:6001"
	_, err = encodeRemoteScope(scope, c.proc)
	require.Error(t, err)
}

func TestBinaryAssignmentLocalAndRetainedPlan(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, hadPrevious := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	t.Cleanup(func() {
		if hadPrevious {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
		} else {
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion125)
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion127)
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion128)
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
		}
	})
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Nodes: []*plan.Node{{ProjectList: []*plan.Expr{binaryAssignmentProtocolExpr()}}}, Steps: []int32{0},
	}}}
	c := &Compile{proc: proc, pn: p}
	for _, version := range []int64{defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion129, defines.MORPCVersion128, defines.MORPCVersion127, defines.MORPCVersion126, defines.MORPCVersion125} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		if version == defines.MORPCVersion130 {
			require.NoError(t, validateTemporalResultProtocol(proc, p))
			continue
		}
		require.ErrorContains(t, validateTemporalResultProtocol(proc, p), "protocol version 130")
		require.ErrorContains(t, c.Compile(proc.Ctx, p, nil), "protocol version 130")
		_, err := c.Run(0)
		require.ErrorContains(t, err, "protocol version 130")
	}
	features := plan.RemoteExpressionFeatures{BinaryAssignmentSemantics: true, LoadFKIgnoreCheck: true, TimestampAddSecondIntervals: true}
	require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(features))
	features.BinaryAssignmentSemantics = false
	require.Equal(t, defines.MORPCVersion125, remoteExpressionProtocolVersion(features))
}

func TestBinaryAssignmentProtocolPreservesMainContracts(t *testing.T) {
	// Main129 adds JSON ELT execution independently of binary assignments.
	require.Less(t, defines.MORPCVersion129, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{BinaryAssignmentSemantics: true}))
	require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{BinaryAssignmentSemantics: true}))
	require.Equal(t, defines.MORPCVersion126, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{ViewMetadataFunctions: true}))
	require.Equal(t, defines.MORPCVersion128, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{JSONNumericComparisons: true}))
	require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{JSONNumericComparisons: true, BinaryAssignmentSemantics: true}))
	for _, features := range []plan.RemoteExpressionFeatures{
		{JSONStringConsumerOverload: true},
		{JSONOpaqueTextContracts: true},
	} {
		require.Equal(t, defines.MORPCVersion129, remoteExpressionProtocolVersion(features))
		features.BinaryAssignmentSemantics = true
		require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(features))
	}
}

// This component control uses the generic handler and actual RPC framing.
// A stale receiver needs no binary-expression walker: its own version reply
// prevents payload send. This proves capability admission, not a rolling
// deployment; the supported upgrade procedure stops all old nodes first.
func TestBinaryAssignmentActualReceiverWire(t *testing.T) {
	const minimum = defines.MORPCVersion130
	for _, version := range []int64{defines.MORPCVersion117, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, minimum} {
		t.Run(strconv.FormatInt(version, 10), func(t *testing.T) {
			sid := t.Name()
			moruntime.SetupServiceBasedRuntime(sid, moruntime.DefaultRuntime())
			rt := moruntime.ServiceRuntime(sid)
			// A prior sender-side observation succeeded. The actual instance changes
			// before this stream is opened, so the fresh reply must dominate that result.
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, minimum)
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			dir, err := os.MkdirTemp("/tmp", "binary-wire-")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			addr := "unix://" + filepath.Join(dir, "b.sock")
			server, err := morpc.NewRPCServer(sid, addr, morpc.NewMessageCodec(sid, func() morpc.Message { return &pipeline.Message{} }))
			require.NoError(t, err)
			var payloads atomic.Int32
			server.RegisterRequestHandler(func(ctx context.Context, request morpc.RPCMessage, _ uint64, cs morpc.ClientSession) error {
				msg := request.Message.(*pipeline.Message)
				if msg.GetCmd() == pipeline.Method_PipelineProtocolCheck {
					return handlePipelineProtocolCheck(ctx, msg, cs, sid, func() morpc.Message { return &pipeline.Message{} })
				}
				payloads.Add(1)
				reply := &pipeline.Message{Id: msg.Id, Cmd: msg.Cmd, Sid: pipeline.Status_Last}
				return cs.Write(ctx, reply)
			})
			require.NoError(t, server.Start())
			defer server.Close()
			client, err := cnclient.NewPipelineClient(sid, "", &cnclient.PipelineConfig{})
			require.NoError(t, err)
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			stream, err := client.NewStream(ctx, addr)
			require.NoError(t, err)
			defer stream.Close(true)
			replies, err := stream.Receive()
			require.NoError(t, err)
			sender := &messageSenderOnClient{ctx: ctx, streamSender: stream, receiveCh: replies}
			err = sender.confirmProtocolOnStream(minimum)
			if version < minimum {
				require.Error(t, err)
				require.Zero(t, payloads.Load())
				return
			}
			require.NoError(t, err)
			message := &pipeline.Message{Id: stream.ID(), Cmd: pipeline.Method_PipelineMessage, Data: []byte("payload"), Sid: pipeline.Status_Last}
			require.NoError(t, stream.Send(ctx, message))
			select {
			case <-replies:
				require.Equal(t, int32(1), payloads.Load())
			case <-ctx.Done():
				t.Fatal("payload acknowledgement timeout")
			}
		})
	}
}
