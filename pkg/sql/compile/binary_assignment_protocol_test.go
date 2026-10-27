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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestBinaryAssignmentFloatConversionComposition(t *testing.T) {
	for _, withBinary := range []bool{false, true} {
		name := "float-only-132"
		if withBinary {
			name = "float-and-binary-135"
		}
		t.Run(name, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			rt := moruntime.ServiceRuntime(c.proc.GetService())
			floatExpr := floatConversionProtocolExpr(function.CAST, types.T_varchar, types.T_float64)
			exprs := []*plan.Expr{floatExpr}
			minimum := defines.MORPCVersion132
			if withBinary {
				exprs = append(exprs, binaryAssignmentProtocolExpr())
				minimum = defines.MORPCVersion135
			}
			qry := &plan.Query{Nodes: []*plan.Node{{ProjectList: exprs}}, Steps: []int32{0}}
			features, err := plan.RequiredRemoteExpressionFeatures(qry)
			require.NoError(t, err)
			require.True(t, features.FloatConversionSemantics)
			require.Equal(t, withBinary, features.BinaryAssignmentSemantics)
			require.Equal(t, minimum, remoteExpressionProtocolVersion(features))

			owner := &plan.TableDef{}
			for _, expr := range exprs {
				owner.Cols = append(owner.Cols, &plan.ColDef{Default: &plan.Default{Expr: expr}})
			}
			data, err := owner.Marshal()
			require.NoError(t, err)
			var persisted plan.TableDef
			require.NoError(t, persisted.Unmarshal(data))
			floor, err := plan2.RequiredPersistedExpressionProtocolVersion(&persisted)
			require.NoError(t, err)
			require.Equal(t, minimum, floor)

			op := projection.NewArgument()
			defer op.Release()
			op.ProjectList = exprs
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
			payload := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: exprs}}}
			cached := &plan.Plan{Plan: &plan.Plan_Query{Query: qry}}
			retained := &Compile{proc: c.proc, pn: cached}
			for _, version := range []int64{defines.MORPCVersion131, defines.MORPCVersion132, defines.MORPCVersion133, defines.MORPCVersion134, defines.MORPCVersion135} {
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
				client.version = version
				c.execType = plan2.ExecTypeAP_MULTICN
				c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
				beforeProbe := client.calls
				require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
				require.Greater(t, client.calls, beforeProbe, "placement must probe the registered fixture worker")
				streamFloor := int64(0)
				_, sendErr := encodeRemoteScopeWithProtocol(scope, c.proc, &streamFloor)
				require.Equal(t, minimum, streamFloor)
				rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
				receiveErr := validateRemoteExpressionPipelineProtocol(c.proc, payload)
				persistedErr := plan2.RequirePersistedExpressionProtocol(c.proc.Ctx, c.proc, &persisted)
				if version < minimum {
					require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType)
					require.ErrorContains(t, sendErr, strconv.FormatInt(minimum, 10))
					require.ErrorContains(t, receiveErr, strconv.FormatInt(minimum, 10))
					require.ErrorContains(t, persistedErr, strconv.FormatInt(minimum, 10))
					if withBinary {
						require.ErrorContains(t, validateTemporalResultProtocol(c.proc, cached), "135")
						require.ErrorContains(t, retained.Compile(c.proc.Ctx, cached, nil), "135")
						_, runErr := retained.Run(0)
						require.ErrorContains(t, runErr, "135")
					}
				} else {
					require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
					require.NoError(t, sendErr)
					require.NoError(t, receiveErr)
					require.NoError(t, persistedErr)
					require.NoError(t, validateTemporalResultProtocol(c.proc, cached))
				}
			}
			require.Equal(t, client.calls, client.releases)
		})
	}
}

func binaryAssignmentProtocolExpr() *plan.Expr {
	typ := plan.Type{Id: int32(types.T_binary), Width: 4}
	return &plan.Expr{Typ: typ, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.CAST_ASSIGN, 0), ObjName: "cast_assign"}, Args: []*plan.Expr{{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}, {Typ: typ, Expr: &plan.Expr_T{T: &plan.TargetType{}}}}}}}
}

func TestBinaryAssignmentPlacementSenderReceiver(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
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
	for _, version := range []int64{defines.MORPCVersion130, defines.MORPCVersion131, defines.MORPCVersion132, defines.MORPCVersion133, defines.MORPCVersion134} {
		client.version = version
		c.cnList = engine.Nodes{{Id: "old-worker", Addr: "remote:6001"}}
		c.execType = plan2.ExecTypeAP_MULTICN
		require.NoError(t, c.constrainRemoteExpressionWorkers(qry))
		require.Equal(t, plan2.ExecTypeAP_ONECN, c.execType, "version=%d", version)
	}
	client.version = defines.MORPCVersion135
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
	require.ErrorContains(t, err, "MORPC version 135")
	client.version = defines.MORPCVersion127
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "MORPC version 135")
	client.version = defines.MORPCVersion128
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "MORPC version 135")
	client.version = defines.MORPCVersion129
	_, err = encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "MORPC version 135")
	for _, version := range []int64{defines.MORPCVersion130, defines.MORPCVersion131, defines.MORPCVersion132, defines.MORPCVersion133, defines.MORPCVersion134} {
		client.version = version
		_, err = encodeRemoteScope(scope, c.proc)
		require.ErrorContains(t, err, "MORPC version 135", "version=%d", version)
	}
	client.version = defines.MORPCVersion135
	minimum := int64(0)
	data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &minimum)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion135, minimum)
	var decoded pipeline.Pipeline
	require.NoError(t, decoded.Unmarshal(data))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion125)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded), "protocol version 135")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion126)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded), "protocol version 135")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion129)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded), "protocol version 135")
	for _, version := range []int64{defines.MORPCVersion130, defines.MORPCVersion131, defines.MORPCVersion132, defines.MORPCVersion133, defines.MORPCVersion134} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded), "protocol version 135", "version=%d", version)
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded))
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion128)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded), "protocol version 135")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion127)
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(c.proc, &decoded), "protocol version 135")
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
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion132)
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion133)
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion134)
			rt.CompareAndDeleteGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion135)
		}
	})
	p := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{
		Nodes: []*plan.Node{{ProjectList: []*plan.Expr{binaryAssignmentProtocolExpr()}}}, Steps: []int32{0},
	}}}
	c := &Compile{proc: proc, pn: p}
	for _, version := range []int64{defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131, defines.MORPCVersion132, defines.MORPCVersion133, defines.MORPCVersion134, defines.MORPCVersion135, defines.MORPCVersion134, defines.MORPCVersion133, defines.MORPCVersion132, defines.MORPCVersion131, defines.MORPCVersion130, defines.MORPCVersion129, defines.MORPCVersion128, defines.MORPCVersion127, defines.MORPCVersion126, defines.MORPCVersion125} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		if version == defines.MORPCVersion135 {
			require.NoError(t, validateTemporalResultProtocol(proc, p))
			continue
		}
		require.ErrorContains(t, validateTemporalResultProtocol(proc, p), "protocol version 135")
		require.ErrorContains(t, c.Compile(proc.Ctx, p, nil), "protocol version 135")
		_, err := c.Run(0)
		require.ErrorContains(t, err, "protocol version 135")
	}
	features := plan.RemoteExpressionFeatures{BinaryAssignmentSemantics: true, LoadFKIgnoreCheck: true, TimestampAddSecondIntervals: true}
	require.Equal(t, defines.MORPCVersion135, remoteExpressionProtocolVersion(features))
	features.BinaryAssignmentSemantics = false
	require.Equal(t, defines.MORPCVersion125, remoteExpressionProtocolVersion(features))
}

func TestBinaryAssignmentProtocolPreservesMainContracts(t *testing.T) {
	// Main assigns typed JSON MIN/MAX to 133 and ODKU result tracking to 134;
	// the independent binary assignment capability follows both.
	require.Less(t, defines.MORPCVersion132, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{BinaryAssignmentSemantics: true}))
	require.Equal(t, defines.MORPCVersion132, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{FloatConversionSemantics: true}))
	require.Equal(t, defines.MORPCVersion135, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{FloatConversionSemantics: true, BinaryAssignmentSemantics: true}))
	require.Equal(t, defines.MORPCVersion135, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{BinaryAssignmentSemantics: true}))
	require.Equal(t, defines.MORPCVersion130, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{IncompleteTemporalStrings: true}))
	require.Equal(t, defines.MORPCVersion135, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{IncompleteTemporalStrings: true, BinaryAssignmentSemantics: true}))
	require.Equal(t, defines.MORPCVersion126, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{ViewMetadataFunctions: true}))
	require.Equal(t, defines.MORPCVersion128, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{JSONNumericComparisons: true}))
	require.Equal(t, defines.MORPCVersion135, remoteExpressionProtocolVersion(
		plan.RemoteExpressionFeatures{JSONNumericComparisons: true, BinaryAssignmentSemantics: true}))
	for _, tc := range []struct {
		features plan.RemoteExpressionFeatures
		floor    int64
	}{
		{plan.RemoteExpressionFeatures{JSONMinMaxTypedComparison: true}, defines.MORPCVersion133},
		{plan.RemoteExpressionFeatures{ODKUStatementResult: true}, defines.MORPCVersion134},
		{plan.RemoteExpressionFeatures{SessionLastInsertIDBaseline: true}, defines.MORPCVersion134},
	} {
		require.Equal(t, tc.floor, remoteExpressionProtocolVersion(tc.features))
		tc.features.BinaryAssignmentSemantics = true
		require.Equal(t, defines.MORPCVersion135, remoteExpressionProtocolVersion(tc.features))
	}
	for _, features := range []plan.RemoteExpressionFeatures{
		{JSONStringConsumerOverload: true},
		{JSONOpaqueTextContracts: true},
	} {
		require.Equal(t, defines.MORPCVersion129, remoteExpressionProtocolVersion(features))
		features.BinaryAssignmentSemantics = true
		require.Equal(t, defines.MORPCVersion135, remoteExpressionProtocolVersion(features))
	}
}

func TestBinaryAssignmentComposesWithMainProtocolAfterSerialization(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	last := &plan.Expr{Typ: plan.Type{Id: 28}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: int64(function.LAST_INSERT_ID) << 32}}}}
	for _, nodes := range [][]*plan.Node{
		{{ProjectList: []*plan.Expr{binaryAssignmentProtocolExpr(), last}, AggList: []*plan.Expr{jsonMinMaxPlanExpr(aggexec.AggIdOfMin)}}, {PreInsertCtx: &plan.PreInsertCtx{TrackOdkuResult: true}}},
		{{PreInsertCtx: &plan.PreInsertCtx{TrackOdkuResult: true}}, {ProjectList: []*plan.Expr{last, binaryAssignmentProtocolExpr()}, AggList: []*plan.Expr{jsonMinMaxPlanExpr(aggexec.AggIdOfMin)}}},
	} {
		owner := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: nodes}}}
		data, err := owner.Marshal()
		require.NoError(t, err)
		var decoded plan.Plan
		require.NoError(t, decoded.Unmarshal(data))
		features, err := plan.RequiredRemoteExpressionFeatures(&decoded)
		require.NoError(t, err)
		require.True(t, features.BinaryAssignmentSemantics)
		require.True(t, features.JSONMinMaxTypedComparison)
		require.True(t, features.ODKUStatementResult)
		require.True(t, features.SessionLastInsertIDBaseline)
		require.Equal(t, defines.MORPCVersion135, remoteExpressionProtocolVersion(features))
		floor, err := plan2.RequiredPersistedExpressionProtocolVersion(&decoded)
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion135, floor)
		for _, version := range []int64{defines.MORPCVersion134, defines.MORPCVersion135} {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			receiveErr := validateRemoteExpressionFeatures(c.proc, features)
			persistedErr := plan2.RequirePersistedExpressionProtocol(c.proc.Ctx, c.proc, &decoded)
			if version < defines.MORPCVersion135 {
				require.ErrorContains(t, receiveErr, "protocol version 135")
				require.ErrorContains(t, persistedErr, "protocol version 135")
			} else {
				require.NoError(t, receiveErr)
				require.NoError(t, persistedErr)
			}
		}
	}
}

// This component control uses the generic handler and actual RPC framing.
// A stale receiver needs no binary-expression walker: its own version reply
// prevents payload send. This proves capability admission, not a rolling
// deployment; the supported upgrade procedure stops all old nodes first.
func TestBinaryAssignmentActualReceiverWire(t *testing.T) {
	const minimum = defines.MORPCVersion135
	for _, version := range []int64{defines.MORPCVersion117, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131, defines.MORPCVersion132, defines.MORPCVersion133, defines.MORPCVersion134, minimum} {
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
