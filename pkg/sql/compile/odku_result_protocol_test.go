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

package compile

import (
	"context"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/preinsert"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	planbuilder "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// These component tests use capability replies and wire decoding; they do not
// execute an old binary or establish a real mixed-binary topology.
func TestODKUTrackingRequiresActualDestinationAndStreamFloor(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	op := dedupjoin.NewArgument()
	defer op.Release()
	op.ODKUResultTracking = true
	op.Conditions = [][]*plan.Expr{{}, {}}
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	client.version = defines.MORPCVersion130
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination does not support versioned expression semantics (MORPC version 131)")
	client.version = defines.MORPCVersion131
	minimum := int64(0)
	data, err := encodeRemoteScopeWithProtocol(scope, c.proc, &minimum)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion131, minimum)
	decoded := new(pipeline.Pipeline)
	require.NoError(t, decoded.Unmarshal(data))
	features, err := pipelineExpressionFeatures(decoded)
	require.NoError(t, err)
	require.True(t, features.ODKUStatementResult)
	require.False(t, features.SessionLastInsertIDBaseline)
	require.Equal(t, defines.MORPCVersion131, remoteExpressionProtocolVersion(features))
	scope.NodeInfo.Addr = "stale:6001"
	_, err = encodeRemoteScope(scope, c.proc)
	require.Error(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion130)
	require.ErrorContains(t, validateRemoteExpressionFeatures(c.proc, features), "ODKU statement result requires MORPC protocol version 131")
}

func TestODKUTrackingRechecksReplacementOnActualStream(t *testing.T) {
	for _, version := range []int64{defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130, defines.MORPCVersion131} {
		stream := &fakeStreamSender{}
		replies := make(chan morpc.Message, 1)
		replies <- &pipeline.Message{Id: 0, Cmd: pipeline.Method_PipelineProtocolCheck, Sid: pipeline.Status_Last, ProtocolVersion: version}
		sender := &messageSenderOnClient{ctx: context.Background(), streamSender: stream, receiveCh: replies}
		err := sender.confirmProtocolOnStream(defines.MORPCVersion131)
		if version < defines.MORPCVersion131 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, defines.MORPCVersion131, stream.sent[0].(*pipeline.Message).ProtocolVersion)
	}
}

func TestLASTInsertIDBaselineUsesActualSerializedOwner(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCVersion131
	op := projection.NewArgument()
	defer op.Release()
	expr := &plan.Expr{Typ: plan.Type{Id: 28}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: int64(function.LAST_INSERT_ID) << 32}}}}
	op.ProjectList = []*plan.Expr{expr}
	actual := c.proc.NewNoContextChildProc(0)
	actual.Ctx = defines.AttachAccountId(context.Background(), 0)
	actual.Base.TxnOperator = fakeTxnOperator{}
	actual.Base.SessionInfo.TimeZone = time.UTC
	actual.SetResolveVariableFunc(nil)
	actual.Base.SessionInfo.LastInsertIDSet = false
	scope := &Scope{Magic: Remote, Proc: actual, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for _, baseline := range []uint64{0, 700} {
		actual.SetLastInsertID(baseline)
		minimum := int64(0)
		_, _, data, _, err := prepareRemoteRunSendingDataWithProtocol("", scope, c.proc, nil, uuid.Nil, &minimum)
		require.NoError(t, err, "a real owned default0 is capturable without resolver/wire presence")
		require.Equal(t, defines.MORPCVersion131, minimum)
		info := new(pipeline.ProcessInfo)
		require.NoError(t, info.Unmarshal(data))
		require.True(t, info.SessionInfo.LastInsertIdSet)
		require.Equal(t, baseline, info.SessionInfo.LastInsertId)
	}
	// The capability/root Process must not lend an owner to the unrelated
	// Process actually serialized by prepareRemoteRunSendingData.
	other := NewMockCompile(t).proc
	other.Ctx = defines.AttachAccountId(context.Background(), 0)
	other.Base.LastInsertID = nil
	scope.Proc = other
	_, _, _, _, err := prepareRemoteRunSendingDataWithProtocol("", scope, c.proc, nil, uuid.Nil, new(int64))
	require.ErrorContains(t, err, "baseline owner")
	wire := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*plan.Expr{expr}}}}
	actual.Base.SessionInfo.LastInsertIDSet = false
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(actual, wire), "explicit session baseline")
	actual.Base.SessionInfo.LastInsertIDSet = true
	actual.SetLastInsertID(0)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(actual, wire))
}

// A decoded remote process owns a bounded ODKU range even for a SELECT.
// Ordinary child sends must not consume it or prevent a sibling from running.
func TestRemoteReadChildrenDoNotConsumeODKUOrdinals(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCLatestVersion
	c.proc.Ctx = defines.AttachAccountId(context.Background(), 0)
	c.proc.Base.TxnOperator = fakeTxnOperator{}
	c.proc.Base.SessionInfo.TimeZone = time.UTC
	require.NoError(t, c.proc.SetODKUInputOrdinalBase(0))
	op := projection.NewArgument()
	defer op.Release()
	scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: op}
	for child := 0; child < 2; child++ {
		_, _, data, _, err := prepareRemoteRunSendingDataWithProtocol("select", scope, c.proc, nil, uuid.Nil, new(int64))
		require.NoError(t, err, "ordinary remote child %d must not exhaust ODKU range", child)
		info := new(pipeline.ProcessInfo)
		require.NoError(t, info.Unmarshal(data))
		require.Zero(t, info.OdkuOrdinalBase)
	}
	ordinal, err := c.proc.NextODKUInputOrdinal(1)
	require.NoError(t, err)
	require.Zero(t, ordinal, "ordinary sends do not advance the producer's input ordinal")
}

func TestRemoteODKUProducerReservationIncludesForwardedSubtrees(t *testing.T) {
	for _, shape := range []string{"root", "operator-child", "remote-descendant"} {
		t.Run(shape, func(t *testing.T) {
			c, client := expressionProtocolTestCompile(t)
			client.version = defines.MORPCLatestVersion
			c.proc.Ctx = defines.AttachAccountId(context.Background(), 0)
			c.proc.Base.TxnOperator = fakeTxnOperator{}
			c.proc.Base.SessionInfo.TimeZone = time.UTC
			require.NoError(t, c.proc.SetODKUInputOrdinalBase(0))
			producer := preinsert.NewArgument()
			defer producer.Release()
			producer.TrackODKUResult = true
			scope := &Scope{Magic: Remote, Proc: c.proc, NodeInfo: engine.Node{Id: "old-worker", Addr: "remote:6001"}, RootOp: producer}
			if shape != "root" {
				parent := projection.NewArgument()
				defer parent.Release()
				scope.RootOp = parent
				if shape == "operator-child" {
					parent.AppendChild(producer)
				} else {
					scope.PreScopes = []*Scope{{Magic: Remote, Proc: c.proc, NodeInfo: scope.NodeInfo, RootOp: producer}}
				}
			}
			_, _, data, _, err := prepareRemoteRunSendingDataWithProtocol("insert", scope, c.proc, nil, uuid.Nil, new(int64))
			require.NoError(t, err)
			info := new(pipeline.ProcessInfo)
			require.NoError(t, info.Unmarshal(data))
			require.Zero(t, info.OdkuOrdinalBase)
			_, err = c.proc.NextODKUInputOrdinal(1)
			require.ErrorContains(t, err, "range exhausted", "the genuine producer reserved the entire bounded range")
			before := *c.proc.Base.ODKUInputOrdinal
			_, _, _, _, err = prepareRemoteRunSendingDataWithProtocol("insert", scope, c.proc, nil, uuid.Nil, new(int64))
			require.ErrorContains(t, err, "range exhausted")
			require.Equal(t, before, *c.proc.Base.ODKUInputOrdinal, "a rejected producer cannot wrap or advance its range")
		})
	}
}

// The old 57/58 capability values and the released-main 128 prefix do not
// implement the complete ODKU summary/baseline contract. Cached and persisted
// owners must retain 131 even when no remote placement is attempted.
func TestODKU131SourceCachedAndPersistedFloors(t *testing.T) {
	c, _ := expressionProtocolTestCompile(t)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	last := &plan.Expr{Typ: plan.Type{Id: 28}, Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: int64(function.LAST_INSERT_ID) << 32}}}}
	cases := []*plan.Plan{
		{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{PreInsertCtx: &plan.PreInsertCtx{TrackOdkuResult: true}}}}}},
		{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{{ProjectList: []*plan.Expr{last}}}}}},
	}
	for _, owner := range cases {
		floor, err := planbuilder.RequiredPersistedExpressionProtocolVersion(owner)
		require.NoError(t, err)
		require.Equal(t, defines.MORPCVersion131, floor)
		for _, version := range []int64{57, 58, 112, 113, 114, 115, 116, 117, 118, 119, defines.MORPCVersion120, defines.MORPCVersion121, defines.MORPCVersion122, defines.MORPCVersion123, defines.MORPCVersion124, defines.MORPCVersion125, defines.MORPCVersion126, defines.MORPCVersion127, defines.MORPCVersion128, defines.MORPCVersion129, defines.MORPCVersion130} {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			require.ErrorContains(t, validateJSONStringConsumerProtocol(c.proc, owner), "protocol version 131")
			cached := &Compile{proc: c.proc, pn: owner}
			require.ErrorContains(t, cached.Compile(context.Background(), owner, nil), "protocol version 131")
			_, err = cached.Run(0)
			require.ErrorContains(t, err, "protocol version 131")
		}
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion131)
		require.NoError(t, validateJSONStringConsumerProtocol(c.proc, owner))
	}
	node := &plan.Node{ProjectList: []*plan.Expr{last, bindJSONStringConsumerExpr(t, "elt")}, AggList: []*plan.Expr{jsonMinMaxPlanExpr(aggexec.AggIdOfMin)}}
	combined := &plan.Plan{Plan: &plan.Plan_Query{Query: &plan.Query{Nodes: []*plan.Node{node}}}}
	features, err := plan.RequiredRemoteExpressionFeatures(combined)
	require.NoError(t, err)
	require.True(t, features.JSONMinMaxTypedComparison)
	require.True(t, features.JSONStringConsumerOverload)
	require.Equal(t, defines.MORPCVersion131, remoteExpressionProtocolVersion(features))
	floor, err := planbuilder.RequiredPersistedExpressionProtocolVersion(combined)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion131, floor)
}
