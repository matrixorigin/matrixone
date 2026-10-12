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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
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
	client.version = defines.MORPCVersion113
	_, err := encodeRemoteScope(scope, c.proc)
	require.ErrorContains(t, err, "remote destination does not support versioned expression semantics (MORPC version 114)")
	client.version = defines.MORPCVersion114
	minimum := int64(0)
	data, err := encodeRemoteScopeWithVectorProtocol(scope, c.proc, &minimum)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion114, minimum)
	decoded := new(pipeline.Pipeline)
	require.NoError(t, decoded.Unmarshal(data))
	features, err := pipelineExpressionFeatures(decoded)
	require.NoError(t, err)
	require.True(t, features.ODKUStatementResult)
	require.False(t, features.SessionLastInsertIDBaseline)
	require.Equal(t, defines.MORPCVersion114, remoteExpressionProtocolVersion(features))
	scope.NodeInfo.Addr = "stale:6001"
	_, err = encodeRemoteScope(scope, c.proc)
	require.Error(t, err)
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion113)
	require.ErrorContains(t, validateRemoteExpressionFeatures(c.proc, features), "ODKU statement result requires MORPC protocol version 114")
}

func TestODKUTrackingRechecksReplacementOnActualStream(t *testing.T) {
	for _, version := range []int64{defines.MORPCVersion113, defines.MORPCVersion114} {
		stream := &fakeStreamSender{}
		replies := make(chan morpc.Message, 1)
		replies <- &pipeline.Message{Id: 0, Cmd: pipeline.Method_PipelineProtocolCheck, Sid: pipeline.Status_Last, ProtocolVersion: version}
		sender := &messageSenderOnClient{ctx: context.Background(), streamSender: stream, receiveCh: replies}
		err := sender.confirmProtocolOnStream(defines.MORPCVersion114)
		if version < defines.MORPCVersion114 {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
		}
		require.Equal(t, defines.MORPCVersion114, stream.sent[0].(*pipeline.Message).ProtocolVersion)
	}
}

func TestLASTInsertIDBaselineUsesActualSerializedOwner(t *testing.T) {
	c, client := expressionProtocolTestCompile(t)
	client.version = defines.MORPCVersion114
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
		_, _, data, _, err := prepareRemoteRunSendingDataWithVectorProtocol("", scope, c.proc, nil, uuid.Nil, &minimum)
		require.NoError(t, err, "a real owned default0 is capturable without resolver/wire presence")
		require.Equal(t, defines.MORPCVersion114, minimum)
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
	_, _, _, _, err := prepareRemoteRunSendingDataWithVectorProtocol("", scope, c.proc, nil, uuid.Nil, new(int64))
	require.ErrorContains(t, err, "baseline owner")
	wire := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{{ProjectList: []*plan.Expr{expr}}}}
	actual.Base.SessionInfo.LastInsertIDSet = false
	require.ErrorContains(t, validateRemoteExpressionPipelineProtocol(actual, wire), "explicit session baseline")
	actual.Base.SessionInfo.LastInsertIDSet = true
	actual.SetLastInsertID(0)
	require.NoError(t, validateRemoteExpressionPipelineProtocol(actual, wire))
}
