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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	mock_morpc "github.com/matrixorigin/matrixone/pkg/common/morpc/mock_morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

func TestVectorScanProtocolCheckOnExecutionStream(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reply   *pipeline.Message
		wantErr bool
	}{
		{name: "current receiver", reply: &pipeline.Message{Id: 0, Cmd: pipeline.Method_PipelineProtocolCheck,
			Sid: pipeline.Status_Last, ProtocolVersion: defines.MORPCVersion96}},
		{name: "replacement version 95 receiver", reply: &pipeline.Message{Id: 0, Cmd: pipeline.Method_PipelineProtocolCheck,
			Sid: pipeline.Status_Last, ProtocolVersion: defines.MORPCVersion95}, wantErr: true},
		{name: "unrecognized method", reply: &pipeline.Message{Id: 0, Cmd: pipeline.Method_UnknownMethod,
			Sid: pipeline.Status_Last, ProtocolVersion: defines.MORPCVersion96}, wantErr: true},
		{name: "wrong stream", reply: &pipeline.Message{Id: 1, Cmd: pipeline.Method_PipelineProtocolCheck,
			Sid: pipeline.Status_Last, ProtocolVersion: defines.MORPCVersion96}, wantErr: true},
		{name: "closed stream", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stream := &fakeStreamSender{}
			receiveCh := make(chan morpc.Message, 1)
			if tc.reply != nil {
				receiveCh <- tc.reply
			} else {
				close(receiveCh)
			}
			sender := &messageSenderOnClient{
				ctx: context.Background(), streamSender: stream, receiveCh: receiveCh,
			}
			err := sender.confirmProtocolOnStream(defines.MORPCVersion96)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, stream.sentCnt)
			request := stream.sent[0].(*pipeline.Message)
			require.Equal(t, pipeline.Method_PipelineProtocolCheck, request.GetCmd())
			require.Equal(t, defines.MORPCVersion96, request.GetProtocolVersion())
			require.Equal(t, stream.ID(), request.GetID())
		})
	}
}

func TestVectorScanProtocolCheckReportsReceivingInstanceVersion(t *testing.T) {
	serviceID := t.Name()
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	runtime := moruntime.ServiceRuntime(serviceID)
	ctrl := gomock.NewController(t)
	session := mock_morpc.NewMockClientSession(ctrl)
	for _, version := range []int64{defines.MORPCVersion95, defines.MORPCVersion96} {
		runtime.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		session.EXPECT().Write(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, message any) error {
				response := message.(*pipeline.Message)
				require.Equal(t, pipeline.Method_PipelineProtocolCheck, response.GetCmd())
				require.Equal(t, uint64(17), response.GetID())
				require.Equal(t, pipeline.Status_Last, response.GetSid())
				require.Equal(t, version, response.GetProtocolVersion())
				return nil
			})
		require.NoError(t, handlePipelineProtocolCheck(context.Background(),
			&pipeline.Message{Id: 17, ProtocolVersion: defines.MORPCVersion96},
			session, serviceID, func() morpc.Message { return &pipeline.Message{} }))
	}
}

func TestIndexSearchScanPlacementRequiresProtocol107(t *testing.T) {
	workers := engine.Nodes{{Id: "b", Addr: "b:6001"}, {Id: "a", Addr: "a:6001"}}
	for _, mode := range []string{"supported", "old worker", "old coordinator", "unknown worker", "probe failure", "canceled", "forced local"} {
		t.Run(mode, func(t *testing.T) {
			c, client := vectorPlacementCompile(t, workers)
			node := vectorPlacementNode()
			switch mode {
			case "old worker":
				client.version = defines.MORPCVersion106
			case "old coordinator":
				moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion106)
			case "unknown worker":
				client.customResponse = true
			case "probe failure":
				client.customResponse, client.sendErr = true, errors.New("unavailable")
			case "canceled":
				ctx, cancel := context.WithCancel(c.proc.Ctx)
				cancel()
				c.proc.Ctx = ctx
			case "forced local":
				node.Stats.ForceOneCN = true
			}
			scopes, err := c.compileIndexSearchScan(node)
			if mode == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, scopes)
				return
			}
			if mode != "supported" && mode != "forced local" {
				require.ErrorContains(t, err, "MORPC protocol version 109")
				require.Empty(t, scopes)
				return
			}
			require.NoError(t, err)
			t.Cleanup(func() { ReleaseScopes(scopes) })
			if mode == "forced local" {
				require.Len(t, scopes, 1)
				require.Equal(t, c.addr, scopes[0].NodeInfo.Addr)
				require.Zero(t, client.calls)
			} else {
				require.Len(t, scopes, 2)
				require.Equal(t, "a", scopes[0].NodeInfo.Id)
			}
			for i, scope := range scopes {
				require.Equal(t, int32(len(scopes)), scope.NodeInfo.CNCNT)
				require.Equal(t, int32(i), scope.NodeInfo.CNIDX)
			}
		})
	}
}

func TestVectorScanPartitionTransportAndRollback(t *testing.T) {
	c, client := vectorPlacementCompile(t, engine.Nodes{{Id: "b", Addr: "b:6001"}, {Id: "a", Addr: "a:6001"}})
	scopes, err := c.compileIndexSearchScan(vectorPlacementNode())
	require.NoError(t, err)
	t.Cleanup(func() { ReleaseScopes(scopes) })
	remote := scopes[0]
	require.Equal(t, "a", remote.NodeInfo.Id)
	require.Zero(t, remote.NodeInfo.CNIDX)
	var requiredVectorProtocol int64
	data, err := encodeRemoteScopeWithVectorProtocol(remote, c.proc, &requiredVectorProtocol)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion109, requiredVectorProtocol, "the encoded remote index search scan needs a bound handshake")
	c.proc.Base.TxnOperator = fakeTxnOperator{}
	c.proc.Base.SessionInfo.TimeZone = time.UTC
	c.proc.Ctx = defines.AttachAccountId(context.Background(), 0)
	remote.Proc.Base.TxnOperator = fakeTxnOperator{}
	remote.Proc.Base.SessionInfo.TimeZone = time.UTC
	remote.Proc.Ctx = defines.AttachAccountId(context.Background(), 0)
	requiredVectorProtocol = 0
	_, _, _, _, err = prepareRemoteRunSendingDataWithVectorProtocol("", remote, c.proc, nil, uuid.Nil, &requiredVectorProtocol)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion109, requiredVectorProtocol, "remoteRun must receive the post-folded pipeline's protocol requirement")
	decoded, err := decodeScope(data, c.proc, true, nil)
	require.NoError(t, err)
	t.Cleanup(decoded.release)
	require.True(t, decoded.IsRemote)
	require.Equal(t, remote.NodeInfo.CNIDX, decoded.NodeInfo.CNIDX)
	require.Equal(t, remote.NodeInfo.CNCNT, decoded.NodeInfo.CNCNT)

	// The execution mapping stays frozen if the destination rolls back.
	client.version = defines.MORPCVersion106
	_, err = encodeRemoteScope(remote, c.proc)
	require.ErrorContains(t, err, "remote destination")
	require.Equal(t, "a", remote.NodeInfo.Id)
	require.Zero(t, remote.NodeInfo.CNIDX)
	require.Equal(t, client.calls, client.releases)
	client.version = defines.MORPCVersion109
	rt := moruntime.ServiceRuntime(c.proc.GetService())
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion106)
	_, err = decodeScope(data, c.proc, true, nil)
	require.ErrorContains(t, err, "version 109")
	_, err = encodeRemoteScope(remote, c.proc)
	require.ErrorContains(t, err, "remote destination")

	// Local decoding needs no capability; every remote partition needs 109.
	local, err := decodeScope(data, c.proc, false, nil)
	require.NoError(t, err)
	t.Cleanup(local.release)
	require.False(t, local.IsRemote)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion109)
	remote.NodeInfo.CNIDX = 1
	nonzero, err := encodeRemoteScopeWithVectorProtocol(remote, c.proc, &requiredVectorProtocol)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion109, requiredVectorProtocol, "a nonzero partition needs the handshake too")
	requiredVectorProtocol = 0
	_, _, _, _, err = prepareRemoteRunSendingDataWithVectorProtocol("", remote, c.proc, nil, uuid.Nil, &requiredVectorProtocol)
	require.NoError(t, err)
	require.Equal(t, defines.MORPCVersion109, requiredVectorProtocol)
	decodedNonzero, err := decodeScope(nonzero, c.proc, true, nil)
	require.NoError(t, err)
	t.Cleanup(decodedNonzero.release)
	require.True(t, decodedNonzero.IsRemote)
	require.Equal(t, int32(1), decodedNonzero.NodeInfo.CNIDX)
}

func TestVectorScanPartitionProtocolNestedScopes(t *testing.T) {
	c, _ := vectorPlacementCompile(t, engine.Nodes{{Id: "b", Addr: "b:6001"}, {Id: "a", Addr: "a:6001"}})
	leaf := &pipeline.Pipeline{
		Node:       &pipeline.NodeInfo{CnCnt: 2, CnIdx: 0},
		DataSource: &pipeline.Source{Node: vectorPlacementNode()},
	}
	root := &pipeline.Pipeline{
		Node:     &pipeline.NodeInfo{Id: "a", Addr: "a:6001"},
		Children: []*pipeline.Pipeline{nil, {Children: []*pipeline.Pipeline{leaf}}},
	}
	require.Equal(t, defines.MORPCVersion109, minimumRemoteVectorProtocol(root))
	var requiredVectorProtocol int64
	require.NoError(t, validateVectorPartitionDestinationWithResult(c.proc, root, &requiredVectorProtocol))
	require.Equal(t, defines.MORPCVersion109, requiredVectorProtocol, "a nested index search scan needs the execution-stream handshake")
	require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, root))
	ctx, cancel := context.WithCancel(c.proc.Ctx)
	cancel()
	c.proc.Ctx = ctx
	require.ErrorIs(t, validateVectorPartitionDestination(c.proc, root), context.Canceled)
	require.Error(t, validateRemoteVectorPartitionProtocol(nil, root))
	require.Error(t, validateVectorPartitionDestination(nil, root))
	root.Node = nil
	require.Error(t, validateVectorPartitionDestination(c.proc, root))
	leaf.Node.CnCnt = 1
	require.Error(t, validateRemoteVectorPartitionProtocol(nil, root), "an unpartitioned remote index search scan needs 109 too")
	leaf.Node.CnCnt = 2
	leaf.DataSource.Node.NodeType = plan.Node_TABLE_SCAN
	require.NoError(t, validateVectorPartitionDestinationWithResult(nil, root, &requiredVectorProtocol))
	require.Zero(t, requiredVectorProtocol, "ordinary nested scans do not need the handshake")
	require.NoError(t, validateRemoteVectorPartitionProtocol(nil, nil))
}

func TestRequiredIVFProtocolCoversEveryPartitionAndNestedFragment(t *testing.T) {
	c, client := vectorPlacementCompile(t, engine.Nodes{{Id: "a", Addr: "a:6001"}, {Id: "b", Addr: "b:6001"}})
	runtime := moruntime.ServiceRuntime(c.proc.GetService())
	for _, ordinal := range []int32{0, 1} {
		node := vectorPlacementNode()
		node.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{{Tag: 7, MustApply: true, UseMembershipFilter: true}}
		child := &pipeline.Pipeline{Node: &pipeline.NodeInfo{Id: "b", Addr: "b:6001", CnCnt: 2, CnIdx: ordinal}, DataSource: &pipeline.Source{Node: node}}
		root := &pipeline.Pipeline{Node: child.Node, Children: []*pipeline.Pipeline{child}}
		require.Equal(t, defines.MORPCVersion109, minimumRemoteVectorProtocol(root))
		for _, version := range []int64{defines.MORPCVersion103, defines.MORPCVersion106, defines.MORPCVersion107, defines.MORPCVersion108, defines.MORPCVersion109} {
			runtime.SetGlobalVariables(moruntime.MOProtocolVersion, version)
			client.version = version
			var required int64
			err := validateVectorPartitionDestinationWithResult(c.proc, root, &required)
			require.Equal(t, defines.MORPCVersion109, required)
			if version < defines.MORPCVersion109 {
				require.Error(t, err)
				require.Error(t, validateRemoteVectorPartitionProtocol(c.proc, root))
			} else {
				require.NoError(t, err)
				require.NoError(t, validateRemoteVectorPartitionProtocol(c.proc, root))
			}
		}
	}
	stream := &fakeStreamSender{}
	ch := make(chan morpc.Message, 1)
	ch <- &pipeline.Message{Id: 0, Cmd: pipeline.Method_PipelineProtocolCheck, Sid: pipeline.Status_Last, ProtocolVersion: defines.MORPCVersion106}
	sender := &messageSenderOnClient{ctx: context.Background(), streamSender: stream, receiveCh: ch}
	require.Error(t, sender.confirmProtocolOnStream(defines.MORPCVersion109))
	require.Equal(t, 1, stream.sentCnt)
}

func TestRequiredIVFWorkersRequireProtocol107(t *testing.T) {
	for _, mode := range []string{"supported", "old", "unknown", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			c, client := vectorPlacementCompile(t, engine.Nodes{{Id: "a", Addr: "a:6001"}, {Id: "b", Addr: "b:6001"}})
			moruntime.ServiceRuntime(c.proc.GetService()).SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion109)
			client.version = defines.MORPCVersion109
			// gpu_mode off: the IVF centroid search runs on CPU workers.
			c.proc.SetResolveVariableFunc(func(name string, _, _ bool) (interface{}, error) {
				if name == "gpu_mode" {
					return int8(0), nil
				}
				return nil, nil
			})
			typ := plan.Type{Id: int32(types.T_int64)}
			col := func(rel int32) *plan.Expr {
				return &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, ColPos: 0}}}
			}
			eq := func() []*plan.Expr {
				return []*plan.Expr{{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{ObjName: "="}, Args: []*plan.Expr{col(0), col(1)}}}}}
			}
			obj := &plan.ObjectRef{Obj: 10, Db: 1}
			def := &plan.TableDef{Cols: []*plan.ColDef{{Name: "id", Typ: typ}, {Name: "v", Typ: plan.Type{Id: int32(types.T_array_float32)}}}, Name2ColIndex: map[string]int32{"id": 0, "v": 1}, Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"}}
			scan := func() *plan.Node {
				return &plan.Node{NodeType: plan.Node_TABLE_SCAN, ObjRef: obj, TableDef: def, ProjectList: []*plan.Expr{col(0)}}
			}
			v := vectorPlacementNode()
			v.TableDef = &plan.TableDef{Cols: []*plan.ColDef{{Name: "pkid", Typ: typ}}}
			v.ProjectList = []*plan.Expr{col(0)}
			v.IndexSearchScan = &plan.IndexSearchScan{SourceTable: obj, SourceTableDef: def, Index: &plan.IndexDef{IndexAlgo: catalog.MoIndexIvfFlatAlgo.ToString(), IndexAlgoParams: `{}`, Parts: []string{"v"}}, QueryPayload: &plan.Expr{Typ: def.Cols[1].Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_VecVal{VecVal: "[0]"}}}}, ScanWork: &plan.IndexSearchScanWork{Objects: 2, Blocks: 2, Rows: 10, VectorBytesPerRow: 512}}
			rf := &plan.RuntimeFilterSpec{Tag: 7, MustApply: true, UseMembershipFilter: true, Expr: col(0)}
			v.RuntimeFilterProbeList = []*plan.RuntimeFilterSpec{rf}
			q := &plan.Query{StmtType: plan.Query_SELECT, Steps: []int32{4}, Nodes: []*plan.Node{scan(), v, scan(), {NodeType: plan.Node_JOIN, JoinType: plan.Node_SEMI, Children: []int32{1, 2}, ProjectList: []*plan.Expr{col(0)}, OnList: eq(), RuntimeFilterBuildList: []*plan.RuntimeFilterSpec{rf}}, {NodeType: plan.Node_JOIN, JoinType: plan.Node_INNER, Children: []int32{0, 3}, OnList: eq()}}}
			for id, n := range q.Nodes {
				n.NodeId = int32(id)
			}
			c.pn = &plan.Plan{Plan: &plan.Plan_Query{Query: q}}
			_, _, _, qualified := plan2.RequiredIVFPlacement(q)
			require.True(t, qualified)
			switch mode {
			case "old":
				client.version = defines.MORPCVersion106
			case "unknown":
				client.customResponse = true
			case "canceled":
				ctx, cancel := context.WithCancel(c.proc.Ctx)
				cancel()
				c.proc.Ctx = ctx
			}
			err := c.constrainRequiredIVFWorkers(q)
			if mode == "canceled" {
				require.ErrorIs(t, err, context.Canceled)
				return
			}
			if mode != "supported" {
				require.ErrorContains(t, err, "MORPC protocol version 109")
				return
			}
			require.NoError(t, err)
			require.Equal(t, plan2.ExecTypeAP_MULTICN, c.execType)
			require.Len(t, c.cnList, 2)
		})
	}
}

func TestApplyIndexSearchScanRequiresProtocol107(t *testing.T) {
	apply := &pipeline.Pipeline{InstructionList: []*pipeline.Instruction{
		{Apply: &pipeline.Apply{IndexSearchScan: &plan.IndexSearchScan{}}},
	}}
	root := &pipeline.Pipeline{Children: []*pipeline.Pipeline{apply}}
	require.Equal(t, defines.MORPCVersion109, minimumRemoteVectorProtocol(root))
	apply.InstructionList[0].Apply.IndexSearchScan = nil
	require.Zero(t, minimumRemoteVectorProtocol(root))
	apply.InstructionList[0].Apply = nil
	require.Zero(t, minimumRemoteVectorProtocol(root))
}
