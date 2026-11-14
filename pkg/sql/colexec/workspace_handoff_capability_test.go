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

package colexec

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	queryclient "github.com/matrixorigin/matrixone/pkg/queryservice/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type handoffCapabilityCluster struct {
	clusterservice.MOCluster
	cns []metadata.CNService
}

func (c handoffCapabilityCluster) GetCNServiceWithoutWorkingState(_ clusterservice.Selector, fn func(metadata.CNService) bool) {
	for _, cn := range c.cns {
		if !fn(cn) {
			return
		}
	}
}

type handoffCapabilityLock struct {
	lockservice.LockService
	sid string
}

func (l handoffCapabilityLock) GetConfig() lockservice.Config {
	return lockservice.Config{ServiceID: l.sid}
}

type handoffCapabilityQuery struct {
	queryclient.QueryClient
	version  uint32
	called   int
	drop     int
	resolver ioutil.RemoteWorkspaceHandoffResolver
}

func (c *handoffCapabilityQuery) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}

func (c *handoffCapabilityQuery) SendMessage(_ context.Context, addr string, req *query.Request) (*query.Response, error) {
	c.called++
	if addr != "query-cn" || req.CmdMethod != query.CmdMethod_GetProtocolVersion || req.GetProtocolVersion == nil {
		return nil, moerr.NewInvalidInputNoCtx("unexpected capability query")
	}
	resp := &query.Response{GetProtocolVersion: &query.GetProtocolVersionResponse{WorkspaceHandoffVersion: c.version}}
	if len(req.GetProtocolVersion.WorkspaceHandoffRequest) > 0 && c.version > 0 {
		var err error
		resp.GetProtocolVersion.WorkspaceHandoffResponse, err = ioutil.ExecuteRemoteWorkspaceHandoff(req.GetProtocolVersion.WorkspaceHandoffRequest, "pipeline-cn", c.resolver)
		if err != nil {
			return nil, err
		}
		if c.drop > 0 {
			c.drop--
			return nil, moerr.NewStreamClosedNoCtx()
		}
	}
	return resp, nil
}

func (*handoffCapabilityQuery) Release(*query.Response) {}

type handoffCapabilityResolver struct {
	states map[ioutil.RemoteWorkspaceHandoffID]ioutil.RemoteWorkspaceHandoffDecision
	accept map[string]bool
}

type handoffFailureProvider struct {
	states map[ioutil.RemoteWorkspaceHandoffID]ioutil.RemoteWorkspaceHandoffDecision
}

func (p handoffFailureProvider) WorkspaceHandoffDecision(id ioutil.RemoteWorkspaceHandoffID) (ioutil.RemoteWorkspaceHandoffDecision, error) {
	return p.states[id], nil
}

func (r *handoffCapabilityResolver) ResolveRemoteWorkspaceHandoff(id ioutil.RemoteWorkspaceHandoffID, decision ioutil.RemoteWorkspaceHandoffDecision) (bool, error) {
	if r.accept != nil && !r.accept[id.ObjectName] {
		return false, nil
	}
	r.states[id] = decision
	return true, nil
}

func (r *handoffCapabilityResolver) QueryRemoteWorkspaceHandoff(id ioutil.RemoteWorkspaceHandoffID) (ioutil.RemoteWorkspaceHandoffDecision, error) {
	return r.states[id], nil
}

func (r *handoffCapabilityResolver) QueryCoordinatorWorkspaceHandoff(id ioutil.RemoteWorkspaceHandoffID) (ioutil.RemoteWorkspaceHandoffDecision, error) {
	return r.states[id], nil
}

func newHandoffCapabilityProcess(t *testing.T, c *handoffCapabilityQuery) *process.Process {
	sid := t.Name()
	moruntime.SetupServiceBasedRuntime(sid, moruntime.DefaultRuntime())
	moruntime.ServiceRuntime(sid).SetGlobalVariables(moruntime.ClusterService, handoffCapabilityCluster{cns: []metadata.CNService{{PipelineServiceAddress: "pipeline-cn", QueryAddress: "query-cn"}}})
	return &process.Process{Base: &process.BaseProcess{LockService: handoffCapabilityLock{sid: sid}, QueryClient: c}}
}

func TestWorkspaceHandoffCapabilityUsesOldCompatibleQuery(t *testing.T) {
	client := &handoffCapabilityQuery{}
	proc := newHandoffCapabilityProcess(t, client)
	err := ProbeRemoteWorkspaceHandoff(context.Background(), proc, "pipeline-cn")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart), "an older response must be rejected before sending new pipeline messages")
	require.Equal(t, 1, client.called)
	scope := ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: "txn", ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, Attempt: 7, ProducerCN: "pipeline-cn"}
	err = ConfirmRemoteWorkspaceHandoffs(context.Background(), proc, scope, []string{"object"}, ioutil.RemoteWorkspaceHandoffAdopted)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart), "a replaced old CN must safely ignore the operation")
	client.version = 2
	err = ProbeRemoteWorkspaceHandoff(context.Background(), proc, "pipeline-cn")
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrRetryForCNRollingRestart), "an unknown future version is not implicitly compatible")
	client.version = 1
	require.NoError(t, ProbeRemoteWorkspaceHandoff(context.Background(), proc, "pipeline-cn"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, ProbeRemoteWorkspaceHandoff(ctx, proc, "pipeline-cn"), context.Canceled)
	require.Equal(t, 4, client.called)
}

func TestWorkspaceHandoffQueryAfterLostResponses(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "all-adopted", true: "partial-unknown"}[partial], func(t *testing.T) {
			resolver := &handoffCapabilityResolver{states: make(map[ioutil.RemoteWorkspaceHandoffID]ioutil.RemoteWorkspaceHandoffDecision)}
			if partial {
				resolver.accept = map[string]bool{"first": true}
			}
			client := &handoffCapabilityQuery{version: 1, drop: 3, resolver: resolver}
			proc := newHandoffCapabilityProcess(t, client)
			scope := ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: "txn", ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, Attempt: 7, ProducerCN: "pipeline-cn"}
			err := ConfirmRemoteWorkspaceHandoffs(context.Background(), proc, scope, []string{"first", "second"}, ioutil.RemoteWorkspaceHandoffAdopted)
			if partial {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 4, client.called)
			require.Equal(t, ioutil.RemoteWorkspaceHandoffAdopted, resolver.states[scope.Object("first")])
		})
	}
}

func TestWorkspaceHandoffQueryCarriesExactIdentityAndRejectsWrongProducer(t *testing.T) {
	resolver := &handoffCapabilityResolver{states: make(map[ioutil.RemoteWorkspaceHandoffID]ioutil.RemoteWorkspaceHandoffDecision)}
	client := &handoffCapabilityQuery{version: 1, resolver: resolver}
	proc := newHandoffCapabilityProcess(t, client)
	scope := ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: "txn", ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, Attempt: 7, ProducerCN: "pipeline-cn"}
	require.NoError(t, ConfirmRemoteWorkspaceHandoffs(context.Background(), proc, scope, []string{"object"}, ioutil.RemoteWorkspaceHandoffAdopted))
	states, err := QueryRemoteWorkspaceHandoffs(context.Background(), proc, scope, []string{"object", "absent"})
	require.NoError(t, err)
	require.Equal(t, []ioutil.RemoteWorkspaceHandoffDecision{ioutil.RemoteWorkspaceHandoffAdopted, ioutil.RemoteWorkspaceHandoffUnknown}, states)
	older := scope
	older.Attempt--
	states, err = QueryRemoteWorkspaceHandoffs(context.Background(), proc, older, []string{"object"})
	require.NoError(t, err)
	require.Equal(t, []ioutil.RemoteWorkspaceHandoffDecision{ioutil.RemoteWorkspaceHandoffUnknown}, states)
	data, err := ioutil.MarshalRemoteWorkspaceHandoffDecisionBatch(scope, []string{"object"}, ioutil.RemoteWorkspaceHandoffRejected)
	require.NoError(t, err)
	_, err = ioutil.ExecuteRemoteWorkspaceHandoff(data, "another-producer", resolver)
	require.Error(t, err)
	require.Equal(t, ioutil.RemoteWorkspaceHandoffAdopted, resolver.states[scope.Object("object")])
}

func TestWorkspaceHandoffDecisionReportsPartialPerObject(t *testing.T) {
	resolver := &handoffCapabilityResolver{states: make(map[ioutil.RemoteWorkspaceHandoffID]ioutil.RemoteWorkspaceHandoffDecision), accept: map[string]bool{"a": true, "c": true}}
	scope := ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: "txn", ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, Attempt: 7, ProducerCN: "pipeline-cn"}
	data, err := ioutil.MarshalRemoteWorkspaceHandoffDecisionBatch(scope, []string{"a", "b", "c"}, ioutil.RemoteWorkspaceHandoffAdopted)
	require.NoError(t, err)
	response, err := ioutil.ExecuteRemoteWorkspaceHandoff(data, "pipeline-cn", resolver)
	require.NoError(t, err)
	require.Equal(t, []byte{1, 0, 1}, response)
	require.Equal(t, ioutil.RemoteWorkspaceHandoffUnknown, resolver.states[scope.Object("b")])
	_, err = ioutil.ExecuteRemoteWorkspaceHandoff([]byte{0}, "pipeline-cn", resolver)
	require.Error(t, err)
}

func TestWorkspaceHandoffFailureSettlementIsPerObjectAndCancelable(t *testing.T) {
	mp := mpool.MustNewZeroNoFixed()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_binary.ToType())
	defer bat.Clean(mp)
	var names []string
	for range 3 {
		name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
		stats := objectio.NewObjectStatsWithObjectID(name.ObjectId(), false, false, true)
		require.NoError(t, vector.AppendBytes(bat.Vecs[0], stats[:], false, mp))
		names = append(names, name.String())
	}
	scope := ioutil.RemoteWorkspaceHandoffScope{Version: 1, TxnID: "txn", ExecutionID: [16]byte{1}, FragmentID: [16]byte{2}, Attempt: 7, ProducerCN: "pipeline-cn"}
	provider := handoffFailureProvider{states: map[ioutil.RemoteWorkspaceHandoffID]ioutil.RemoteWorkspaceHandoffDecision{
		scope.Object(names[0]): ioutil.RemoteWorkspaceHandoffAdopted,
		scope.Object(names[1]): ioutil.RemoteWorkspaceHandoffRejected,
	}}
	resolver := &handoffCapabilityResolver{states: make(map[ioutil.RemoteWorkspaceHandoffID]ioutil.RemoteWorkspaceHandoffDecision)}
	client := &handoffCapabilityQuery{version: 1, resolver: resolver}
	proc := newHandoffCapabilityProcess(t, client)
	proc.Ctx = context.Background()
	require.NoError(t, SettleFailedWorkspaceHandoff(proc, provider, scope, bat, 0))
	require.Equal(t, ioutil.RemoteWorkspaceHandoffAdopted, resolver.states[scope.Object(names[0])])
	require.Equal(t, ioutil.RemoteWorkspaceHandoffRejected, resolver.states[scope.Object(names[1])])
	require.Equal(t, ioutil.RemoteWorkspaceHandoffUnknown, resolver.states[scope.Object(names[2])])
	require.Equal(t, 2, client.called)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	proc.Ctx = ctx
	require.NoError(t, SettleFailedWorkspaceHandoff(proc, provider, scope, bat, 0))
	require.Equal(t, 2, client.called, "cancellation must not start a new decision RPC")
}
