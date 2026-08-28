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

package cnservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/gossip"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/util"
)

type ddlVisibilityTestCluster struct {
	cnServices   []metadata.CNService
	refreshCalls int
	refreshHook  func()
}

func (c *ddlVisibilityTestCluster) GetCNService(
	selector clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	c.forEachCN(selector, apply)
}

func (*ddlVisibilityTestCluster) GetTNService(
	clusterservice.Selector,
	func(metadata.TNService) bool,
) {
}

func (*ddlVisibilityTestCluster) GetAllTNServices() []metadata.TNService { return nil }

func (c *ddlVisibilityTestCluster) GetCNServiceWithoutWorkingState(
	selector clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	c.forEachCN(selector, apply)
}

func (c *ddlVisibilityTestCluster) forEachCN(
	selector clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	for _, cn := range c.cnServices {
		if selector.MatchCN(cn) && !apply(cn) {
			return
		}
	}
}

func (*ddlVisibilityTestCluster) ForceRefresh(bool) {}
func (c *ddlVisibilityTestCluster) Refresh(context.Context) error {
	c.refreshCalls++
	if c.refreshHook != nil {
		c.refreshHook()
	}
	return nil
}
func (*ddlVisibilityTestCluster) Close() {}
func (*ddlVisibilityTestCluster) DebugUpdateCNLabel(string, map[string][]string) error {
	return nil
}
func (*ddlVisibilityTestCluster) DebugUpdateCNWorkState(string, int) error { return nil }
func (*ddlVisibilityTestCluster) RemoveCN(string)                          {}
func (*ddlVisibilityTestCluster) AddCN(metadata.CNService)                 {}
func (*ddlVisibilityTestCluster) UpdateCN(metadata.CNService)              {}

type ddlVisibilityTestQueryClient struct {
	serviceID string
	frontiers map[string]timestamp.Timestamp
	requests  []string
	methods   []query.CmdMethod
	releases  int
}

func (c *ddlVisibilityTestQueryClient) ServiceID() string { return c.serviceID }
func (c *ddlVisibilityTestQueryClient) SendMessage(
	_ context.Context,
	address string,
	req *query.Request,
) (*query.Response, error) {
	c.requests = append(c.requests, address)
	c.methods = append(c.methods, req.CmdMethod)
	return &query.Response{GetCommit: &query.GetCommitResponse{
		CurrentCommitTS: c.frontiers[address],
	}}, nil
}
func (*ddlVisibilityTestQueryClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}
func (c *ddlVisibilityTestQueryClient) Release(*query.Response) { c.releases++ }
func (*ddlVisibilityTestQueryClient) Close() error              { return nil }

type ddlVisibilityWithdrawalHAKeeperClient struct {
	logservice.CNHAKeeperClient
	cluster               *ddlVisibilityTestCluster
	queryClosed           <-chan struct{}
	sendErr               error
	sendErrors            map[int]error
	heartbeats            []logservicepb.CNStoreHeartbeat
	queryClosedBeforeSend bool
	closeCalls            int
}

func (c *ddlVisibilityWithdrawalHAKeeperClient) SendCNHeartbeat(
	_ context.Context,
	hb logservicepb.CNStoreHeartbeat,
) (logservicepb.CommandBatch, error) {
	select {
	case <-c.queryClosed:
		c.queryClosedBeforeSend = true
	default:
	}
	c.heartbeats = append(c.heartbeats, hb)
	if err := c.sendErrors[len(c.heartbeats)]; err != nil {
		return logservicepb.CommandBatch{}, err
	}
	if c.sendErr != nil {
		return logservicepb.CommandBatch{}, c.sendErr
	}
	for i := range c.cluster.cnServices {
		cn := &c.cluster.cnServices[i]
		if cn.ServiceID == hb.UUID {
			cn.ViewMetadataAdmissionGeneration = hb.ViewMetadataAdmissionGeneration
			cn.DDLVisibilityBarrierReady = hb.DDLVisibilityBarrierReady
		}
	}
	return logservicepb.CommandBatch{}, nil
}

func (c *ddlVisibilityWithdrawalHAKeeperClient) Close() error {
	c.closeCalls++
	return nil
}

type ddlVisibilityCloseLockService struct {
	lockservice.LockService
	closeCalls int
}

func (s *ddlVisibilityCloseLockService) Close() error {
	s.closeCalls++
	return nil
}

type ddlVisibilityCloseTestState struct {
	service                  *service
	hakeeperClient           *ddlVisibilityWithdrawalHAKeeperClient
	queryService             *closeRecordingQueryService
	lockService              *ddlVisibilityCloseLockService
	cluster                  *ddlVisibilityTestCluster
	queryClosedBeforeRefresh bool
}

func newDDLVisibilityCloseTestService(t *testing.T, sendErr error) *ddlVisibilityCloseTestState {
	t.Helper()
	const generation = uint64(7)
	moruntime.SetupServiceBasedRuntime(t.Name(), moruntime.DefaultRuntime())
	gossipNode, err := gossip.NewNode(context.Background(), t.Name())
	require.NoError(t, err)
	cfg := &Config{UUID: t.Name()}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
	state := &ddlVisibilityCloseTestState{
		queryService: &closeRecordingQueryService{closed: make(chan struct{})},
		lockService:  &ddlVisibilityCloseLockService{},
		cluster: &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
			ServiceID: t.Name(), QueryAddress: "self:6001",
			ViewMetadataAdmissionGeneration: generation, DDLVisibilityBarrierReady: true,
		}}},
	}
	state.cluster.refreshHook = func() {
		select {
		case <-state.queryService.closed:
			state.queryClosedBeforeRefresh = true
		default:
		}
	}
	state.hakeeperClient = &ddlVisibilityWithdrawalHAKeeperClient{
		cluster: state.cluster, queryClosed: state.queryService.closed, sendErr: sendErr,
	}
	state.service = &service{
		cfg:                             cfg,
		logger:                          zap.NewNop(),
		stopper:                         stopper.NewStopper("ddl-visibility-close-test"),
		_hakeeperClient:                 state.hakeeperClient,
		moCluster:                       state.cluster,
		queryService:                    state.queryService,
		mo:                              closeErrorMOServer{},
		cancelMoServerFunc:              func() {},
		server:                          closeOnlyRPCServer{},
		lockService:                     state.lockService,
		gossipNode:                      gossipNode,
		config:                          util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: generation,
	}
	state.service.viewMetadataIngressReady.Store(true)
	state.service.ddlVisibilityBarrierReady.Store(true)
	return state
}

func TestPrepareDDLVisibilityBarrier(t *testing.T) {
	const serviceID = "ddl-visibility-startup-test"
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	targetTS := timestamp.Timestamp{PhysicalTime: 200, LogicalTime: 3}
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{
		{
			ServiceID: serviceID, QueryAddress: "self:6001",
			ViewMetadataAdmissionGeneration: 7, DDLVisibilityBarrierReady: true,
		},
		{
			ServiceID: "peer", QueryAddress: "peer:6001",
			ViewMetadataAdmissionGeneration: 9, DDLVisibilityBarrierReady: true,
		},
	}}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{
			"self:6001": {PhysicalTime: 100},
			"peer:6001": targetTS,
		},
	}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), targetTS).Return(targetTS.Next(), nil)
	txnClient.EXPECT().SyncLatestCommitTS(targetTS)

	s := &service{
		cfg:                             &Config{UUID: serviceID},
		moCluster:                       cluster,
		queryClient:                     queryClient,
		_txnClient:                      txnClient,
		viewMetadataAdmissionGeneration: 7,
	}
	require.NoError(t, s.prepareDDLVisibilityBarrier())
	require.True(t, s.ddlVisibilityBarrierPrepared.Load())
	require.True(t, s.ddlVisibilityBarrierReady.Load())
	require.Equal(t, 1, cluster.refreshCalls)
	require.Equal(t, []string{"self:6001", "peer:6001"}, queryClient.requests)
	require.Equal(t, []query.CmdMethod{query.CmdMethod_GetCommit, query.CmdMethod_GetCommit}, queryClient.methods)
	require.Equal(t, 2, queryClient.releases)
}

func TestPrepareDDLVisibilityBarrierRejectsMissingProductionDependencies(t *testing.T) {
	const serviceID = "ddl-visibility-missing-dependency-test"
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	s := &service{
		cfg:                             &Config{UUID: serviceID},
		viewMetadataAdmissionGeneration: 1,
	}

	err := s.prepareDDLVisibilityBarrier()
	require.ErrorContains(t, err, "dependencies are unavailable")
	require.False(t, s.ddlVisibilityBarrierPrepared.Load())
	require.False(t, s.ddlVisibilityBarrierReady.Load())
}

func TestPrepareDDLVisibilityBarrierSkipsFrontierSyncDuringRollingUpgrade(t *testing.T) {
	const serviceID = "ddl-visibility-mixed-version-test"
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)

	s := &service{cfg: &Config{UUID: serviceID}}
	require.NoError(t, s.prepareDDLVisibilityBarrier())
	require.True(t, s.ddlVisibilityBarrierPrepared.Load())
	require.True(t, s.ddlVisibilityBarrierReady.Load())
}

func TestHandleSetProtocolVersionFencesRunningCNActivation(t *testing.T) {
	const serviceID = "ddl-visibility-live-activation-test"
	const generation = uint64(7)
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	targetTS := timestamp.Timestamp{PhysicalTime: 300, LogicalTime: 4}
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{
		{
			ServiceID: serviceID, QueryAddress: "self:6001",
			ViewMetadataAdmissionGeneration: generation, DDLVisibilityBarrierReady: true,
		},
		{
			ServiceID: "peer", QueryAddress: "peer:6001",
			ViewMetadataAdmissionGeneration: 9, DDLVisibilityBarrierReady: true,
		},
	}}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{"peer:6001": targetTS},
	}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), targetTS).DoAndReturn(
		func(context.Context, timestamp.Timestamp) (timestamp.Timestamp, error) {
			version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			require.True(t, ok)
			require.Equal(t, defines.MORPCVersion34, version,
				"v35 must not become visible before the activation frontier is applied")
			require.False(t, cluster.cnServices[0].DDLVisibilityBarrierReady,
				"the transitioning CN must be authoritatively withdrawn while applying the frontier")
			return targetTS.Next(), nil
		})
	txnClient.EXPECT().SyncLatestCommitTS(targetTS)
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
	hakeeperClient := &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster}
	s := &service{
		cfg:                             cfg,
		_hakeeperClient:                 hakeeperClient,
		moCluster:                       cluster,
		queryClient:                     queryClient,
		_txnClient:                      txnClient,
		config:                          util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: generation,
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)

	req := &query.Request{SetProtocolVersion: &query.SetProtocolVersionRequest{
		Version: defines.MORPCVersion35,
	}}
	resp := &query.Response{}
	require.NoError(t, s.handleSetProtocolVersion(context.Background(), req, resp, nil))
	require.Equal(t, defines.MORPCVersion35, resp.SetProtocolVersion.Version)
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion35, version)
	require.Equal(t, []string{"peer:6001"}, queryClient.requests)
	require.Equal(t, []query.CmdMethod{query.CmdMethod_GetCommit}, queryClient.methods)
	require.Equal(t, 1, queryClient.releases)
	require.Len(t, hakeeperClient.heartbeats, 2)
	require.False(t, hakeeperClient.heartbeats[0].DDLVisibilityBarrierReady)
	require.False(t, hakeeperClient.heartbeats[0].ViewMetadataIngressReady)
	require.True(t, hakeeperClient.heartbeats[1].DDLVisibilityBarrierReady)
	require.True(t, hakeeperClient.heartbeats[1].ViewMetadataIngressReady)
	require.Equal(t, 2, cluster.refreshCalls)
	require.True(t, s.ddlVisibilityBarrierReady.Load())
	require.True(t, s.viewMetadataIngressReady.Load())

	// Repeating an already-active version must not withdraw or re-run the fence.
	resp = &query.Response{}
	require.NoError(t, s.handleSetProtocolVersion(context.Background(), req, resp, nil))
	require.Equal(t, defines.MORPCVersion35, resp.SetProtocolVersion.Version)
	require.Len(t, hakeeperClient.heartbeats, 2)
	require.Equal(t, 2, cluster.refreshCalls)
	require.Len(t, queryClient.requests, 1)
}

func TestHandleSetProtocolVersionFailsClosedWhenActivationSyncFails(t *testing.T) {
	const serviceID = "ddl-visibility-live-activation-failure-test"
	const generation = uint64(7)
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	targetTS := timestamp.Timestamp{PhysicalTime: 300, LogicalTime: 4}
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{
		{
			ServiceID: serviceID, QueryAddress: "self:6001",
			ViewMetadataAdmissionGeneration: generation, DDLVisibilityBarrierReady: true,
		},
		{ServiceID: "peer", QueryAddress: "peer:6001", DDLVisibilityBarrierReady: true},
	}}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{"peer:6001": targetTS},
	}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	syncErr := errors.New("logtail activation sync failed")
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), targetTS).Return(timestamp.Timestamp{}, syncErr)
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
	hakeeperClient := &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster}
	s := &service{
		cfg:                             cfg,
		_hakeeperClient:                 hakeeperClient,
		moCluster:                       cluster,
		queryClient:                     queryClient,
		_txnClient:                      txnClient,
		config:                          util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: generation,
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)

	resp := &query.Response{}
	err := s.handleSetProtocolVersion(context.Background(), &query.Request{
		SetProtocolVersion: &query.SetProtocolVersionRequest{Version: defines.MORPCVersion35},
	}, resp, nil)
	require.ErrorIs(t, err, syncErr)
	require.Nil(t, resp.SetProtocolVersion)
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion34, version)
	require.Len(t, hakeeperClient.heartbeats, 1)
	require.False(t, hakeeperClient.heartbeats[0].DDLVisibilityBarrierReady)
	require.False(t, s.ddlVisibilityBarrierReady.Load())
	require.False(t, s.viewMetadataIngressReady.Load())
	require.False(t, cluster.cnServices[0].DDLVisibilityBarrierReady)
}

func TestHandleSetProtocolVersionWithdrawsAfterActivationPublishFails(t *testing.T) {
	const serviceID = "ddl-visibility-live-activation-publish-failure-test"
	const generation = uint64(7)
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	targetTS := timestamp.Timestamp{PhysicalTime: 300, LogicalTime: 4}
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{
		{
			ServiceID: serviceID, QueryAddress: "self:6001",
			ViewMetadataAdmissionGeneration: generation, DDLVisibilityBarrierReady: true,
		},
		{ServiceID: "peer", QueryAddress: "peer:6001", DDLVisibilityBarrierReady: true},
	}}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{"peer:6001": targetTS},
	}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), targetTS).Return(targetTS.Next(), nil)
	txnClient.EXPECT().SyncLatestCommitTS(targetTS)
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
	publishErr := errors.New("activation publication failed")
	hakeeperClient := &ddlVisibilityWithdrawalHAKeeperClient{
		cluster: cluster, sendErrors: map[int]error{2: publishErr},
	}
	s := &service{
		cfg:                             cfg,
		_hakeeperClient:                 hakeeperClient,
		moCluster:                       cluster,
		queryClient:                     queryClient,
		_txnClient:                      txnClient,
		config:                          util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: generation,
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)

	err := s.handleSetProtocolVersion(context.Background(), &query.Request{
		SetProtocolVersion: &query.SetProtocolVersionRequest{Version: defines.MORPCVersion35},
	}, &query.Response{}, nil)
	require.ErrorIs(t, err, publishErr)
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion34, version)
	require.Len(t, hakeeperClient.heartbeats, 3)
	require.False(t, hakeeperClient.heartbeats[0].DDLVisibilityBarrierReady)
	require.True(t, hakeeperClient.heartbeats[1].DDLVisibilityBarrierReady)
	require.False(t, hakeeperClient.heartbeats[2].DDLVisibilityBarrierReady)
	require.False(t, s.ddlVisibilityBarrierReady.Load())
	require.False(t, s.viewMetadataIngressReady.Load())
	require.False(t, cluster.cnServices[0].DDLVisibilityBarrierReady)
	require.Equal(t, 2, cluster.refreshCalls)
}

func TestSetProtocolVersionBeforeBarrierPreparationDefersToStartupFence(t *testing.T) {
	const serviceID = "ddl-visibility-pre-start-activation-test"
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	s := &service{cfg: &Config{UUID: serviceID}}

	require.NoError(t, s.setProtocolVersion(context.Background(), defines.MORPCVersion35))
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion35, version)
	require.False(t, s.ddlVisibilityBarrierPrepared.Load())
	require.False(t, s.ddlVisibilityBarrierReady.Load())
}

func TestInitQueryCommandHandlerOverridesProtocolActivation(t *testing.T) {
	const serviceID = "ddl-visibility-public-protocol-handler-test"
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	queryService := &closeRecordingQueryService{
		handlers: make(map[query.CmdMethod]func(context.Context, *query.Request, *query.Response, *morpc.Buffer) error),
	}
	s := &service{cfg: &Config{UUID: serviceID}, queryService: queryService}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierClosing.Store(true)
	s.initQueryCommandHandler()

	handler, ok := queryService.handlers[query.CmdMethod_SetProtocolVersion]
	require.True(t, ok)
	resp := &query.Response{}
	err := handler(context.Background(), &query.Request{
		SetProtocolVersion: &query.SetProtocolVersionRequest{Version: defines.MORPCVersion35},
	}, resp, nil)
	require.ErrorContains(t, err, "CN is closing")
	require.Nil(t, resp.SetProtocolVersion)
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion34, version)
}

func TestSetProtocolVersionRejectsActivationDuringClose(t *testing.T) {
	const serviceID = "ddl-visibility-closing-activation-test"
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	s := &service{cfg: &Config{UUID: serviceID}}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierClosing.Store(true)

	err := s.setProtocolVersion(context.Background(), defines.MORPCVersion35)
	require.ErrorContains(t, err, "CN is closing")
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion34, version)
}

func TestWithdrawDDLVisibilityBarrierHandlesPartialService(t *testing.T) {
	t.Run("zero generation has no authoritative publication", func(t *testing.T) {
		s := &service{}
		s.ddlVisibilityBarrierReady.Store(true)
		s.viewMetadataIngressReady.Store(true)
		require.NoError(t, s.withdrawDDLVisibilityBarrierLocked(context.Background()))
		require.False(t, s.ddlVisibilityBarrierReady.Load())
		require.False(t, s.viewMetadataIngressReady.Load())
	})

	t.Run("published generation requires dependencies", func(t *testing.T) {
		s := &service{cfg: &Config{}, viewMetadataAdmissionGeneration: 1}
		err := s.withdrawDDLVisibilityBarrierLocked(context.Background())
		require.ErrorContains(t, err, "withdrawal dependencies are unavailable")
	})
}

func TestWaitForDDLVisibilityBarrierPublicationHonorsCancellation(t *testing.T) {
	cluster := &ddlVisibilityTestCluster{}
	s := &service{
		cfg:                             &Config{UUID: "unpublished-cn"},
		moCluster:                       cluster,
		viewMetadataAdmissionGeneration: 1,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := s.waitForDDLVisibilityBarrierPublication(ctx, time.Second)
	require.Error(t, err)
	require.Equal(t, 1, cluster.refreshCalls)
}

func TestServiceCloseWithdrawsDDLVisibilityBeforeQueryService(t *testing.T) {
	state := newDDLVisibilityCloseTestService(t, nil)

	require.NoError(t, state.service.Close())
	require.False(t, state.hakeeperClient.queryClosedBeforeSend)
	require.False(t, state.queryClosedBeforeRefresh)
	require.Len(t, state.hakeeperClient.heartbeats, 1)
	require.False(t, state.hakeeperClient.heartbeats[0].DDLVisibilityBarrierReady)
	require.False(t, state.hakeeperClient.heartbeats[0].ViewMetadataIngressReady)
	require.Equal(t, 1, state.cluster.refreshCalls)
	require.False(t, state.cluster.cnServices[0].DDLVisibilityBarrierReady)
	require.Equal(t, 1, state.hakeeperClient.closeCalls)
	require.Equal(t, 2, state.lockService.closeCalls)
	select {
	case <-state.queryService.closed:
	default:
		t.Fatal("QueryService was not closed after authoritative barrier withdrawal")
	}
}

func TestServiceCloseContinuesAfterDDLVisibilityWithdrawalFailure(t *testing.T) {
	withdrawErr := errors.New("withdraw heartbeat failed")
	state := newDDLVisibilityCloseTestService(t, withdrawErr)

	err := state.service.Close()
	require.ErrorIs(t, err, withdrawErr)
	require.False(t, state.hakeeperClient.queryClosedBeforeSend)
	require.Len(t, state.hakeeperClient.heartbeats, 1)
	require.Zero(t, state.cluster.refreshCalls)
	require.Equal(t, 1, state.hakeeperClient.closeCalls)
	require.Equal(t, 2, state.lockService.closeCalls)
	select {
	case <-state.queryService.closed:
	default:
		t.Fatal("QueryService cleanup was skipped after barrier withdrawal failure")
	}
}

func TestWaitForDDLVisibilityBarrierWithdrawalAcceptsNewerGeneration(t *testing.T) {
	const serviceID = "ddl-visibility-newer-generation-test"
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
		ServiceID: serviceID, ViewMetadataAdmissionGeneration: 8,
		DDLVisibilityBarrierReady: true,
	}}}
	s := &service{
		cfg:                             &Config{UUID: serviceID},
		moCluster:                       cluster,
		viewMetadataAdmissionGeneration: 7,
	}

	require.NoError(t, s.waitForDDLVisibilityBarrierWithdrawal(context.Background(), time.Second))
	require.Equal(t, 1, cluster.refreshCalls)
}

func TestWaitForDDLVisibilityBarrierWithdrawalHonorsCancellation(t *testing.T) {
	const serviceID = "ddl-visibility-withdrawal-timeout-test"
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
		ServiceID: serviceID, ViewMetadataAdmissionGeneration: 7,
		DDLVisibilityBarrierReady: true,
	}}}
	s := &service{
		cfg:                             &Config{UUID: serviceID},
		moCluster:                       cluster,
		viewMetadataAdmissionGeneration: 7,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := s.waitForDDLVisibilityBarrierWithdrawal(ctx, time.Second)
	require.Error(t, err)
	require.Equal(t, 1, cluster.refreshCalls)
}

func TestSyncStartupDDLVisibilityFrontierAllowsEmptyFrontier(t *testing.T) {
	const serviceID = "ddl-visibility-empty-frontier-test"
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
		ServiceID: serviceID, QueryAddress: "self:6001",
		DDLVisibilityBarrierReady: true,
	}}}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{"self:6001": {}},
	}
	s := &service{moCluster: cluster, queryClient: queryClient}

	require.NoError(t, s.syncStartupDDLVisibilityFrontier(context.Background()))
	require.Equal(t, []string{"self:6001"}, queryClient.requests)
	require.Equal(t, 1, queryClient.releases)
}
