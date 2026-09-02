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
	"sync"
	"sync/atomic"
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
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
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

type failReplaceMetadataFS struct {
	fileservice.ReplaceableFileService
	failAt int
	calls  int
}

func (f *failReplaceMetadataFS) Replace(ctx context.Context, vector fileservice.IOVector) error {
	f.calls++
	if f.calls == f.failAt {
		return errors.New("injected metadata replace failure")
	}
	return f.ReplaceableFileService.Replace(ctx, vector)
}

func newDDLVisibilityMetadataFS(t *testing.T) fileservice.ReplaceableFileService {
	t.Helper()
	fs, err := fileservice.NewMemoryFS(defines.LocalFileServiceName, fileservice.DisabledCacheConfig, nil)
	require.NoError(t, err)
	return fs
}

type ddlVisibilityTestCluster struct {
	cnServices     []metadata.CNService
	refreshCalls   int
	refreshHook    func()
	phaseMu        sync.Mutex
	phases         map[string]query.GetProtocolVersionResponse
	frontiers      map[string]timestamp.Timestamp
	globalFrontier timestamp.Timestamp
	phaseHook      func(string) query.GetProtocolVersionResponse
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
	serviceID   string
	frontiers   map[string]timestamp.Timestamp
	protocols   map[string]query.GetProtocolVersionResponse
	protocolFn  func(string) query.GetProtocolVersionResponse
	nilProtocol map[string]bool
	requests    []string
	methods     []query.CmdMethod
	releases    int
}

func (c *ddlVisibilityTestQueryClient) ServiceID() string { return c.serviceID }
func (c *ddlVisibilityTestQueryClient) SendMessage(
	_ context.Context,
	address string,
	req *query.Request,
) (*query.Response, error) {
	c.requests = append(c.requests, address)
	c.methods = append(c.methods, req.CmdMethod)
	if req.CmdMethod == query.CmdMethod_GetProtocolVersion {
		if req.GetProtocolVersion == nil {
			return nil, errors.New("missing GetProtocolVersion request payload")
		}
		if c.nilProtocol[address] {
			return &query.Response{}, nil
		}
		protocol := c.protocols[address]
		if c.protocolFn != nil {
			protocol = c.protocolFn(address)
		}
		return &query.Response{GetProtocolVersion: &protocol}, nil
	}
	return &query.Response{GetCommit: &query.GetCommitResponse{
		CurrentCommitTS: c.frontiers[address],
	}}, nil
}
func (*ddlVisibilityTestQueryClient) NewRequest(method query.CmdMethod) *query.Request {
	return &query.Request{CmdMethod: method}
}
func (c *ddlVisibilityTestQueryClient) Release(*query.Response) { c.releases++ }
func (*ddlVisibilityTestQueryClient) Close() error              { return nil }

func activationTestPeerProtocols() map[string]query.GetProtocolVersionResponse {
	return map[string]query.GetProtocolVersionResponse{
		"peer:6001": {
			Version:                         defines.MORPCVersion44,
			DDLVisibilityActivationPrepared: true,
			DDLVisibilityActivationFenced:   true,
		},
	}
}

type ddlVisibilityWithdrawalHAKeeperClient struct {
	logservice.CNHAKeeperClient
	cluster                  *ddlVisibilityTestCluster
	queryClosed              <-chan struct{}
	sendErr                  error
	sendErrors               map[int]error
	heartbeats               []logservicepb.CNStoreHeartbeat
	queryClosedBeforeSend    bool
	closeCalls               int
	clusterDeployedProtocol  int64
	oldHAKeeperReplica       bool
	oldDDLFrontierRSM        bool
	authoritativeGenerations map[string]uint64
}

func (c *ddlVisibilityWithdrawalHAKeeperClient) GetClusterDetails(
	context.Context,
) (logservicepb.ClusterDetails, error) {
	details := logservicepb.ClusterDetails{
		DDLVisibilityDeployedProtocol: c.clusterDeployedProtocol,
		LogStores: []logservicepb.LogStore{{
			UUID: "log-1", DDLVisibilityDeployedProtocolSupported: !c.oldHAKeeperReplica,
		}},
	}
	if c.cluster != nil {
		c.cluster.phaseMu.Lock()
		defer c.cluster.phaseMu.Unlock()
		details.DDLVisibilityFrontier = c.cluster.globalFrontier
		for _, frontier := range c.cluster.frontiers {
			if details.DDLVisibilityFrontier.Less(frontier) {
				details.DDLVisibilityFrontier = frontier
			}
		}
		for _, cn := range c.cluster.cnServices {
			phase, ok := c.cluster.phases[cn.ServiceID]
			if c.cluster.phaseHook != nil && len(c.cluster.phases) > 0 {
				phase = c.cluster.phaseHook(cn.QueryAddress)
				ok = true
			}
			if !ok {
				phase.DDLVisibilityActivationPrepared = cn.DDLVisibilityBarrierReady
				phase.DDLVisibilityActivationFenced = cn.DDLVisibilityBarrierReady
			}
			details.CNStores = append(details.CNStores, logservicepb.CNStore{
				UUID: cn.ServiceID, QueryAddress: cn.QueryAddress,
				ViewMetadataAdmissionGeneration: cn.ViewMetadataAdmissionGeneration,
				DDLVisibilityBarrierReady:       cn.DDLVisibilityBarrierReady,
				DDLVisibilityActivationPrepared: phase.DDLVisibilityActivationPrepared,
				DDLVisibilityActivationFenced:   phase.DDLVisibilityActivationFenced,
				DDLVisibilityFrontier:           c.cluster.frontiers[cn.ServiceID],
			})
		}
	}
	return details, nil
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
	authoritativeGeneration := hb.ViewMetadataAdmissionGeneration
	if generation := c.authoritativeGenerations[hb.UUID]; generation > authoritativeGeneration {
		if !c.oldDDLFrontierRSM && c.cluster.globalFrontier.Less(hb.DDLVisibilityFrontier) {
			c.cluster.globalFrontier = hb.DDLVisibilityFrontier
		}
		return logservicepb.CommandBatch{
			DDLVisibilityDeployedProtocol: c.clusterDeployedProtocol,
			DDLVisibilityFrontier:         &c.cluster.globalFrontier,
			ViewMetadataAdmission: &logservicepb.ViewMetadataAdmission{
				Generation: generation,
			},
		}, nil
	}
	if c.cluster != nil {
		c.cluster.phaseMu.Lock()
		if c.cluster.phases == nil {
			c.cluster.phases = make(map[string]query.GetProtocolVersionResponse)
		}
		c.cluster.phases[hb.UUID] = query.GetProtocolVersionResponse{
			Version:                         hb.DDLVisibilityDeployedProtocol,
			DDLVisibilityActivationPrepared: hb.DDLVisibilityActivationPrepared,
			DDLVisibilityActivationFenced:   hb.DDLVisibilityActivationFenced,
		}
		if c.cluster.frontiers == nil {
			c.cluster.frontiers = make(map[string]timestamp.Timestamp)
		}
		if !c.oldDDLFrontierRSM {
			c.cluster.frontiers[hb.UUID] = hb.DDLVisibilityFrontier
			if c.cluster.globalFrontier.Less(hb.DDLVisibilityFrontier) {
				c.cluster.globalFrontier = hb.DDLVisibilityFrontier
			}
		}
		c.cluster.phaseMu.Unlock()
	}
	for i := range c.cluster.cnServices {
		cn := &c.cluster.cnServices[i]
		if cn.ServiceID == hb.UUID {
			cn.ViewMetadataAdmissionGeneration = hb.ViewMetadataAdmissionGeneration
			cn.DDLVisibilityBarrierReady = hb.DDLVisibilityBarrierReady
			cn.ViewMetadataIngressReady = hb.ViewMetadataIngressReady
			if c.clusterDeployedProtocol >= defines.MORPCVersion44 &&
				hb.DDLVisibilityDeployedProtocol < c.clusterDeployedProtocol {
				cn.ViewMetadataIngressReady = false
			}
		}
	}
	if len(hb.DDLVisibilityEpochCommitTargets) > 0 {
		targets := make(map[string]logservicepb.DDLVisibilityActivationTarget,
			len(hb.DDLVisibilityEpochCommitTargets))
		for _, target := range hb.DDLVisibilityEpochCommitTargets {
			targets[target.ServiceID] = target
		}
		matched := 0
		valid := len(targets) == len(hb.DDLVisibilityEpochCommitTargets)
		for _, cn := range c.cluster.cnServices {
			if cn.QueryAddress == "" || cn.ViewMetadataAdmissionGeneration == 0 {
				continue
			}
			matched++
			target, ok := targets[cn.ServiceID]
			valid = valid && ok && target.Generation == cn.ViewMetadataAdmissionGeneration &&
				target.QueryAddress == cn.QueryAddress && cn.DDLVisibilityBarrierReady
		}
		if valid && matched == len(targets) {
			c.clusterDeployedProtocol = hb.DDLVisibilityDeployedProtocol
		}
	}
	return logservicepb.CommandBatch{
		DDLVisibilityDeployedProtocol: c.clusterDeployedProtocol,
		DDLVisibilityFrontier:         &c.cluster.globalFrontier,
		ViewMetadataAdmission: &logservicepb.ViewMetadataAdmission{
			Generation: authoritativeGeneration,
		},
	}, nil
}

func (c *ddlVisibilityWithdrawalHAKeeperClient) Close() error {
	c.closeCalls++
	return nil
}

type ddlVisibilityHeartbeatOrderClient struct {
	logservice.CNHAKeeperClient
	mu           sync.Mutex
	calls        int
	firstStarted chan struct{}
	releaseFirst chan struct{}
	completed    []logservicepb.CNStoreHeartbeat
}

func (c *ddlVisibilityHeartbeatOrderClient) SendCNHeartbeat(
	_ context.Context,
	hb logservicepb.CNStoreHeartbeat,
) (logservicepb.CommandBatch, error) {
	c.mu.Lock()
	c.calls++
	call := c.calls
	c.mu.Unlock()
	if call == 1 {
		close(c.firstStarted)
		<-c.releaseFirst
	}
	c.mu.Lock()
	c.completed = append(c.completed, hb)
	c.mu.Unlock()
	return logservicepb.CommandBatch{}, errors.New("stop heartbeat after ordering observation")
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
		ddlCommitGate:                   frontend.NewDDLCommitGate(),
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
	}, frontiers: map[string]timestamp.Timestamp{"peer": targetTS}}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{
			"self:6001": {PhysicalTime: 100},
			"peer:6001": targetTS,
		},
	}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().GetLatestCommitTS().Return(targetTS).AnyTimes()
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), targetTS).Return(targetTS.Next(), nil)
	txnClient.EXPECT().SyncLatestCommitTS(targetTS)

	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	metadataFS := newDDLVisibilityMetadataFS(t)
	writer := &service{
		cfg: cfg, metadata: metadata.CNStore{UUID: serviceID}, metadataFS: metadataFS,
	}
	// Persist with the old process, then construct a distinct service and load
	// through the production metadata initialization path.
	require.NoError(t, writer.persistDDLVisibilityDeployedProtocol(defines.MORPCVersion44))
	s := &service{
		cfg: cfg, metadata: metadata.CNStore{UUID: serviceID}, metadataFS: metadataFS,
		logger:    zap.NewNop(),
		moCluster: cluster, queryClient: queryClient, _txnClient: txnClient,
		_hakeeperClient:                 &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster},
		config:                          util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: 7,
		ddlCommitGate:                   frontend.NewDDLCommitGate(),
	}
	require.NoError(t, s.initMetadata())
	require.Equal(t, defines.MORPCVersion44, s.metadata.DDLVisibilityDeployedProtocol)
	require.False(t, s.ddlVisibilityActivationComplete.Load())
	require.NoError(t, s.prepareDDLVisibilityBarrier())
	require.True(t, s.ddlVisibilityBarrierPrepared.Load())
	require.True(t, s.ddlVisibilityActivationPrepared.Load())
	require.True(t, s.ddlVisibilityActivationFenced.Load())
	require.True(t, s.ddlVisibilityActivationComplete.Load())
	require.True(t, cluster.phases[serviceID].DDLVisibilityActivationPrepared)
	require.True(t, cluster.phases[serviceID].DDLVisibilityActivationFenced,
		"a restarted committed CN must publish proof for its new incarnation")
	require.True(t, s.ddlVisibilityBarrierReady.Load())
	require.Equal(t, 1, cluster.refreshCalls)
	require.Empty(t, queryClient.requests)
	require.Empty(t, queryClient.methods)
	require.Zero(t, queryClient.releases)
	require.NoError(t, s.publishDDLVisibilityIngressAfterStart())
	version, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion44, version)
	require.True(t, s.viewMetadataIngressReady.Load())
	require.True(t, s.ddlCommitGate.PublicDDLEnabled())
}

func TestDefaultV43StartupUsesLastDeployedProtocolUntilActivation(t *testing.T) {
	const serviceID = "ddl-visibility-default-v43-startup-protocol-test"
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	gate := frontend.NewDDLCommitGate()
	s := &service{cfg: &Config{UUID: serviceID}, ddlCommitGate: gate}

	require.NoError(t, s.prepareDDLVisibilityBarrier())
	require.NoError(t, s.publishDDLVisibilityIngressAfterStart())
	version, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion42, version)
	require.True(t, s.ddlVisibilityListenersReady.Load())
	require.True(t, s.viewMetadataIngressReady.Load())
	require.True(t, gate.PublicDDLEnabled())
	require.False(t, s.ddlVisibilityActivationPrepared.Load())
	require.False(t, s.ddlVisibilityActivationFenced.Load())
}

func TestDDLVisibilityEpochHAKeeperCapabilityRejectsOldLeaderView(t *testing.T) {
	client := &ddlVisibilityWithdrawalHAKeeperClient{}
	s := &service{
		cfg: &Config{}, _hakeeperClient: client,
	}
	s.cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	require.NoError(t, s.requireDDLVisibilityEpochHAKeeperCapability(context.Background()))

	// A leader failover to an old RSM returns the additive field as false. The
	// activation entry point must interpret that as unsupported, never epoch 0.
	client.oldHAKeeperReplica = true
	err := s.requireDDLVisibilityEpochHAKeeperCapability(context.Background())
	require.ErrorContains(t, err, "does not support the durable DDL visibility deployment epoch")
}

func TestActivationFinalMembershipScanRejectsConcurrentJoin(t *testing.T) {
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{
		{ServiceID: "target-cn", QueryAddress: "target:6001", ViewMetadataAdmissionGeneration: 1,
			DDLVisibilityBarrierReady: true},
		{ServiceID: "joining-cn", QueryAddress: "joining:6001", ViewMetadataAdmissionGeneration: 2,
			DDLVisibilityBarrierReady: true},
	}}
	s := &service{_hakeeperClient: &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster}}
	_, err := s.revalidateDDLVisibilityActivationMembership(
		context.Background(), map[string]struct{}{"target-cn": {}})
	require.ErrorContains(t, err, "authoritative CN joining-cn is not fenced")
}

func TestMarkerlessCNIngressHandshakeLinearizesWithCommittedCut(t *testing.T) {
	const serviceID = "ddl-visibility-markerless-join-race-test"
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{ServiceID: serviceID}}}
	client := &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster}
	gate := frontend.NewDDLCommitGate()
	s := &service{
		cfg: &Config{UUID: serviceID}, ddlCommitGate: gate,
		_hakeeperClient: client, config: util.NewConfigData(nil),
	}

	// The startup read precedes the cut, but the atomic ingress heartbeat lands
	// after it. HAKeeper must reject ingress and return the committed epoch.
	require.NoError(t, s.prepareDDLVisibilityBarrier())
	client.clusterDeployedProtocol = defines.MORPCVersion44
	require.NoError(t, s.publishDDLVisibilityIngressAfterStart())
	version, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion44, version)
	require.False(t, s.viewMetadataIngressReady.Load())
	require.False(t, cluster.cnServices[0].ViewMetadataIngressReady)
	require.False(t, gate.PublicDDLEnabled())
}

func TestMarkerlessCNIngressRejectsGenerationTakeover(t *testing.T) {
	const serviceID = "markerless-ingress-generation-takeover"
	const localGeneration = uint64(7)
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
		ServiceID: serviceID, ViewMetadataAdmissionGeneration: localGeneration,
	}}}
	client := &ddlVisibilityWithdrawalHAKeeperClient{
		cluster:                  cluster,
		authoritativeGenerations: map[string]uint64{serviceID: localGeneration + 1},
	}
	gate := frontend.NewDDLCommitGate()
	s := &service{
		cfg: &Config{UUID: serviceID}, logger: zap.NewNop(),
		ddlCommitGate: gate, _hakeeperClient: client, config: util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: localGeneration,
	}

	require.NoError(t, s.prepareDDLVisibilityBarrier())
	err := s.publishDDLVisibilityIngressAfterStart()
	require.ErrorContains(t, err, "generation revoked")
	require.True(t, s.viewMetadataGenerationRevoked.Load())
	require.False(t, s.viewMetadataIngressReady.Load())
	require.False(t, gate.PublicDDLEnabled())
	require.False(t, cluster.cnServices[0].ViewMetadataIngressReady)
}

func TestMarkerlessCNJoinsCommittedClusterFailClosed(t *testing.T) {
	const serviceID = "ddl-visibility-markerless-post-cut-test"
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	cluster := &ddlVisibilityTestCluster{}
	gate := frontend.NewDDLCommitGate()
	s := &service{
		cfg: &Config{UUID: serviceID}, ddlCommitGate: gate, config: util.NewConfigData(nil),
		_hakeeperClient: &ddlVisibilityWithdrawalHAKeeperClient{
			cluster: cluster, clusterDeployedProtocol: defines.MORPCVersion44,
		},
	}

	require.NoError(t, s.prepareDDLVisibilityBarrier())
	require.NoError(t, s.publishDDLVisibilityIngressAfterStart())
	version, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion44, version)
	require.False(t, s.ddlVisibilityActivationComplete.Load())
	require.False(t, s.viewMetadataIngressReady.Load())
	require.False(t, gate.PublicDDLEnabled())
}

func TestProvisionalV43RestartRemainsFailClosed(t *testing.T) {
	const serviceID = "ddl-visibility-provisional-v43-restart-test"
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	metadataFS := newDDLVisibilityMetadataFS(t)
	cfg := &Config{UUID: serviceID}
	writer := &service{cfg: cfg, metadata: metadata.CNStore{UUID: serviceID}, metadataFS: metadataFS}
	require.NoError(t, writer.persistDDLVisibilityDeployedProtocol(-defines.MORPCVersion44))

	gate := frontend.NewDDLCommitGate()
	restarted := &service{
		cfg: cfg, metadata: metadata.CNStore{UUID: serviceID}, metadataFS: metadataFS,
		logger: zap.NewNop(), ddlCommitGate: gate,
	}
	require.NoError(t, restarted.initMetadata())
	require.NoError(t, restarted.prepareDDLVisibilityBarrier())
	require.NoError(t, restarted.publishDDLVisibilityIngressAfterStart())
	version, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion44, version)
	require.False(t, restarted.ddlVisibilityActivationComplete.Load())
	require.False(t, restarted.viewMetadataIngressReady.Load())
	require.False(t, gate.PublicDDLEnabled())
}

func TestPrepareDDLVisibilityBarrierRejectsMissingProductionDependencies(t *testing.T) {
	const serviceID = "ddl-visibility-missing-dependency-test"
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	s := &service{
		cfg: &Config{UUID: serviceID}, metadata: metadata.CNStore{UUID: serviceID},
		metadataFS: newDDLVisibilityMetadataFS(t), viewMetadataAdmissionGeneration: 1,
	}
	require.NoError(t, s.persistDDLVisibilityDeployedProtocol(defines.MORPCVersion44))

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
	require.False(t, s.ddlVisibilityActivationPrepared.Load())
	require.False(t, s.ddlVisibilityActivationFenced.Load())
	require.True(t, s.ddlVisibilityBarrierReady.Load())
}

func TestHandleSetProtocolVersionRejectsStaleRecoveryIdentity(t *testing.T) {
	const serviceID = "ddl-visibility-stale-recovery-identity-test"
	const generation = uint64(7)
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	s := &service{
		cfg: &Config{UUID: serviceID}, viewMetadataAdmissionGeneration: generation,
	}
	err := s.handleSetProtocolVersion(context.Background(), &query.Request{
		SetProtocolVersion: &query.SetProtocolVersionRequest{
			Version: defines.MORPCVersion44, DDLVisibilityActivationTargets: []string{serviceID},
			DDLVisibilityTargetGeneration:   generation + 1,
			DDLVisibilityTargetQueryAddress: "stale:6001",
		},
	}, &query.Response{}, nil)
	require.ErrorContains(t, err, "stale DDL visibility activation target identity")
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion34, version)
}

func TestActivationDoesNotPublishFencedBeforeProvisionalPersistence(t *testing.T) {
	const serviceID = "ddl-visibility-provisional-persist-failure-test"
	const generation = uint64(7)
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion40)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
		ServiceID: serviceID, QueryAddress: "self:6001",
		ViewMetadataAdmissionGeneration: generation, DDLVisibilityBarrierReady: true,
		ViewMetadataIngressReady: true,
	}}}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{"self:6001": {PhysicalTime: 100}},
	}
	txnClient := mock_frontend.NewMockTxnClient(gomock.NewController(t))
	txnClient.EXPECT().GetLatestCommitTS().Return(timestamp.Timestamp{})
	baseFS := newDDLVisibilityMetadataFS(t)
	metadataFS := &failReplaceMetadataFS{ReplaceableFileService: baseFS, failAt: 1}
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
	s := &service{
		cfg: cfg, metadata: metadata.CNStore{UUID: serviceID}, metadataFS: metadataFS,
		_hakeeperClient: &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster},
		moCluster:       cluster, queryClient: queryClient, _txnClient: txnClient,
		config: util.NewConfigData(nil), viewMetadataAdmissionGeneration: generation,
		ddlCommitGate: frontend.NewDDLCommitGate(),
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)

	err := s.setProtocolVersion(context.Background(), defines.MORPCVersion44, []string{serviceID})
	require.ErrorContains(t, err, "injected metadata replace failure")
	require.True(t, s.ddlVisibilityActivationPrepared.Load())
	require.False(t, s.ddlVisibilityActivationFenced.Load())
	require.False(t, s.ddlVisibilityActivationComplete.Load())
	require.False(t, s.viewMetadataIngressReady.Load())
	require.Equal(t, int64(0), s.loadDDLVisibilityDeployedProtocol())
}

func TestCommittedPersistenceFailureRestartsFromProvisionalFailClosed(t *testing.T) {
	const serviceID = "ddl-visibility-committed-persist-failure-test"
	const generation = uint64(7)
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion40)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
		ServiceID: serviceID, QueryAddress: "self:6001",
		ViewMetadataAdmissionGeneration: generation, DDLVisibilityBarrierReady: true,
		ViewMetadataIngressReady: true,
	}}}
	targetTS := timestamp.Timestamp{PhysicalTime: 100}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{"self:6001": targetTS},
	}
	txnClient := mock_frontend.NewMockTxnClient(gomock.NewController(t))
	txnClient.EXPECT().GetLatestCommitTS().Return(targetTS)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), targetTS).Return(targetTS.Next(), nil)
	txnClient.EXPECT().SyncLatestCommitTS(targetTS)
	baseFS := newDDLVisibilityMetadataFS(t)
	metadataFS := &failReplaceMetadataFS{ReplaceableFileService: baseFS, failAt: 2}
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
	s := &service{
		cfg: cfg, metadata: metadata.CNStore{UUID: serviceID}, metadataFS: metadataFS,
		_hakeeperClient: &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster},
		moCluster:       cluster, queryClient: queryClient, _txnClient: txnClient,
		config: util.NewConfigData(nil), viewMetadataAdmissionGeneration: generation,
		ddlCommitGate: frontend.NewDDLCommitGate(),
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)

	err := s.setProtocolVersion(context.Background(), defines.MORPCVersion44, []string{serviceID})
	require.ErrorContains(t, err, "injected metadata replace failure")
	require.True(t, s.ddlVisibilityActivationFenced.Load())
	require.False(t, s.ddlVisibilityActivationComplete.Load())
	require.False(t, s.viewMetadataIngressReady.Load())
	require.Equal(t, -defines.MORPCVersion44, s.loadDDLVisibilityDeployedProtocol())

	// A distinct process reloads the provisional marker through initMetadata and
	// must remain a v43, non-routable producer until activation is retried.
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	restarted := &service{
		cfg: cfg, metadata: metadata.CNStore{UUID: serviceID}, metadataFS: baseFS,
		logger: zap.NewNop(), ddlCommitGate: frontend.NewDDLCommitGate(),
	}
	require.NoError(t, restarted.initMetadata())
	require.NoError(t, restarted.prepareDDLVisibilityBarrier())
	require.NoError(t, restarted.publishDDLVisibilityIngressAfterStart())
	version, ok := moruntime.ServiceRuntime(serviceID).GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion44, version)
	require.False(t, restarted.viewMetadataIngressReady.Load())
	require.False(t, restarted.ddlCommitGate.PublicDDLEnabled())
}

func TestActivationWithdrawalFailureStillBlocksNewDDL(t *testing.T) {
	const serviceID = "ddl-visibility-withdrawal-failure-gate-test"
	const generation = uint64(7)
	withdrawErr := errors.New("injected withdrawal failure")
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion35)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
		ServiceID: serviceID, QueryAddress: "self:6001",
		ViewMetadataAdmissionGeneration: generation, DDLVisibilityBarrierReady: true,
		ViewMetadataIngressReady: true,
	}}}
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	gate := frontend.NewDDLCommitGate()
	s := &service{
		cfg: cfg, _hakeeperClient: &ddlVisibilityWithdrawalHAKeeperClient{
			cluster: cluster, sendErr: withdrawErr,
		},
		moCluster: cluster, queryClient: &ddlVisibilityTestQueryClient{serviceID: serviceID},
		_txnClient: mock_frontend.NewMockTxnClient(gomock.NewController(t)),
		config:     util.NewConfigData(nil), viewMetadataAdmissionGeneration: generation,
		ddlCommitGate: gate,
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)

	err := s.setProtocolVersion(context.Background(), defines.MORPCVersion44, []string{serviceID})
	require.ErrorIs(t, err, withdrawErr)
	require.True(t, cluster.cnServices[0].ViewMetadataIngressReady,
		"the injected heartbeat failure leaves authoritative ingress unchanged")
	blockedCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = gate.Enter(blockedCtx)
	require.ErrorIs(t, err, context.Canceled,
		"withdrawal failure must not reopen old-protocol DDL admission")
}

func TestActivationDrainTimeoutKeepsIngressWithdrawn(t *testing.T) {
	const serviceID = "ddl-visibility-drain-timeout-test"
	const generation = uint64(7)
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion35)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
		ServiceID: serviceID, QueryAddress: "self:6001",
		ViewMetadataAdmissionGeneration: generation, DDLVisibilityBarrierReady: true,
		ViewMetadataIngressReady: true,
	}}}
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = 10 * time.Millisecond
	cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
	gate := frontend.NewDDLCommitGate()
	releaseActive, err := gate.Enter(context.Background())
	require.NoError(t, err)
	defer releaseActive()
	s := &service{
		cfg: cfg, _hakeeperClient: &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster},
		moCluster: cluster, queryClient: &ddlVisibilityTestQueryClient{serviceID: serviceID},
		_txnClient: mock_frontend.NewMockTxnClient(gomock.NewController(t)),
		config:     util.NewConfigData(nil), viewMetadataAdmissionGeneration: generation,
		ddlCommitGate: gate,
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)

	err = s.setProtocolVersion(context.Background(), defines.MORPCVersion44, []string{serviceID})
	require.Error(t, err)
	require.True(t, s.ddlVisibilityActivationPending.Load())
	require.False(t, s.viewMetadataIngressReady.Load())
	require.False(t, cluster.cnServices[0].ViewMetadataIngressReady)
	blockedCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = gate.Enter(blockedCtx)
	require.Error(t, err)
}

func TestDefaultV43StillRunsCompleteTargetActivation(t *testing.T) {
	const serviceID = "ddl-visibility-default-v43-activation-test"
	const generation = uint64(7)
	moruntime.SetupServiceBasedRuntime(serviceID, moruntime.DefaultRuntime())
	targetTS := timestamp.Timestamp{PhysicalTime: 300, LogicalTime: 4}
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{
		{ServiceID: serviceID, QueryAddress: "self:6001",
			ViewMetadataAdmissionGeneration: generation, DDLVisibilityBarrierReady: true,
			ViewMetadataIngressReady: true},
		{ServiceID: "legacy-peer", QueryAddress: "peer:6001",
			ViewMetadataAdmissionGeneration: 8, DDLVisibilityBarrierReady: false,
			ViewMetadataIngressReady: true},
	}, frontiers: map[string]timestamp.Timestamp{"legacy-peer": targetTS}}
	cluster.refreshHook = func() {
		// The control plane dispatches activation concurrently. Model the legacy
		// peer completing its local drain before this CN checks global phases.
		cluster.cnServices[1].DDLVisibilityBarrierReady = true
	}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{
			"self:6001": {PhysicalTime: 100}, "peer:6001": targetTS,
		},
		protocols: map[string]query.GetProtocolVersionResponse{"peer:6001": {
			Version:                         defines.MORPCVersion44,
			DDLVisibilityActivationPrepared: true, DDLVisibilityActivationFenced: true,
		}},
	}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().GetLatestCommitTS().Return(targetTS).AnyTimes()
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), targetTS).Return(targetTS.Next(), nil)
	txnClient.EXPECT().SyncLatestCommitTS(targetTS)
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
	s := &service{
		cfg: cfg, metadata: metadata.CNStore{UUID: serviceID}, metadataFS: newDDLVisibilityMetadataFS(t),
		_hakeeperClient: &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster},
		moCluster:       cluster, queryClient: queryClient, _txnClient: txnClient,
		config: util.NewConfigData(nil), viewMetadataAdmissionGeneration: generation,
		ddlCommitGate: frontend.NewDDLCommitGate(),
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.ddlVisibilityActivationPrepared.Store(true)
	s.ddlVisibilityActivationFenced.Store(true)
	s.viewMetadataIngressReady.Store(true)

	require.False(t, s.ddlVisibilityActivationComplete.Load())
	require.NoError(t, s.setProtocolVersion(
		context.Background(), defines.MORPCVersion44, []string{serviceID, "legacy-peer"}))
	require.True(t, s.ddlVisibilityActivationComplete.Load())
	require.Equal(t, defines.MORPCVersion44, s.loadDDLVisibilityDeployedProtocol())
	require.True(t, s.viewMetadataIngressReady.Load())
	require.True(t, s.ddlCommitGate.PublicDDLEnabled())
	require.Empty(t, queryClient.requests)
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
	cluster.frontiers = map[string]timestamp.Timestamp{"peer": targetTS}
	var peerReady atomic.Bool
	var protocolObserved atomic.Bool
	firstProtocol := make(chan struct{})
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{
			"self:6001": {PhysicalTime: 200},
			"peer:6001": targetTS,
		},
		protocolFn: func(string) query.GetProtocolVersionResponse {
			if protocolObserved.CompareAndSwap(false, true) {
				close(firstProtocol)
			}
			ready := peerReady.Load()
			return query.GetProtocolVersionResponse{
				Version:                         defines.MORPCVersion44,
				DDLVisibilityActivationPrepared: ready,
				DDLVisibilityActivationFenced:   ready,
			}
		},
	}
	cluster.phaseHook = queryClient.protocolFn
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().GetLatestCommitTS().Return(targetTS).AnyTimes()
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), targetTS).DoAndReturn(
		func(context.Context, timestamp.Timestamp) (timestamp.Timestamp, error) {
			version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
			require.True(t, ok)
			require.Equal(t, defines.MORPCVersion44, version,
				"sender capability must be v43 after every local v34 DDL producer is drained")
			require.True(t, cluster.cnServices[0].DDLVisibilityBarrierReady,
				"the transitioning CN must remain reachable by v43 DDL fan-out")
			require.False(t, cluster.cnServices[0].ViewMetadataIngressReady,
				"the transitioning CN must not accept new proxy sessions before the fence")
			return targetTS.Next(), nil
		})
	txnClient.EXPECT().SyncLatestCommitTS(targetTS)
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
	hakeeperClient := &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster}
	ddlCommitGate := frontend.NewDDLCommitGate()
	s := &service{
		cfg:                             cfg,
		_hakeeperClient:                 hakeeperClient,
		moCluster:                       cluster,
		queryClient:                     queryClient,
		_txnClient:                      txnClient,
		config:                          util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: generation,
		ddlCommitGate:                   ddlCommitGate,
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)

	req := &query.Request{SetProtocolVersion: &query.SetProtocolVersionRequest{
		Version:                         defines.MORPCVersion44,
		DDLVisibilityActivationTargets:  []string{serviceID, "peer"},
		DDLVisibilityTargetGeneration:   generation,
		DDLVisibilityTargetQueryAddress: "self:6001",
	}}
	resp := &query.Response{}
	activationDone := make(chan error, 1)
	go func() {
		activationDone <- s.handleSetProtocolVersion(context.Background(), req, resp, nil)
	}()
	<-firstProtocol
	blockedCtx, cancelBlocked := context.WithCancel(context.Background())
	cancelBlocked()
	_, err := ddlCommitGate.Enter(blockedCtx)
	require.ErrorIs(t, err, context.Canceled,
		"DDL producer must remain blocked until every target is prepared and fenced")
	peerReady.Store(true)
	require.NoError(t, <-activationDone)
	releaseDDL, err := ddlCommitGate.Enter(context.Background())
	require.NoError(t, err, "activation must release DDL producers after global convergence")
	releaseDDL()
	require.Equal(t, defines.MORPCVersion44, resp.SetProtocolVersion.Version)
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion44, version)
	require.Empty(t, queryClient.requests)
	require.Empty(t, queryClient.methods)
	require.Zero(t, queryClient.releases)
	require.Len(t, hakeeperClient.heartbeats, 5)
	require.False(t, hakeeperClient.heartbeats[0].ViewMetadataIngressReady)
	require.True(t, hakeeperClient.heartbeats[1].DDLVisibilityActivationPrepared)
	require.False(t, hakeeperClient.heartbeats[1].DDLVisibilityActivationFenced)
	require.True(t, hakeeperClient.heartbeats[2].DDLVisibilityActivationFenced)
	require.NotEmpty(t, hakeeperClient.heartbeats[3].DDLVisibilityEpochCommitTargets)
	require.False(t, hakeeperClient.heartbeats[3].ViewMetadataIngressReady)
	require.True(t, hakeeperClient.heartbeats[4].ViewMetadataIngressReady)
	require.Equal(t, 2, cluster.refreshCalls)
	require.True(t, s.ddlVisibilityBarrierReady.Load())
	require.True(t, s.viewMetadataIngressReady.Load())

	// Repeating an already-active version must not withdraw or re-run the fence.
	resp = &query.Response{}
	require.NoError(t, s.handleSetProtocolVersion(context.Background(), req, resp, nil))
	require.Equal(t, defines.MORPCVersion44, resp.SetProtocolVersion.Version)
	require.Len(t, hakeeperClient.heartbeats, 5)
	require.Equal(t, 2, cluster.refreshCalls)
	require.Empty(t, queryClient.requests)
}

func TestHandleSetProtocolVersionPreservesPreStartIngress(t *testing.T) {
	const serviceID = "ddl-visibility-pre-start-ingress-test"
	const generation = uint64(7)
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{
		{ServiceID: serviceID, QueryAddress: "self:6001",
			ViewMetadataAdmissionGeneration: generation, DDLVisibilityBarrierReady: true},
		{ServiceID: "peer", QueryAddress: "peer:6001",
			ViewMetadataAdmissionGeneration: 8, DDLVisibilityBarrierReady: true},
	}}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{"self:6001": {}, "peer:6001": {}},
		protocols: activationTestPeerProtocols(),
	}
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
	hakeeperClient := &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster}
	txnClient := mock_frontend.NewMockTxnClient(gomock.NewController(t))
	txnClient.EXPECT().GetLatestCommitTS().Return(timestamp.Timestamp{}).AnyTimes()
	s := &service{
		cfg: cfg, _hakeeperClient: hakeeperClient, moCluster: cluster,
		queryClient: queryClient, _txnClient: txnClient,
		config: util.NewConfigData(nil), viewMetadataAdmissionGeneration: generation,
		ddlCommitGate: frontend.NewDDLCommitGate(),
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(false)

	resp := &query.Response{}
	require.NoError(t, s.handleSetProtocolVersion(context.Background(), &query.Request{
		SetProtocolVersion: &query.SetProtocolVersionRequest{
			Version:                        defines.MORPCVersion44,
			DDLVisibilityActivationTargets: []string{serviceID, "peer"},
			DDLVisibilityTargetGeneration:  generation, DDLVisibilityTargetQueryAddress: "self:6001",
		},
	}, resp, nil))
	require.Equal(t, defines.MORPCVersion44, resp.SetProtocolVersion.Version)
	require.False(t, s.viewMetadataIngressReady.Load())
	require.False(t, cluster.cnServices[0].ViewMetadataIngressReady)
	require.Len(t, hakeeperClient.heartbeats, 5)
	require.False(t, hakeeperClient.heartbeats[0].ViewMetadataIngressReady)
	require.True(t, hakeeperClient.heartbeats[1].DDLVisibilityActivationPrepared)
	require.True(t, hakeeperClient.heartbeats[2].DDLVisibilityActivationFenced)
	require.NotEmpty(t, hakeeperClient.heartbeats[3].DDLVisibilityEpochCommitTargets)
	require.False(t, hakeeperClient.heartbeats[4].ViewMetadataIngressReady)
	release, err := s.ddlCommitGate.Enter(context.Background())
	require.NoError(t, err)
	release()

	// Re-run the transition and linearize listener-ready publication while
	// activation still owns the barrier lock. The later Start publication must
	// win; activation must not restore its stale pre-listener false sample.
	serviceRuntime := moruntime.ServiceRuntime(serviceID)
	serviceRuntime.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	value, ok := serviceRuntime.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion34, value)
	s.ddlVisibilityActivationPending.Store(true)
	s.ddlVisibilityActivationPrepared.Store(false)
	s.ddlVisibilityActivationFenced.Store(false)
	s.viewMetadataIngressReady.Store(false)
	cluster.cnServices[0].ViewMetadataIngressReady = false
	phaseEntered := make(chan struct{})
	allowPhase := make(chan struct{})
	var phaseCalls atomic.Int32
	queryClient.protocolFn = func(string) query.GetProtocolVersionResponse {
		if phaseCalls.Add(1) == 1 {
			close(phaseEntered)
			<-allowPhase
		}
		return query.GetProtocolVersionResponse{
			Version:                         defines.MORPCVersion44,
			DDLVisibilityActivationPrepared: true, DDLVisibilityActivationFenced: true,
		}
	}
	cluster.phaseHook = queryClient.protocolFn
	activationDone := make(chan error, 1)
	go func() {
		activationDone <- s.handleSetProtocolVersion(context.Background(), &query.Request{
			SetProtocolVersion: &query.SetProtocolVersionRequest{
				Version:                        defines.MORPCVersion44,
				DDLVisibilityActivationTargets: []string{serviceID, "peer"},
				DDLVisibilityTargetGeneration:  generation, DDLVisibilityTargetQueryAddress: "self:6001",
			},
		}, &query.Response{}, nil)
	}()
	select {
	case <-phaseEntered:
	case err := <-activationDone:
		require.NoError(t, err)
		t.Fatal("activation completed before entering phase convergence")
	}
	publishDone := make(chan error, 1)
	go func() { publishDone <- s.publishDDLVisibilityIngressAfterStart() }()
	close(allowPhase)
	require.NoError(t, <-activationDone)
	require.NoError(t, <-publishDone)
	require.True(t, s.viewMetadataIngressReady.Load())
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
		{ServiceID: "peer", QueryAddress: "peer:6001", ViewMetadataAdmissionGeneration: 8,
			DDLVisibilityBarrierReady: true},
	}}
	cluster.frontiers = map[string]timestamp.Timestamp{"peer": targetTS}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{
			"self:6001": {PhysicalTime: 200}, "peer:6001": targetTS,
		},
		protocols: activationTestPeerProtocols(),
	}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().GetLatestCommitTS().Return(targetTS)
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
		ddlCommitGate:                   frontend.NewDDLCommitGate(),
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)

	resp := &query.Response{}
	err := s.handleSetProtocolVersion(context.Background(), &query.Request{
		SetProtocolVersion: &query.SetProtocolVersionRequest{
			Version: defines.MORPCVersion44, DDLVisibilityActivationTargets: []string{serviceID, "peer"},
			DDLVisibilityTargetGeneration: generation, DDLVisibilityTargetQueryAddress: "self:6001",
		},
	}, resp, nil)
	require.ErrorIs(t, err, syncErr)
	require.Nil(t, resp.SetProtocolVersion)
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion44, version)
	require.True(t, s.ddlVisibilityActivationPending.Load())
	require.True(t, s.ddlVisibilityActivationPrepared.Load())
	require.False(t, s.ddlVisibilityActivationFenced.Load())
	blockedCtx, cancelBlocked := context.WithCancel(context.Background())
	cancelBlocked()
	_, gateErr := s.ddlCommitGate.Enter(blockedCtx)
	require.ErrorIs(t, gateErr, context.Canceled)
	require.Len(t, hakeeperClient.heartbeats, 2)
	require.True(t, hakeeperClient.heartbeats[0].DDLVisibilityBarrierReady)
	require.True(t, hakeeperClient.heartbeats[1].DDLVisibilityActivationPrepared)
	require.False(t, s.viewMetadataIngressReady.Load())
	require.True(t, cluster.cnServices[0].DDLVisibilityBarrierReady)
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
		{ServiceID: "peer", QueryAddress: "peer:6001", ViewMetadataAdmissionGeneration: 8,
			DDLVisibilityBarrierReady: true},
	}}
	cluster.frontiers = map[string]timestamp.Timestamp{"peer": targetTS}
	queryClient := &ddlVisibilityTestQueryClient{
		serviceID: serviceID,
		frontiers: map[string]timestamp.Timestamp{
			"self:6001": {PhysicalTime: 200}, "peer:6001": targetTS,
		},
		protocols: activationTestPeerProtocols(),
	}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().GetLatestCommitTS().Return(targetTS).AnyTimes()
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), targetTS).Return(targetTS.Next(), nil)
	txnClient.EXPECT().SyncLatestCommitTS(targetTS)
	cfg := &Config{UUID: serviceID}
	cfg.HAKeeper.DiscoveryTimeout.Duration = time.Second
	cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
	publishErr := errors.New("activation publication failed")
	hakeeperClient := &ddlVisibilityWithdrawalHAKeeperClient{
		cluster: cluster, sendErrors: map[int]error{5: publishErr},
	}
	s := &service{
		cfg:                             cfg,
		_hakeeperClient:                 hakeeperClient,
		moCluster:                       cluster,
		queryClient:                     queryClient,
		_txnClient:                      txnClient,
		config:                          util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: generation,
		ddlCommitGate:                   frontend.NewDDLCommitGate(),
	}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)

	err := s.handleSetProtocolVersion(context.Background(), &query.Request{
		SetProtocolVersion: &query.SetProtocolVersionRequest{
			Version: defines.MORPCVersion44, DDLVisibilityActivationTargets: []string{serviceID, "peer"},
			DDLVisibilityTargetGeneration: generation, DDLVisibilityTargetQueryAddress: "self:6001",
		},
	}, &query.Response{}, nil)
	require.ErrorIs(t, err, publishErr)
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion44, version)
	require.True(t, s.ddlVisibilityActivationPending.Load())
	require.True(t, s.ddlVisibilityActivationFenced.Load())
	blockedCtx, cancelBlocked := context.WithCancel(context.Background())
	cancelBlocked()
	_, gateErr := s.ddlCommitGate.Enter(blockedCtx)
	require.ErrorIs(t, gateErr, context.Canceled)
	require.Len(t, hakeeperClient.heartbeats, 6)
	require.True(t, hakeeperClient.heartbeats[0].DDLVisibilityBarrierReady)
	require.True(t, hakeeperClient.heartbeats[1].DDLVisibilityActivationPrepared)
	require.True(t, hakeeperClient.heartbeats[2].DDLVisibilityActivationFenced)
	require.NotEmpty(t, hakeeperClient.heartbeats[3].DDLVisibilityEpochCommitTargets)
	require.True(t, hakeeperClient.heartbeats[4].ViewMetadataIngressReady)
	require.False(t, hakeeperClient.heartbeats[5].ViewMetadataIngressReady)
	require.False(t, s.viewMetadataIngressReady.Load())
	require.True(t, cluster.cnServices[0].DDLVisibilityBarrierReady)
	require.Equal(t, 2, cluster.refreshCalls)
}

func TestSetProtocolVersionBeforeBarrierPreparationDefersToStartupFence(t *testing.T) {
	const serviceID = "ddl-visibility-pre-start-activation-test"
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	s := &service{cfg: &Config{UUID: serviceID}}

	require.NoError(t, s.setProtocolVersion(context.Background(), defines.MORPCVersion44, nil))
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion44, version)
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
	s.ddlVisibilityActivationPrepared.Store(true)
	s.ddlVisibilityActivationFenced.Store(true)
	s.initQueryCommandHandler()

	getHandler, ok := queryService.handlers[query.CmdMethod_GetProtocolVersion]
	require.True(t, ok)
	getResp := &query.Response{}
	require.NoError(t, getHandler(context.Background(), &query.Request{
		GetProtocolVersion: &query.GetProtocolVersionRequest{},
	}, getResp, nil))
	require.Equal(t, defines.MORPCVersion34, getResp.GetProtocolVersion.Version)
	require.True(t, getResp.GetProtocolVersion.DDLVisibilityActivationPrepared)
	require.True(t, getResp.GetProtocolVersion.DDLVisibilityActivationFenced)
	require.ErrorContains(t,
		getHandler(context.Background(), &query.Request{}, &query.Response{}, nil),
		"bad request")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, "invalid")
	require.ErrorContains(t, getHandler(context.Background(), &query.Request{
		GetProtocolVersion: &query.GetProtocolVersionRequest{},
	}, &query.Response{}, nil), "invalid protocol version")
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion34)

	handler, ok := queryService.handlers[query.CmdMethod_SetProtocolVersion]
	require.True(t, ok)
	resp := &query.Response{}
	err := handler(context.Background(), &query.Request{
		SetProtocolVersion: &query.SetProtocolVersionRequest{Version: defines.MORPCVersion44},
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

	err := s.setProtocolVersion(context.Background(), defines.MORPCVersion44, nil)
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

func TestValidateDDLVisibilityActivationTargets(t *testing.T) {
	_, err := validateDDLVisibilityActivationTargets("self", []string{"self", ""})
	require.ErrorContains(t, err, "empty")
	_, err = validateDDLVisibilityActivationTargets("self", []string{"self", "self"})
	require.ErrorContains(t, err, "duplicated")
	_, err = validateDDLVisibilityActivationTargets("self", []string{"peer"})
	require.ErrorContains(t, err, "do not include local CN")
	targets, err := validateDDLVisibilityActivationTargets("self", []string{"self", "peer"})
	require.NoError(t, err)
	require.Equal(t, map[string]struct{}{"self": {}, "peer": {}}, targets)
}

func TestSetProtocolVersionDowngradeGuards(t *testing.T) {
	const serviceID = "ddl-visibility-downgrade-test"
	rt := moruntime.DefaultRuntime()
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion44)
	moruntime.SetupServiceBasedRuntime(serviceID, rt)
	s := &service{cfg: &Config{UUID: serviceID}}
	s.ddlVisibilityBarrierPrepared.Store(true)
	s.ddlVisibilityActivationPrepared.Store(true)
	s.ddlVisibilityActivationFenced.Store(true)
	s.ddlVisibilityActivationComplete.Store(true)

	err := s.setProtocolVersion(context.Background(), defines.MORPCVersion40, nil)
	require.ErrorContains(t, err, "cannot downgrade after the DDL visibility cluster epoch is committed")
	version, ok := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion44, version)

	s.ddlVisibilityActivationComplete.Store(false)
	s._hakeeperClient = &ddlVisibilityWithdrawalHAKeeperClient{
		clusterDeployedProtocol: defines.MORPCVersion44,
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion40)
	err = s.setProtocolVersion(context.Background(), defines.MORPCVersion34, nil)
	require.ErrorContains(t, err, "cannot downgrade after the DDL visibility cluster epoch is committed")

	s._hakeeperClient = nil
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion40)
	require.NoError(t, s.setProtocolVersion(context.Background(), defines.MORPCVersion34, nil))
	version, ok = rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, ok)
	require.Equal(t, defines.MORPCVersion34, version)

	s.ddlVisibilityActivationPending.Store(true)
	err = s.setProtocolVersion(context.Background(), defines.MORPCVersion34, nil)
	require.ErrorContains(t, err, "cannot downgrade during DDL visibility activation")
}

func TestWaitForDDLVisibilityActivationPhaseRejectsInvalidInventory(t *testing.T) {
	const serviceID = "activation-phase-inventory-test"
	newService := func(cluster *ddlVisibilityTestCluster) *service {
		cfg := &Config{UUID: serviceID}
		cfg.HAKeeper.HeatbeatInterval.Duration = time.Millisecond
		s := &service{cfg: cfg, moCluster: cluster,
			_hakeeperClient: &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster}}
		s.ddlVisibilityActivationPrepared.Store(true)
		s.ddlVisibilityActivationFenced.Store(true)
		return s
	}
	targets := map[string]struct{}{serviceID: {}, "peer": {}}

	t.Run("target address is required", func(t *testing.T) {
		cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{
			{ServiceID: serviceID, DDLVisibilityBarrierReady: true},
			{ServiceID: "peer", DDLVisibilityBarrierReady: true},
		}}
		s := newService(cluster)
		err := s.waitForDDLVisibilityActivationPhase(context.Background(), targets, false)
		require.ErrorContains(t, err, "has no authoritative identity")
	})

	t.Run("target list must include every barrier participant", func(t *testing.T) {
		cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{
			{ServiceID: serviceID, QueryAddress: "self:6001", ViewMetadataAdmissionGeneration: 7,
				DDLVisibilityBarrierReady: true},
			{ServiceID: "peer", QueryAddress: "peer:6001", ViewMetadataAdmissionGeneration: 8,
				DDLVisibilityBarrierReady: true},
			{ServiceID: "extra", QueryAddress: "extra:6001", ViewMetadataAdmissionGeneration: 9,
				DDLVisibilityBarrierReady: true},
		}}
		s := newService(cluster)
		err := s.waitForDDLVisibilityActivationPhase(context.Background(), targets, false)
		require.ErrorContains(t, err, "omits authoritative CN")
	})
}

func TestWaitForDDLVisibilityIngressHonorsCancellation(t *testing.T) {
	const serviceID = "ddl-visibility-ingress-cancel-test"
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
		ServiceID: serviceID, ViewMetadataAdmissionGeneration: 1,
		DDLVisibilityBarrierReady: true, ViewMetadataIngressReady: true,
	}}}
	s := &service{
		cfg: &Config{UUID: serviceID}, moCluster: cluster,
		viewMetadataAdmissionGeneration: 1,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	err := s.waitForDDLVisibilityIngress(ctx, time.Second, false)
	require.ErrorContains(t, err, "was not published")
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

func TestPeriodicHeartbeatCannotRepublishStaleIngressDuringActivation(t *testing.T) {
	client := &ddlVisibilityHeartbeatOrderClient{
		firstStarted: make(chan struct{}), releaseFirst: make(chan struct{}),
	}
	cfg := &Config{UUID: "ddl-visibility-heartbeat-order-test"}
	cfg.HAKeeper.HeatbeatTimeout.Duration = time.Second
	s := &service{
		cfg: cfg, logger: zap.NewNop(), _hakeeperClient: client,
		config: util.NewConfigData(nil),
	}
	s.ddlVisibilityBarrierReady.Store(true)
	s.viewMetadataIngressReady.Store(true)

	heartbeatDone := make(chan struct{})
	go func() {
		s.heartbeat(context.Background())
		close(heartbeatDone)
	}()
	<-client.firstStarted
	activationStarted := make(chan struct{})
	activationDone := make(chan struct{})
	go func() {
		close(activationStarted)
		s.ddlVisibilityHeartbeatMu.Lock()
		s.viewMetadataIngressReady.Store(false)
		_, _ = client.SendCNHeartbeat(context.Background(), s.newCNStoreHeartbeat())
		s.ddlVisibilityHeartbeatMu.Unlock()
		close(activationDone)
	}()
	<-activationStarted
	close(client.releaseFirst)
	<-heartbeatDone
	<-activationDone

	client.mu.Lock()
	defer client.mu.Unlock()
	require.Len(t, client.completed, 2)
	require.True(t, client.completed[0].ViewMetadataIngressReady)
	require.False(t, client.completed[1].ViewMetadataIngressReady,
		"the activation withdrawal must complete after every older true heartbeat")
}

func TestServiceCloseWithdrawsDDLVisibilityBeforeQueryService(t *testing.T) {
	state := newDDLVisibilityCloseTestService(t, nil)

	require.NoError(t, state.service.Close())
	require.True(t, state.hakeeperClient.queryClosedBeforeSend,
		"view-admission withdrawal intentionally follows QueryService drain")
	require.False(t, state.queryClosedBeforeRefresh)
	require.Len(t, state.hakeeperClient.heartbeats, 2)
	require.False(t, state.hakeeperClient.heartbeats[0].DDLVisibilityBarrierReady)
	require.False(t, state.hakeeperClient.heartbeats[0].ViewMetadataIngressReady,
		"shutdown seals local view ingress before either authoritative withdrawal")
	require.False(t, state.hakeeperClient.heartbeats[1].DDLVisibilityBarrierReady)
	require.False(t, state.hakeeperClient.heartbeats[1].ViewMetadataIngressReady)
	require.Equal(t, 1, state.cluster.refreshCalls)
	require.False(t, state.cluster.cnServices[0].DDLVisibilityBarrierReady)
	require.Equal(t, 1, state.hakeeperClient.closeCalls)
	require.Equal(t, 2, state.lockService.closeCalls)
	_, err := state.service.ddlCommitGate.Enter(context.Background())
	require.ErrorContains(t, err, "closed")
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
	require.True(t, state.hakeeperClient.queryClosedBeforeSend,
		"clean view-admission withdrawal is still attempted after the DDL withdrawal failure")
	require.Len(t, state.hakeeperClient.heartbeats, 2)
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

func TestAcknowledgedDDLFrontierSurvivesCrashBeforePeriodicHeartbeat(t *testing.T) {
	const producerID = "ddl-producer"
	frontier := timestamp.Timestamp{PhysicalTime: 300}
	cluster := &ddlVisibilityTestCluster{cnServices: []metadata.CNService{{
		ServiceID: producerID, QueryAddress: "old:6001",
		ViewMetadataAdmissionGeneration: 1, DDLVisibilityBarrierReady: true,
	}}}
	client := &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster}
	producer := &service{
		cfg: &Config{UUID: producerID}, _hakeeperClient: client,
		ddlCommitGate: frontend.NewDDLCommitGate(), config: util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: 1,
	}

	// This is the synchronous publication in the successful DDL commit path;
	// deliberately do not run a periodic heartbeat before replacing the CN.
	producer.ddlCommitGate.RecordDDLFrontier(frontier)
	require.NoError(t, producer.publishDDLCommitFrontier(context.Background(), frontier))
	require.Equal(t, frontier, cluster.globalFrontier)

	// A fresh same-UUID incarnation starts with an empty process-local gate.
	replacement := &service{
		cfg: &Config{UUID: producerID}, _hakeeperClient: client,
		ddlCommitGate: frontend.NewDDLCommitGate(), config: util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: 2,
	}
	require.NoError(t, replacement.publishDDLCommitFrontier(context.Background(), timestamp.Timestamp{}))
	require.Equal(t, frontier, cluster.globalFrontier,
		"replacement must not erase the synchronously published commit frontier")

	ctrl := gomock.NewController(t)
	laggingTxnClient := mock_frontend.NewMockTxnClient(ctrl)
	laggingTxnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), frontier).Return(frontier, nil)
	laggingTxnClient.EXPECT().SyncLatestCommitTS(frontier)
	target := &service{
		cfg: &Config{UUID: "lagging-peer"}, _txnClient: laggingTxnClient,
		_hakeeperClient: client,
	}
	require.NoError(t, target.syncStartupDDLVisibilityFrontier(context.Background()))
}

func TestLegacyHAKeeperCannotAcknowledgeDroppedDDLFrontier(t *testing.T) {
	const producerID = "legacy-hakeeper-ddl-producer"
	oldFrontier := timestamp.Timestamp{PhysicalTime: 200}
	commitFrontier := timestamp.Timestamp{PhysicalTime: 300}
	cluster := &ddlVisibilityTestCluster{
		cnServices: []metadata.CNService{{
			ServiceID: producerID, QueryAddress: "producer:6001",
			ViewMetadataAdmissionGeneration: 1, DDLVisibilityBarrierReady: true,
		}},
		globalFrontier: oldFrontier,
	}
	client := &ddlVisibilityWithdrawalHAKeeperClient{
		cluster: cluster, oldDDLFrontierRSM: true,
	}
	producer := &service{
		cfg: &Config{UUID: producerID}, _hakeeperClient: client,
		ddlCommitGate: frontend.NewDDLCommitGate(), config: util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: 1, logger: zap.NewNop(),
	}
	producer.ddlCommitGate.SetFrontierPublisher(producer.publishDDLCommitFrontier)

	err := producer.ddlCommitGate.PublishDDLFrontier(context.Background(), commitFrontier)
	require.ErrorContains(t, err, "was not durably acknowledged")
	require.Equal(t, oldFrontier, cluster.globalFrontier)

	ctrl := gomock.NewController(t)
	restartedTxnClient := mock_frontend.NewMockTxnClient(ctrl)
	restartedTxnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), oldFrontier).Return(oldFrontier, nil)
	restartedTxnClient.EXPECT().SyncLatestCommitTS(oldFrontier)
	restarted := &service{
		cfg: &Config{UUID: producerID}, _txnClient: restartedTxnClient,
		_hakeeperClient: client,
	}
	require.NoError(t, restarted.syncStartupDDLVisibilityFrontier(context.Background()))
}

func TestStaleGenerationCannotAcknowledgeDDLFrontierPublication(t *testing.T) {
	const producerID = "generation-fenced-ddl-producer"
	oldFrontier := timestamp.Timestamp{PhysicalTime: 200}
	commitFrontier := timestamp.Timestamp{PhysicalTime: 300}
	cluster := &ddlVisibilityTestCluster{
		cnServices: []metadata.CNService{{
			ServiceID: producerID, QueryAddress: "new:6001",
			ViewMetadataAdmissionGeneration: 2, DDLVisibilityBarrierReady: true,
		}},
		globalFrontier: oldFrontier,
	}
	client := &ddlVisibilityWithdrawalHAKeeperClient{
		cluster:                  cluster,
		authoritativeGenerations: map[string]uint64{producerID: 2},
	}
	oldIncarnation := &service{
		cfg: &Config{UUID: producerID}, _hakeeperClient: client,
		ddlCommitGate: frontend.NewDDLCommitGate(), config: util.NewConfigData(nil),
		viewMetadataAdmissionGeneration: 1, logger: zap.NewNop(),
	}
	runner := &blockedStopTaskRunner{
		testRunner: &testRunner{}, stopEntered: make(chan struct{}),
		releaseStop: make(chan struct{}), stopDone: make(chan struct{}),
	}
	oldIncarnation.task.runner = runner
	oldIncarnation.task.runnerReady.Store(true)
	oldIncarnation.ddlCommitGate.SetFrontierPublisher(oldIncarnation.publishDDLCommitFrontier)

	// Model the stale publication running inside the TaskRunner SQL task. The
	// rejection must return before asynchronous runner.Stop can observe this task
	// exit; otherwise each side waits permanently for the other.
	publishDone := make(chan error, 1)
	go func() {
		publishDone <- oldIncarnation.ddlCommitGate.PublishDDLFrontier(
			context.Background(), commitFrontier)
	}()
	var err error
	select {
	case err = <-publishDone:
	case <-time.After(time.Second):
		t.Fatal("generation rejection self-waited on its own TaskRunner SQL task")
	}
	require.ErrorContains(t, err, "generation 1 rejected by authoritative generation 2")
	select {
	case <-runner.stopEntered:
	case <-time.After(time.Second):
		t.Fatal("asynchronous revocation did not begin TaskRunner drain")
	}
	close(runner.releaseStop)
	select {
	case <-runner.stopDone:
	case <-time.After(time.Second):
		t.Fatal("TaskRunner drain did not finish after the rejected SQL task exited")
	}
	require.True(t, oldIncarnation.viewMetadataGenerationRevoked.Load())
	require.Equal(t, commitFrontier, cluster.globalFrontier,
		"an already-admitted stale-generation commit must advance the durable frontier")

	// After the stale process exits, every future incarnation observes T even
	// though no incarnation-scoped admission state was accepted from generation 1.
	ctrl := gomock.NewController(t)
	restartedTxnClient := mock_frontend.NewMockTxnClient(ctrl)
	restartedTxnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), commitFrontier).Return(commitFrontier, nil)
	restartedTxnClient.EXPECT().SyncLatestCommitTS(commitFrontier)
	restarted := &service{
		cfg: &Config{UUID: producerID}, _txnClient: restartedTxnClient,
		_hakeeperClient: client,
	}
	require.NoError(t, restarted.syncStartupDDLVisibilityFrontier(context.Background()))
}

func TestSyncStartupDDLVisibilityFrontierSurvivesProducerReplacement(t *testing.T) {
	const serviceID = "ddl-visibility-durable-frontier-test"
	oldFrontier := timestamp.Timestamp{PhysicalTime: 300}
	cluster := &ddlVisibilityTestCluster{
		cnServices: []metadata.CNService{{
			ServiceID: "replaced-producer", QueryAddress: "new:6001",
			ViewMetadataAdmissionGeneration: 2, DDLVisibilityBarrierReady: true,
		}},
		globalFrontier: oldFrontier,
	}
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().WaitLogTailAppliedAt(gomock.Any(), oldFrontier).Return(oldFrontier, nil)
	txnClient.EXPECT().SyncLatestCommitTS(oldFrontier)
	s := &service{
		cfg: &Config{UUID: serviceID}, _txnClient: txnClient,
		_hakeeperClient: &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster},
	}

	require.NoError(t, s.syncStartupDDLVisibilityFrontier(context.Background()))
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
	s := &service{
		cfg: &Config{UUID: serviceID}, moCluster: cluster, queryClient: queryClient,
		_hakeeperClient: &ddlVisibilityWithdrawalHAKeeperClient{cluster: cluster},
	}

	require.NoError(t, s.syncStartupDDLVisibilityFrontier(context.Background()))
	require.Empty(t, queryClient.requests)
	require.Zero(t, queryClient.releases)
}
