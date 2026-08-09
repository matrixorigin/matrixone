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

package frontend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type globalSysVarSyncRequest struct {
	address  string
	method   querypb.CmdMethod
	commitTS timestamp.Timestamp
}

type globalSysVarSyncQueryClient struct {
	mu           sync.Mutex
	serviceID    string
	requests     []globalSysVarSyncRequest
	errors       map[string]error
	entered      map[string]chan struct{}
	blocks       map[string]chan struct{}
	releaseCount int
}

func newGlobalSysVarSyncQueryClient() *globalSysVarSyncQueryClient {
	return &globalSysVarSyncQueryClient{
		serviceID: "global-sysvar-sync-test",
		errors:    make(map[string]error),
		entered:   make(map[string]chan struct{}),
		blocks:    make(map[string]chan struct{}),
	}
}

func (m *globalSysVarSyncQueryClient) ServiceID() string {
	return m.serviceID
}

func (m *globalSysVarSyncQueryClient) SendMessage(
	ctx context.Context,
	address string,
	req *querypb.Request,
) (*querypb.Response, error) {
	m.mu.Lock()
	record := globalSysVarSyncRequest{address: address, method: req.CmdMethod}
	if req.SycnCommit != nil {
		record.commitTS = req.SycnCommit.LatestCommitTS
	}
	m.requests = append(m.requests, record)
	err := m.errors[address]
	entered := m.entered[address]
	block := m.blocks[address]
	m.mu.Unlock()

	if entered != nil {
		close(entered)
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	return &querypb.Response{}, nil
}

func (m *globalSysVarSyncQueryClient) NewRequest(method querypb.CmdMethod) *querypb.Request {
	return &querypb.Request{CmdMethod: method}
}

func (m *globalSysVarSyncQueryClient) Release(*querypb.Response) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseCount++
}

func (m *globalSysVarSyncQueryClient) Close() error {
	return nil
}

func (m *globalSysVarSyncQueryClient) snapshot() ([]globalSysVarSyncRequest, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	requests := append([]globalSysVarSyncRequest(nil), m.requests...)
	return requests, m.releaseCount
}

type globalSysVarSyncCluster struct {
	mu               sync.Mutex
	cnServices       []metadata.CNService
	refreshSnapshots [][]metadata.CNService
	refreshErr       error
	refreshBlock     <-chan struct{}
	refreshCalls     int
}

func (m *globalSysVarSyncCluster) GetCNService(
	_ clusterservice.Selector,
	apply func(metadata.CNService) bool,
) {
	m.mu.Lock()
	services := append([]metadata.CNService(nil), m.cnServices...)
	m.mu.Unlock()
	for _, cn := range services {
		if !apply(cn) {
			return
		}
	}
}

func (m *globalSysVarSyncCluster) Refresh(ctx context.Context) error {
	if m.refreshBlock != nil {
		select {
		case <-m.refreshBlock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.refreshErr != nil {
		return m.refreshErr
	}
	if m.refreshCalls < len(m.refreshSnapshots) {
		m.cnServices = append([]metadata.CNService(nil), m.refreshSnapshots[m.refreshCalls]...)
	}
	m.refreshCalls++
	return nil
}

func (m *globalSysVarSyncCluster) refreshCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.refreshCalls
}

func (*globalSysVarSyncCluster) GetTNService(clusterservice.Selector, func(metadata.TNService) bool) {
}

func (*globalSysVarSyncCluster) GetAllTNServices() []metadata.TNService {
	return nil
}

func (*globalSysVarSyncCluster) GetCNServiceWithoutWorkingState(
	clusterservice.Selector,
	func(metadata.CNService) bool,
) {
}

func (*globalSysVarSyncCluster) ForceRefresh(bool) {}
func (*globalSysVarSyncCluster) Close()            {}
func (*globalSysVarSyncCluster) DebugUpdateCNLabel(string, map[string][]string) error {
	return nil
}
func (*globalSysVarSyncCluster) DebugUpdateCNWorkState(string, int) error { return nil }
func (*globalSysVarSyncCluster) RemoveCN(string)                          {}
func (*globalSysVarSyncCluster) AddCN(metadata.CNService)                 {}
func (*globalSysVarSyncCluster) UpdateCN(metadata.CNService)              {}

func TestSyncCommitTimestampToCNs(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	qc := newGlobalSysVarSyncQueryClient()
	cluster := &globalSysVarSyncCluster{cnServices: []metadata.CNService{
		{QueryAddress: "cn-1"},
		{QueryAddress: ""},
		{QueryAddress: "cn-2"},
	}}

	require.NoError(t, syncCommitTimestampToCNs(context.Background(), qc, cluster, commitTS))

	requests, releaseCount := qc.snapshot()
	require.ElementsMatch(t, []globalSysVarSyncRequest{
		{address: "cn-1", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS},
		{address: "cn-2", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS},
	}, requests)
	require.Equal(t, 2, releaseCount)
}

func TestSyncGlobalSysVarCommitUsesLatestTxnClientCommit(t *testing.T) {
	ctrl := gomock.NewController(t)
	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().GetLatestCommitTS().Return(commitTS)

	serviceID := t.Name()
	qc := newGlobalSysVarSyncQueryClient()
	qc.serviceID = serviceID
	cluster := &globalSysVarSyncCluster{cnServices: []metadata.CNService{{QueryAddress: "cn-1"}}}
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, txnClient, nil)
	pu.QueryClient = qc
	InitServerLevelVars(serviceID)
	setPu(serviceID, pu)
	rt := runtime.NewRuntime(metadata.ServiceType_CN, serviceID, zap.NewNop())
	rt.SetGlobalVariables(runtime.ClusterService, cluster)
	runtime.SetupServiceBasedRuntime(serviceID, rt)
	ses := &Session{feSessionImpl: feSessionImpl{service: serviceID}}

	require.NoError(t, syncGlobalSysVarCommit(context.Background(), ses))

	requests, releaseCount := qc.snapshot()
	require.Equal(t, []globalSysVarSyncRequest{
		{address: "cn-1", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS},
	}, requests)
	require.Equal(t, 1, releaseCount)
}

func TestSyncGlobalSysVarCommitRejectsEmptyCommitTimestamp(t *testing.T) {
	ctrl := gomock.NewController(t)
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().GetLatestCommitTS().Return(timestamp.Timestamp{})

	serviceID := t.Name()
	qc := newGlobalSysVarSyncQueryClient()
	qc.serviceID = serviceID
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, txnClient, nil)
	pu.QueryClient = qc
	InitServerLevelVars(serviceID)
	setPu(serviceID, pu)
	ses := &Session{feSessionImpl: feSessionImpl{service: serviceID}}

	err := syncGlobalSysVarCommit(context.Background(), ses)
	require.Error(t, err)
	require.Contains(t, err.Error(), "commit timestamp is empty")
	requests, _ := qc.snapshot()
	require.Empty(t, requests)
}

func TestSyncCommitTimestampToCNsWaitsForEveryCN(t *testing.T) {
	qc := newGlobalSysVarSyncQueryClient()
	entered := make(chan struct{})
	unblock := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-unblock:
		default:
			close(unblock)
		}
	})
	qc.entered["cn-1"] = entered
	qc.blocks["cn-1"] = unblock
	cluster := &globalSysVarSyncCluster{cnServices: []metadata.CNService{{QueryAddress: "cn-1"}}}
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		done <- syncCommitTimestampToCNs(
			ctx,
			qc,
			cluster,
			timestamp.Timestamp{PhysicalTime: 100},
		)
	}()

	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("sync did not send the CN request")
	}
	select {
	case err := <-done:
		t.Fatalf("sync returned before the CN responded: %v", err)
	default:
	}
	close(unblock)
	require.NoError(t, <-done)
}

func TestSyncCommitTimestampToCNsReturnsPartialFailure(t *testing.T) {
	qc := newGlobalSysVarSyncQueryClient()
	qc.errors["cn-2"] = errors.New("send failed")
	cluster := &globalSysVarSyncCluster{cnServices: []metadata.CNService{
		{QueryAddress: "cn-1"},
		{QueryAddress: "cn-2"},
	}}

	err := syncCommitTimestampToCNs(
		context.Background(),
		qc,
		cluster,
		timestamp.Timestamp{PhysicalTime: 100},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cn-2")

	requests, releaseCount := qc.snapshot()
	require.Len(t, requests, 2)
	require.Equal(t, 1, releaseCount)
}

func TestSyncCommitTimestampToCNsRefreshesStaleMembership(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	qc := newGlobalSysVarSyncQueryClient()
	cluster := &globalSysVarSyncCluster{
		cnServices: []metadata.CNService{{QueryAddress: "cn-1"}},
		refreshSnapshots: [][]metadata.CNService{
			{{QueryAddress: "cn-1"}, {QueryAddress: "cn-2"}},
			{{QueryAddress: "cn-1"}, {QueryAddress: "cn-2"}},
		},
	}

	require.NoError(t, syncCommitTimestampToCNs(context.Background(), qc, cluster, commitTS))
	requests, releaseCount := qc.snapshot()
	require.ElementsMatch(t, []globalSysVarSyncRequest{
		{address: "cn-1", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS},
		{address: "cn-2", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS},
	}, requests)
	require.Equal(t, 2, releaseCount)
	require.Equal(t, 2, cluster.refreshCount())
}

func TestSyncCommitTimestampToCNsConvergesGrowingMembership(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	qc := newGlobalSysVarSyncQueryClient()
	cluster := &globalSysVarSyncCluster{refreshSnapshots: [][]metadata.CNService{
		{{QueryAddress: "cn-1"}},
		{{QueryAddress: "cn-1"}, {QueryAddress: "cn-2"}},
		{{QueryAddress: "cn-1"}, {QueryAddress: "cn-2"}},
	}}

	require.NoError(t, syncCommitTimestampToCNs(context.Background(), qc, cluster, commitTS))
	requests, releaseCount := qc.snapshot()
	require.Equal(t, []globalSysVarSyncRequest{
		{address: "cn-1", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS},
		{address: "cn-2", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS},
	}, requests)
	require.Equal(t, 2, releaseCount)
	require.Equal(t, 3, cluster.refreshCount())
}

func TestSyncCommitTimestampToCNsResyncsReplacedCNGeneration(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	qc := newGlobalSysVarSyncQueryClient()
	cluster := &globalSysVarSyncCluster{refreshSnapshots: [][]metadata.CNService{
		{{ServiceID: "cn-old", QueryAddress: "cn-address"}},
		{{ServiceID: "cn-new", QueryAddress: "cn-address"}},
		{{ServiceID: "cn-new", QueryAddress: "cn-address"}},
	}}

	require.NoError(t, syncCommitTimestampToCNs(context.Background(), qc, cluster, commitTS))
	requests, releaseCount := qc.snapshot()
	require.Equal(t, []globalSysVarSyncRequest{
		{address: "cn-address", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS},
		{address: "cn-address", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS},
	}, requests)
	require.Equal(t, 2, releaseCount)
	require.Equal(t, 3, cluster.refreshCount())
}

func TestSyncCommitTimestampToCNsRefreshFailure(t *testing.T) {
	qc := newGlobalSysVarSyncQueryClient()
	refreshErr := errors.New("hakeeper unavailable")
	cluster := &globalSysVarSyncCluster{refreshErr: refreshErr}

	err := syncCommitTimestampToCNs(
		context.Background(), qc, cluster, timestamp.Timestamp{PhysicalTime: 100})
	require.ErrorIs(t, err, refreshErr)
	requests, releaseCount := qc.snapshot()
	require.Empty(t, requests)
	require.Zero(t, releaseCount)
}

func TestSyncCommitTimestampToCNsRefreshHonorsContext(t *testing.T) {
	qc := newGlobalSysVarSyncQueryClient()
	cluster := &globalSysVarSyncCluster{refreshBlock: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := syncCommitTimestampToCNs(ctx, qc, cluster, timestamp.Timestamp{PhysicalTime: 100})
	require.ErrorIs(t, err, context.Canceled)
	requests, releaseCount := qc.snapshot()
	require.Empty(t, requests)
	require.Zero(t, releaseCount)
}

func TestSyncCommitTimestampToCNsNoop(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100}

	t.Run("empty commit timestamp", func(t *testing.T) {
		qc := newGlobalSysVarSyncQueryClient()
		cluster := &globalSysVarSyncCluster{cnServices: []metadata.CNService{{QueryAddress: "cn-1"}}}
		require.NoError(t, syncCommitTimestampToCNs(context.Background(), qc, cluster, timestamp.Timestamp{}))
		requests, _ := qc.snapshot()
		require.Empty(t, requests)
	})

	t.Run("nil query client", func(t *testing.T) {
		cluster := &globalSysVarSyncCluster{cnServices: []metadata.CNService{{QueryAddress: "cn-1"}}}
		require.NoError(t, syncCommitTimestampToCNs(context.Background(), nil, cluster, commitTS))
	})

	t.Run("nil cluster", func(t *testing.T) {
		qc := newGlobalSysVarSyncQueryClient()
		require.NoError(t, syncCommitTimestampToCNs(context.Background(), qc, nil, commitTS))
		requests, _ := qc.snapshot()
		require.Empty(t, requests)
	})

	t.Run("no routable CN", func(t *testing.T) {
		qc := newGlobalSysVarSyncQueryClient()
		cluster := &globalSysVarSyncCluster{cnServices: []metadata.CNService{{QueryAddress: ""}}}
		require.NoError(t, syncCommitTimestampToCNs(context.Background(), qc, cluster, commitTS))
		requests, _ := qc.snapshot()
		require.Empty(t, requests)
	})
}
