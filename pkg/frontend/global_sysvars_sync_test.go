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

	"github.com/golang/mock/gomock"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

type globalSysVarFenceHAKeeper struct {
	logservice.CNHAKeeperClient
	mu        sync.Mutex
	updates   []timestamp.Timestamp
	details   []logpb.ClusterDetails
	updateErr error
	detailErr error
	gets      int
}

func (m *globalSysVarFenceHAKeeper) UpdateGlobalSysVarCommitTS(
	_ context.Context,
	ts timestamp.Timestamp,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.updates = append(m.updates, ts)
	return m.updateErr
}

func (m *globalSysVarFenceHAKeeper) GetClusterDetails(
	_ context.Context,
) (logpb.ClusterDetails, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.detailErr != nil {
		return logpb.ClusterDetails{}, m.detailErr
	}
	if len(m.details) == 0 {
		m.gets++
		return logpb.ClusterDetails{}, nil
	}
	i := m.gets
	if i >= len(m.details) {
		i = len(m.details) - 1
	}
	m.gets++
	return m.details[i], nil
}

func (m *globalSysVarFenceHAKeeper) snapshot() ([]timestamp.Timestamp, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]timestamp.Timestamp(nil), m.updates...), m.gets
}

func setupGlobalSysVarFenceSession(
	t *testing.T,
	version int64,
	hakeeper logservice.CNHAKeeperClient,
	txnClient *mock_frontend.MockTxnClient,
) *Session {
	t.Helper()
	serviceID := t.Name()
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, txnClient, nil)
	pu.HAKeeperClient = hakeeper
	InitServerLevelVars(serviceID)
	setPu(serviceID, pu)
	rt := runtime.NewRuntime(metadata.ServiceType_CN, serviceID, zap.NewNop())
	rt.SetGlobalVariables(runtime.MOProtocolVersion, version)
	runtime.SetupServiceBasedRuntime(serviceID, rt)
	return &Session{feSessionImpl: feSessionImpl{service: serviceID}}
}

func TestValidateGlobalSysVarSyncProtocolRollingUpgrade(t *testing.T) {
	t.Run("previous latest version fails closed", func(t *testing.T) {
		hakeeper := &globalSysVarFenceHAKeeper{}
		ses := setupGlobalSysVarFenceSession(
			t, defines.MORPCVersion13, hakeeper, nil)
		err := validateGlobalSysVarSyncProtocol(context.Background(), ses)
		require.ErrorContains(t, err, "protocol version 14")
		updates, gets := hakeeper.snapshot()
		require.Empty(t, updates)
		require.Zero(t, gets)
	})

	t.Run("version 14 enables fence", func(t *testing.T) {
		ses := setupGlobalSysVarFenceSession(
			t, defines.MORPCVersion14, &globalSysVarFenceHAKeeper{}, nil)
		require.NoError(t, validateGlobalSysVarSyncProtocol(context.Background(), ses))
	})

	t.Run("missing capability fails closed", func(t *testing.T) {
		hakeeper := struct{ logservice.CNHAKeeperClient }{}
		ses := setupGlobalSysVarFenceSession(t, defines.MORPCVersion14, hakeeper, nil)
		require.ErrorContains(t,
			validateGlobalSysVarSyncProtocol(context.Background(), ses),
			"does not support global system variable fencing")
	})

	t.Run("standalone remains compatible", func(t *testing.T) {
		ses := setupGlobalSysVarFenceSession(t, defines.MORPCVersion13, nil, nil)
		require.NoError(t, validateGlobalSysVarSyncProtocol(context.Background(), ses))
	})
}

func TestSetGlobalSysVarRollingUpgradeRejectsBeforeCatalogWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	previousRuntime := runtime.ServiceRuntime(ses.GetService())
	t.Cleanup(func() {
		if previousRuntime != nil {
			runtime.SetupServiceBasedRuntime(ses.GetService(), previousRuntime)
		}
	})
	hakeeper := &globalSysVarFenceHAKeeper{}
	getPuIfPresent(ses.GetService()).HAKeeperClient = hakeeper
	rt := runtime.NewRuntime(metadata.ServiceType_CN, ses.GetService(), zap.NewNop())
	rt.SetGlobalVariables(runtime.MOProtocolVersion, defines.MORPCVersion13)
	runtime.SetupServiceBasedRuntime(ses.GetService(), rt)

	background := &backgroundExecTest{}
	background.init()
	stub := gostub.StubFunc(&NewBackgroundExec, background)
	t.Cleanup(stub.Reset)

	err := ses.SetGlobalSysVar(context.Background(), "autocommit", int64(0))
	require.ErrorContains(t, err, "protocol version 14")
	require.Empty(t, background.executedSQLs,
		"rolling-upgrade rejection must happen before opening the catalog transaction")
	updates, gets := hakeeper.snapshot()
	require.Empty(t, updates)
	require.Zero(t, gets)
}

func TestSyncGlobalSysVarCommitPublishesAndWaitsForFence(t *testing.T) {
	ctrl := gomock.NewController(t)
	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	txnClient := mock_frontend.NewMockTxnClient(ctrl)
	txnClient.EXPECT().GetLatestCommitTS().Return(commitTS)
	hakeeper := &globalSysVarFenceHAKeeper{details: []logpb.ClusterDetails{{
		GlobalSysVarCommitTS: commitTS,
		ProxyStores: []logpb.ProxyStore{{
			UUID: "proxy-1", GlobalSysVarCommitTS: commitTS,
		}},
		CNStores: []logpb.CNStore{
			{UUID: "cn-1", SQLAddress: "sql-1", State: logpb.NormalState,
				WorkState: metadata.WorkState_Working, GlobalSysVarCommitTS: commitTS},
			{UUID: "cn-draining", SQLAddress: "sql-2", State: logpb.NormalState,
				WorkState: metadata.WorkState_Draining},
			{UUID: "cn-expired", SQLAddress: "sql-3", State: logpb.TimeoutState,
				WorkState: metadata.WorkState_Working},
		},
	}}}
	ses := setupGlobalSysVarFenceSession(
		t, defines.MORPCVersion14, hakeeper, txnClient)

	require.NoError(t, syncGlobalSysVarCommit(context.Background(), ses))
	updates, gets := hakeeper.snapshot()
	require.Equal(t, []timestamp.Timestamp{commitTS}, updates)
	require.Equal(t, 1, gets)
}

func TestWaitGlobalSysVarCommitFenceWaitsForProxyRouteBarrier(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100}
	hakeeper := &globalSysVarFenceHAKeeper{details: []logpb.ClusterDetails{
		{
			GlobalSysVarCommitTS: commitTS,
			ProxyStores:          []logpb.ProxyStore{{UUID: "proxy-1"}},
		},
		{
			GlobalSysVarCommitTS: commitTS,
			ProxyStores: []logpb.ProxyStore{{
				UUID: "proxy-1", GlobalSysVarCommitTS: commitTS,
			}},
		},
	}}
	require.NoError(t, waitGlobalSysVarCommitFence(
		context.Background(), hakeeper.GetClusterDetails, commitTS))
	_, gets := hakeeper.snapshot()
	require.Equal(t, 2, gets)
}

func TestWaitGlobalSysVarCommitFenceIgnoresExpiredProxy(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100}
	details := logpb.ClusterDetails{
		GlobalSysVarCommitTS: commitTS,
		ProxyStores: []logpb.ProxyStore{
			{UUID: "proxy-live", State: logpb.NormalState, GlobalSysVarCommitTS: commitTS},
			{UUID: "proxy-expired", State: logpb.TimeoutState},
		},
	}
	require.NoError(t, waitGlobalSysVarCommitFence(
		context.Background(), func(context.Context) (logpb.ClusterDetails, error) {
			return details, nil
		}, commitTS))
}

func TestWaitGlobalSysVarCommitFenceIncludesLateJoin(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100}
	hakeeper := &globalSysVarFenceHAKeeper{details: []logpb.ClusterDetails{
		{
			GlobalSysVarCommitTS: commitTS,
			CNStores: []logpb.CNStore{{
				UUID: "cn-a", SQLAddress: "sql-a", State: logpb.NormalState,
				WorkState: metadata.WorkState_Working, GlobalSysVarCommitTS: commitTS,
			}},
		},
		{
			GlobalSysVarCommitTS: commitTS,
			CNStores: []logpb.CNStore{
				{UUID: "cn-a", SQLAddress: "sql-a", State: logpb.NormalState,
					WorkState: metadata.WorkState_Working, GlobalSysVarCommitTS: commitTS},
				{UUID: "cn-b", SQLAddress: "sql-b", State: logpb.NormalState,
					WorkState: metadata.WorkState_Working},
			},
		},
		{
			GlobalSysVarCommitTS: commitTS,
			CNStores: []logpb.CNStore{
				{UUID: "cn-a", SQLAddress: "sql-a", State: logpb.NormalState,
					WorkState: metadata.WorkState_Working, GlobalSysVarCommitTS: commitTS},
				{UUID: "cn-b", SQLAddress: "sql-b", State: logpb.NormalState,
					WorkState: metadata.WorkState_Working, GlobalSysVarCommitTS: commitTS},
			},
		},
	}}

	// The first snapshot alone is intentionally not used as a success oracle:
	// CN-B appears after it and must also acknowledge before the barrier opens.
	_, err := hakeeper.GetClusterDetails(context.Background())
	require.NoError(t, err)
	require.NoError(t, waitGlobalSysVarCommitFence(
		context.Background(), hakeeper.GetClusterDetails, commitTS))
	_, gets := hakeeper.snapshot()
	require.Equal(t, 3, gets)
}

func TestWaitGlobalSysVarCommitFenceErrorsAndCancellation(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100}
	t.Run("hakeeper error", func(t *testing.T) {
		want := errors.New("hakeeper unavailable")
		hakeeper := &globalSysVarFenceHAKeeper{detailErr: want}
		require.ErrorIs(t, waitGlobalSysVarCommitFence(
			context.Background(), hakeeper.GetClusterDetails, commitTS), want)
	})

	t.Run("caller cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		hakeeper := &globalSysVarFenceHAKeeper{details: []logpb.ClusterDetails{{
			GlobalSysVarCommitTS: commitTS,
			CNStores: []logpb.CNStore{{
				SQLAddress: "sql", State: logpb.NormalState,
				WorkState: metadata.WorkState_Working,
			}},
		}}}
		require.ErrorIs(t, waitGlobalSysVarCommitFence(
			ctx, hakeeper.GetClusterDetails, commitTS), context.Canceled)
	})
}

func TestSyncGlobalSysVarCommitRejectsInvalidState(t *testing.T) {
	t.Run("empty commit timestamp", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().GetLatestCommitTS().Return(timestamp.Timestamp{})
		hakeeper := &globalSysVarFenceHAKeeper{}
		ses := setupGlobalSysVarFenceSession(
			t, defines.MORPCVersion14, hakeeper, txnClient)
		require.ErrorContains(t,
			syncGlobalSysVarCommit(context.Background(), ses),
			"commit timestamp is empty")
		updates, gets := hakeeper.snapshot()
		require.Empty(t, updates)
		require.Zero(t, gets)
	})

	t.Run("watermark update error", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		commitTS := timestamp.Timestamp{PhysicalTime: 100}
		txnClient := mock_frontend.NewMockTxnClient(ctrl)
		txnClient.EXPECT().GetLatestCommitTS().Return(commitTS)
		want := errors.New("raft unavailable")
		hakeeper := &globalSysVarFenceHAKeeper{updateErr: want}
		ses := setupGlobalSysVarFenceSession(
			t, defines.MORPCVersion14, hakeeper, txnClient)
		require.ErrorIs(t, syncGlobalSysVarCommit(context.Background(), ses), want)
		_, gets := hakeeper.snapshot()
		require.Zero(t, gets)
	})
}
