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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
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
	mu sync.Mutex

	serviceID    string
	requests     []globalSysVarSyncRequest
	errors       map[string]error
	applied      map[string]timestamp.Timestamp
	empty        map[string]bool
	entered      map[string]chan struct{}
	blocks       map[string]chan struct{}
	releaseCount int
}

func newGlobalSysVarSyncQueryClient() *globalSysVarSyncQueryClient {
	return &globalSysVarSyncQueryClient{
		serviceID: "global-sysvar-sync-test",
		errors:    make(map[string]error),
		applied:   make(map[string]timestamp.Timestamp),
		empty:     make(map[string]bool),
		entered:   make(map[string]chan struct{}),
		blocks:    make(map[string]chan struct{}),
	}
}

func (m *globalSysVarSyncQueryClient) ServiceID() string { return m.serviceID }

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
	applied, hasApplied := m.applied[address]
	empty := m.empty[address]
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
	if empty {
		return &querypb.Response{}, nil
	}
	if !hasApplied {
		applied = record.commitTS
	}
	return &querypb.Response{SyncCommit: &querypb.SyncCommitResponse{
		CurrentCommitTS: applied,
	}}, nil
}

func (m *globalSysVarSyncQueryClient) NewRequest(method querypb.CmdMethod) *querypb.Request {
	return &querypb.Request{CmdMethod: method}
}

func (m *globalSysVarSyncQueryClient) Release(*querypb.Response) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.releaseCount++
}

func (*globalSysVarSyncQueryClient) Close() error { return nil }

func (m *globalSysVarSyncQueryClient) snapshot() ([]globalSysVarSyncRequest, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]globalSysVarSyncRequest(nil), m.requests...), m.releaseCount
}

func normalCN(address string, workState metadata.WorkState) logpb.CNStore {
	return logpb.CNStore{
		QueryAddress: address,
		State:        logpb.NormalState,
		WorkState:    workState,
	}
}

func TestSyncCommitTimestampToCNs(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	qc := newGlobalSysVarSyncQueryClient()
	cnStores := []logpb.CNStore{
		normalCN("cn-1", metadata.WorkState_Working),
		normalCN("cn-1", metadata.WorkState_Working),
		normalCN("cn-draining", metadata.WorkState_Draining),
		normalCN("", metadata.WorkState_Working),
		normalCN("cn-2", metadata.WorkState_Unknown),
		{
			QueryAddress: "cn-timeout",
			State:        logpb.TimeoutState,
			WorkState:    metadata.WorkState_Working,
		},
	}

	require.NoError(t, syncCommitTimestampToCNs(
		context.Background(), qc, cnStores, commitTS))

	requests, releaseCount := qc.snapshot()
	require.ElementsMatch(t, []globalSysVarSyncRequest{
		{address: "cn-1", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS},
		{address: "cn-2", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS},
	}, requests)
	require.Equal(t, 2, releaseCount)
}

func TestSyncCommitTimestampToCNsWaitsAndReturnsPartialFailure(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100}
	qc := newGlobalSysVarSyncQueryClient()
	qc.errors["cn-2"] = errors.New("send failed")
	entered := make(chan struct{})
	unblock := make(chan struct{})
	qc.entered["cn-1"] = entered
	qc.blocks["cn-1"] = unblock
	cnStores := []logpb.CNStore{
		normalCN("cn-1", metadata.WorkState_Working),
		normalCN("cn-2", metadata.WorkState_Working),
	}
	done := make(chan error, 1)
	go func() {
		done <- syncCommitTimestampToCNs(context.Background(), qc, cnStores, commitTS)
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("sync did not send the blocking CN request")
	}
	select {
	case err := <-done:
		t.Fatalf("sync returned before every started request completed: %v", err)
	default:
	}
	close(unblock)
	err := <-done
	require.Error(t, err)
	require.Contains(t, err.Error(), "cn-2")
	requests, releaseCount := qc.snapshot()
	require.Len(t, requests, 2)
	require.Equal(t, 1, releaseCount)
}

func TestSyncCommitTimestampToCNsValidatesAndReleasesResponse(t *testing.T) {
	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	qc := newGlobalSysVarSyncQueryClient()
	qc.applied["cn-old"] = timestamp.Timestamp{PhysicalTime: 99}
	qc.empty["cn-empty"] = true
	cnStores := []logpb.CNStore{
		normalCN("cn-old", metadata.WorkState_Working),
		normalCN("cn-empty", metadata.WorkState_Working),
	}

	err := syncCommitTimestampToCNs(context.Background(), qc, cnStores, commitTS)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cn-old")
	require.NotContains(t, err.Error(), "cn-empty",
		"an empty legacy response is still a successful SyncCommit acknowledgement")
	_, releaseCount := qc.snapshot()
	require.Equal(t, 2, releaseCount)
}

func TestSyncCommitTimestampToCNsHonorsCancellation(t *testing.T) {
	qc := newGlobalSysVarSyncQueryClient()
	entered := make(chan struct{})
	qc.entered["cn-1"] = entered
	qc.blocks["cn-1"] = make(chan struct{})
	cnStores := []logpb.CNStore{normalCN("cn-1", metadata.WorkState_Working)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- syncCommitTimestampToCNs(
			ctx, qc, cnStores, timestamp.Timestamp{PhysicalTime: 100})
	}()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("sync did not enter the query client")
	}
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(time.Second):
		t.Fatal("fan-out did not stop after cancellation")
	}
}

func TestSyncCommitTimestampToCNsRejectsUnusableInventory(t *testing.T) {
	qc := newGlobalSysVarSyncQueryClient()
	err := syncCommitTimestampToCNs(
		context.Background(),
		qc,
		[]logpb.CNStore{normalCN("", metadata.WorkState_Working)},
		timestamp.Timestamp{PhysicalTime: 100},
	)
	require.ErrorContains(t, err, "no CN query service")
}

func TestSetGlobalSysVarPublishesCacheOnlyAfterCommitSync(t *testing.T) {
	serviceID := t.Name()
	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	qc := newGlobalSysVarSyncQueryClient()
	qc.serviceID = serviceID
	qc.errors["cn-1"] = errors.New("sync failed")
	hakeeper := newMockHAKeeperClient()
	hakeeper.clusterDetails.CNStores = []logpb.CNStore{
		normalCN("cn-1", metadata.WorkState_Working),
	}
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.QueryClient = qc
	pu.HAKeeperClient = hakeeper
	InitServerLevelVars(serviceID)
	setPu(serviceID, pu)

	bh := &backgroundExecTest{}
	bh.init()
	bh.sql2result[getSqlForGetSysVarWithAccount(42, PasswordHistory)] =
		newMrsForSystemVariableNameOfAccount(nil)
	stub := gostub.StubFunc(&NewBackgroundExec, bh)
	t.Cleanup(stub.Reset)

	globalVars := &SystemVariables{mp: map[string]interface{}{PasswordHistory: int64(0)}}
	ses := &Session{feSessionImpl: feSessionImpl{
		tenant: &TenantInfo{
			Tenant:   "account-a",
			TenantID: 42,
		},
		service:      serviceID,
		gSysVars:     globalVars,
		sesSysVars:   globalVars.Clone(),
		lastCommitTS: commitTS,
	}}

	err := ses.SetGlobalSysVar(context.Background(), PasswordHistory, int64(5))
	require.ErrorContains(t, err, "sync failed")
	require.Equal(t, int64(0), globalVars.Get(PasswordHistory),
		"a failed cross-CN fence must not publish an unfenced local cache value")
	require.NotContains(t, strings.Join(bh.executedSQLs, "\n"), "mo_catalog.mo_account")

	delete(qc.errors, "cn-1")
	require.NoError(t, ses.SetGlobalSysVar(
		context.Background(), PasswordHistory, int64(5)))
	require.Equal(t, int64(5), globalVars.Get(PasswordHistory))
}

func TestSyncGlobalSysVarCommitUsesSessionCommitTimestamp(t *testing.T) {
	serviceID := t.Name()
	commitTS := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 7}
	qc := newGlobalSysVarSyncQueryClient()
	qc.serviceID = serviceID
	hakeeper := newMockHAKeeperClient()
	hakeeper.clusterDetails.CNStores = []logpb.CNStore{
		normalCN("cn-1", metadata.WorkState_Working),
	}
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.QueryClient = qc
	pu.HAKeeperClient = hakeeper
	InitServerLevelVars(serviceID)
	setPu(serviceID, pu)
	ses := &Session{feSessionImpl: feSessionImpl{
		service:      serviceID,
		lastCommitTS: commitTS,
	}}

	require.NoError(t, syncGlobalSysVarCommit(context.Background(), ses))
	requests, releaseCount := qc.snapshot()
	require.Equal(t, []globalSysVarSyncRequest{{
		address: "cn-1", method: querypb.CmdMethod_SyncCommit, commitTS: commitTS,
	}}, requests)
	require.Equal(t, 1, releaseCount)
}

func TestSyncGlobalSysVarCommitPropagatesInventoryFailure(t *testing.T) {
	serviceID := t.Name()
	qc := newGlobalSysVarSyncQueryClient()
	qc.serviceID = serviceID
	hakeeper := newMockHAKeeperClient()
	hakeeper.clusterDetailsErr = errors.New("inventory failed")
	pu := config.NewParameterUnit(&config.FrontendParameters{}, nil, nil, nil)
	pu.QueryClient = qc
	pu.HAKeeperClient = hakeeper
	InitServerLevelVars(serviceID)
	setPu(serviceID, pu)
	ses := &Session{feSessionImpl: feSessionImpl{
		service:      serviceID,
		lastCommitTS: timestamp.Timestamp{PhysicalTime: 100},
	}}

	err := syncGlobalSysVarCommit(context.Background(), ses)
	require.ErrorContains(t, err, "inventory failed")
	requests, _ := qc.snapshot()
	require.Empty(t, requests)
}
