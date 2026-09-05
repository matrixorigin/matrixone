// Copyright 2021 Matrix Origin
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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	querypb "github.com/matrixorigin/matrixone/pkg/pb/query"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Mock QueryClient for testing
type mockQueryClient struct {
	cnResponses  map[string]*querypb.Response // CN address -> Response
	sendError    map[string]error             // CN address -> Error
	newReqCalls  int
	sendCalls    int
	releaseCalls int
}

func newMockQueryClient() *mockQueryClient {
	return &mockQueryClient{
		cnResponses: make(map[string]*querypb.Response),
		sendError:   make(map[string]error),
	}
}

func (m *mockQueryClient) ServiceID() string {
	return "test-service"
}

func (m *mockQueryClient) SendMessage(ctx context.Context, address string, req *querypb.Request) (*querypb.Response, error) {
	m.sendCalls++
	if err, ok := m.sendError[address]; ok && err != nil {
		return nil, err
	}
	if resp, ok := m.cnResponses[address]; ok {
		return resp, nil
	}
	return &querypb.Response{}, nil
}

func (m *mockQueryClient) NewRequest(method querypb.CmdMethod) *querypb.Request {
	m.newReqCalls++
	return &querypb.Request{
		CmdMethod: method,
	}
}

func (m *mockQueryClient) Release(resp *querypb.Response) {
	m.releaseCalls++
}

func (m *mockQueryClient) Close() error {
	return nil
}

// Mock MOCluster for testing
type mockMOCluster struct {
	cnServices []metadata.CNService
}

func (m *mockMOCluster) GetCNService(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
	for _, cn := range m.cnServices {
		if !apply(cn) {
			break
		}
	}
}

func (m *mockMOCluster) GetTNService(selector clusterservice.Selector, apply func(metadata.TNService) bool) {
	// Not used in syncCommitTimestamp
}

func (m *mockMOCluster) GetAllTNServices() []metadata.TNService {
	return nil
}

func (m *mockMOCluster) GetCNServiceWithoutWorkingState(selector clusterservice.Selector, apply func(metadata.CNService) bool) {
	// Not used in syncCommitTimestamp
}

func (m *mockMOCluster) ForceRefresh(sync bool) {
	// Not used in syncCommitTimestamp
}

func (m *mockMOCluster) Close() {
	// Not used in syncCommitTimestamp
}

func (m *mockMOCluster) DebugUpdateCNLabel(uuid string, kvs map[string][]string) error {
	return nil
}

func (m *mockMOCluster) DebugUpdateCNWorkState(uuid string, state int) error {
	return nil
}

func (m *mockMOCluster) RemoveCN(id string) {
	// Not used in syncCommitTimestamp
}

func (m *mockMOCluster) AddCN(metadata.CNService) {
	// Not used in syncCommitTimestamp
}

func (m *mockMOCluster) UpdateCN(metadata.CNService) {
	// Not used in syncCommitTimestamp
}

// Mock TxnClient for testing
type mockTxnClient struct {
	client.TxnClient
	syncCalled     bool
	syncCalledWith timestamp.Timestamp
}

func (m *mockTxnClient) SyncLatestCommitTS(ts timestamp.Timestamp) {
	m.syncCalled = true
	m.syncCalledWith = ts
}

// Helper function to create a test timestamp
func newTestTimestamp(physical int64, logical uint32) timestamp.Timestamp {
	return timestamp.Timestamp{
		PhysicalTime: physical,
		LogicalTime:  logical,
	}
}

// Test syncCommitTimestamp with normal flow
func TestCDCDao_syncCommitTimestamp_Success(t *testing.T) {
	ctx := context.Background()

	// Create mock objects
	mockQC := newMockQueryClient()
	mockMC := &mockMOCluster{
		cnServices: []metadata.CNService{
			{QueryAddress: "cn1:6001"},
			{QueryAddress: "cn2:6001"},
			{QueryAddress: "cn3:6001"},
		},
	}
	mockTC := &mockTxnClient{}

	// Set up responses with different timestamps
	ts1 := newTestTimestamp(100, 1)
	ts2 := newTestTimestamp(100, 2) // This should be the max
	ts3 := newTestTimestamp(99, 100)

	mockQC.cnResponses["cn1:6001"] = &querypb.Response{
		GetCommit: &querypb.GetCommitResponse{
			CurrentCommitTS: ts1,
		},
	}
	mockQC.cnResponses["cn2:6001"] = &querypb.Response{
		GetCommit: &querypb.GetCommitResponse{
			CurrentCommitTS: ts2,
		},
	}
	mockQC.cnResponses["cn3:6001"] = &querypb.Response{
		GetCommit: &querypb.GetCommitResponse{
			CurrentCommitTS: ts3,
		},
	}

	// Create test session and process
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	proc := &process.Process{
		Base: &process.BaseProcess{
			QueryClient: mockQC,
			TxnClient:   mockTC,
		},
	}

	ses := &Session{
		proc: proc,
		feSessionImpl: feSessionImpl{
			service: "test-service",
		},
	}

	// Setup runtime with mock cluster
	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("test-service", rt)
	rt.SetGlobalVariables(runtime.ClusterService, mockMC)

	// Create CDCDao and call syncCommitTimestamp
	dao := NewCDCDao(ses)
	err = dao.syncCommitTimestamp(ctx)

	// Assertions
	assert.NoError(t, err)
	assert.True(t, mockTC.syncCalled, "SyncLatestCommitTS should be called")
	assert.Equal(t, ts2, mockTC.syncCalledWith, "Should sync with max timestamp")
	assert.Equal(t, 3, mockQC.sendCalls, "Should query all 3 CNs")
	assert.Equal(t, 3, mockQC.releaseCalls, "Should release all responses")
}

// Test syncCommitTimestamp with partial CN failures
func TestCDCDao_syncCommitTimestamp_PartialFailure(t *testing.T) {
	ctx := context.Background()

	mockQC := newMockQueryClient()
	mockMC := &mockMOCluster{
		cnServices: []metadata.CNService{
			{QueryAddress: "cn1:6001"},
			{QueryAddress: "cn2:6001"},
			{QueryAddress: "cn3:6001"},
		},
	}
	mockTC := &mockTxnClient{}

	// Set up responses: cn1 fails, cn2 and cn3 succeed
	ts2 := newTestTimestamp(100, 2) // This should be the max
	ts3 := newTestTimestamp(100, 1)

	mockQC.sendError["cn1:6001"] = moerr.NewInternalError(ctx, "connection failed")
	mockQC.cnResponses["cn2:6001"] = &querypb.Response{
		GetCommit: &querypb.GetCommitResponse{
			CurrentCommitTS: ts2,
		},
	}
	mockQC.cnResponses["cn3:6001"] = &querypb.Response{
		GetCommit: &querypb.GetCommitResponse{
			CurrentCommitTS: ts3,
		},
	}

	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	proc := &process.Process{
		Base: &process.BaseProcess{
			QueryClient: mockQC,
			TxnClient:   mockTC,
		},
	}

	ses := &Session{
		proc: proc,
		feSessionImpl: feSessionImpl{
			service: "test-service",
		},
	}

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("test-service", rt)
	rt.SetGlobalVariables(runtime.ClusterService, mockMC)

	dao := NewCDCDao(ses)
	err = dao.syncCommitTimestamp(ctx)

	// Should succeed even with one CN failing
	assert.NoError(t, err)
	assert.True(t, mockTC.syncCalled)
	assert.Equal(t, ts2, mockTC.syncCalledWith, "Should use max from successful CNs")
}

// Test syncCommitTimestamp with all CN failures
func TestCDCDao_syncCommitTimestamp_AllCNsFailed(t *testing.T) {
	ctx := context.Background()

	mockQC := newMockQueryClient()
	mockMC := &mockMOCluster{
		cnServices: []metadata.CNService{
			{QueryAddress: "cn1:6001"},
			{QueryAddress: "cn2:6001"},
		},
	}
	mockTC := &mockTxnClient{}

	// All CNs fail
	mockQC.sendError["cn1:6001"] = moerr.NewInternalError(ctx, "connection failed")
	mockQC.sendError["cn2:6001"] = moerr.NewInternalError(ctx, "connection failed")

	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	proc := &process.Process{
		Base: &process.BaseProcess{
			QueryClient: mockQC,
			TxnClient:   mockTC,
		},
	}

	ses := &Session{
		proc: proc,
		feSessionImpl: feSessionImpl{
			service: "test-service",
		},
	}

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("test-service", rt)
	rt.SetGlobalVariables(runtime.ClusterService, mockMC)

	dao := NewCDCDao(ses)
	err = dao.syncCommitTimestamp(ctx)

	// Should return nil (not fail the operation)
	assert.NoError(t, err)
	assert.False(t, mockTC.syncCalled, "Should not call SyncLatestCommitTS when all CNs fail")
}

// Test syncCommitTimestamp with no CNs found
func TestCDCDao_syncCommitTimestamp_NoCNs(t *testing.T) {
	ctx := context.Background()

	mockQC := newMockQueryClient()
	mockMC := &mockMOCluster{
		cnServices: []metadata.CNService{}, // Empty
	}
	mockTC := &mockTxnClient{}

	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	proc := &process.Process{
		Base: &process.BaseProcess{
			QueryClient: mockQC,
			TxnClient:   mockTC,
		},
	}

	ses := &Session{
		proc: proc,
		feSessionImpl: feSessionImpl{
			service: "test-service",
		},
	}

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("test-service", rt)
	rt.SetGlobalVariables(runtime.ClusterService, mockMC)

	dao := NewCDCDao(ses)
	err = dao.syncCommitTimestamp(ctx)

	// Should return nil (not fail the operation)
	assert.NoError(t, err)
	assert.False(t, mockTC.syncCalled)
	assert.Equal(t, 0, mockQC.sendCalls, "Should not send any requests")
}

// Test syncCommitTimestamp with nil QueryClient
func TestCDCDao_syncCommitTimestamp_NilQueryClient(t *testing.T) {
	ctx := context.Background()

	proc := &process.Process{
		Base: &process.BaseProcess{
			QueryClient: nil, // Nil QueryClient
		},
	}

	ses := &Session{
		proc: proc,
		feSessionImpl: feSessionImpl{
			service: "test-service",
		},
	}

	dao := NewCDCDao(ses)
	err := dao.syncCommitTimestamp(ctx)

	// Should return error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query client is nil")
}

// Test syncCommitTimestamp with nil TxnClient
func TestCDCDao_syncCommitTimestamp_NilTxnClient(t *testing.T) {
	ctx := context.Background()

	mockQC := newMockQueryClient()
	mockMC := &mockMOCluster{
		cnServices: []metadata.CNService{
			{QueryAddress: "cn1:6001"},
		},
	}

	ts1 := newTestTimestamp(100, 1)
	mockQC.cnResponses["cn1:6001"] = &querypb.Response{
		GetCommit: &querypb.GetCommitResponse{
			CurrentCommitTS: ts1,
		},
	}

	proc := &process.Process{
		Base: &process.BaseProcess{
			QueryClient: mockQC,
			TxnClient:   nil, // Nil TxnClient
		},
	}

	ses := &Session{
		proc: proc,
		feSessionImpl: feSessionImpl{
			service: "test-service",
		},
	}

	rt := runtime.DefaultRuntime()
	runtime.SetupServiceBasedRuntime("test-service", rt)
	rt.SetGlobalVariables(runtime.ClusterService, mockMC)

	dao := NewCDCDao(ses)
	err := dao.syncCommitTimestamp(ctx)

	// Should return error
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "txn client is nil")
}

// Test syncCommitTimestamp with nil session/process
func TestCDCDao_syncCommitTimestamp_NilSession(t *testing.T) {
	ctx := context.Background()

	dao := &CDCDao{
		ses: nil, // Nil session
	}
	err := dao.syncCommitTimestamp(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session or process is nil")
}

func TestCDCDao_syncCommitTimestamp_NilProcess(t *testing.T) {
	ctx := context.Background()

	ses := &Session{
		proc: nil, // Nil process
	}

	dao := &CDCDao{
		ses: ses,
	}
	err := dao.syncCommitTimestamp(ctx)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session or process is nil")
}
