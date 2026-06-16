// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package publication

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- QueryGCStatus with mock ----

func TestQueryGCStatus_ExecError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("exec fail")
		},
	}
	_, err := QueryGCStatus(context.Background(), mock)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to query GC status")
}

func TestQueryGCStatus_NoRows(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return &Result{internalResult: &InternalResult{}}, func() {}, nil
		},
	}
	_, err := QueryGCStatus(context.Background(), mock)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no rows returned")
}

func TestQueryGCStatus_EmptyResponse(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{""})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	_, err := QueryGCStatus(context.Background(), mock)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestQueryGCStatus_InvalidJSON(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{"not json"})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	_, err := QueryGCStatus(context.Background(), mock)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse")
}

func TestQueryGCStatus_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	gcJSON, _ := json.Marshal(GCStatus{Running: true, Protections: 3, TS: 100})
	bat := makeStringBatch(t, mp, []string{string(gcJSON)})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	status, err := QueryGCStatus(context.Background(), mock)
	require.NoError(t, err)
	assert.True(t, status.Running)
	assert.Equal(t, 3, status.Protections)
}

// ---- RegisterSyncProtection ----

func TestRegisterSyncProtection_ExecError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("exec fail")
		},
	}
	err := RegisterSyncProtection(context.Background(), mock, "job1", "bf", 0, 100, "task1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to register")
}

func TestRegisterSyncProtection_NoRows(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return &Result{internalResult: &InternalResult{}}, func() {}, nil
		},
	}
	err := RegisterSyncProtection(context.Background(), mock, "job1", "bf", 0, 100, "task1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no rows returned")
}

func TestRegisterSyncProtection_EmptyResponse(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{""})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := RegisterSyncProtection(context.Background(), mock, "job1", "bf", 0, 100, "task1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestRegisterSyncProtection_InvalidMoCtlJSON(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{"not json"})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := RegisterSyncProtection(context.Background(), mock, "job1", "bf", 0, 100, "task1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse mo_ctl")
}

func TestRegisterSyncProtection_InvalidInnerJSON(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	outer := MoCtlResponse{Method: "test", Result: []MoCtlResultEntry{{ReturnStr: "not json"}}}
	b, _ := json.Marshal(outer)
	bat := makeStringBatch(t, mp, []string{string(b)})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := RegisterSyncProtection(context.Background(), mock, "job1", "bf", 0, 100, "task1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse register")
}

func TestRegisterSyncProtection_GCRunning(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	inner, _ := json.Marshal(SyncProtectionResponse{Status: "error", Code: "ErrGCRunning", Message: "GC is running"})
	outer := MoCtlResponse{Method: "test", Result: []MoCtlResultEntry{{ReturnStr: string(inner)}}}
	b, _ := json.Marshal(outer)
	bat := makeStringBatch(t, mp, []string{string(b)})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := RegisterSyncProtection(context.Background(), mock, "job1", "bf", 0, 100, "task1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ErrGCRunning")
}

func TestRegisterSyncProtection_OtherError(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	inner, _ := json.Marshal(SyncProtectionResponse{Status: "error", Code: "SomeCode", Message: "msg"})
	outer := MoCtlResponse{Method: "test", Result: []MoCtlResultEntry{{ReturnStr: string(inner)}}}
	b, _ := json.Marshal(outer)
	bat := makeStringBatch(t, mp, []string{string(b)})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := RegisterSyncProtection(context.Background(), mock, "job1", "bf", 0, 100, "task1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "register sync protection failed")
}

func TestRegisterSyncProtection_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	inner, _ := json.Marshal(SyncProtectionResponse{Status: "ok"})
	outer := MoCtlResponse{Method: "test", Result: []MoCtlResultEntry{{ReturnStr: string(inner)}}}
	b, _ := json.Marshal(outer)
	bat := makeStringBatch(t, mp, []string{string(b)})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := RegisterSyncProtection(context.Background(), mock, "job1", "bf", 0, 100, "task1")
	assert.NoError(t, err)
}

// ---- RenewSyncProtection ----

func TestRenewSyncProtection_ExecError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("exec fail")
		},
	}
	err := RenewSyncProtection(context.Background(), mock, "job1", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to renew")
}

func TestRenewSyncProtection_NoRows(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return &Result{internalResult: &InternalResult{}}, func() {}, nil
		},
	}
	err := RenewSyncProtection(context.Background(), mock, "job1", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no rows returned")
}

func TestRenewSyncProtection_EmptyResponse(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{""})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := RenewSyncProtection(context.Background(), mock, "job1", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestRenewSyncProtection_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	inner, _ := json.Marshal(SyncProtectionResponse{Status: "ok"})
	outer := MoCtlResponse{Method: "test", Result: []MoCtlResultEntry{{ReturnStr: string(inner)}}}
	b, _ := json.Marshal(outer)
	bat := makeStringBatch(t, mp, []string{string(b)})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := RenewSyncProtection(context.Background(), mock, "job1", 100)
	assert.NoError(t, err)
}

func TestRenewSyncProtection_StatusNotOk(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	inner, _ := json.Marshal(SyncProtectionResponse{Status: "error", Code: "X", Message: "fail"})
	outer := MoCtlResponse{Method: "test", Result: []MoCtlResultEntry{{ReturnStr: string(inner)}}}
	b, _ := json.Marshal(outer)
	bat := makeStringBatch(t, mp, []string{string(b)})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := RenewSyncProtection(context.Background(), mock, "job1", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "renew sync protection failed")
}

func TestRenewSyncProtection_InvalidMoCtlJSON(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{"not json"})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := RenewSyncProtection(context.Background(), mock, "job1", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse mo_ctl")
}

func TestRenewSyncProtection_InvalidInnerJSON(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	outer := MoCtlResponse{Method: "test", Result: []MoCtlResultEntry{{ReturnStr: "not json"}}}
	b, _ := json.Marshal(outer)
	bat := makeStringBatch(t, mp, []string{string(b)})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := RenewSyncProtection(context.Background(), mock, "job1", 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse renew")
}

// ---- UnregisterSyncProtection ----

func TestUnregisterSyncProtection_ExecError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("exec fail")
		},
	}
	err := UnregisterSyncProtection(context.Background(), mock, "job1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to unregister")
}

func TestUnregisterSyncProtection_NoRows(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return &Result{internalResult: &InternalResult{}}, func() {}, nil
		},
	}
	err := UnregisterSyncProtection(context.Background(), mock, "job1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no rows returned")
}

func TestUnregisterSyncProtection_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{"anything"})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := UnregisterSyncProtection(context.Background(), mock, "job1")
	assert.NoError(t, err)
}

func TestUnregisterSyncProtection_ScanError(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeInt64Batch(t, mp, []int64{42}) // wrong type for string scan
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := UnregisterSyncProtection(context.Background(), mock, "job1")
	assert.Error(t, err)
}

// ---- BuildBloomFilterFromObjectMap non-empty ----

func TestBuildBloomFilterFromObjectMap_NonEmpty(t *testing.T) {
	mp, _ := mpool.NewMPool("test_bf", 0, mpool.NoFixed)
	objMap := make(map[objectio.ObjectId]*ObjectWithTableInfo)
	var id objectio.ObjectId
	copy(id[:], []byte("test-object-id-1234"))
	objMap[id] = &ObjectWithTableInfo{}
	s, err := BuildBloomFilterFromObjectMap(objMap, mp)
	require.NoError(t, err)
	assert.NotEmpty(t, s)
}

// ---- RegisterSyncProtectionWithRetry ----

func TestRegisterSyncProtectionWithRetry_NoRetryOnZeroMaxTime(t *testing.T) {
	orig := RegisterSyncProtectionOnDownstreamFn
	defer func() { RegisterSyncProtectionOnDownstreamFn = orig }()
	RegisterSyncProtectionOnDownstreamFn = func(ctx context.Context, exec SQLExecutor, objMap map[objectio.ObjectId]*ObjectWithTableInfo, mp *mpool.MPool, taskID string) (string, int64, bool, error) {
		return "job1", 100, false, nil
	}
	jobID, ttl, err := RegisterSyncProtectionWithRetry(context.Background(), nil, nil, nil, &SyncProtectionRetryOption{MaxTotalTime: 0}, "task1")
	require.NoError(t, err)
	assert.Equal(t, "job1", jobID)
	assert.Equal(t, int64(100), ttl)
}

func TestRegisterSyncProtectionWithRetry_NonRetryableError(t *testing.T) {
	orig := RegisterSyncProtectionOnDownstreamFn
	defer func() { RegisterSyncProtectionOnDownstreamFn = orig }()
	RegisterSyncProtectionOnDownstreamFn = func(ctx context.Context, exec SQLExecutor, objMap map[objectio.ObjectId]*ObjectWithTableInfo, mp *mpool.MPool, taskID string) (string, int64, bool, error) {
		return "", 0, false, errors.New("permanent fail")
	}
	_, _, err := RegisterSyncProtectionWithRetry(context.Background(), nil, nil, nil, &SyncProtectionRetryOption{MaxTotalTime: time.Second, InitialInterval: time.Millisecond}, "task1")
	assert.Error(t, err)
}

func TestRegisterSyncProtectionWithRetry_ContextCancelled(t *testing.T) {
	orig := RegisterSyncProtectionOnDownstreamFn
	defer func() { RegisterSyncProtectionOnDownstreamFn = orig }()
	RegisterSyncProtectionOnDownstreamFn = func(ctx context.Context, exec SQLExecutor, objMap map[objectio.ObjectId]*ObjectWithTableInfo, mp *mpool.MPool, taskID string) (string, int64, bool, error) {
		return "", 0, true, errors.New("retryable")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := RegisterSyncProtectionWithRetry(ctx, nil, nil, nil, &SyncProtectionRetryOption{MaxTotalTime: time.Minute, InitialInterval: time.Millisecond}, "task1")
	assert.Error(t, err)
}

func TestRegisterSyncProtectionWithRetry_NilOption(t *testing.T) {
	orig := RegisterSyncProtectionOnDownstreamFn
	defer func() { RegisterSyncProtectionOnDownstreamFn = orig }()
	RegisterSyncProtectionOnDownstreamFn = func(ctx context.Context, exec SQLExecutor, objMap map[objectio.ObjectId]*ObjectWithTableInfo, mp *mpool.MPool, taskID string) (string, int64, bool, error) {
		return "j", 1, false, nil
	}
	jobID, _, err := RegisterSyncProtectionWithRetry(context.Background(), nil, nil, nil, nil, "task1")
	require.NoError(t, err)
	assert.Equal(t, "j", jobID)
}

func TestRegisterSyncProtectionWithRetry_RetryThenSuccess(t *testing.T) {
	orig := RegisterSyncProtectionOnDownstreamFn
	defer func() { RegisterSyncProtectionOnDownstreamFn = orig }()
	attempt := 0
	RegisterSyncProtectionOnDownstreamFn = func(ctx context.Context, exec SQLExecutor, objMap map[objectio.ObjectId]*ObjectWithTableInfo, mp *mpool.MPool, taskID string) (string, int64, bool, error) {
		attempt++
		if attempt < 3 {
			return "", 0, true, errors.New("retryable")
		}
		return "j", 1, false, nil
	}
	jobID, _, err := RegisterSyncProtectionWithRetry(context.Background(), nil, nil, nil, &SyncProtectionRetryOption{MaxTotalTime: 5 * time.Second, InitialInterval: time.Millisecond}, "task1")
	require.NoError(t, err)
	assert.Equal(t, "j", jobID)
	assert.Equal(t, 3, attempt)
}

func TestRegisterSyncProtectionWithRetry_Timeout(t *testing.T) {
	orig := RegisterSyncProtectionOnDownstreamFn
	defer func() { RegisterSyncProtectionOnDownstreamFn = orig }()
	RegisterSyncProtectionOnDownstreamFn = func(ctx context.Context, exec SQLExecutor, objMap map[objectio.ObjectId]*ObjectWithTableInfo, mp *mpool.MPool, taskID string) (string, int64, bool, error) {
		return "", 0, true, errors.New("retryable")
	}
	_, _, err := RegisterSyncProtectionWithRetry(context.Background(), nil, nil, nil, &SyncProtectionRetryOption{MaxTotalTime: 10 * time.Millisecond, InitialInterval: 5 * time.Millisecond}, "task1")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}
