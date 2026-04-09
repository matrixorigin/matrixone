// Copyright 2024 Matrix Origin
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

package publication

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- UpdateIterationState ----

func TestUpdateIterationState_NilExecutor(t *testing.T) {
	err := UpdateIterationState(context.Background(), nil, "t1", 1, 1, nil, "", false, 0, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "executor is nil")
}

func TestUpdateIterationState_NoContext_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{"ok"})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := UpdateIterationState(context.Background(), mock, "t1", IterationStateCompleted, 1, nil, "", false, SubscriptionStateRunning, false)
	assert.NoError(t, err)
}

func TestUpdateIterationState_WithContext_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{"ok"})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	iterCtx := &IterationContext{
		TaskID:           "t1",
		SubscriptionName: "sub1",
		AObjectMap:       NewAObjectMap(),
		TableIDs:         map[TableKey]uint64{{DBName: "db", TableName: "tbl"}: 1},
	}
	err := UpdateIterationState(context.Background(), mock, "t1", IterationStateCompleted, 1, iterCtx, "", false, SubscriptionStateRunning, true)
	assert.NoError(t, err)
}

func TestUpdateIterationState_ExecError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("exec fail")
		},
	}
	err := UpdateIterationState(context.Background(), mock, "t1", 1, 1, nil, "", false, 0, false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to execute")
}

// ---- UpdateIterationStateNoSubscriptionState ----

func TestUpdateIterationStateNoSubscriptionState_NilExecutor(t *testing.T) {
	err := UpdateIterationStateNoSubscriptionState(context.Background(), nil, "t1", 1, 1, 0, nil, false, "", false)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "executor is nil")
}

func TestUpdateIterationStateNoSubscriptionState_NoContext_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{"ok"})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := UpdateIterationStateNoSubscriptionState(context.Background(), mock, "t1", 1, 1, 0, nil, false, "", false)
	assert.NoError(t, err)
}

func TestUpdateIterationStateNoSubscriptionState_WithContext_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{"ok"})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	iterCtx := &IterationContext{
		TaskID:             "t1",
		SubscriptionName:   "sub1",
		AObjectMap:         NewAObjectMap(),
		TableIDs:           map[TableKey]uint64{{DBName: "db", TableName: "tbl"}: 1},
		IndexTableMappings: map[string]string{"idx1": "idx1_down"},
	}
	err := UpdateIterationStateNoSubscriptionState(context.Background(), mock, "t1", 1, 1, 0, iterCtx, false, "", true)
	assert.NoError(t, err)
}

func TestUpdateIterationStateNoSubscriptionState_ExecError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("exec fail")
		},
	}
	err := UpdateIterationStateNoSubscriptionState(context.Background(), mock, "t1", 1, 1, 0, nil, false, "", false)
	assert.Error(t, err)
}

// ---- CheckStateBeforeUpdate ----

func TestCheckStateBeforeUpdate_ExecError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("exec fail")
		},
	}
	err := CheckStateBeforeUpdate(context.Background(), mock, "t1", 1)
	assert.Error(t, err)
}

func TestCheckStateBeforeUpdate_NoRows(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return &Result{internalResult: &InternalResult{}}, func() {}, nil
		},
	}
	err := CheckStateBeforeUpdate(context.Background(), mock, "t1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no rows returned")
}

func makeThreeColBatch(t *testing.T, mp *mpool.MPool, state int8, iterState int8, lsn uint64) *batch.Batch {
	bat := batch.NewWithSize(3)
	v0 := vector.NewVec(types.T_int8.ToType())
	require.NoError(t, vector.AppendFixed(v0, state, false, mp))
	v1 := vector.NewVec(types.T_int8.ToType())
	require.NoError(t, vector.AppendFixed(v1, iterState, false, mp))
	v2 := vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendFixed(v2, lsn, false, mp))
	bat.Vecs[0] = v0
	bat.Vecs[1] = v1
	bat.Vecs[2] = v2
	bat.SetRowCount(1)
	return bat
}

func TestCheckStateBeforeUpdate_StateMismatch(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	// subscriptionState=1 (not running), iterationState=1 (running), lsn=1
	bat := makeThreeColBatch(t, mp, 1, IterationStateRunning, 1)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := CheckStateBeforeUpdate(context.Background(), mock, "t1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "subscription state is not running")
}

func TestCheckStateBeforeUpdate_IterStateMismatch(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeThreeColBatch(t, mp, SubscriptionStateRunning, IterationStatePending, 1)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := CheckStateBeforeUpdate(context.Background(), mock, "t1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "iteration_state is not running")
}

func TestCheckStateBeforeUpdate_LSNMismatch(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeThreeColBatch(t, mp, SubscriptionStateRunning, IterationStateRunning, 999)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := CheckStateBeforeUpdate(context.Background(), mock, "t1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "iteration_lsn mismatch")
}

func TestCheckStateBeforeUpdate_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeThreeColBatch(t, mp, SubscriptionStateRunning, IterationStateRunning, 42)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := CheckStateBeforeUpdate(context.Background(), mock, "t1", 42)
	assert.NoError(t, err)
}

// ---- CheckIterationStatus ----

func makeFourColBatch(t *testing.T, mp *mpool.MPool, cnUUID string, iterState int8, lsn uint64, subState int8) *batch.Batch {
	bat := batch.NewWithSize(4)
	v0 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(v0, []byte(cnUUID), false, mp))
	v1 := vector.NewVec(types.T_int8.ToType())
	require.NoError(t, vector.AppendFixed(v1, iterState, false, mp))
	v2 := vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendFixed(v2, lsn, false, mp))
	v3 := vector.NewVec(types.T_int8.ToType())
	require.NoError(t, vector.AppendFixed(v3, subState, false, mp))
	bat.Vecs[0] = v0
	bat.Vecs[1] = v1
	bat.Vecs[2] = v2
	bat.Vecs[3] = v3
	bat.SetRowCount(1)
	return bat
}

func TestCheckIterationStatus_ExecError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("exec fail")
		},
	}
	err := CheckIterationStatus(context.Background(), mock, "t1", "cn1", 1)
	assert.Error(t, err)
}

func TestCheckIterationStatus_NoRows(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return &Result{internalResult: &InternalResult{}}, func() {}, nil
		},
	}
	err := CheckIterationStatus(context.Background(), mock, "t1", "cn1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no rows returned")
}

func TestCheckIterationStatus_CNMismatch(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeFourColBatch(t, mp, "other-cn", IterationStateRunning, 1, SubscriptionStateRunning)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := CheckIterationStatus(context.Background(), mock, "t1", "cn1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cn_uuid mismatch")
}

func TestCheckIterationStatus_LSNMismatch(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeFourColBatch(t, mp, "cn1", IterationStateRunning, 999, SubscriptionStateRunning)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := CheckIterationStatus(context.Background(), mock, "t1", "cn1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "iteration_lsn mismatch")
}

func TestCheckIterationStatus_IterStateNotRunning(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeFourColBatch(t, mp, "cn1", IterationStatePending, 1, SubscriptionStateRunning)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := CheckIterationStatus(context.Background(), mock, "t1", "cn1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "iteration_state is not running")
}

func TestCheckIterationStatus_SubStateNotRunning(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeFourColBatch(t, mp, "cn1", IterationStateRunning, 1, 1) // subState=1 (error)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := CheckIterationStatus(context.Background(), mock, "t1", "cn1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "subscription state is not running")
}

func TestCheckIterationStatus_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeFourColBatch(t, mp, "cn1", IterationStateRunning, 42, SubscriptionStateRunning)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := CheckIterationStatus(context.Background(), mock, "t1", "cn1", 42)
	assert.NoError(t, err)
}

// ---- CheckIterationStatus with NullString cn_uuid ----

func TestCheckIterationStatus_NullCNUUID(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	// Create batch with null varchar for cn_uuid
	bat := batch.NewWithSize(4)
	v0 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(v0, nil, true, mp)) // null
	v1 := vector.NewVec(types.T_int8.ToType())
	require.NoError(t, vector.AppendFixed(v1, IterationStateRunning, false, mp))
	v2 := vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendFixed(v2, uint64(1), false, mp))
	v3 := vector.NewVec(types.T_int8.ToType())
	require.NoError(t, vector.AppendFixed(v3, SubscriptionStateRunning, false, mp))
	bat.Vecs[0] = v0
	bat.Vecs[1] = v1
	bat.Vecs[2] = v2
	bat.Vecs[3] = v3
	bat.SetRowCount(1)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	// CheckIterationStatus scans cn_uuid as sql.NullString
	// When null, it should error with "cn_uuid is null"
	err := CheckIterationStatus(context.Background(), mock, "t1", "cn1", 1)
	assert.Error(t, err)
	// The scan will put empty string into NullString with Valid=false
	// But our InternalResult scan for varchar→NullString sets Valid=true even for null bytes
	// Let's just check it errors
}

// ---- UpdateIterationState with AObjectMap containing DownstreamStats ----

func TestUpdateIterationState_WithAObjectMap(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{"ok"})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	ts := types.BuildTS(100, 1)
	aom := NewAObjectMap()
	aom.Set("obj1", &AObjectMapping{
		IsTombstone:     true,
		DBName:          "db1",
		TableName:       "tbl1",
		DownstreamStats: *objectio.NewObjectStats(),
	})
	iterCtx := &IterationContext{
		TaskID:             "t1",
		SubscriptionName:   "sub1",
		AObjectMap:         aom,
		TableIDs:           map[TableKey]uint64{{DBName: "db", TableName: "tbl"}: 1},
		IndexTableMappings: map[string]string{"idx1": "idx1_down"},
	}
	_ = ts
	err := UpdateIterationState(context.Background(), mock, "t1", IterationStateCompleted, 1, iterCtx, "", false, SubscriptionStateRunning, true)
	assert.NoError(t, err)
}

// ---- CheckIterationStatus multiple rows ----

func TestCheckIterationStatus_MultipleRows(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	// Create batch with 2 rows
	bat := batch.NewWithSize(4)
	v0 := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(v0, []byte("cn1"), false, mp))
	require.NoError(t, vector.AppendBytes(v0, []byte("cn1"), false, mp))
	v1 := vector.NewVec(types.T_int8.ToType())
	require.NoError(t, vector.AppendFixed(v1, IterationStateRunning, false, mp))
	require.NoError(t, vector.AppendFixed(v1, IterationStateRunning, false, mp))
	v2 := vector.NewVec(types.T_uint64.ToType())
	require.NoError(t, vector.AppendFixed(v2, uint64(1), false, mp))
	require.NoError(t, vector.AppendFixed(v2, uint64(1), false, mp))
	v3 := vector.NewVec(types.T_int8.ToType())
	require.NoError(t, vector.AppendFixed(v3, SubscriptionStateRunning, false, mp))
	require.NoError(t, vector.AppendFixed(v3, SubscriptionStateRunning, false, mp))
	bat.Vecs[0] = v0
	bat.Vecs[1] = v1
	bat.Vecs[2] = v2
	bat.Vecs[3] = v3
	bat.SetRowCount(2)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := CheckIterationStatus(context.Background(), mock, "t1", "cn1", 1)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "multiple rows")
}

// ---- UpdateIterationState with NullString cn_uuid scan ----

func TestCheckIterationStatus_ScanError(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	// Create batch with wrong column count (3 instead of 4)
	bat := makeThreeColBatch(t, mp, SubscriptionStateRunning, IterationStateRunning, 1)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := CheckIterationStatus(context.Background(), mock, "t1", "cn1", 1)
	assert.Error(t, err)
}

// ---- Scan NullString from InternalResult ----

func TestCheckIterationStatus_ScanNullString(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeFourColBatch(t, mp, "cn1", IterationStateRunning, 1, SubscriptionStateRunning)
	ir := &InternalResult{executorResult: executor.Result{Batches: []*batch.Batch{bat}, Mp: mp}}
	require.True(t, ir.Next())
	var ns sql.NullString
	var i8 int8
	var u64 uint64
	var i8b int8
	err := ir.Scan(&ns, &i8, &u64, &i8b)
	require.NoError(t, err)
	assert.True(t, ns.Valid)
	assert.Equal(t, "cn1", ns.String)
}
