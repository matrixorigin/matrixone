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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	executor "github.com/matrixorigin/matrixone/pkg/util/executor"
	mock_executor "github.com/matrixorigin/matrixone/pkg/util/executor/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// InternalSQLExecutor.ExecSQL tests
// ============================================================

func newTestInternalExecutor(t *testing.T, ctrl *gomock.Controller) (*InternalSQLExecutor, *mock_executor.MockSQLExecutor) {
	mockExec := mock_executor.NewMockSQLExecutor(ctrl)
	e := &InternalSQLExecutor{
		internalExec: mockExec,
		retryOpt: &SQLExecutorRetryOption{
			MaxRetries:    0,
			RetryInterval: time.Millisecond,
		},
	}
	return e, mockExec
}

func TestInternalExecSQL_UseTxnNoTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, _ := newTestInternalExecutor(t, ctrl)
	_, _, err := e.ExecSQL(context.Background(), nil, InvalidAccountID, "SELECT 1", true, false, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transaction required")
}

func TestInternalExecSQL_Paused(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, _ := newTestInternalExecutor(t, ctrl)
	ar := NewActiveRoutine()
	close(ar.Pause) // signal pause
	_, _, err := e.ExecSQL(context.Background(), ar, InvalidAccountID, "SELECT 1", false, false, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "paused")
}

func TestInternalExecSQL_Cancelled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, _ := newTestInternalExecutor(t, ctrl)
	ar := NewActiveRoutine()
	close(ar.Cancel) // signal cancel
	_, _, err := e.ExecSQL(context.Background(), ar, InvalidAccountID, "SELECT 1", false, false, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cancelled")
}

func TestInternalExecSQL_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, mockExec := newTestInternalExecutor(t, ctrl)
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	mockExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		executor.Result{Mp: mp}, nil,
	)
	result, cancel, err := e.ExecSQL(context.Background(), nil, InvalidAccountID, "SELECT 1", false, false, 0)
	require.NoError(t, err)
	assert.NotNil(t, result)
	cancel()
}

func TestInternalExecSQL_SuccessWithTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, mockExec := newTestInternalExecutor(t, ctrl)
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	mockExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		executor.Result{Mp: mp}, nil,
	)
	result, cancel, err := e.ExecSQL(context.Background(), nil, InvalidAccountID, "SELECT 1", false, false, 5*time.Second)
	require.NoError(t, err)
	assert.NotNil(t, result)
	cancel()
}

func TestInternalExecSQL_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, mockExec := newTestInternalExecutor(t, ctrl)
	mockExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		executor.Result{}, moerr.NewInternalErrorNoCtx("exec fail"),
	)
	_, _, err := e.ExecSQL(context.Background(), nil, InvalidAccountID, "SELECT 1", false, false, 0)
	assert.Error(t, err)
}

func TestInternalExecSQL_UTHelperInjectError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, _ := newTestInternalExecutor(t, ctrl)
	e.utHelper = &testUTHelper{
		onSQLExecFailed: func(ctx context.Context, query string, errorCount int) error {
			return moerr.NewInternalErrorNoCtx("injected")
		},
	}
	_, _, err := e.ExecSQL(context.Background(), nil, InvalidAccountID, "SELECT 1", false, false, 0)
	assert.Error(t, err)
}

func TestInternalExecSQL_WithAccountID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, mockExec := newTestInternalExecutor(t, ctrl)
	e.useAccountID = true
	e.accountID = 42
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	mockExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		executor.Result{Mp: mp}, nil,
	)
	result, cancel, err := e.ExecSQL(context.Background(), nil, InvalidAccountID, "SELECT 1", false, false, 0)
	require.NoError(t, err)
	assert.NotNil(t, result)
	cancel()
}

// ============================================================
// InternalSQLExecutor.ExecSQLInDatabase tests
// ============================================================

func TestInternalExecSQLInDB_UseTxnNoTxn(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, _ := newTestInternalExecutor(t, ctrl)
	_, _, err := e.ExecSQLInDatabase(context.Background(), nil, InvalidAccountID, "SELECT 1", "mydb", true, false, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "transaction required")
}

func TestInternalExecSQLInDB_Paused(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, _ := newTestInternalExecutor(t, ctrl)
	ar := NewActiveRoutine()
	close(ar.Pause)
	_, _, err := e.ExecSQLInDatabase(context.Background(), ar, InvalidAccountID, "SELECT 1", "mydb", false, false, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "paused")
}

func TestInternalExecSQLInDB_Cancelled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, _ := newTestInternalExecutor(t, ctrl)
	ar := NewActiveRoutine()
	close(ar.Cancel)
	_, _, err := e.ExecSQLInDatabase(context.Background(), ar, InvalidAccountID, "SELECT 1", "mydb", false, false, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cancelled")
}

func TestInternalExecSQLInDB_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, mockExec := newTestInternalExecutor(t, ctrl)
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	mockExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		executor.Result{Mp: mp}, nil,
	)
	result, cancel, err := e.ExecSQLInDatabase(context.Background(), nil, InvalidAccountID, "SELECT 1", "mydb", false, false, 0)
	require.NoError(t, err)
	assert.NotNil(t, result)
	cancel()
}

func TestInternalExecSQLInDB_SuccessWithTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, mockExec := newTestInternalExecutor(t, ctrl)
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	mockExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		executor.Result{Mp: mp}, nil,
	)
	result, cancel, err := e.ExecSQLInDatabase(context.Background(), nil, InvalidAccountID, "SELECT 1", "mydb", false, false, 5*time.Second)
	require.NoError(t, err)
	assert.NotNil(t, result)
	cancel()
}

func TestInternalExecSQLInDB_Error(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, mockExec := newTestInternalExecutor(t, ctrl)
	mockExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		executor.Result{}, moerr.NewInternalErrorNoCtx("fail"),
	)
	_, _, err := e.ExecSQLInDatabase(context.Background(), nil, InvalidAccountID, "SELECT 1", "mydb", false, false, 0)
	assert.Error(t, err)
}

func TestInternalExecSQLInDB_WithAccountID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, mockExec := newTestInternalExecutor(t, ctrl)
	e.useAccountID = true
	e.accountID = 10
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	mockExec.EXPECT().Exec(gomock.Any(), gomock.Any(), gomock.Any()).Return(
		executor.Result{Mp: mp}, nil,
	)
	result, cancel, err := e.ExecSQLInDatabase(context.Background(), nil, InvalidAccountID, "SELECT 1", "", false, false, 0)
	require.NoError(t, err)
	assert.NotNil(t, result)
	cancel()
}

func TestInternalExecSQLInDB_UTHelperInjectError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, _ := newTestInternalExecutor(t, ctrl)
	e.utHelper = &testUTHelper{
		onSQLExecFailed: func(ctx context.Context, query string, errorCount int) error {
			return moerr.NewInternalErrorNoCtx("injected")
		},
	}
	_, _, err := e.ExecSQLInDatabase(context.Background(), nil, InvalidAccountID, "SELECT 1", "mydb", false, false, 0)
	assert.Error(t, err)
}

func TestInternalExecSQLInDB_PausedInRetryLoop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	e, _ := newTestInternalExecutor(t, ctrl)
	e.retryOpt.MaxRetries = 2
	ar := NewActiveRoutine()
	// Don't close pause yet - let the first check pass, then close in retry
	go func() {
		time.Sleep(10 * time.Millisecond)
		close(ar.Pause)
	}()
	// First attempt will succeed the initial ar check, but fail in retry loop ar check
	e.utHelper = &testUTHelper{
		onSQLExecFailed: func(ctx context.Context, query string, errorCount int) error {
			if errorCount == 0 {
				return moerr.NewInternalErrorNoCtx("retry me")
			}
			return nil
		},
	}
	_, _, err := e.ExecSQLInDatabase(context.Background(), ar, InvalidAccountID, "SELECT 1", "mydb", false, false, 0)
	assert.Error(t, err)
}

// ============================================================
// InternalResult.Scan - nil vector branch
// ============================================================

func TestInternalResult_Scan_NilVector(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = nil
	bat.SetRowCount(1)
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	r.Next()
	var s string
	err := r.Scan(&s)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "vector 0 is nil")
}

func TestInternalResult_Scan_InvalidRowIndex(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := batch.NewWithSize(1)
	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec, []byte("hello"), false, mp))
	bat.Vecs[0] = vec
	bat.SetRowCount(1)
	r := &InternalResult{executorResult: buildResult(mp, bat)}
	// Don't call Next() - currentRow is 0
	var s string
	err := r.Scan(&s)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid row index")
}

// ============================================================
// ParseUpstreamConn tests
// ============================================================

func TestParseUpstreamConn_Empty(t *testing.T) {
	_, err := ParseUpstreamConn("")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "empty")
}

func TestParseUpstreamConn_NoMysqlPrefix(t *testing.T) {
	_, err := ParseUpstreamConn("postgres://user:pass@host:3306")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expected mysql://")
}

func TestParseUpstreamConn_NoAtSign(t *testing.T) {
	_, err := ParseUpstreamConn("mysql://userpass")
	assert.Error(t, err)
}

func TestParseUpstreamConn_NoColon(t *testing.T) {
	_, err := ParseUpstreamConn("mysql://user@host:3306")
	assert.Error(t, err)
}

func TestParseUpstreamConn_EmptyUser(t *testing.T) {
	_, err := ParseUpstreamConn("mysql://:pass@host:3306")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "user cannot be empty")
}

func TestParseUpstreamConn_EmptyPassword(t *testing.T) {
	_, err := ParseUpstreamConn("mysql://user:@host:3306")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "password cannot be empty")
}

func TestParseUpstreamConn_EmptyHost(t *testing.T) {
	_, err := ParseUpstreamConn("mysql://user:pass@:3306")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "host cannot be empty")
}

func TestParseUpstreamConn_InvalidPort(t *testing.T) {
	_, err := ParseUpstreamConn("mysql://user:pass@host:abc")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid port")
}

func TestParseUpstreamConn_Valid(t *testing.T) {
	cfg, err := ParseUpstreamConn("mysql://user:pass@127.0.0.1:6001")
	require.NoError(t, err)
	assert.Equal(t, "", cfg.Account)
	assert.Equal(t, "user", cfg.User)
	assert.Equal(t, "pass", cfg.Password)
	assert.Equal(t, "127.0.0.1", cfg.Host)
	assert.Equal(t, 6001, cfg.Port)
}

func TestParseUpstreamConn_WithAccount(t *testing.T) {
	cfg, err := ParseUpstreamConn("mysql://acc#user:pass@127.0.0.1:6001")
	require.NoError(t, err)
	assert.Equal(t, "acc", cfg.Account)
	assert.Equal(t, "user", cfg.User)
}

func TestParseUpstreamConn_AccountEmptyUser(t *testing.T) {
	_, err := ParseUpstreamConn("mysql://acc#:pass@127.0.0.1:6001")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "user cannot be empty")
}

func TestParseUpstreamConn_WithPath(t *testing.T) {
	cfg, err := ParseUpstreamConn("mysql://user:pass@127.0.0.1:6001/mydb")
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1", cfg.Host)
	assert.Equal(t, 6001, cfg.Port)
}

func TestParseUpstreamConn_PasswordWithColon(t *testing.T) {
	cfg, err := ParseUpstreamConn("mysql://user:pa:ss:word@127.0.0.1:6001")
	require.NoError(t, err)
	assert.Equal(t, "pa:ss:word", cfg.Password)
}

func TestParseUpstreamConn_BadHostPort(t *testing.T) {
	_, err := ParseUpstreamConn("mysql://user:pass@host")
	assert.Error(t, err)
}

// ============================================================
// testUTHelper for injecting errors
// ============================================================

type testUTHelper struct {
	onSnapshotCreated func(ctx context.Context, snapshotName string, snapshotTS types.TS) error
	onSQLExecFailed   func(ctx context.Context, query string, errorCount int) error
}

func (h *testUTHelper) OnSnapshotCreated(ctx context.Context, snapshotName string, snapshotTS types.TS) error {
	if h.onSnapshotCreated != nil {
		return h.onSnapshotCreated(ctx, snapshotName, snapshotTS)
	}
	return nil
}

func (h *testUTHelper) OnSQLExecFailed(ctx context.Context, query string, errorCount int) error {
	if h.onSQLExecFailed != nil {
		return h.onSQLExecFailed(ctx, query, errorCount)
	}
	return nil
}
