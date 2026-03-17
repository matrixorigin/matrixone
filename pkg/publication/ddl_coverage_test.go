package publication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- GetUpstreamDDLUsingGetDdl nil checks ----

func TestGetUpstreamDDLUsingGetDdl_NilCtx(t *testing.T) {
	_, err := GetUpstreamDDLUsingGetDdl(context.Background(), nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "iteration context is nil")
}

func TestGetUpstreamDDLUsingGetDdl_NilExecutor(t *testing.T) {
	_, err := GetUpstreamDDLUsingGetDdl(context.Background(), &IterationContext{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "upstream executor is nil")
}

func TestGetUpstreamDDLUsingGetDdl_EmptySnapshot(t *testing.T) {
	mock := &mockSQLExecutor{}
	_, err := GetUpstreamDDLUsingGetDdl(context.Background(), &IterationContext{
		UpstreamExecutor: mock,
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "current snapshot name is required")
}

func TestGetUpstreamDDLUsingGetDdl_ExecError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("exec fail")
		},
	}
	_, err := GetUpstreamDDLUsingGetDdl(context.Background(), &IterationContext{
		UpstreamExecutor:    mock,
		CurrentSnapshotName: "snap1",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to execute GETDDL")
}

func TestGetUpstreamDDLUsingGetDdl_EmptyResult(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	ddlMap, err := GetUpstreamDDLUsingGetDdl(context.Background(), &IterationContext{
		UpstreamExecutor:    mock,
		CurrentSnapshotName: "snap1",
	})
	require.NoError(t, err)
	assert.Empty(t, ddlMap)
}

// ---- parseCreateTableSQL_Empty ----

func TestParseCreateTableSQL_EmptyString(t *testing.T) {
	_, err := parseCreateTableSQL(context.Background(), "")
	assert.Error(t, err)
}

// ---- insertCCPRDb parse error ----

func TestInsertCCPRDb_InvalidDBID(t *testing.T) {
	mock := &mockSQLExecutor{}
	err := insertCCPRDb(context.Background(), mock, "not-a-number", "task1", "db1", 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse database ID")
}

// ---- insertCCPRDb success ----

func TestInsertCCPRDb_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{"ok"})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := insertCCPRDb(context.Background(), mock, "123", "task1", "db1", 0)
	assert.NoError(t, err)
}

// ---- insertCCPRTable success ----

func TestInsertCCPRTable_Success(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	bat := makeStringBatch(t, mp, []string{"ok"})
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			ir := &InternalResult{executorResult: buildResult(mp, bat)}
			return &Result{internalResult: ir}, func() {}, nil
		},
	}
	err := insertCCPRTable(context.Background(), mock, 1, "task1", "db1", "tbl1", 0)
	assert.NoError(t, err)
}

// ---- insertCCPRTable exec error ----

func TestInsertCCPRTable_ExecError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("exec fail")
		},
	}
	err := insertCCPRTable(context.Background(), mock, 1, "task1", "db1", "tbl1", 0)
	assert.Error(t, err)
}

// ---- insertCCPRDb exec error ----

func TestInsertCCPRDb_ExecError(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("exec fail")
		},
	}
	err := insertCCPRDb(context.Background(), mock, "123", "task1", "db1", 0)
	assert.Error(t, err)
}
