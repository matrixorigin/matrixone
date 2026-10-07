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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

// ---------------------------------------------------------------------------
// Helpers local to this test file
// ---------------------------------------------------------------------------

// mockSQLExecutorCB2 implements SQLExecutor via a pluggable function.
type mockSQLExecutorCB2 struct {
	execFn func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error)
}

func (m *mockSQLExecutorCB2) Close() error                           { return nil }
func (m *mockSQLExecutorCB2) Connect() error                         { return nil }
func (m *mockSQLExecutorCB2) EndTxn(_ context.Context, _ bool) error { return nil }
func (m *mockSQLExecutorCB2) ExecSQL(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
	return m.execFn(ctx, ar, accountID, query, useTxn, needRetry, timeout)
}
func (m *mockSQLExecutorCB2) ExecSQLInDatabase(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, _ string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
	return m.execFn(ctx, ar, accountID, query, useTxn, needRetry, timeout)
}

// mockCCPRTxnCacheWriterCB2 implements CCPRTxnCacheWriter for testing.
type mockCCPRTxnCacheWriterCB2 struct {
	writeObjectFn   func(ctx context.Context, objectName string, txnID []byte) (bool, error)
	onFileWrittenFn func(objectName string)
}

func (m *mockCCPRTxnCacheWriterCB2) WriteObject(ctx context.Context, objectName string, txnID []byte) (bool, error) {
	if m.writeObjectFn != nil {
		return m.writeObjectFn(ctx, objectName, txnID)
	}
	return false, nil
}

func (m *mockCCPRTxnCacheWriterCB2) WriteNewObject(
	ctx context.Context,
	object ioutil.UnpublishedObject,
	txnID []byte,
) error {
	isNew, err := m.WriteObject(ctx, object.File, txnID)
	if err != nil {
		return err
	}
	if !isNew {
		return moerr.NewInternalErrorNoCtxf(
			"attempt-unique CCPR object %s already exists", object.File)
	}
	return nil
}

func (m *mockCCPRTxnCacheWriterCB2) OnFileWritten(objectName string) {
	if m.onFileWrittenFn != nil {
		m.onFileWrittenFn(objectName)
	}
}

// ---------------------------------------------------------------------------
// filter_object.go — AObjectMap methods
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// filter_object.go — rewriteTombstoneRowidsBatch edge cases
// ---------------------------------------------------------------------------

func TestRewriteTombstoneRowidsBatch_ZeroRows(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)
	bat := &batch.Batch{Vecs: []*vector.Vector{vector.NewVec(types.T_Rowid.ToType())}}
	bat.SetRowCount(0)
	err = rewriteTombstoneRowidsBatch(context.Background(), bat, nil, mp)
	assert.NoError(t, err)
}

func TestRewriteTombstoneRowidsBatch_NoMapping(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mp.Free(nil)

	upstreamObjID := types.NewObjectid()
	rid := types.NewRowIDWithObjectIDBlkNumAndRowID(upstreamObjID, 0, 7)

	rowidVec := vector.NewVec(types.T_Rowid.ToType())
	require.NoError(t, vector.AppendFixed(rowidVec, rid, false, mp))
	bat := &batch.Batch{Vecs: []*vector.Vector{rowidVec}}
	bat.SetRowCount(1)

	// Empty aobjectMap - no mapping exists
	amap := NewAObjectMap()
	err = rewriteTombstoneRowidsBatch(context.Background(), bat, amap, mp)
	assert.NoError(t, err)

	// Rowid should be unchanged
	rowids := vector.MustFixedColWithTypeCheck[types.Rowid](rowidVec)
	assert.Equal(t, uint32(7), rowids[0].GetRowOffset())

	rowidVec.Free(mp)
}

// ---------------------------------------------------------------------------
// filter_object_batch.go — filterBatchBySnapshotTS nil batch path
// (already tested but we cover additional branch: commitTS column type check)
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// filter_object_job.go — WriteObjectJob paths
// ---------------------------------------------------------------------------

func newTestMemoryFS(t *testing.T) fileservice.FileService {
	t.Helper()
	fs, err := fileservice.NewMemoryFS("test", fileservice.CacheConfig{}, nil)
	require.NoError(t, err)
	return fs
}

func TestWriteObjectJob_Execute_NilCache_Success(t *testing.T) {
	fs := newTestMemoryFS(t)
	job := NewWriteObjectJob(context.Background(), fs, "test-obj-1", []byte("content"), nil, nil)
	job.Execute()
	res := job.WaitDone().(*WriteObjectJobResult)
	assert.NoError(t, res.Err)
}

func TestWriteObjectJob_Execute_NilCache_FileExists(t *testing.T) {
	fs := newTestMemoryFS(t)
	// Write once
	job1 := NewWriteObjectJob(context.Background(), fs, "test-obj-dup", []byte("content"), nil, nil)
	job1.Execute()
	res1 := job1.WaitDone().(*WriteObjectJobResult)
	require.NoError(t, res1.Err)
	// Write again → ErrFileAlreadyExists → should be silently ignored
	job2 := NewWriteObjectJob(context.Background(), fs, "test-obj-dup", []byte("content"), nil, nil)
	job2.Execute()
	res2 := job2.WaitDone().(*WriteObjectJobResult)
	assert.NoError(t, res2.Err)
}

func TestWriteObjectJob_Execute_WithCache_CacheError(t *testing.T) {
	cache := &mockCCPRTxnCacheWriterCB2{
		writeObjectFn: func(ctx context.Context, objectName string, txnID []byte) (bool, error) {
			return false, moerr.NewInternalErrorNoCtx("cache error")
		},
	}
	job := NewWriteObjectJob(context.Background(), nil, "test-obj", []byte("content"), cache, []byte("txn1"))
	job.Execute()
	res := job.WaitDone().(*WriteObjectJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "cache error")
}

func TestWriteObjectJob_Execute_WithCache_NotNewFile(t *testing.T) {
	cache := &mockCCPRTxnCacheWriterCB2{
		writeObjectFn: func(ctx context.Context, objectName string, txnID []byte) (bool, error) {
			return false, nil // not a new file
		},
	}
	job := NewWriteObjectJob(context.Background(), nil, "test-obj", []byte("content"), cache, []byte("txn1"))
	job.Execute()
	res := job.WaitDone().(*WriteObjectJobResult)
	assert.NoError(t, res.Err)
}

func TestWriteObjectJob_Execute_WithCache_NewFile_Success(t *testing.T) {
	fs := newTestMemoryFS(t)
	notified := false
	cache := &mockCCPRTxnCacheWriterCB2{
		writeObjectFn: func(ctx context.Context, objectName string, txnID []byte) (bool, error) {
			return true, nil // new file
		},
		onFileWrittenFn: func(objectName string) {
			notified = true
		},
	}
	job := NewWriteObjectJob(context.Background(), fs, "test-obj-cache", []byte("content"), cache, []byte("txn1"))
	job.Execute()
	res := job.WaitDone().(*WriteObjectJobResult)
	assert.NoError(t, res.Err)
	assert.True(t, notified)
}

// ---------------------------------------------------------------------------
// filter_object_job.go — FilterObjectJob Execute paths
// ---------------------------------------------------------------------------

func TestFilterObjectJob_Execute_TTLPassesButFilterFails(t *testing.T) {
	// TTL checker passes, but FilterObject fails due to invalid stats bytes
	job := NewFilterObjectJob(
		context.Background(),
		[]byte("short"), // invalid length
		types.TS{},
		nil, false, nil, nil, nil, nil, "acc", "pub", nil, nil, nil,
		func() bool { return true }, // TTL passes
	)
	job.Execute()
	res := job.WaitDone().(*FilterObjectJobResult)
	assert.Error(t, res.Err)
	assert.Contains(t, res.Err.Error(), "invalid object stats length")
}

func TestUpstreamExecutor_EndTxn_NilTx_Rollback(t *testing.T) {
	e := &UpstreamExecutor{}
	err := e.EndTxn(context.Background(), false)
	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// sql_executor.go — tryDecryptPassword with executor but empty cnUUID
// ---------------------------------------------------------------------------

func TestTryDecryptPassword_ExecutorNotNil_EmptyCnUUID(t *testing.T) {
	exec := &mockSQLExecutorCB2{
		execFn: func(_ context.Context, _ *ActiveRoutine, _ uint32, _ string, _ bool, _ bool, _ time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, nil
		},
	}
	validHex := "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"
	// cnUUID is empty, so no decryption attempt
	result := tryDecryptPassword(context.Background(), validHex, exec, "")
	assert.Equal(t, validHex, result)
}

// ---------------------------------------------------------------------------
// executor.go — Cancel, Pause, Restart when not running
// ---------------------------------------------------------------------------

func TestPublicationTaskExecutor_Cancel_NotRunning(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	err := exec.Cancel()
	assert.NoError(t, err)
	assert.False(t, exec.IsRunning())
}

func TestPublicationTaskExecutor_Pause_NotRunning(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	err := exec.Pause()
	assert.NoError(t, err)
	assert.False(t, exec.IsRunning())
}

func TestPublicationTaskExecutor_Start_AlreadyRunning(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.running = true
	err := exec.Start()
	assert.NoError(t, err) // returns nil immediately
}

// ---------------------------------------------------------------------------
// executor.go — GCInMemoryTask
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// executor.go — getCandidateTasks
// ---------------------------------------------------------------------------

func TestGetCandidateTasks_Mixed(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	exec.setTask(TaskEntry{TaskID: "t1", SubscriptionState: SubscriptionStateRunning, State: IterationStateCompleted})
	exec.setTask(TaskEntry{TaskID: "t2", SubscriptionState: SubscriptionStateRunning, State: IterationStateRunning})
	exec.setTask(TaskEntry{TaskID: "t3", SubscriptionState: SubscriptionStatePause, State: IterationStateCompleted})
	exec.setTask(TaskEntry{TaskID: "t4", SubscriptionState: SubscriptionStateDropped, State: IterationStateCompleted})
	exec.setTask(TaskEntry{TaskID: "t5", SubscriptionState: SubscriptionStateRunning, State: IterationStateCompleted})

	candidates := exec.getCandidateTasks()
	assert.Equal(t, 2, len(candidates)) // only t1 and t5
	taskIDs := make(map[string]bool)
	for _, c := range candidates {
		taskIDs[c.TaskID] = true
	}
	assert.True(t, taskIDs["t1"])
	assert.True(t, taskIDs["t5"])
}

func TestGetCandidateTasks_Empty(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	candidates := exec.getCandidateTasks()
	assert.Empty(t, candidates)
}

// ---------------------------------------------------------------------------
// executor.go — addOrUpdateTask
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// executor.go — getAllTasks
// ---------------------------------------------------------------------------

func TestGetAllTasks(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	exec.setTask(TaskEntry{TaskID: "a"})
	exec.setTask(TaskEntry{TaskID: "b"})
	exec.setTask(TaskEntry{TaskID: "c"})
	tasks := exec.getAllTasks()
	assert.Equal(t, 3, len(tasks))
}

// ---------------------------------------------------------------------------
// filter_object.go — FilterObject with valid stats length but invalid content
// ---------------------------------------------------------------------------

// mockClassifierCB2 for this test file
type mockClassifierCB2 struct {
	retryable bool
}

func (m *mockClassifierCB2) IsRetryable(err error) bool {
	return m.retryable
}

// ---------------------------------------------------------------------------
// sql_executor.go — ExecSQL with ActiveRoutine (not nil)
// ---------------------------------------------------------------------------

func TestUpstreamExecutor_ExecWithRetry_ActiveRoutine_Pause(t *testing.T) {
	e := &UpstreamExecutor{
		retryTimes:    5,
		retryDuration: time.Minute,
	}
	e.initRetryPolicy(&mockClassifierCB2{retryable: true})

	ar := NewActiveRoutine()
	ar.ClosePause()

	_, _, err := e.execWithRetry(context.Background(), ar, time.Second, func(ctx context.Context) (*Result, error) {
		return nil, moerr.NewInternalErrorNoCtx("fail")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task paused")
}

func TestUpstreamExecutor_ExecWithRetry_ActiveRoutine_Cancel(t *testing.T) {
	e := &UpstreamExecutor{
		retryTimes:    5,
		retryDuration: time.Minute,
	}
	e.initRetryPolicy(&mockClassifierCB2{retryable: true})

	ar := NewActiveRoutine()
	ar.CloseCancel()

	_, _, err := e.execWithRetry(context.Background(), ar, time.Second, func(ctx context.Context) (*Result, error) {
		return nil, moerr.NewInternalErrorNoCtx("fail")
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task cancelled")
}
