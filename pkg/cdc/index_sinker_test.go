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

package cdc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

func newTestTableDef(pkName string, pkType types.T, vecColName string, vecType types.T, vecWidth int32) *plan.TableDef {
	return &plan.TableDef{
		Name: "test_orig_tbl",
		Name2ColIndex: map[string]int32{
			pkName:     0,
			vecColName: 1,
			"dummy":    2, // Add another col to make sure pk/vec col indices are used
		},
		Cols: []*plan.ColDef{
			{Name: pkName, Typ: plan.Type{Id: int32(pkType)}},
			{Name: vecColName, Typ: plan.Type{Id: int32(vecType), Width: vecWidth}},
			{Name: "dummy", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names:       []string{pkName},
			PkeyColName: pkName,
		},
		Indexes: []*plan.IndexDef{
			{
				IndexName:          "hnsw_idx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Metadata,
				IndexTableName:     "meta_tbl",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"m":"16","ef_construction":"200","ef_search":"100","op_type":"vector_l2_ops"}`,
			},
			{
				IndexName:          "hnsw_idx",
				TableExist:         true,
				IndexAlgo:          catalog.MoIndexHnswAlgo.ToString(),
				IndexAlgoTableType: catalog.Hnsw_TblType_Storage,
				IndexTableName:     "storage_tbl",
				Parts:              []string{vecColName},
				IndexAlgoParams:    `{"m":"16","ef_construction":"200","ef_search":"100","op_type":"vector_l2_ops"}`,
			},
		},
	}
}

func newTestDbTableInfo() *DbTableInfo {
	return &DbTableInfo{
		SourceDbName:  "test_db",
		SourceTblName: "test_tbl",
		SinkDbName:    "sink_db",
		SinkTblName:   "sink_tbl",
	}
}

func newTestActiveRoutine() *ActiveRoutine {
	ar := NewCdcActiveRoutine()
	// ar.Start() // Don't start by default, let tests control
	return ar
}

type MockSQLExecutor struct {
}

// Exec exec a sql in a exists txn.
func (exec MockSQLExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {

	return executor.Result{}, nil
}

// ExecTxn executor sql in a txn. execFunc can use TxnExecutor to exec multiple sql
// in a transaction.
// NOTE: Pass SQL stmts one by one to TxnExecutor.Exec(). If you pass multiple SQL stmts to
// TxnExecutor.Exec() as `\n` seperated string, it will only execute the first SQL statement causing Bug.
func (exec MockSQLExecutor) ExecTxn(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {
	return nil
}

type MockErrorTxnExecutor struct {
	database string
	ctx      context.Context
}

func (exec *MockErrorTxnExecutor) Use(db string) {
	exec.database = db
}

func (exec *MockErrorTxnExecutor) Exec(
	sql string,
	statementOption executor.StatementOption,
) (executor.Result, error) {
	if strings.Contains(sql, "FAILSQL") {
		return executor.Result{}, moerr.NewInternalErrorNoCtx("db error")
	} else if strings.Contains(sql, "MULTI_ERROR_NO_ROLLBACK") {
		var errs error
		errs = errors.Join(errs, moerr.NewInternalErrorNoCtx("db error"))
		errs = errors.Join(errs, moerr.NewInternalErrorNoCtx("db error 2"))
		return executor.Result{}, errs
	} else if strings.Contains(sql, "MULTI_ERROR_ROLLBACK") {
		var errs error
		errs = errors.Join(errs, moerr.NewInternalErrorNoCtx("db error"))
		errs = errors.Join(errs, moerr.NewQueryInterrupted(exec.ctx))
		return executor.Result{}, errs
	}

	return executor.Result{}, nil
}

func (exec *MockErrorTxnExecutor) LockTable(table string) error {
	return nil
}

func (exec *MockErrorTxnExecutor) Txn() client.TxnOperator {
	return nil
}

/*
func (exec *MockErrorTxnExecutor) commit() error {
	return nil
}

func (exec *MockErrorTxnExecutor) getDatabase() string {
	return ""
}

func (exec *MockErrorTxnExecutor) rollback(err error) error {
	return nil
}
*/

var _ executor.TxnExecutor = new(MockErrorTxnExecutor)

type MockErrorSQLExecutor struct {
}

// Exec exec a sql in a exists txn.
func (exec MockErrorSQLExecutor) Exec(ctx context.Context, sql string, opts executor.Options) (executor.Result, error) {
	if strings.Contains(sql, "FAILSQL") {
		return executor.Result{}, moerr.NewInternalError(ctx, "db error")
	}
	return executor.Result{}, nil
}

// ExecTxn executor sql in a txn. execFunc can use TxnExecutor to exec multiple sql
// in a transaction.
// NOTE: Pass SQL stmts one by one to TxnExecutor.Exec(). If you pass multiple SQL stmts to
// TxnExecutor.Exec() as `\n` seperated string, it will only execute the first SQL statement causing Bug.
func (exec MockErrorSQLExecutor) ExecTxn(ctx context.Context, execFunc func(txn executor.TxnExecutor) error, opts executor.Options) error {

	txnexec := &MockErrorTxnExecutor{ctx: ctx}
	err := execFunc(txnexec)
	if moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted) {
		fmt.Printf("ROLLBACK...\n")
		return nil // Simulating successful handling of rollback signal
	}
	return err
}

var _ executor.SQLExecutor = new(MockSQLExecutor)
var _ executor.SQLExecutor = new(MockErrorSQLExecutor)

func mockSqlExecutorFactory(uuid string) (executor.SQLExecutor, error) {
	return MockSQLExecutor{}, nil
}

func mockErrorSqlExecutorFactory(uuid string) (executor.SQLExecutor, error) {
	return MockErrorSQLExecutor{}, nil
}

func NewMockWatermarkUpdater(ctx context.Context) *CDCWatermarkUpdater {
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater("test", ie)
	key1 := new(WatermarkKey)
	key1.AccountId = 0
	key1.TaskId = "taskid"
	key1.DBName = "test_db"
	key1.TableName = "test_tbl"
	wm1 := types.BuildTS(1, 1)
	err := u.UpdateWatermarkOnly(ctx, key1, &wm1)
	if err != nil {
		fmt.Printf("ERIC %v", err)
	}

	key2 := new(WatermarkKey)
	key2.AccountId = 0
	key2.TaskId = "taskid"
	key2.DBName = "test_db"
	key2.TableName = "test_tbl"
	watermark, err := u.GetFromCache(ctx, key2)
	if err != nil {
		fmt.Printf("ERIC2 %v", err)
	}

	var _ = watermark

	return u
}

// Constants that might be missing in the test context
//const sqlBufReserved = 0 // Assuming 0 for tests, original code might have a value

var (
// Define these if they are not exported or available in the test package context
// For this test, we'll assume they are defined as in the original package.
// If not, they would be:
// begin    = []byte("BEGIN")
// commit   = []byte("COMMIT")
// rollback = []byte("ROLLBACK")
// dummy    = []byte("DUMMY")
)

// --- Test Cases ---

func TestNewIndexSyncSinker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbTblInfo := newTestDbTableInfo()
	ar := newTestActiveRoutine()
	watermarkUpdater := NewMockWatermarkUpdater(ctx)

	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	defer sqlexecstub.Reset()

	t.Run("success float32", func(t *testing.T) {
		tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 128)
		sinker, err := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", dbTblInfo, watermarkUpdater, tblDef, 3, time.Second, ar, 1024, "10s")
		require.NoError(t, err)
		require.NotNil(t, sinker)
		sinker.Close()
	})

	/*
		t.Run("success float64", func(t *testing.T) {
			tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float64, 128)
			sinker, err := NewIndexSyncSinker("test-uuid", UriInfo{}, dbTblInfo, watermarkUpdater, tblDef, 3, time.Second, ar, 1024, "10s")
			require.NoError(t, err)
			require.NotNil(t, sinker)
			sinker.Close()
		})
	*/

	t.Run("invalid pkey count", func(t *testing.T) {
		tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 128)
		tblDef.Pkey.Names = []string{"pk1", "pk2"}
		_, err := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", dbTblInfo, watermarkUpdater, tblDef, 3, time.Second, ar, 1024, "10s")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})

	t.Run("invalid hnsw index count", func(t *testing.T) {
		tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 128)
		tblDef.Indexes = []*plan.IndexDef{tblDef.Indexes[0]} // Only one index
		_, err := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", dbTblInfo, watermarkUpdater, tblDef, 3, time.Second, ar, 1024, "10s")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})

	t.Run("invalid index parts count", func(t *testing.T) {
		tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 128)
		tblDef.Indexes[0].Parts = []string{"vec1", "vec2"}
		_, err := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", dbTblInfo, watermarkUpdater, tblDef, 3, time.Second, ar, 1024, "10s")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})

	t.Run("invalid pkey type", func(t *testing.T) {
		tblDef := newTestTableDef("pk", types.T_int32, "vec", types.T_array_float32, 128) // PK is int32
		_, err := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", dbTblInfo, watermarkUpdater, tblDef, 3, time.Second, ar, 1024, "10s")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})

	t.Run("missing meta index", func(t *testing.T) {
		tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 128)
		tblDef.Indexes[0].IndexAlgoTableType = "invalid" // Corrupt meta index type
		_, err := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", dbTblInfo, watermarkUpdater, tblDef, 3, time.Second, ar, 1024, "10s")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})

	t.Run("invalid hnsw params json", func(t *testing.T) {
		tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 128)
		tblDef.Indexes[0].IndexAlgoParams = `{"M":16, efConstruction":200 ...` // Invalid JSON
		_, err := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", dbTblInfo, watermarkUpdater, tblDef, 3, time.Second, ar, 1024, "10s")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})

	t.Run("unsupported vector type", func(t *testing.T) {
		tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_int32, 128) // Vector is int32
		_, err := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", dbTblInfo, watermarkUpdater, tblDef, 3, time.Second, ar, 1024, "10s")
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	})
}

func TestHnswSyncSinker_Run(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	defer sqlexecstub.Reset()
	watermarkUpdater := NewMockWatermarkUpdater(ctx)

	tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 4)
	sinker, err := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", newTestDbTableInfo(), watermarkUpdater, tblDef, 0, 0, newTestActiveRoutine(), 1024, "1s")
	require.NoError(t, err)
	defer sinker.Close()

	s := sinker.(*indexSyncSinker)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Run(ctx, nil) // ActiveRoutine not used by Run directly
	}()

	t.Run("happy path commit", func(t *testing.T) {
		s.ClearError()
		//var executedSqls []string

		s.SendBegin()
		s.sqlBufSendCh <- []byte("SELECT 1")
		s.SendCommit()

		// Wait for processing or timeout
		time.Sleep(100 * time.Millisecond)
		require.NoError(t, s.Error())
		//require.Contains(t, executedSqls, "SELECT 1")
	})

	// To properly test sinker.Close() and stop the Run goroutine:
	cancel() // Signal Run to stop its loop if it checks ctx.Done()
	// Closing sqlBufSendCh is done by sinker.Close(), which should cause Run to exit.
	wg.Wait() // Wait for Run goroutine to finish
}

func TestHnswSyncSinker_RunError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockErrorSqlExecutorFactory)
	defer sqlexecstub.Reset()
	watermarkUpdater := NewMockWatermarkUpdater(ctx)

	tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 4)
	sinker, err := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", newTestDbTableInfo(), watermarkUpdater, tblDef, 0, 0, newTestActiveRoutine(), 1024, "1s")
	require.NoError(t, err)
	defer sinker.Close()

	s := sinker.(*indexSyncSinker)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Run(ctx, nil) // ActiveRoutine not used by Run directly
	}()

	t.Run("exec error", func(t *testing.T) {
		s.ClearError()
		s.SendBegin()
		s.sqlBufSendCh <- []byte("FAILSQL")
		s.SendCommit() // This commit might not be reached if error handling is strict

		time.Sleep(100 * time.Millisecond)
		err := s.Error()
		require.Error(t, err)
	})

	t.Run("multi-error no rollback error", func(t *testing.T) {
		s.ClearError()
		s.SendBegin()
		s.sqlBufSendCh <- []byte("MULTI_ERROR_NO_ROLLBACK")
		s.SendCommit() // This commit might not be reached if error handling is strict

		time.Sleep(100 * time.Millisecond)
		err := s.Error()
		require.Error(t, err)
	})

	t.Run("multi-error with rollback error", func(t *testing.T) {
		s.ClearError()
		s.SendBegin()
		s.sqlBufSendCh <- []byte("MULTI_ERROR_ROLLBACK")
		s.SendCommit() // This commit might not be reached if error handling is strict

		time.Sleep(100 * time.Millisecond)
		err := s.Error()
		require.NoError(t, err)
	})

	t.Run("rollback", func(t *testing.T) {
		s.ClearError()
		s.SendBegin()
		s.SendRollback()

		time.Sleep(100 * time.Millisecond)
		require.NoError(t, s.Error())
		//require.True(t, rolledBack, "Rollback was not processed as expected")
	})

	fmt.Printf("finihsed......\n")
	// To properly test sinker.Close() and stop the Run goroutine:
	cancel() // Signal Run to stop its loop if it checks ctx.Done()
	// Closing sqlBufSendCh is done by sinker.Close(), which should cause Run to exit.
	// We already defer sinker.Close(), but for this test, let's be explicit.
	wg.Wait() // Wait for Run goroutine to finish
}

func TestHnswSyncSinker_Sink(t *testing.T) {

	ctx := context.Background()
	proc := testutil.NewProcess(t)

	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	defer sqlexecstub.Reset()

	tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	watermarkUpdater := NewMockWatermarkUpdater(ctx)
	sinker, err := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", newTestDbTableInfo(), watermarkUpdater, tblDef, 0, 0, newTestActiveRoutine(), 1024, "1s")
	require.NoError(t, err)
	defer sinker.Close()

	s := sinker.(*indexSyncSinker)

	t.Run("snapshot", func(t *testing.T) {
		s.Reset()

		bat := testutil.NewBatchWithVectors(
			[]*vector.Vector{
				testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2}),
				testutil.NewVector(2, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{0.1, 0.2}, {0.3, 0.4}}),
				testutil.NewVector(2, types.T_int32.ToType(), proc.Mp(), false, []int32{1, 2}),
			}, nil)

		defer bat.Clean(testutil.TestUtilMp)

		output := &DecoderOutput{
			fromTs:         types.BuildTS(1, 0),
			toTs:           types.BuildTS(2, 0),
			outputTyp:      OutputTypeSnapshot,
			checkpointBat:  bat,
			insertAtmBatch: nil, // Not used for snapshot
			deleteAtmBatch: nil, // Not used for snapshot
			noMoreData:     false,
		}
		s.Sink(ctx, output)
		require.NoError(t, s.Error())
		sql, err := s.sqlWriters[0].ToSql()
		require.NoError(t, err)
		require.Equal(t, string(sql), `SELECT hnsw_cdc_update('sink_db', 'sink_tbl', 2, '{"cdc":[{"t":"U","pk":1,"v":[0.1,0.2]},{"t":"U","pk":2,"v":[0.3,0.4]}]}');`)
	})

	t.Run("noMoreData", func(t *testing.T) {
		rowdata := []any{int64(100), []float32{1.0, 2.0}}
		s.Reset()
		s.sqlWriters[0].Upsert(ctx, rowdata) // Add some data
		require.False(t, s.sqlWriters[0].Empty())

		var sqlSent bool
		doneCh := make(chan struct{})
		go func() {
			// Need to consume from sqlBufSendCh or Sink will block
			for range s.sqlBufSendCh {
				sqlSent = true
				close(doneCh) // Signal that SQL was processed
				return        // Assume only one SQL for this test
			}
		}()

		output := &DecoderOutput{
			fromTs:     types.BuildTS(3, 0),
			toTs:       types.BuildTS(4, 0),
			outputTyp:  OutputTypeSnapshot, // Can be any type
			noMoreData: true,
		}
		s.Sink(ctx, output)
		require.NoError(t, s.Error())

		select {
		case <-doneCh:
			// SQL was sent and consumed
		case <-time.After(1 * time.Second):
			t.Fatal("timed out waiting for SQL to be sent on noMoreData")
		}
		require.True(t, sqlSent)
		require.True(t, s.sqlWriters[0].Empty(), "CDC should be reset after noMoreData flush")
	})

}

func TestHnswSyncSinker_SendSql(t *testing.T) {

	dbTblInfo := newTestDbTableInfo()
	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	defer sqlexecstub.Reset()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watermarkUpdater := NewMockWatermarkUpdater(ctx)

	tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	sinker, _ := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", newTestDbTableInfo(), watermarkUpdater, tblDef, 0, 0, newTestActiveRoutine(), 1024, "1s")
	s := sinker.(*indexSyncSinker)
	defer s.Close() // Closes sqlBufSendCh

	t.Run("send sql happy path", func(t *testing.T) {
		var err error
		row1 := []any{int64(1), []float32{0.1, 0.2}}
		row2 := []any{int64(2), []float32{0.3, 0.4}}

		s.Reset()
		err = s.sqlWriters[0].Upsert(ctx, row1)
		require.NoError(t, err)
		err = s.sqlWriters[0].Delete(ctx, row2)
		require.NoError(t, err)
		/*
			s.cdc.Start = "ts1"
			s.cdc.End = "ts2"
		*/

		var receivedSql []byte
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			receivedSql = <-s.sqlBufSendCh
		}()

		err = s.sendSql(s.sqlWriters[0])
		require.NoError(t, err)
		wg.Wait() // Wait for the goroutine to receive the SQL

		require.NotNil(t, receivedSql)
		sqlStr := string(receivedSql)

		/*
			expectedJsonPayload, _ := json.Marshal(map[string]any{
				"start_ts": s.cdc.Start, // These were reset before json was made
				"end_ts":   s.cdc.End,
				"op_type":  "hnsw_cdc_v1",
				"upserts":  []map[string]any{{"pk": int64(1), "vector": []float32{0.1, 0.2}}},
				"deletes":  []int64{int64(2)},
			})
		*/

		// The cdc.Start and cdc.End are part of the ToJson output, but sendSql resets cdc *after* ToJson
		// So we need to capture the state of cdc *before* it's reset for the expected JSON.
		// Let's reconstruct the expected JSON more carefully.
		writer, err := NewHnswSqlWriter("hnsw", dbTblInfo, tblDef, tblDef.Indexes)
		require.NoError(t, err)
		writer.Upsert(ctx, row1)
		writer.Delete(ctx, row2)
		/*
			cdcForJson.Start = "ts1"
			cdcForJson.End = "ts2"
		*/
		expectedSqlBytes, _ := writer.ToSql()
		require.Equal(t, string(expectedSqlBytes), sqlStr)
		require.True(t, s.sqlWriters[0].Empty(), "CDC should be reset after sending SQL")
	})

	t.Run("send sql empty cdc", func(t *testing.T) {
		s.Reset() // Ensure CDC is empty
		err := s.sendSql(s.sqlWriters[0])
		require.NoError(t, err)
		select {
		case <-s.sqlBufSendCh:
			t.Fatal("SQL should not have been sent for empty CDC")
		case <-time.After(50 * time.Millisecond):
			// Expected behavior
		}
	})
}

func TestHnswSyncSinker_ErrorHandling(t *testing.T) {
	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	defer sqlexecstub.Reset()

	s := &indexSyncSinker{}    // Minimal struct for error testing
	s.err.Store((*error)(nil)) // Initialize with nil error pointer

	require.Nil(t, s.Error())

	testErr := moerr.NewInternalErrorNoCtx("test error")
	s.SetError(testErr)
	err := s.Error()
	require.Error(t, err)
	require.Equal(t, "internal error: test error", err.Error())

	// Test with moerr
	moTestErr := moerr.NewInternalErrorNoCtx("mo test error")
	s.SetError(moTestErr)
	err = s.Error()
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrInternal))
	require.Equal(t, moTestErr.Error(), err.Error())

	s.ClearError()
	require.Nil(t, s.Error())
}

func TestHnswSyncSinker_Sink_AtomicBatch(t *testing.T) {

	proc := testutil.NewProcess()

	dbTblInfo := newTestDbTableInfo()
	ctx := context.Background()
	sqlexecstub := gostub.Stub(&sqlExecutorFactory, mockSqlExecutorFactory)
	defer sqlexecstub.Reset()

	watermarkUpdater := NewMockWatermarkUpdater(ctx)

	tblDef := newTestTableDef("pk", types.T_int64, "vec", types.T_array_float32, 2)
	sinker, _ := NewIndexSyncSinker("test-uuid", UriInfo{}, 0, "taskid", newTestDbTableInfo(), watermarkUpdater, tblDef, 0, 0, newTestActiveRoutine(), 1024, "1s")
	s := sinker.(*indexSyncSinker)
	defer s.Close() // Closes sqlBufSendCh

	bat := testutil.NewBatchWithVectors(
		[]*vector.Vector{
			testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2}),
			testutil.NewVector(2, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{0.1, 0.2}, {0.3, 0.4}}),
			testutil.NewVector(2, types.T_int32.ToType(), proc.Mp(), false, []int64{1, 2}),
		}, nil)
	defer bat.Clean(testutil.TestUtilMp)

	fromTs := types.BuildTS(1, 0)
	insertAtomicBat := &AtomicBatch{
		Mp:      nil,
		Batches: []*batch.Batch{bat},
		Rows:    btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
	}
	insertAtomicBat.Rows.Set(AtomicBatchRow{Ts: fromTs, Pk: []byte{1}, Offset: 0, Src: bat})
	insertAtomicBat.Rows.Set(AtomicBatchRow{Ts: fromTs, Pk: []byte{2}, Offset: 1, Src: bat})

	delbat := testutil.NewBatchWithVectors(
		[]*vector.Vector{
			testutil.NewVector(2, types.T_int64.ToType(), proc.Mp(), false, []int64{1, 2}),
			testutil.NewVector(2, types.T_array_float32.ToType(), proc.Mp(), false, [][]float32{{0.1, 0.2}, {0.3, 0.4}}),
			testutil.NewVector(2, types.T_int32.ToType(), proc.Mp(), false, []int64{1, 2}),
		}, nil)

	defer delbat.Clean(testutil.TestUtilMp)

	delfromTs := types.BuildTS(2, 0)
	delAtomicBat := &AtomicBatch{
		Mp:      nil,
		Batches: []*batch.Batch{delbat},
		Rows:    btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
	}
	delAtomicBat.Rows.Set(AtomicBatchRow{Ts: delfromTs, Pk: []byte{1}, Offset: 0, Src: bat})
	delAtomicBat.Rows.Set(AtomicBatchRow{Ts: delfromTs, Pk: []byte{2}, Offset: 1, Src: bat})

	dout := &DecoderOutput{
		fromTs:         types.BuildTS(1, 0),
		toTs:           types.BuildTS(2, 0),
		outputTyp:      OutputTypeTail,
		insertAtmBatch: insertAtomicBat,
		deleteAtmBatch: delAtomicBat,
	}

	var receivedSql []byte
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		receivedSql = <-s.sqlBufSendCh
		require.NotNil(t, receivedSql)
	}()

	s.Sink(context.Background(), dout)

	wg.Wait() // Wait for the goroutine to receive the SQL
	sqlStr := string(receivedSql)

	row1 := []any{int64(1), []float32{0.1, 0.2}}
	row2 := []any{int64(2), []float32{0.3, 0.4}}

	writer, _ := NewHnswSqlWriter("hnsw", dbTblInfo, tblDef, tblDef.Indexes)
	writer.Upsert(ctx, row1)
	writer.Upsert(ctx, row2)
	writer.Delete(ctx, row1)
	writer.Delete(ctx, row2)
	/*
		cdcForJson.Start = "1-0"
		cdcForJson.End = "2-0"
	*/
	expectedSqlBytes, _ := writer.ToSql()

	require.Equal(t, string(expectedSqlBytes), sqlStr)
	require.True(t, s.sqlWriters[0].Empty(), "CDC should be reset after sending SQL")

}
