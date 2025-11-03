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

package test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cdc"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"

	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"

	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

type internalExecResult struct {
	batch     *batch.Batch
	err       error
	stringErr error
}

func (res *internalExecResult) GetUint64(ctx context.Context, u uint64, u2 uint64) (uint64, error) {
	return 0, nil
}

func (res *internalExecResult) Error() error {
	return res.err
}

func (res *internalExecResult) ColumnCount() uint64 {
	return 1
}

func (res *internalExecResult) Column(ctx context.Context, i uint64) (name string, typ uint8, signed bool, err error) {
	return "test", 1, true, nil
}

func (res *internalExecResult) RowCount() uint64 {
	if res.batch == nil {
		return 0
	}
	return uint64(res.batch.RowCount())
}

func (res *internalExecResult) Row(ctx context.Context, i uint64) ([]interface{}, error) {
	return nil, nil
}

func (res *internalExecResult) Value(ctx context.Context, ridx uint64, cidx uint64) (interface{}, error) {
	return nil, nil
}

func (res *internalExecResult) GetFloat64(ctx context.Context, ridx uint64, cid uint64) (float64, error) {
	return 0.0, nil
}
func (res *internalExecResult) GetString(ctx context.Context, ridx uint64, cid uint64) (string, error) {
	if res.stringErr != nil {
		return "", res.stringErr
	}
	return res.batch.Vecs[cid].GetStringAt(int(ridx)), nil
}

type mockCDCIE struct {
	de        *testutil.TestDisttaeEngine
	err       error
	stringErr error
}

func (m *mockCDCIE) setError(err error) {
	m.err = err
}

func (m *mockCDCIE) setStringError(err error) {
	m.stringErr = err
}

func (m *mockCDCIE) Exec(ctx context.Context, s string, options ie.SessionOverrideOptions) error {
	panic("implement me")
}

func (m *mockCDCIE) Query(ctx context.Context, s string, options ie.SessionOverrideOptions) ie.InternalExecResult {
	if m.err != nil {
		return &internalExecResult{
			batch: nil,
			err:   m.err,
		}
	}
	res, err := execSql(m.de, ctx, s)
	var bat *batch.Batch
	if len(res.Batches) > 0 {
		bat = res.Batches[0]
	}
	if m.stringErr != nil {
		return &internalExecResult{
			batch:     bat,
			err:       err,
			stringErr: m.stringErr,
		}
	}
	return &internalExecResult{
		batch: bat,
		err:   err,
	}
}
func (m *mockCDCIE) ApplySessionOverride(options ie.SessionOverrideOptions) {
	panic("implement me")
}

func mock_mo_intra_system_change_propagation_log(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	sql := frontend.MoCatalogMoISCPLogDDL

	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	if err != nil {
		return err
	}
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	_, err = exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return err
}

func exec_sql(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	sql string,
) (err error) {

	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	_, err = exec.Exec(ctx, sql, executor.Options{})
	if err != nil {
		return err
	}
	return nil
}
func mock_mo_indexes(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	sql := "CREATE TABLE `mo_catalog`.`mo_indexes` ( " +
		"`id` bigint unsigned NOT NULL," +
		"`table_id` bigint unsigned NOT NULL," +
		"`database_id` bigint unsigned NOT NULL," +
		"`name` varchar(64) NOT NULL," +
		"`type` varchar(11) NOT NULL," +
		"`algo` varchar(11) DEFAULT NULL," +
		"`algo_table_type` varchar(11) DEFAULT NULL," +
		"`algo_params` varchar(2048) DEFAULT NULL," +
		"`is_visible` tinyint NOT NULL," +
		"`hidden` tinyint NOT NULL," +
		"`comment` varchar(2048) NOT NULL," +
		"`column_name` varchar(256) NOT NULL," +
		"`ordinal_position` int unsigned NOT NULL," +
		"`options` text DEFAULT NULL," +
		"`index_table_name` varchar(5000) DEFAULT NULL," +
		"PRIMARY KEY (`table_id`,`column_name`)" + // use table_id as primary key instead of id to avoid duplicate
		")"

	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	if err != nil {
		return err
	}
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	_, err = exec.Exec(ctx, sql, opts)
	if err != nil {
		return err
	}
	if err = txn.Commit(ctx); err != nil {
		return err
	}
	return err
}
func mock_mo_foreign_keys(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
) (err error) {
	sql := "CREATE TABLE `mo_catalog`.`mo_foreign_keys` (" +
		"`constraint_name` varchar(5000) NOT NULL," +
		"`constraint_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`db_name` varchar(5000) NOT NULL," +
		"`db_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`table_name` varchar(5000) NOT NULL," +
		"`table_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`column_name` varchar(256) NOT NULL," +
		"`column_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_db_name` varchar(5000) NOT NULL," +
		"`refer_db_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_table_name` varchar(5000) NOT NULL," +
		"`refer_table_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`refer_column_name` varchar(256) NOT NULL," +
		"`refer_column_id` bigint unsigned NOT NULL DEFAULT 0," +
		"`on_delete` varchar(128) NOT NULL," +
		"`on_update` varchar(128) NOT NULL," +
		"PRIMARY KEY (`constraint_name`,`constraint_id`,`db_name`,`db_id`,`table_name`,`table_id`,`column_name`,`column_id`,`refer_db_name`,`refer_db_id`,`refer_table_name`,`refer_table_id`,`refer_column_name`,`refer_column_id`)" +
		")"

	result, err := execSql(de, ctx, sql)
	result.Close()
	return err
}

func execSql(
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	sql string,
) (result executor.Result, err error) {
	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	if err != nil {
		return
	}
	opts := executor.Options{}.
		// All runSql and runSqlWithResult is a part of input sql, can not incr statement.
		// All these sub-sql's need to be rolled back and retried en masse when they conflict in pessimistic mode
		WithDisableIncrStatement().
		WithTxn(txn)

	result, err = exec.Exec(ctx, sql, opts)
	if err != nil {
		return
	}
	if err = txn.Commit(ctx); err != nil {
		return result, err
	}
	return result, nil
}

func CreateDBAndTableForHNSWAndGetAppendData(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	databaseName string,
	tableName string,
	rowCount int,
) *containers.Batch {
	// int64 is column 3, array_float32 is column 18
	schema := catalog2.MockSchemaAll(20, 3)
	txn, err := de.NewTxnOperator(ctx, de.Now())
	assert.NoError(t, err)

	err = de.Engine.Create(ctx, databaseName, txn)
	assert.NoError(t, err)

	database, err := de.Engine.Database(ctx, databaseName, txn)
	assert.NoError(t, err)

	engineTblDef, err := testutil.EngineTableDefBySchema(schema)
	assert.NoError(t, err)

	// add index
	indexColName := schema.ColDefs[18].Name
	engineTblDef = testutil.EngineDefAddIndex(engineTblDef, indexColName)

	err = database.Create(ctx, tableName, engineTblDef)
	assert.NoError(t, err)

	_, err = database.Relation(ctx, tableName, nil)
	assert.NoError(t, err)

	err = txn.Commit(ctx)
	assert.NoError(t, err)

	return catalog2.MockBatch(schema, rowCount)
}

func CreateDBAndTableForCNConsumerAndGetAppendData(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	databaseName string,
	tableName string,
	rowCount int,
) *containers.Batch {
	createDBSql := fmt.Sprintf("create database if not exists %s", databaseName)
	createTableSql := fmt.Sprintf(
		`create table %s.%s (
		id int primary key,
		name varchar,
		i1 bool,
		i2 TINYINT,
		i3 SMALLINT,
		i4 INT,
		i5 BIGINT,
		i6 FLOAT,
		i7 DOUBLE,
		i8 TINYINT UNSIGNED,
		i9 SMALLINT UNSIGNED,
		i10 INT UNSIGNED,
		i11 BIGINT UNSIGNED
		)`, databaseName, tableName)

	v, ok := moruntime.ServiceRuntime("").
		GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing lock service")
	}

	exec := v.(executor.SQLExecutor)
	_, err := exec.Exec(ctx, createDBSql, executor.Options{})
	assert.NoError(t, err)
	_, err = exec.Exec(ctx, createTableSql, executor.Options{})
	assert.NoError(t, err)

	return containers.MockBatchWithAttrs(
		[]types.Type{
			types.T_int32.ToType(),
			types.T_varchar.ToType(),
			types.T_bool.ToType(), //i1
			types.T_int8.ToType(),
			types.T_int16.ToType(),
			types.T_int32.ToType(),
			types.T_int64.ToType(), //i5
			types.T_float32.ToType(),
			types.T_float64.ToType(),
			types.T_uint8.ToType(),
			types.T_uint16.ToType(),
			types.T_uint32.ToType(),
			types.T_uint64.ToType(), //i11
		},
		[]string{"id", "name", "i1", "i2", "i3", "i4", "i5", "i6", "i7", "i8", "i9", "i10", "i11"},
		rowCount,
		0,
		nil,
	)
}

func GetTestISCPExecutorOption() *iscp.ISCPExecutorOption {
	return &iscp.ISCPExecutorOption{
		GCInterval:             time.Millisecond * 100,
		GCTTL:                  time.Millisecond,
		SyncTaskInterval:       time.Millisecond * 100,
		FlushWatermarkInterval: time.Millisecond * 500,
		RetryTimes:             1,
	}
}

func CheckTableDataWithName(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	srcDB string,
	srcTable string,
	destDB string,
	destTable string,
) {
	sql1 := fmt.Sprintf(
		"SELECT * FROM %v.%v EXCEPT SELECT * FROM %v.%v;",
		srcDB, srcTable,
		destDB, destTable,
	)
	result1, err := execSql(de, ctx, sql1)
	assert.NoError(t, err)
	defer result1.Close()
	rowCount := 0
	result1.ReadRows(func(rows int, cols []*vector.Vector) bool {
		rowCount += rows
		return true
	})
	assert.Equal(t, rowCount, 0)

	sql2 := fmt.Sprintf(
		"SELECT * FROM %v.%v EXCEPT SELECT * FROM %v.%v;",
		destDB, destTable,
		srcDB, srcTable,
	)
	result2, err := execSql(de, ctx, sql2)
	assert.NoError(t, err)
	defer result2.Close()
	rowCount = 0
	result2.ReadRows(func(rows int, cols []*vector.Vector) bool {
		rowCount += rows
		return true
	})
	assert.Equal(t, rowCount, 0)
}

func CheckTableData(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	dbName string,
	tableName string,
	tableID uint64,
	indexName string,
) {
	asyncIndexDBName := iscp.TargetDbName
	asyncIndexTableName := fmt.Sprintf("test_table_%d_%v", tableID, indexName)
	CheckTableDataWithName(t, de, ctx, dbName, tableName, asyncIndexDBName, asyncIndexTableName)
}

type MockEngineSink struct {
	engine    *testutil.TestDisttaeEngine
	accountId uint32

	// SQL executor
	sqlExecutor executor.SQLExecutor
	currentTxn  client.TxnOperator

	ExecutedSQLs  []string
	SendCount     int
	BeginCount    int
	CommitCount   int
	RollbackCount int
}

func NewMockEngineSink(
	engine *testutil.TestDisttaeEngine,
	accountId uint32,
) *MockEngineSink {
	v, ok := moruntime.ServiceRuntime("").GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing SQL executor")
	}

	return &MockEngineSink{
		engine:       engine,
		accountId:    accountId,
		sqlExecutor:  v.(executor.SQLExecutor),
		ExecutedSQLs: make([]string, 0),
	}
}

func (s *MockEngineSink) Send(ctx context.Context, ar *cdc.ActiveRoutine, sqlBuf []byte, needRetry bool) error {

	sql := string(sqlBuf[cdc.SqlBufReserved:])
	if strings.HasPrefix(sql, "use ") {
		logutil.Warnf("CDC-MockEngineSink skip use sql: %s", sql)
		return nil
	}
	sql = strings.TrimSpace(sql)

	if sql == "" || sql == "fakeSql" {
		return nil
	}

	s.ExecutedSQLs = append(s.ExecutedSQLs, sql)
	s.SendCount++

	ctx = defines.AttachAccountId(ctx, s.accountId)

	opts := executor.Options{}.WithDisableIncrStatement()
	if s.currentTxn != nil {
		opts = opts.WithTxn(s.currentTxn)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := s.sqlExecutor.Exec(ctxWithTimeout, sql, opts)
	return err
}

func (s *MockEngineSink) SendBegin(ctx context.Context) error {

	ctx = defines.AttachAccountId(ctx, s.accountId)
	txn, err := s.engine.NewTxnOperator(ctx, s.engine.Now())
	if err != nil {
		return err
	}

	s.currentTxn = txn
	s.BeginCount++

	return nil
}

func (s *MockEngineSink) SendCommit(ctx context.Context) error {
	ctx = defines.AttachAccountId(ctx, s.accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Check for injected fault using ISCPExecutorInjected
	// Only fail on the first commit attempt (commitAttempts == 0)
	if msg, injected := objectio.ISCPExecutorInjected(); injected {
		if strings.Contains(msg, "commit error") {
			return sql.ErrConnDone
		}
	}

	err := s.currentTxn.Commit(ctxWithTimeout)
	s.currentTxn = nil
	s.CommitCount++
	return err
}

func (s *MockEngineSink) SendRollback(ctx context.Context) error {
	ctx = defines.AttachAccountId(ctx, s.accountId)
	err := s.currentTxn.Rollback(ctx)
	s.currentTxn = nil
	s.RollbackCount++
	return err
}

func (s *MockEngineSink) Reset() {
	s.currentTxn = nil
}

func (s *MockEngineSink) Close() {

	if s.currentTxn != nil {
		ctx := context.Background()
		ctx = defines.AttachAccountId(ctx, s.accountId)
		_ = s.currentTxn.Rollback(ctx)
		s.currentTxn = nil
	}
}

func (s *MockEngineSink) GetStats() (sends, begins, commits, rollbacks int, sqls []string) {
	return s.SendCount, s.BeginCount, s.CommitCount, s.RollbackCount, s.ExecutedSQLs
}

func (s *MockEngineSink) ClearStats() {
	s.ExecutedSQLs = make([]string, 0)
	s.SendCount = 0
	s.BeginCount = 0
	s.CommitCount = 0
	s.RollbackCount = 0
}

// MockWatermarkUpdater is a mock implementation of WatermarkUpdater for CDC unit tests
// It stores watermarks in memory without database persistence
type MockWatermarkUpdater struct {
	mu sync.RWMutex

	// In-memory watermark storage
	watermarks map[cdc.WatermarkKey]types.TS
	errMsgs    map[cdc.WatermarkKey]string

	// Statistics for test verification
	GetOrAddCommittedCount int
	UpdateOnlyCount        int
	UpdateErrMsgCount      int
	RemoveCachedCount      int
	ForceFlushCount        int
}

func NewMockWatermarkUpdater() *MockWatermarkUpdater {
	return &MockWatermarkUpdater{
		watermarks: make(map[cdc.WatermarkKey]types.TS),
		errMsgs:    make(map[cdc.WatermarkKey]string),
	}
}

func (m *MockWatermarkUpdater) Start() {
	// No-op for mock
}

func (m *MockWatermarkUpdater) Stop() {
	// No-op for mock
}

func (m *MockWatermarkUpdater) GetFromCache(
	ctx context.Context,
	key *cdc.WatermarkKey,
) (watermark types.TS, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	wm, ok := m.watermarks[*key]
	if !ok {
		return types.TS{}, cdc.ErrNoWatermarkFound
	}
	return wm, nil
}

func (m *MockWatermarkUpdater) UpdateWatermarkErrMsg(
	ctx context.Context,
	key *cdc.WatermarkKey,
	errMsg string,
) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.errMsgs[*key] = errMsg
	m.UpdateErrMsgCount++
	return nil
}

func (m *MockWatermarkUpdater) UpdateWatermarkOnly(
	ctx context.Context,
	key *cdc.WatermarkKey,
	watermark *types.TS,
) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.watermarks[*key] = *watermark
	m.UpdateOnlyCount++
	return nil
}

func (m *MockWatermarkUpdater) RemoveCachedWM(
	ctx context.Context,
	key *cdc.WatermarkKey,
) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.watermarks, *key)
	delete(m.errMsgs, *key)
	m.RemoveCachedCount++
	return nil
}

func (m *MockWatermarkUpdater) ForceFlush(ctx context.Context) (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ForceFlushCount++
	return nil
}

func (m *MockWatermarkUpdater) GetOrAddCommitted(
	ctx context.Context,
	key *cdc.WatermarkKey,
	watermark *types.TS,
) (ret types.TS, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if watermark exists
	if existing, ok := m.watermarks[*key]; ok {
		// Return the larger watermark
		if existing.GE(watermark) {
			ret = existing
		} else {
			m.watermarks[*key] = *watermark
			ret = *watermark
		}
	} else {
		// Add new watermark
		m.watermarks[*key] = *watermark
		ret = *watermark
	}

	m.GetOrAddCommittedCount++
	return ret, nil
}

// GetWatermark retrieves a watermark for testing verification
func (m *MockWatermarkUpdater) GetWatermark(key cdc.WatermarkKey) (types.TS, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	wm, ok := m.watermarks[key]
	return wm, ok
}

// GetErrMsg retrieves an error message for testing verification
func (m *MockWatermarkUpdater) GetErrMsg(key cdc.WatermarkKey) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	errMsg, ok := m.errMsgs[key]
	return errMsg, ok
}

// GetStats returns statistics for test verification
func (m *MockWatermarkUpdater) GetStats() (getOrAdd, updateOnly, updateErr, remove, flush int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.GetOrAddCommittedCount, m.UpdateOnlyCount, m.UpdateErrMsgCount,
		m.RemoveCachedCount, m.ForceFlushCount
}

// ClearStats clears all statistics
func (m *MockWatermarkUpdater) ClearStats() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.GetOrAddCommittedCount = 0
	m.UpdateOnlyCount = 0
	m.UpdateErrMsgCount = 0
	m.RemoveCachedCount = 0
	m.ForceFlushCount = 0
}

func StubCDCSinkAndWatermarkUpdater(
	engine *testutil.TestDisttaeEngine,
	accountId uint32,
) (
	*gostub.Stubs,
	*gostub.Stubs,
	*gostub.Stubs,
	*gostub.Stubs,
	*gostub.Stubs,
	*MockWatermarkUpdater,
	chan string,
) {
	errChan := make(chan string, 10)
	mockWatermarkUpdater := NewMockWatermarkUpdater()
	stub1 := gostub.Stub(
		&cdc.GetCDCWatermarkUpdater,
		func(
			cnUUID string,
			executor ie.InternalExecutor,
		) cdc.WatermarkUpdater {
			return mockWatermarkUpdater
		},
	)
	stub2 := gostub.Stub(
		&cdc.NewMysqlSink,
		func(
			user, password string,
			ip string, port int,
			retryTimes int,
			retryDuration time.Duration,
			timeout string,
			doRecord bool,
		) (cdc.Sink, error) {
			return NewMockEngineSink(engine, accountId), nil
		},
	)
	stub3 := gostub.Stub(
		&frontend.UpdateErrMsg,
		func(ctx context.Context, exec *frontend.CDCTaskExecutor, errMsg string) error {
			// Only send non-empty error messages to errChan
			if errMsg != "" {
				logutil.Infof("MockWatermarkUpdater UpdateErrMsg: %s", errMsg)
				errChan <- errMsg
			}
			return nil
		},
	)
	stub4 := gostub.Stub(
		&frontend.RetrieveCdcTask,
		MockRetrieveCdcTask,
	)
	stub5 := gostub.Stub(
		&frontend.GetTableErrMsg,
		func(
			ctx context.Context,
			accountId uint32,
			ieExecutor ie.InternalExecutor,
			taskId string,
			tbl *cdc.DbTableInfo,
		) (
			hasError bool,
			startTS int64,
			retryTimes int,
			err error,
		) {
			watermarkKey := cdc.WatermarkKey{
				AccountId: uint64(accountId),
				TaskId:    taskId,
				DBName:    tbl.SourceDbName,
				TableName: tbl.SourceTblName,
			}
			errMsg, ok := mockWatermarkUpdater.GetErrMsg(watermarkKey)
			if !ok {
				return false, 0, 0, nil
			}
			retryable, startTS, retryTimes := cdc.ParseRetryableError(errMsg)
			return !retryable, startTS, retryTimes, nil
		},
	)
	return stub1, stub2, stub3, stub4, stub5, mockWatermarkUpdater, errChan
}

// MockRetrieveCdcTask mocks the retrieveCdcTask function for testing
func MockRetrieveCdcTask(ctx context.Context, exec *frontend.CDCTaskExecutor) error {
	// Set sinkUri with Console type (no external sink needed for testing)
	exec.SetSinkUri(cdc.UriInfo{
		SinkTyp: cdc.CDCSinkType_MySQL,
	})

	// Note: tables are already set in NewMockCDCExecutor, so we don't override them here

	// Set exclude to nil (no exclusion pattern)
	exec.SetExclude(nil)

	// Set startTs to zero (start from beginning)
	exec.SetStartTs(types.TS{})

	// Set endTs to max (no end time)
	exec.SetEndTs(types.BuildTS(1<<63-1, 0))

	// Set noFull to false (include full snapshot)
	exec.SetNoFull(false)

	// Set additionalConfig with default values
	exec.SetAdditionalConfig(map[string]interface{}{
		cdc.CDCTaskExtraOptions_MaxSqlLength:         float64(1024 * 1024),
		cdc.CDCTaskExtraOptions_SendSqlTimeout:       "5s",
		cdc.CDCTaskExtraOptions_InitSnapshotSplitTxn: false,
		cdc.CDCTaskExtraOptions_Frequency:            "",
	})

	return nil
}

// NewMockCDCExecutor creates a CDC executor for testing with mock components
func NewMockCDCExecutor(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	accountId uint32,
	taskId string,
	taskName string,
	tables cdc.PatternTuples,
) *frontend.CDCTaskExecutor {

	// Create CDC task spec
	spec := &task.CreateCdcDetails{
		TaskId:   taskId,
		TaskName: taskName,
		Accounts: []*task.Account{
			{
				Id: uint64(accountId),
			},
		},
	}

	// Create mock internal executor
	mockIE := &mockCDCIE{de: de}

	// Create CDC executor
	cnUUID := de.Engine.GetService()
	cdcExecutor := frontend.NewCDCTaskExecutor(
		logutil.GetGlobalLogger(),
		mockIE,
		spec,
		cnUUID,
		de.Engine.FS(),
		de.GetTxnClient(),
		de.Engine,
		common.DebugAllocator,
	)
	cdcExecutor.SetActiveRoutine(cdc.NewCdcActiveRoutine())

	// Set tables that will be used by the mocked RetrieveCdcTask
	cdcExecutor.SetTables(tables)

	return cdcExecutor
}

// SetupMockCDCExecutorFields sets up internal fields for CDC executor testing
func SetupMockCDCExecutorFields(
	cdcExecutor *frontend.CDCTaskExecutor,
	tables cdc.PatternTuples,
) {
	// Use reflection or accessor methods if available
	// For now, we'll rely on the retrieveCdcTask being mocked via the IE
}

// CreateDBAndTableForCDC creates a database and table for CDC testing
func CreateDBAndTableForCDC(
	t *testing.T,
	de *testutil.TestDisttaeEngine,
	ctx context.Context,
	databaseName string,
	tableName string,
	rowCount int,
) *containers.Batch {
	createDBSql := fmt.Sprintf("create database if not exists %s", databaseName)
	createTableSql := fmt.Sprintf(
		`create table %s.%s (
		id int primary key,
		name varchar(100),
		age int
		)`, databaseName, tableName)

	v, ok := moruntime.ServiceRuntime("").
		GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing SQL executor")
	}

	exec := v.(executor.SQLExecutor)
	_, err := exec.Exec(ctx, createDBSql, executor.Options{})
	assert.NoError(t, err)
	_, err = exec.Exec(ctx, createTableSql, executor.Options{})
	assert.NoError(t, err)

	return containers.MockBatchWithAttrs(
		[]types.Type{
			types.T_int32.ToType(),
			types.T_varchar.ToType(),
			types.T_int32.ToType(),
		},
		[]string{"id", "name", "age"},
		rowCount,
		0,
		nil,
	)
}
