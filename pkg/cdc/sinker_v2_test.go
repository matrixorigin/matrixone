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
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMysqlSinker2_ErrorHandling(t *testing.T) {
	t.Run("ErrorIsNilSafe", func(t *testing.T) {
		sinker := &mysqlSinker2{}

		// Should not panic
		err := sinker.Error()
		assert.Nil(t, err)
	})

	t.Run("SetAndGetError", func(t *testing.T) {
		sinker := &mysqlSinker2{}

		// Initially no error
		assert.Nil(t, sinker.Error())

		// Set error
		testErr := moerr.NewInternalErrorNoCtx("test error")
		sinker.SetError(testErr)

		// Get error
		err := sinker.Error()
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "test error")
	})

	t.Run("ClearError", func(t *testing.T) {
		sinker := &mysqlSinker2{}

		// Set error
		sinker.SetError(moerr.NewInternalErrorNoCtx("test error"))
		assert.NotNil(t, sinker.Error())

		// Clear error
		sinker.ClearError()
		assert.Nil(t, sinker.Error())
	})

	t.Run("SetNilError", func(t *testing.T) {
		sinker := &mysqlSinker2{}

		// Set nil error (should not panic)
		sinker.SetError(nil)
		assert.Nil(t, sinker.Error())
	})
}

func TestMysqlSinker2_TransactionLifecycle(t *testing.T) {
	ctx := context.Background()

	t.Run("BeginTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{conn: db}

		tableDef := &plan.TableDef{
			Name: "test",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
			Name2ColIndex: map[string]int32{"id": 0},
		}

		builder, err := NewCDCStatementBuilder("test_db", "test", tableDef, 1024*1024, false)
		require.NoError(t, err)

		sinker := NewMysqlSinker2(
			executor,
			1,
			"task-1",
			&DbTableInfo{SourceDbName: "src", SourceTblName: "test"},
			nil,
			builder,
			NewCdcActiveRoutine(),
		)

		// Initially IDLE
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())

		// Begin transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)

		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("CommitTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{conn: db}

		tableDef := &plan.TableDef{
			Name: "test",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
			Name2ColIndex: map[string]int32{"id": 0},
		}

		builder, err := NewCDCStatementBuilder("test_db", "test", tableDef, 1024*1024, false)
		require.NoError(t, err)

		sinker := NewMysqlSinker2(
			executor,
			1,
			"task-1",
			&DbTableInfo{SourceDbName: "src", SourceTblName: "test"},
			nil,
			builder,
			NewCdcActiveRoutine(),
		)

		// Begin a real transaction first
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		// Commit transaction
		mock.ExpectCommit()
		err = sinker.handleCommit(ctx)

		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("RollbackTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{conn: db}

		tableDef := &plan.TableDef{
			Name: "test",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			},
			Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
			Name2ColIndex: map[string]int32{"id": 0},
		}

		builder, err := NewCDCStatementBuilder("test_db", "test", tableDef, 1024*1024, false)
		require.NoError(t, err)

		sinker := NewMysqlSinker2(
			executor,
			1,
			"task-1",
			&DbTableInfo{SourceDbName: "src", SourceTblName: "test"},
			nil,
			builder,
			NewCdcActiveRoutine(),
		)

		// Begin new transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)

		// Rollback
		mock.ExpectRollback()
		err = sinker.handleRollback(ctx)

		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestMysqlSinker2_CommandProcessing(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	executor := &Executor{conn: db}

	tableDef := &plan.TableDef{
		Name: "users",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
			{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar)}},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "name": 1},
	}

	builder, err := NewCDCStatementBuilder("test_db", "users", tableDef, 1024*1024, false)
	require.NoError(t, err)

	sinker := NewMysqlSinker2(
		executor,
		1,
		"task-1",
		&DbTableInfo{SourceDbName: "src", SourceTblName: "users", SinkDbName: "test_db", SinkTblName: "users"},
		nil,
		builder,
		NewCdcActiveRoutine(),
	)

	ctx := context.Background()

	t.Run("ProcessBeginCommand", func(t *testing.T) {
		cmd := NewBeginCommand()

		mock.ExpectBegin()

		sinker.processCommand(ctx, cmd)

		assert.Nil(t, sinker.Error())
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ProcessDummyCommand", func(t *testing.T) {
		cmd := NewDummyCommand()

		// Dummy should not cause any side effects
		sinker.processCommand(ctx, cmd)

		assert.Nil(t, sinker.Error())
	})

	t.Run("SkipCommandsWhenErrorExists", func(t *testing.T) {
		// Set error
		sinker.SetError(moerr.NewInternalErrorNoCtx("previous error"))

		// Try to process a command
		cmd := NewDummyCommand()
		sinker.processCommand(ctx, cmd)

		// Error should still exist (command was skipped)
		assert.NotNil(t, sinker.Error())
		assert.Contains(t, sinker.Error().Error(), "previous error")

		// Clear error for next test
		sinker.ClearError()
	})
}

func TestMysqlSinker2_Reset(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	executor := &Executor{conn: db}

	tableDef := &plan.TableDef{
		Name: "test",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0},
	}

	builder, err := NewCDCStatementBuilder("test_db", "test", tableDef, 1024*1024, false)
	require.NoError(t, err)

	sinker := NewMysqlSinker2(
		executor,
		1,
		"task-1",
		&DbTableInfo{SourceDbName: "src", SourceTblName: "test"},
		nil,
		builder,
		NewCdcActiveRoutine(),
	)

	t.Run("ResetWithActiveTransaction", func(t *testing.T) {
		// Start transaction
		mock.ExpectBegin()
		err := sinker.handleBegin(context.Background())
		require.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		// Reset should rollback
		mock.ExpectRollback()
		sinker.Reset()

		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.Nil(t, sinker.Error())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ResetWithError", func(t *testing.T) {
		// Set error
		sinker.SetError(moerr.NewInternalErrorNoCtx("test error"))
		assert.NotNil(t, sinker.Error())

		// Reset should clear error
		sinker.Reset()

		assert.Nil(t, sinker.Error())
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
	})

	t.Run("ResetWhenIdle", func(t *testing.T) {
		// Reset when already idle (should be idempotent)
		sinker.Reset()

		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.Nil(t, sinker.Error())
	})
}

func TestMysqlSinker2_SendMethods(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	executor := &Executor{conn: db}

	tableDef := &plan.TableDef{
		Name: "test",
		Cols: []*plan.ColDef{
			{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0},
	}

	builder, err := NewCDCStatementBuilder("test_db", "test", tableDef, 1024*1024, false)
	require.NoError(t, err)

	sinker := NewMysqlSinker2(
		executor,
		1,
		"task-1",
		&DbTableInfo{SourceDbName: "src", SourceTblName: "test"},
		nil,
		builder,
		NewCdcActiveRoutine(),
	)

	t.Run("SendCommandsToChannel", func(t *testing.T) {
		// Start a goroutine to receive commands
		received := make(chan CommandType, 4)
		go func() {
			for i := 0; i < 4; i++ {
				cmd := <-sinker.cmdCh
				received <- cmd.Type
			}
		}()

		// Send commands
		sinker.SendBegin()
		sinker.SendDummy()
		sinker.SendCommit()
		sinker.SendRollback()

		// Verify commands were sent
		assert.Equal(t, CmdBegin, <-received)
		assert.Equal(t, CmdDummy, <-received)
		assert.Equal(t, CmdCommit, <-received)
		assert.Equal(t, CmdRollback, <-received)
	})
}

func TestMysqlSinker2_HandleInsertBatch(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	require.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	t.Run("SuccessfulInsert", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn:          db,
			retryTimes:    0, // No retry
			retryDuration: 1 * time.Second,
		}

		tableDef := &plan.TableDef{
			Name: "users",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
				{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar)}},
			},
			Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
			Name2ColIndex: map[string]int32{"id": 0, "name": 1},
		}

		builder, err := NewCDCStatementBuilder("test_db", "users", tableDef, 1024*1024, false)
		require.NoError(t, err)

		sinker := NewMysqlSinker2(
			executor,
			1,
			"task-1",
			&DbTableInfo{SourceDbName: "src", SourceTblName: "users"},
			nil,
			builder,
			NewCdcActiveRoutine(),
		)

		// Create batch
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

		vector.AppendFixed(bat.Vecs[0], int32(1), false, mp)
		vector.AppendBytes(bat.Vecs[1], []byte("Alice"), false, mp)
		bat.SetRowCount(1)

		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)
		cmd := NewInsertBatchCommand(bat, fromTs, toTs)

		// Expect SQL execution
		mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(1, 1))

		ctx := context.Background()
		err = sinker.handleInsertBatch(ctx, cmd)

		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("InsertFailure", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn:          db,
			retryTimes:    0, // No retry
			retryDuration: 1 * time.Second,
		}

		tableDef := &plan.TableDef{
			Name: "users",
			Cols: []*plan.ColDef{
				{Name: "id", Typ: plan.Type{Id: int32(types.T_int32)}},
				{Name: "name", Typ: plan.Type{Id: int32(types.T_varchar)}},
			},
			Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
			Name2ColIndex: map[string]int32{"id": 0, "name": 1},
		}

		builder, err := NewCDCStatementBuilder("test_db", "users", tableDef, 1024*1024, false)
		require.NoError(t, err)

		sinker := NewMysqlSinker2(
			executor,
			1,
			"task-1",
			&DbTableInfo{SourceDbName: "src", SourceTblName: "users"},
			nil,
			builder,
			NewCdcActiveRoutine(),
		)

		// Create batch
		bat := batch.NewWithSize(2)
		bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
		bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())

		vector.AppendFixed(bat.Vecs[0], int32(2), false, mp)
		vector.AppendBytes(bat.Vecs[1], []byte("Bob"), false, mp)
		bat.SetRowCount(1)

		fromTs := types.BuildTS(100, 0)
		toTs := types.BuildTS(200, 0)
		cmd := NewInsertBatchCommand(bat, fromTs, toTs)

		// Expect SQL execution to fail
		mock.ExpectExec("fakeSql").WillReturnError(sqlmock.ErrCancelled)

		ctx := context.Background()
		err = sinker.handleInsertBatch(ctx, cmd)

		assert.Error(t, err)
	})
}

// Skipping complex async workflow tests for now
// Will be tested through integration tests with reader
