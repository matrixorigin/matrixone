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

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandleBegin_Comprehensive tests handleBegin with all possible scenarios
func TestHandleBegin_Comprehensive(t *testing.T) {
	ctx := context.Background()

	t.Run("Success_FromIdle", func(t *testing.T) {
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

		// Verify initial state
		require.Equal(t, v2TxnStateIdle, sinker.GetTxnState())

		// Begin transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)

		// Verify success
		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Idempotent_AlreadyActive", func(t *testing.T) {
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

		// First BEGIN
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		require.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		// Second BEGIN (idempotent - should succeed without calling BeginTx again)
		// Note: Executor.BeginTx will return error "transaction already active"
		// but handleBegin treats this as idempotent and returns success
		err = sinker.handleBegin(ctx)

		// Should succeed (idempotent behavior)
		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ResetFromCommitted_ThenBegin", func(t *testing.T) {
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

		// Begin and commit first transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)

		mock.ExpectCommit()
		err = sinker.handleCommit(ctx)
		require.NoError(t, err)
		require.Equal(t, v2TxnStateIdle, sinker.GetTxnState())

		// Now BEGIN from COMMITTED state (should reset to IDLE and begin)
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)

		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ResetFromRolledBack_ThenBegin", func(t *testing.T) {
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

		// Begin and rollback first transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)

		mock.ExpectRollback()
		err = sinker.handleRollback(ctx)
		require.NoError(t, err)
		require.Equal(t, v2TxnStateIdle, sinker.GetTxnState())

		// Now BEGIN from ROLLED_BACK state (should reset to IDLE and begin)
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)

		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("BeginTxFailure", func(t *testing.T) {
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

		// BeginTx fails
		beginErr := moerr.NewInternalErrorNoCtx("connection lost")
		mock.ExpectBegin().WillReturnError(beginErr)
		err = sinker.handleBegin(ctx)

		// Should return error, state remains IDLE
		assert.Error(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestHandleCommit_Comprehensive tests handleCommit with all possible scenarios
func TestHandleCommit_Comprehensive(t *testing.T) {
	ctx := context.Background()

	t.Run("Success_FromActive", func(t *testing.T) {
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

		// Begin transaction first
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		require.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		// Commit transaction
		mock.ExpectCommit()
		err = sinker.handleCommit(ctx)

		// Verify success and state transition: ACTIVE -> COMMITTED -> IDLE
		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Idempotent_FromIdle", func(t *testing.T) {
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

		// Commit from IDLE state (idempotent)
		err = sinker.handleCommit(ctx)

		// Should succeed (idempotent), state remains IDLE
		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		// No database calls expected (idempotent)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Idempotent_FromCommitted", func(t *testing.T) {
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

		// Begin and commit first transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)

		mock.ExpectCommit()
		err = sinker.handleCommit(ctx)
		require.NoError(t, err)
		require.Equal(t, v2TxnStateIdle, sinker.GetTxnState())

		// Commit again from IDLE (idempotent)
		err = sinker.handleCommit(ctx)

		// Should succeed (idempotent), state remains IDLE
		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Idempotent_FromRolledBack", func(t *testing.T) {
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

		// Begin and rollback first transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)

		mock.ExpectRollback()
		err = sinker.handleRollback(ctx)
		require.NoError(t, err)
		require.Equal(t, v2TxnStateIdle, sinker.GetTxnState())

		// Commit from IDLE (idempotent)
		err = sinker.handleCommit(ctx)

		// Should succeed (idempotent), state remains IDLE
		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("CommitTxFailure", func(t *testing.T) {
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

		// Begin transaction first
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		require.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		// CommitTx fails
		commitErr := moerr.NewInternalErrorNoCtx("commit failed")
		mock.ExpectCommit().WillReturnError(commitErr)
		err = sinker.handleCommit(ctx)

		// Should return error, state transitions: ACTIVE -> ROLLED_BACK
		// Note: On commit failure, state is set to ROLLED_BACK but NOT to IDLE
		// This is different from successful commit which goes ACTIVE -> COMMITTED -> IDLE
		assert.Error(t, err)
		assert.Equal(t, v2TxnStateRolledBack, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("StateTransition_ActiveToCommittedToIdle", func(t *testing.T) {
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

		// Begin
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		// Commit - verify intermediate COMMITTED state (if we could check it)
		// Note: The state transitions ACTIVE -> COMMITTED -> IDLE happen atomically
		mock.ExpectCommit()
		err = sinker.handleCommit(ctx)
		require.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestHandleRollback_Comprehensive tests handleRollback with all possible scenarios
func TestHandleRollback_Comprehensive(t *testing.T) {
	ctx := context.Background()

	t.Run("Success_FromActive", func(t *testing.T) {
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

		// Begin transaction first
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		require.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		// Rollback transaction
		mock.ExpectRollback()
		err = sinker.handleRollback(ctx)

		// Verify success and state transition: ACTIVE -> ROLLED_BACK -> IDLE
		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Idempotent_FromIdle", func(t *testing.T) {
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

		// Rollback from IDLE state (idempotent)
		err = sinker.handleRollback(ctx)

		// Should succeed (idempotent), state remains IDLE
		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		// No database calls expected (idempotent)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Idempotent_FromCommitted", func(t *testing.T) {
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

		// Begin and commit first transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)

		mock.ExpectCommit()
		err = sinker.handleCommit(ctx)
		require.NoError(t, err)
		require.Equal(t, v2TxnStateIdle, sinker.GetTxnState())

		// Rollback from IDLE (idempotent)
		err = sinker.handleRollback(ctx)

		// Should succeed (idempotent), state remains IDLE
		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("Idempotent_FromRolledBack", func(t *testing.T) {
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

		// Begin and rollback first transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)

		mock.ExpectRollback()
		err = sinker.handleRollback(ctx)
		require.NoError(t, err)
		require.Equal(t, v2TxnStateIdle, sinker.GetTxnState())

		// Rollback again from IDLE (idempotent)
		err = sinker.handleRollback(ctx)

		// Should succeed (idempotent), state remains IDLE
		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("RollbackTxFailure", func(t *testing.T) {
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

		// Begin transaction first
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		require.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		// RollbackTx fails
		rollbackErr := moerr.NewInternalErrorNoCtx("rollback failed")
		mock.ExpectRollback().WillReturnError(rollbackErr)
		err = sinker.handleRollback(ctx)

		// Should return error, state transitions: ACTIVE -> ROLLED_BACK -> IDLE
		assert.Error(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("StateTransition_ActiveToRolledBackToIdle", func(t *testing.T) {
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

		// Begin
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		// Rollback - verify intermediate ROLLED_BACK state (if we could check it)
		// Note: The state transitions ACTIVE -> ROLLED_BACK -> IDLE happen atomically
		mock.ExpectRollback()
		err = sinker.handleRollback(ctx)
		require.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// TestTransactionLifecycle_RealWorldScenarios tests realistic transaction lifecycle patterns
func TestTransactionLifecycle_RealWorldScenarios(t *testing.T) {
	ctx := context.Background()

	t.Run("NormalTransactionFlow", func(t *testing.T) {
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

		// Normal flow: BEGIN -> COMMIT
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		mock.ExpectCommit()
		err = sinker.handleCommit(ctx)
		require.NoError(t, err)
		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ErrorRecoveryFlow", func(t *testing.T) {
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

		// Error recovery flow: BEGIN -> COMMIT fails -> ROLLBACK -> BEGIN (retry)
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		// Commit fails
		mock.ExpectCommit().WillReturnError(moerr.NewInternalErrorNoCtx("commit failed"))
		err = sinker.handleCommit(ctx)
		assert.Error(t, err)
		// On commit failure, state is set to ROLLED_BACK (not IDLE)
		assert.Equal(t, v2TxnStateRolledBack, sinker.GetTxnState())

		// Rollback from ROLLED_BACK state (idempotent - returns immediately without DB call)
		err = sinker.handleRollback(ctx)
		assert.NoError(t, err)
		// Rollback from non-ACTIVE state is idempotent, state remains ROLLED_BACK
		assert.Equal(t, v2TxnStateRolledBack, sinker.GetTxnState())

		// Retry: Begin again (handleBegin will reset ROLLED_BACK to IDLE, then begin)
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		assert.NoError(t, err)
		assert.Equal(t, v2TxnStateActive, sinker.GetTxnState())

		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("MultipleTransactions", func(t *testing.T) {
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

		// First transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		mock.ExpectCommit()
		err = sinker.handleCommit(ctx)
		require.NoError(t, err)

		// Second transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		mock.ExpectRollback()
		err = sinker.handleRollback(ctx)
		require.NoError(t, err)

		// Third transaction
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)
		mock.ExpectCommit()
		err = sinker.handleCommit(ctx)
		require.NoError(t, err)

		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("IdempotentOperations", func(t *testing.T) {
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

		// Multiple idempotent commits (no active transaction)
		err = sinker.handleCommit(ctx)
		assert.NoError(t, err)
		err = sinker.handleCommit(ctx)
		assert.NoError(t, err)
		err = sinker.handleCommit(ctx)
		assert.NoError(t, err)

		// Multiple idempotent rollbacks (no active transaction)
		err = sinker.handleRollback(ctx)
		assert.NoError(t, err)
		err = sinker.handleRollback(ctx)
		assert.NoError(t, err)

		// Begin
		mock.ExpectBegin()
		err = sinker.handleBegin(ctx)
		require.NoError(t, err)

		// Multiple idempotent begins (already active)
		err = sinker.handleBegin(ctx)
		assert.NoError(t, err)
		err = sinker.handleBegin(ctx)
		assert.NoError(t, err)

		// Commit
		mock.ExpectCommit()
		err = sinker.handleCommit(ctx)
		require.NoError(t, err)

		// Multiple idempotent commits again
		err = sinker.handleCommit(ctx)
		assert.NoError(t, err)
		err = sinker.handleCommit(ctx)
		assert.NoError(t, err)

		assert.Equal(t, v2TxnStateIdle, sinker.GetTxnState())
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

// BenchmarkHandleBegin benchmarks handleBegin performance
func BenchmarkHandleBegin(b *testing.B) {
	ctx := context.Background()
	db, mock, _ := sqlmock.New()
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
	builder, _ := NewCDCStatementBuilder("test_db", "test", tableDef, 1024*1024, false)
	sinker := NewMysqlSinker2(
		executor,
		1,
		"task-1",
		&DbTableInfo{SourceDbName: "src", SourceTblName: "test"},
		nil,
		builder,
		NewCdcActiveRoutine(),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mock.ExpectBegin()
		mock.ExpectCommit()
		_ = sinker.handleBegin(ctx)
		_ = sinker.handleCommit(ctx)
	}
}

// BenchmarkHandleCommit benchmarks handleCommit performance
func BenchmarkHandleCommit(b *testing.B) {
	ctx := context.Background()
	db, mock, _ := sqlmock.New()
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
	builder, _ := NewCDCStatementBuilder("test_db", "test", tableDef, 1024*1024, false)
	sinker := NewMysqlSinker2(
		executor,
		1,
		"task-1",
		&DbTableInfo{SourceDbName: "src", SourceTblName: "test"},
		nil,
		builder,
		NewCdcActiveRoutine(),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mock.ExpectBegin()
		mock.ExpectCommit()
		_ = sinker.handleBegin(ctx)
		_ = sinker.handleCommit(ctx)
	}
}

// BenchmarkHandleRollback benchmarks handleRollback performance
func BenchmarkHandleRollback(b *testing.B) {
	ctx := context.Background()
	db, mock, _ := sqlmock.New()
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
	builder, _ := NewCDCStatementBuilder("test_db", "test", tableDef, 1024*1024, false)
	sinker := NewMysqlSinker2(
		executor,
		1,
		"task-1",
		&DbTableInfo{SourceDbName: "src", SourceTblName: "test"},
		nil,
		builder,
		NewCdcActiveRoutine(),
	)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mock.ExpectBegin()
		mock.ExpectRollback()
		_ = sinker.handleBegin(ctx)
		_ = sinker.handleRollback(ctx)
	}
}
