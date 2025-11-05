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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutor_BeginTx(t *testing.T) {
	t.Run("SuccessfulBegin", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		mock.ExpectBegin()

		ctx := context.Background()
		err = executor.BeginTx(ctx)

		assert.NoError(t, err)
		assert.NotNil(t, executor.tx, "Transaction should be active")
		assert.True(t, executor.HasActiveTx())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("BeginTxFails", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		expectedErr := sqlmock.ErrCancelled
		mock.ExpectBegin().WillReturnError(expectedErr)

		ctx := context.Background()
		err = executor.BeginTx(ctx)

		assert.Error(t, err)
		assert.Nil(t, executor.tx, "Transaction should not be active on failure")
		assert.False(t, executor.HasActiveTx())
	})

	t.Run("BeginTxWhenAlreadyActive", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start first transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Try to start second transaction
		err = executor.BeginTx(ctx)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already active")
		assert.NotNil(t, executor.tx, "First transaction should still be active")
	})
}

func TestExecutor_CommitTx(t *testing.T) {
	t.Run("SuccessfulCommit", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Commit transaction
		mock.ExpectCommit()
		err = executor.CommitTx(ctx)

		assert.NoError(t, err)
		assert.Nil(t, executor.tx, "Transaction should be nil after commit")
		assert.False(t, executor.HasActiveTx())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("CommitTxFails", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Commit fails
		expectedErr := sqlmock.ErrCancelled
		mock.ExpectCommit().WillReturnError(expectedErr)
		err = executor.CommitTx(ctx)

		assert.Error(t, err)
		assert.Nil(t, executor.tx, "Transaction should be nil even after failed commit")
		assert.False(t, executor.HasActiveTx())
	})

	t.Run("CommitTxWhenNoTransaction_Idempotent", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
			tx:   nil, // No active transaction
		}

		ctx := context.Background()
		err = executor.CommitTx(ctx)

		// Should not error - idempotent behavior
		assert.NoError(t, err)
		assert.Nil(t, executor.tx)
	})
}

func TestExecutor_RollbackTx(t *testing.T) {
	t.Run("SuccessfulRollback", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Rollback transaction
		mock.ExpectRollback()
		err = executor.RollbackTx(ctx)

		assert.NoError(t, err)
		assert.Nil(t, executor.tx, "Transaction should be nil after rollback")
		assert.False(t, executor.HasActiveTx())
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("RollbackTxFails", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Rollback fails
		expectedErr := sqlmock.ErrCancelled
		mock.ExpectRollback().WillReturnError(expectedErr)
		err = executor.RollbackTx(ctx)

		assert.Error(t, err)
		assert.Nil(t, executor.tx, "Transaction should be nil even after failed rollback")
		assert.False(t, executor.HasActiveTx())
	})

	t.Run("RollbackTxWhenNoTransaction_Idempotent", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
			tx:   nil, // No active transaction
		}

		ctx := context.Background()
		err = executor.RollbackTx(ctx)

		// Should not error - idempotent behavior
		assert.NoError(t, err)
		assert.Nil(t, executor.tx)
	})
}

func TestExecutor_ExecSQL(t *testing.T) {
	t.Run("ExecWithinTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Execute SQL within transaction
		sqlBuf := []byte("     INSERT INTO test VALUES (1)")
		mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(1, 1))

		ar := &ActiveRoutine{
			Pause:  make(chan struct{}),
			Cancel: make(chan struct{}),
		}
		err = executor.ExecSQL(ctx, ar, sqlBuf, false)

		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ExecWithoutTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
			tx:   nil, // No transaction
		}

		// Execute SQL without transaction
		sqlBuf := []byte("     CREATE DATABASE test")
		mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(0, 0))

		ctx := context.Background()
		ar := &ActiveRoutine{
			Pause:  make(chan struct{}),
			Cancel: make(chan struct{}),
		}
		err = executor.ExecSQL(ctx, ar, sqlBuf, false)

		assert.NoError(t, err)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("ExecSQLTooShort", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// SQL buffer too short (less than 5 bytes)
		sqlBuf := []byte("ABC")

		ctx := context.Background()
		ar := &ActiveRoutine{
			Pause:  make(chan struct{}),
			Cancel: make(chan struct{}),
		}
		err = executor.ExecSQL(ctx, ar, sqlBuf, false)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too short")
	})
}

func TestExecutor_Close(t *testing.T) {
	t.Run("CloseWithActiveTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)

		// Close should rollback active transaction
		mock.ExpectRollback()
		mock.ExpectClose()

		err = executor.Close()

		assert.NoError(t, err)
		assert.Nil(t, executor.tx)
		assert.Nil(t, executor.conn)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("CloseWithoutTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)

		executor := &Executor{
			conn: db,
			tx:   nil,
		}

		mock.ExpectClose()

		err = executor.Close()

		assert.NoError(t, err)
		assert.Nil(t, executor.conn)
	})

	t.Run("CloseWhenAlreadyClosed_Idempotent", func(t *testing.T) {
		executor := &Executor{
			conn: nil,
			tx:   nil,
		}

		err := executor.Close()

		// Should not error - idempotent
		assert.NoError(t, err)
	})
}

func TestExecutor_TransactionLifecycle(t *testing.T) {
	t.Run("CompleteTransactionLifecycle", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		ctx := context.Background()
		ar := &ActiveRoutine{
			Pause:  make(chan struct{}),
			Cancel: make(chan struct{}),
		}

		// 1. No transaction initially
		assert.False(t, executor.HasActiveTx())

		// 2. Begin transaction
		mock.ExpectBegin()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)
		assert.True(t, executor.HasActiveTx())

		// 3. Execute SQL
		sqlBuf := []byte("     INSERT INTO test VALUES (1)")
		mock.ExpectExec("fakeSql").WillReturnResult(sqlmock.NewResult(1, 1))
		err = executor.ExecSQL(ctx, ar, sqlBuf, false)
		require.NoError(t, err)
		assert.True(t, executor.HasActiveTx(), "Transaction should still be active")

		// 4. Commit transaction
		mock.ExpectCommit()
		err = executor.CommitTx(ctx)
		require.NoError(t, err)
		assert.False(t, executor.HasActiveTx())

		// 5. Can start new transaction after commit
		mock.ExpectBegin()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)
		assert.True(t, executor.HasActiveTx())

		// 6. Rollback new transaction
		mock.ExpectRollback()
		err = executor.RollbackTx(ctx)
		require.NoError(t, err)
		assert.False(t, executor.HasActiveTx())

		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestExecutor_IdempotentOperations(t *testing.T) {
	t.Run("MultipleCommitsWithoutTransaction", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
			tx:   nil,
		}

		ctx := context.Background()

		// Multiple commits should all succeed (idempotent)
		err = executor.CommitTx(ctx)
		assert.NoError(t, err)

		err = executor.CommitTx(ctx)
		assert.NoError(t, err)

		err = executor.CommitTx(ctx)
		assert.NoError(t, err)
	})

	t.Run("MultipleRollbacksWithoutTransaction", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
			tx:   nil,
		}

		ctx := context.Background()

		// Multiple rollbacks should all succeed (idempotent)
		err = executor.RollbackTx(ctx)
		assert.NoError(t, err)

		err = executor.RollbackTx(ctx)
		assert.NoError(t, err)

		err = executor.RollbackTx(ctx)
		assert.NoError(t, err)
	})
}

func TestExecutor_TransactionCleanupOnError(t *testing.T) {
	t.Run("CommitFailure_ClearsTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)
		assert.NotNil(t, executor.tx)

		// Commit fails
		mock.ExpectCommit().WillReturnError(sqlmock.ErrCancelled)
		err = executor.CommitTx(ctx)

		assert.Error(t, err)
		// Critical: Transaction should be cleared even on failure
		assert.Nil(t, executor.tx, "Transaction must be cleared on commit failure")
		assert.False(t, executor.HasActiveTx())

		// Should be able to start new transaction after failed commit
		mock.ExpectBegin()
		err = executor.BeginTx(ctx)
		assert.NoError(t, err)
	})

	t.Run("RollbackFailure_ClearsTransaction", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		executor := &Executor{
			conn: db,
		}

		// Start transaction
		mock.ExpectBegin()
		ctx := context.Background()
		err = executor.BeginTx(ctx)
		require.NoError(t, err)
		assert.NotNil(t, executor.tx)

		// Rollback fails
		mock.ExpectRollback().WillReturnError(sqlmock.ErrCancelled)
		err = executor.RollbackTx(ctx)

		assert.Error(t, err)
		// Critical: Transaction should be cleared even on failure
		assert.Nil(t, executor.tx, "Transaction must be cleared on rollback failure")
		assert.False(t, executor.HasActiveTx())

		// Should be able to start new transaction after failed rollback
		mock.ExpectBegin()
		err = executor.BeginTx(ctx)
		assert.NoError(t, err)
	})
}
