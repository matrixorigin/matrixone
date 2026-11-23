// Copyright 2026 Matrix Origin
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

package taskservice

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestSQLTaskMetadataTransaction(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("UPDATE test").WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectExec("COMMIT").WillReturnResult(sqlmock.NewResult(0, 0))
		attempted, err := runSQLTaskMetadataTransaction(context.Background(), db, func(ctx context.Context, c *sql.Conn) error {
			_, err := c.ExecContext(ctx, "UPDATE test")
			return err
		})
		require.NoError(t, err)
		require.True(t, attempted)
		require.Equal(t, 1, db.Stats().Idle)
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("work failure rolls back", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		problem := errors.New("work failed")
		mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("ROLLBACK").WillReturnResult(sqlmock.NewResult(0, 0))
		attempted, err := runSQLTaskMetadataTransaction(context.Background(), db, func(context.Context, *sql.Conn) error { return problem })
		require.ErrorIs(t, err, problem)
		require.False(t, attempted)
		require.Equal(t, 1, db.Stats().Idle)
		mock.ExpectClose()
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("commit deadline discards connection", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		mock.ExpectExec("BEGIN").WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec("COMMIT").WillDelayFor(time.Second).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectClose()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		attempted, err := runSQLTaskMetadataTransaction(ctx, db, func(context.Context, *sql.Conn) error { return nil })
		require.Error(t, err)
		require.True(t, attempted)
		require.Zero(t, db.Stats().OpenConnections)
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
