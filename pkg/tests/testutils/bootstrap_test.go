// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testutils

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestWaitSystemBootstrapReturnsAfterAllTaskTablesExist(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("show tables from mo_task").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).
			AddRow("SYS_ASYNC_TASK").
			AddRow("sys_cron_task").
			AddRow("SYS_DAEMON_TASK").
			AddRow("sql_task").
			AddRow("SQL_TASK_RUN")).
		RowsWillBeClosed()

	require.NoError(t, WaitSystemBootstrap(context.Background(), db))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWaitSystemBootstrapRetriesAfterRowScanError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery("show tables from mo_task").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).AddRow(nil)).
		RowsWillBeClosed()
	mock.ExpectQuery("show tables from mo_task").
		WillReturnRows(sqlmock.NewRows([]string{"table_name"}).
			AddRow("sys_async_task").
			AddRow("sys_cron_task").
			AddRow("sys_daemon_task").
			AddRow("sql_task").
			AddRow("sql_task_run")).
		RowsWillBeClosed()

	require.NoError(t, WaitSystemBootstrap(context.Background(), db))
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWaitSystemBootstrapReturnsContextErrorWhenCanceled(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, WaitSystemBootstrap(ctx, db), context.Canceled)
}
