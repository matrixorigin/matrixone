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

package taskservice

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestForeachQueriedRow(t *testing.T) {
	for _, failCallback := range []bool{false, true} {
		t.Run(map[bool]string{false: "count selected rows", true: "callback error closes rows"}[failCallback], func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectQuery("SELECT n").WillReturnRows(sqlmock.NewRows([]string{"n"}).AddRow(1).AddRow(2).AddRow(3)).RowsWillBeClosed()
			injected := errors.New("callback failed")
			count, err := ForeachQueriedRow(context.Background(), db, "SELECT n", func(_ context.Context, rows *sql.Rows) (bool, error) {
				var n int
				if err := rows.Scan(&n); err != nil {
					return false, err
				}
				if failCallback && n == 2 {
					return false, injected
				}
				return n%2 == 1, nil
			})
			if failCallback {
				require.ErrorIs(t, err, injected)
				require.EqualValues(t, 1, count)
			} else {
				require.NoError(t, err)
				require.EqualValues(t, 2, count)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()
	injected := errors.New("query failed")
	mock.ExpectQuery("SELECT n").WillReturnError(injected)
	count, err := ForeachQueriedRow(context.Background(), db, "SELECT n", func(context.Context, *sql.Rows) (bool, error) {
		t.Fatal("callback called after query error")
		return false, nil
	})
	require.ErrorIs(t, err, injected)
	require.Zero(t, count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestForeachQueriedRowReportsIterationError(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()
	injected := errors.New("row stream failed")
	rows := sqlmock.NewRows([]string{"n"}).AddRow(1).AddRow(2).RowError(1, injected)
	mock.ExpectQuery("SELECT n").WillReturnRows(rows).RowsWillBeClosed()
	count, err := ForeachQueriedRow(context.Background(), db, "SELECT n", func(_ context.Context, rows *sql.Rows) (bool, error) {
		var n int
		return true, rows.Scan(&n)
	})
	require.ErrorIs(t, err, injected)
	require.EqualValues(t, 1, count)
	require.NoError(t, mock.ExpectationsWereMet())
}
