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
	readErr := errors.New("read failed")
	callbackErr := errors.New("callback failed")
	for _, tc := range []struct {
		name        string
		values      []string
		rowError    int
		callbackErr bool
		wantCount   int64
		wantErr     error
	}{
		{name: "empty", rowError: -1},
		{name: "count accepted rows", values: []string{"accepted", "skipped"}, rowError: -1, wantCount: 1},
		{name: "first row read error", values: []string{"accepted"}, rowError: 0, wantErr: readErr},
		{name: "later row read error", values: []string{"accepted", "skipped"}, rowError: 1, wantCount: 1, wantErr: readErr},
		{name: "callback error", values: []string{"accepted", "skipped"}, rowError: -1, callbackErr: true, wantCount: 1, wantErr: callbackErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()
			rows := sqlmock.NewRows([]string{"value"})
			for _, value := range tc.values {
				rows.AddRow(value)
			}
			if tc.rowError >= 0 {
				rows.RowError(tc.rowError, readErr)
			}
			mock.ExpectQuery("SELECT value").WillReturnRows(rows).RowsWillBeClosed()
			count, err := ForeachQueriedRow(context.Background(), db, "SELECT value", func(_ context.Context, row *sql.Rows) (bool, error) {
				var value string
				if err := row.Scan(&value); err != nil {
					return false, err
				}
				if tc.callbackErr && value == "skipped" {
					return false, callbackErr
				}
				return value == "accepted", nil
			})
			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, tc.wantCount, count)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestForeachQueriedRowQueryError(t *testing.T) {
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
