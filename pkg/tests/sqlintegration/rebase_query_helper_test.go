// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlintegration

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestReadRebaseQueryClosesAndReportsErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	rows := sqlmock.NewRows([]string{"value"}).AddRow(1)
	mock.ExpectQuery("one").WillReturnRows(rows).RowsWillBeClosed()
	count, queryErr, closeErr := readRebaseQuery(context.Background(), db, "one")
	require.Equal(t, 1, count)
	require.NoError(t, queryErr)
	require.NoError(t, closeErr)

	want := errors.New("injected row error")
	rows = sqlmock.NewRows([]string{"value"}).AddRow(1).RowError(0, want)
	mock.ExpectQuery("bad-row").WillReturnRows(rows).RowsWillBeClosed()
	_, queryErr, closeErr = readRebaseQuery(context.Background(), db, "bad-row")
	require.ErrorIs(t, queryErr, want)
	require.NoError(t, closeErr)

	want = errors.New("injected query error")
	mock.ExpectQuery("bad-query").WillReturnError(want)
	_, queryErr, closeErr = readRebaseQuery(context.Background(), db, "bad-query")
	require.ErrorIs(t, queryErr, want)
	require.NoError(t, closeErr)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReadRebaseSummaryRequiresOneRow(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	for _, tc := range []struct {
		name string
		rows int
		ok   bool
	}{{"one", 1, true}, {"empty", 0, false}, {"many", 2, false}} {
		t.Run(tc.name, func(t *testing.T) {
			rows := sqlmock.NewRows([]string{"value"})
			for i := 0; i < tc.rows; i++ {
				rows.AddRow(i)
			}
			mock.ExpectQuery(tc.name).WillReturnRows(rows).RowsWillBeClosed()
			err := readRebaseSummary(context.Background(), db, tc.name)
			if tc.ok {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "invalid provisional summary")
			}
		})
	}
	require.NoError(t, mock.ExpectationsWereMet())
}
