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

package issues

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestIssue29876CleanupLifecycle(t *testing.T) {
	t.Run("independent of cancelled workload and old short budgets", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			workload, cancel := context.WithCancel(t.Context())
			cancel()
			_, err = execIssue29876SQL(workload, db, "cancelled workload")
			require.ErrorIs(t, err, context.Canceled)

			started := time.Now()
			for _, resource := range []struct {
				statement string
				delay     time.Duration
			}{
				{"drop database db", 15 * time.Second},
				{"drop account acct", 25 * time.Second},
			} {
				mock.ExpectExec(resource.statement).WillDelayFor(resource.delay).WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery("verify absent").WithArgs(resource.statement).
					WillReturnRows(sqlmock.NewRows([]string{"remaining"}).AddRow(0))
				require.NoError(t, cleanupIssue29876Resource(db, resource.statement, "verify absent", resource.statement))
			}
			require.Equal(t, 40*time.Second, time.Since(started))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	})

	t.Run("timeout remains failure and next cleanup runs", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			defer db.Close()
			mock.ExpectExec("drop slow").WillDelayFor(issue29876SQLOperationTimeout + time.Second).
				WillReturnResult(sqlmock.NewResult(0, 1))
			started := time.Now()
			err = cleanupIssue29876Resource(db, "drop slow", "verify absent")
			require.ErrorIs(t, err, sqlmock.ErrCancelled)
			require.ErrorContains(t, err, "drop slow")
			require.Equal(t, issue29876SQLOperationTimeout, time.Since(started))

			mock.ExpectExec("drop next").WillDelayFor(25 * time.Second).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery("verify absent").WillReturnRows(sqlmock.NewRows([]string{"remaining"}).AddRow(0))
			require.NoError(t, cleanupIssue29876Resource(db, "drop next", "verify absent"))
			require.NoError(t, mock.ExpectationsWereMet())
		})
	})

	t.Run("verification cannot hide a remaining resource or read failure", func(t *testing.T) {
		for _, queryFailure := range []bool{false, true} {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			func() {
				defer db.Close()
				mock.ExpectExec("drop resource").WillReturnResult(sqlmock.NewResult(0, 1))
				query := mock.ExpectQuery("verify absent")
				failure := errors.New("catalog read failed")
				if queryFailure {
					query.WillReturnError(failure)
				} else {
					query.WillReturnRows(sqlmock.NewRows([]string{"remaining"}).AddRow(1))
				}
				err = cleanupIssue29876Resource(db, "drop resource", "verify absent")
				if queryFailure {
					require.ErrorIs(t, err, failure)
				} else {
					require.ErrorContains(t, err, "left 1 catalog records")
				}
				require.NoError(t, mock.ExpectationsWereMet())
			}()
		}
	})
}
