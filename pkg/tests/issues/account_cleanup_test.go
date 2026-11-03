// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issues

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
	"github.com/stretchr/testify/require"
)

// Match the finite SQL-operation allowance used by the existing issue 29876
// integration test. Destructive cleanup can execute dozens of catalog DDLs;
// unrelated resources and verification must not spend one shared short budget.
const issueCleanupOperationTimeout = 2 * time.Minute

const issueAccountAbsentSQL = "select count(*) from mo_catalog.mo_account where account_name = ?"

func cleanupIssueAccounts(db *sql.DB, accounts ...string) error {
	var cleanupErr error
	for _, account := range accounts {
		ctx, cancel := context.WithTimeout(context.Background(), issueCleanupOperationTimeout)
		_, err := db.ExecContext(ctx, "drop account if exists "+sqlquote.Ident(account))
		cancel()
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("drop account %s: %w", account, err))
			continue
		}
		ctx, cancel = context.WithTimeout(context.Background(), issueCleanupOperationTimeout)
		var remaining int
		err = db.QueryRowContext(ctx, issueAccountAbsentSQL, account).Scan(&remaining)
		cancel()
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("verify account %s cleanup: %w", account, err))
		} else if remaining != 0 {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("account %s cleanup left %d rows", account, remaining))
		}
	}
	return cleanupErr
}

func TestIssueAccountCleanupIndependentBudgets(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.NoError(t, err)
		defer db.Close()
		workload, cancel := context.WithCancel(t.Context())
		cancel()
		_, err = db.ExecContext(workload, "cancelled workload")
		require.ErrorIs(t, err, context.Canceled)
		for _, account := range []string{"first-account", "second-account"} {
			mock.ExpectExec("drop account if exists " + sqlquote.Ident(account)).
				WillDelayFor(35 * time.Second).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectQuery(issueAccountAbsentSQL).WithArgs(account).
				WillDelayFor(5 * time.Second).WillReturnRows(sqlmock.NewRows([]string{"remaining"}).AddRow(0))
		}
		started := time.Now()
		require.NoError(t, cleanupIssueAccounts(db, "first-account", "second-account"))
		require.Equal(t, 80*time.Second, time.Since(started), "must exceed the old shared 45-second cleanup budget")
		require.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestIssueAccountCleanupPreservesFailuresAndContinues(t *testing.T) {
	for _, failure := range []string{"timeout", "query", "remaining"} {
		t.Run(failure, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
				require.NoError(t, err)
				defer db.Close()
				readErr := errors.New("catalog read failed")
				drop := mock.ExpectExec("drop account if exists `first`")
				if failure == "timeout" {
					drop.WillDelayFor(issueCleanupOperationTimeout + time.Second).WillReturnResult(sqlmock.NewResult(0, 1))
				} else {
					drop.WillReturnResult(sqlmock.NewResult(0, 1))
					query := mock.ExpectQuery(issueAccountAbsentSQL).WithArgs("first")
					if failure == "query" {
						query.WillReturnError(readErr)
					} else {
						query.WillReturnRows(sqlmock.NewRows([]string{"remaining"}).AddRow(1))
					}
				}
				mock.ExpectExec("drop account if exists `next`").WillReturnResult(sqlmock.NewResult(0, 1))
				mock.ExpectQuery(issueAccountAbsentSQL).WithArgs("next").WillReturnRows(sqlmock.NewRows([]string{"remaining"}).AddRow(0))
				err = cleanupIssueAccounts(db, "first", "next")
				require.ErrorContains(t, err, "first")
				switch failure {
				case "timeout":
					require.ErrorIs(t, err, sqlmock.ErrCancelled)
				case "query":
					require.ErrorIs(t, err, readErr)
				case "remaining":
					require.ErrorContains(t, err, "left 1 rows")
				}
				require.NoError(t, mock.ExpectationsWereMet())
			})
		})
	}
}

func TestIssueAccountCleanupVerificationHasFreshBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
		require.NoError(t, err)
		defer db.Close()
		mock.ExpectExec("drop account if exists `account`").
			WillDelayFor(issueCleanupOperationTimeout - time.Second).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectQuery(issueAccountAbsentSQL).WithArgs("account").WillDelayFor(5 * time.Second).
			WillReturnRows(sqlmock.NewRows([]string{"remaining"}).AddRow(0))
		started := time.Now()
		require.NoError(t, cleanupIssueAccounts(db, "account"))
		require.Equal(t, issueCleanupOperationTimeout+4*time.Second, time.Since(started))
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
