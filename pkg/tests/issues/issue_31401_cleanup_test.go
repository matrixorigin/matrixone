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

// The same DROP DATABASE already runs within this allowance during setup.
// Cleanup has its own budget per database and never renews an active operation.
const issue31401ScalarPhaseTimeout = time.Minute

func cleanupIssue31401Databases(ctx context.Context, db *sql.DB, databases ...string) error {
	var cleanupErr error
	for _, database := range databases {
		operation, cancel := context.WithTimeout(ctx, issue31401ScalarPhaseTimeout)
		_, err := db.ExecContext(operation, "drop database if exists "+sqlquote.Ident(database))
		cancel()
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("drop database %s: %w", database, err))
		}
	}
	return cleanupErr
}

func TestIssue31401DatabaseCleanup(t *testing.T) {
	for _, outcome := range []string{"slow success", "timeout", "unknown result", "cancelled caller"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
				require.NoError(t, err)
				defer db.Close()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				unknown := errors.New("unknown DDL outcome")
				if outcome == "cancelled caller" {
					cancel()
				} else {
					first := mock.ExpectExec("drop database if exists `source`")
					switch outcome {
					case "slow success":
						first.WillDelayFor(20 * time.Second).WillReturnResult(sqlmock.NewResult(0, 0))
					case "timeout":
						first.WillDelayFor(issue31401ScalarPhaseTimeout + time.Second).WillReturnResult(sqlmock.NewResult(0, 0))
					case "unknown result":
						first.WillReturnError(unknown)
					}
					mock.ExpectExec("drop database if exists `peer`").
						WillDelayFor(20 * time.Second).WillReturnResult(sqlmock.NewResult(0, 0))
				}
				started := time.Now()
				err = cleanupIssue31401Databases(ctx, db, "source", "peer")
				switch outcome {
				case "slow success":
					require.NoError(t, err)
					require.Equal(t, 40*time.Second, time.Since(started))
				case "timeout":
					require.ErrorIs(t, err, sqlmock.ErrCancelled)
					require.Equal(t, issue31401ScalarPhaseTimeout+20*time.Second, time.Since(started))
				case "unknown result":
					require.ErrorIs(t, err, unknown)
				case "cancelled caller":
					require.ErrorIs(t, err, context.Canceled)
				}
				require.NoError(t, mock.ExpectationsWereMet(), "independent cleanup must continue without retrying a failed DROP")
			})
		})
	}
}
