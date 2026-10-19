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

package arrowload

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestArrowLoadFrontendReadinessBudget(t *testing.T) {
	for _, delay := range []time.Duration{15 * time.Second, arrowLoadFrontendReadyTimeout + time.Second} {
		t.Run(delay.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
				require.NoError(t, err)
				defer db.Close()
				mock.ExpectPing().WillDelayFor(delay)
				started := time.Now()
				err = pingArrowLoadFrontend(t.Context(), db)
				if delay < arrowLoadFrontendReadyTimeout {
					require.NoError(t, err)
					require.Equal(t, delay, time.Since(started))
				} else {
					require.ErrorIs(t, err, sqlmock.ErrCancelled)
					require.Equal(t, arrowLoadFrontendReadyTimeout, time.Since(started))
				}
				require.NoError(t, mock.ExpectationsWereMet())
			})
		})
	}
	t.Run("caller cancellation is retained", func(t *testing.T) {
		db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
		require.NoError(t, err)
		defer db.Close()
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		require.ErrorIs(t, pingArrowLoadFrontend(ctx, db), context.Canceled)
		require.NoError(t, mock.ExpectationsWereMet())
	})
}
