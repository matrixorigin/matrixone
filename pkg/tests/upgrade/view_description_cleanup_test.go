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

package upgrade

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestViewDescriptionCleanupAfterContextCancellation(t *testing.T) {
	t.Parallel()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	require.NoError(t, err)
	defer db.Close()

	const query = "drop database view_description_cleanup"
	mock.ExpectExec(query).WillReturnResult(sqlmock.NewResult(0, 0))
	if !t.Run("canceled test context", func(t *testing.T) {
		t.Cleanup(func() {
			// testing cancels the subtest context before running its cleanups.
			// Using that context for SQL would fail without reaching the driver.
			require.ErrorIs(t, t.Context().Err(), context.Canceled)
			cleanupViewDescriptionSQL(t, db, query)
		})
	}) {
		return
	}
	require.NoError(t, mock.ExpectationsWereMet())
}
