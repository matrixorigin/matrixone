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

package isolated

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func registerAndPingIssue28012And28013DB(ctx context.Context, db *sql.DB, dbs *[]*sql.DB) error {
	*dbs = append(*dbs, db)
	return db.PingContext(ctx)
}

func TestRegisterAndPingIssue28012And28013DBRegistersBeforePingFailure(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	require.NoError(t, err)
	pingErr := errors.New("ping failed")
	mock.ExpectPing().WillReturnError(pingErr)
	mock.ExpectClose()
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})

	var dbs []*sql.DB
	err = registerAndPingIssue28012And28013DB(context.Background(), db, &dbs)

	require.ErrorIs(t, err, pingErr)
	require.Equal(t, []*sql.DB{db}, dbs)
}

func TestRegisterAndPingIssue28012And28013DBHonorsCanceledContext(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	mock.ExpectClose()
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, mock.ExpectationsWereMet())
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var dbs []*sql.DB
	err = registerAndPingIssue28012And28013DB(ctx, db, &dbs)

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, []*sql.DB{db}, dbs)
}
