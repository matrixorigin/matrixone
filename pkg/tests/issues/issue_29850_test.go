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

package issues

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue29850PreparedPrimaryKeyUpsertIdentityGuard(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, conn.Close()) })

		database := strings.ToLower(testutils.GetDatabaseName(t))
		exec := func(statement string, args ...any) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement, args...)
			require.NoError(t, err, statement)
		}
		exec("CREATE DATABASE `" + database + "`")
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cleanupCancel()
			_, err := conn.ExecContext(cleanupCtx, "DROP DATABASE IF EXISTS `"+database+"`")
			require.NoError(t, err)
		})
		exec("USE `" + database + "`")

		exec("CREATE TABLE pk_only (id INT PRIMARY KEY, payload VARCHAR(40))")
		exec("INSERT INTO pk_only VALUES (1, 'seed')")
		pkStmt, err := conn.PrepareContext(ctx,
			"INSERT INTO pk_only(id, payload) VALUES (?, ?) ON DUPLICATE KEY UPDATE payload=VALUES(payload), id=?")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, pkStmt.Close()) })

		_, err = pkStmt.ExecContext(ctx, 1, "equal", 1)
		require.NoError(t, err, "an equal prepared primary-key assignment should succeed")
		_, err = pkStmt.ExecContext(ctx, 1, "must-rollback", 2)
		require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
		assertPKOnlyRow(t, ctx, conn, 1, "equal")

		_, err = pkStmt.ExecContext(ctx, 1, "null-must-rollback", nil)
		require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
		assertPKOnlyRow(t, ctx, conn, 1, "equal")

		// Reuse the same binary-protocol statement after both failures.
		_, err = pkStmt.ExecContext(ctx, 1, "reused", 1)
		require.NoError(t, err)
		assertPKOnlyRow(t, ctx, conn, 1, "reused")

		exec("CREATE TABLE secondary_unique (id INT PRIMARY KEY, natural_key INT UNIQUE, payload VARCHAR(40))")
		exec("INSERT INTO secondary_unique VALUES (1, 10, 'seed-one'), (2, 20, 'seed-two')")
		secondaryStmt, err := conn.PrepareContext(ctx,
			"INSERT INTO secondary_unique(id, natural_key, payload) VALUES (?, ?, ?) "+
				"ON DUPLICATE KEY UPDATE payload=VALUES(payload), id=?")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, secondaryStmt.Close()) })

		// A UNIQUE-only conflict must guard the PK assignment against the row
		// selected by the arbiter, rather than against the incoming PK value.
		_, err = secondaryStmt.ExecContext(ctx, 3, 10, "unique-target", 1)
		require.NoError(t, err)
		_, err = secondaryStmt.ExecContext(ctx, 3, 10, "must-rollback", 3)
		require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
		assertSecondaryUniqueRows(t, ctx, conn, "unique-target", "seed-two")

		// When PK and UNIQUE conflicts select different rows, the resolved
		// PRIMARY-key target owns the update and its identity guard.
		_, err = secondaryStmt.ExecContext(ctx, 2, 10, "primary-target", 2)
		require.NoError(t, err)
		assertSecondaryUniqueRows(t, ctx, conn, "unique-target", "primary-target")
	})
}

func assertPKOnlyRow(t *testing.T, ctx context.Context, conn *sql.Conn, wantID int, wantPayload string) {
	t.Helper()
	var id int
	var payload string
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT id, payload FROM pk_only").Scan(&id, &payload))
	require.Equal(t, wantID, id)
	require.Equal(t, wantPayload, payload)
}

func assertSecondaryUniqueRows(t *testing.T, ctx context.Context, conn *sql.Conn, wantFirst, wantSecond string) {
	t.Helper()
	rows, err := conn.QueryContext(ctx, "SELECT id, payload FROM secondary_unique ORDER BY id")
	require.NoError(t, err)
	defer rows.Close()
	got := make([]string, 0, 2)
	for rows.Next() {
		var id int
		var payload string
		require.NoError(t, rows.Scan(&id, &payload))
		require.Equal(t, len(got)+1, id)
		got = append(got, payload)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{wantFirst, wantSecond}, got)
}
