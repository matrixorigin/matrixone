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

func TestPrimaryKeyUpsertIdentity(t *testing.T) {
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
		result, err := conn.ExecContext(ctx,
			"INSERT INTO pk_only VALUES (1, 'ignored') ON DUPLICATE KEY UPDATE id=id")
		require.NoError(t, err)
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Zero(t, affected)
		assertPKOnlyRow(t, ctx, conn, 1, "seed")
		// Even with no mutable columns to compare, a guard must execute before
		// the no-op decision and a following self assignment cannot erase it.
		_, err = conn.ExecContext(ctx,
			"INSERT INTO pk_only VALUES (1,'ignored') ON DUPLICATE KEY UPDATE id=2, id=id")
		require.ErrorContains(t, err, "unsupported DML: update primary key on duplicate")
		assertPKOnlyRow(t, ctx, conn, 1, "seed")

		// Both explicit identity spellings suppress implicit ON UPDATE on a PK.
		exec("CREATE TABLE timestamp_pk (id TIMESTAMP PRIMARY KEY ON UPDATE CURRENT_TIMESTAMP, payload INT)")
		exec("INSERT INTO timestamp_pk VALUES ('2000-01-01 00:00:00', 1)")
		for _, assignment := range []string{"id=VALUES(id)", "id=(timestamp_pk.ID)"} {
			exec("INSERT INTO timestamp_pk VALUES ('2000-01-01 00:00:00', 2) ON DUPLICATE KEY UPDATE " + assignment + ", payload=payload+1")
		}
		var keyUnchanged bool
		var timestampPayload int
		require.NoError(t, conn.QueryRowContext(ctx,
			"SELECT id='2000-01-01 00:00:00', payload FROM timestamp_pk").Scan(&keyUnchanged, &timestampPayload))
		require.True(t, keyUnchanged)
		require.Equal(t, 3, timestampPayload)

		// Duplicate rows in the input must see the previous update's row image.
		exec("CREATE TABLE composite_pk (a INT, b INT, v INT, PRIMARY KEY(a,b))")
		exec("INSERT INTO composite_pk VALUES (1,10,100), (2,20,200)")
		exec("INSERT INTO composite_pk VALUES (2,20,1), (3,30,300), (2,20,5) " +
			"ON DUPLICATE KEY UPDATE a=a, b=b, v=v+VALUES(v)")
		rows, err := conn.QueryContext(ctx, "SELECT a,b,v FROM composite_pk ORDER BY a,b")
		require.NoError(t, err)
		defer rows.Close()
		var compositeRows [][3]int
		for rows.Next() {
			var row [3]int
			require.NoError(t, rows.Scan(&row[0], &row[1], &row[2]))
			compositeRows = append(compositeRows, row)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, [][3]int{{1, 10, 100}, {2, 20, 206}, {3, 30, 300}}, compositeRows)

		// MySQL parses a negative numeric literal as a unary expression around
		// the number. It is still a direct key value and should pass the runtime
		// identity guard when the duplicate row already has that key.
		exec("CREATE TABLE negative_pk (id INT PRIMARY KEY, payload VARCHAR(20))")
		exec("INSERT INTO negative_pk VALUES (-1, 'old')")
		exec("INSERT INTO negative_pk VALUES (-1, 'new') " +
			"ON DUPLICATE KEY UPDATE id = -1, payload = VALUES(payload)")
		var negativeID int
		var negativePayload string
		require.NoError(t, conn.QueryRowContext(ctx,
			"SELECT id, payload FROM negative_pk WHERE id = -1").Scan(&negativeID, &negativePayload))
		require.Equal(t, -1, negativeID)
		require.Equal(t, "new", negativePayload)

		pkStmt, err := conn.PrepareContext(ctx,
			"INSERT INTO pk_only(id, payload) VALUES (?, ?) ON DUPLICATE KEY UPDATE payload=VALUES(payload), id=?, id=id")
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
		selfStmt, err := conn.PrepareContext(ctx,
			"INSERT INTO secondary_unique VALUES (?,?,?) ON DUPLICATE KEY UPDATE id=id, payload=VALUES(payload)")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, selfStmt.Close()) })
		for _, id := range []int{3, 4} {
			_, err = selfStmt.ExecContext(ctx, id, 10, "self-target")
			require.NoError(t, err)
		}
		assertSecondaryUniqueRows(t, ctx, conn, "self-target", "seed-two")
		secondaryStmt, err := conn.PrepareContext(ctx,
			"INSERT INTO secondary_unique(id, natural_key, payload) VALUES (?, ?, ?) "+
				"ON DUPLICATE KEY UPDATE id=id, payload=VALUES(payload), id=?")
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
