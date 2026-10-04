// Copyright 2021 - 2026 Matrix Origin
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
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

// TestIssue26685PreparedMultiSet exercises both public prepared-statement
// entry points. Text PREPARE/EXECUTE reaches the same execution path through
// COM_QUERY, while interpolateParams=false forces the driver to use the binary
// COM_STMT_PREPARE/COM_STMT_EXECUTE protocol.
func TestIssue26685PreparedMultiSet(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		dsn := fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port)
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()
		mustExec(t, ctx, conn, fmt.Sprintf("create table `%s`.multi_set_values (v int)", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("insert into `%s`.multi_set_values values (1), (2)", dbName))

		t.Run("text protocol", func(t *testing.T) {
			mustExec(t, ctx, conn, "set @plain_a = 1, @plain_b = @plain_a + 1")
			var plainA, plainB string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select cast(@plain_a as char), cast(@plain_b as char)").Scan(&plainA, &plainB))
			require.Equal(t, "1", plainA)
			require.Equal(t, "2", plainB)

			mustExec(t, ctx, conn, "set @target_a = 99, @target_b = 'before'")
			mustExec(t, ctx, conn, "prepare issue26685_text from 'set @target_a = ? + 1, @target_b = concat(?, \"-text\")'")
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare issue26685_text") }()

			mustExec(t, ctx, conn, "set @input_a = 41, @input_b = 'first'")
			_, executeErr := conn.ExecContext(ctx, "execute issue26685_text using @input_a, @input_b")
			a, b := queryIssue26685Vars(t, ctx, conn)
			require.NoErrorf(t, executeErr, "failed execute left @target_a=%s, @target_b=%s", a, b)
			require.Equal(t, "42", a)
			require.Equal(t, "first-text", b)

			mustExec(t, ctx, conn, "set @input_a = 9, @input_b = null")
			mustExec(t, ctx, conn, "execute issue26685_text using @input_a, @input_b")
			a, b = queryIssue26685Vars(t, ctx, conn)
			require.Equal(t, "10", a)
			require.Equal(t, "NULL", b)

			mustExec(t, ctx, conn, "set @visibility_a = 5, @visibility_b = 0, @visibility_input = 41")
			mustExec(t, ctx, conn,
				"prepare issue26685_text_visibility from 'set @visibility_a = ?, @visibility_b = @visibility_a + 1'")
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare issue26685_text_visibility") }()
			mustExec(t, ctx, conn, "execute issue26685_text_visibility using @visibility_input")
			var visibilityA, visibilityB string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select cast(@visibility_a as char), cast(@visibility_b as char)").Scan(&visibilityA, &visibilityB))
			require.Equal(t, "41", visibilityA)
			require.Equal(t, "42", visibilityB)

			mustExec(t, ctx, conn, "set @self_ref = 5")
			mustExec(t, ctx, conn,
				"prepare issue26685_text_self from 'set @self_ref = ?, @self_ref = @self_ref + 1'")
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare issue26685_text_self") }()
			mustExec(t, ctx, conn, "execute issue26685_text_self using @visibility_input")
			var selfRef string
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(@self_ref as char)").Scan(&selfRef))
			require.Equal(t, "42", selfRef)

			repeatedFailureSQL := fmt.Sprintf(
				"prepare issue26685_text_self_error from 'set @self_ref = ?, @self_ref = @self_ref + 1, @target_b = (select v from `%s`.multi_set_values)'",
				dbName,
			)
			mustExec(t, ctx, conn, repeatedFailureSQL)
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare issue26685_text_self_error") }()
			mustExec(t, ctx, conn, "set @self_ref = 5, @target_b = 'unchanged'")
			_, err = conn.ExecContext(ctx, "execute issue26685_text_self_error using @visibility_input")
			require.ErrorContains(t, err, "Subquery returns more than 1 row")
			require.NoError(t, conn.QueryRowContext(ctx,
				"select cast(@self_ref as char), cast(@target_b as char)").Scan(&selfRef, &visibilityB))
			require.Equal(t, "5", selfRef)
			require.Equal(t, "unchanged", visibilityB)

			mustExec(t, ctx, conn, "set @target_a = 77, @target_b = 'stable'")
			_, err = conn.ExecContext(ctx, "execute issue26685_text using @input_a")
			require.ErrorContains(t, err, "Incorrect arguments to EXECUTE")
			a, b = queryIssue26685Vars(t, ctx, conn)
			require.Equal(t, "77", a)
			require.Equal(t, "stable", b)

			failingSQL := fmt.Sprintf(
				"prepare issue26685_text_error from 'set @target_a = ?, @target_b = (select v from `%s`.multi_set_values)'",
				dbName,
			)
			mustExec(t, ctx, conn, failingSQL)
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare issue26685_text_error") }()
			mustExec(t, ctx, conn, "set @target_a = 88, @target_b = 'unchanged'")
			_, err = conn.ExecContext(ctx, "execute issue26685_text_error using @input_a")
			require.ErrorContains(t, err, "Subquery returns more than 1 row")
			a, b = queryIssue26685Vars(t, ctx, conn)
			require.Equal(t, "88", a)
			require.Equal(t, "unchanged", b)

			reservedFailureSQL := fmt.Sprintf(
				"prepare issue26685_text_reserved_error from 'set @names = ?, @target_b = (select v from `%s`.multi_set_values)'",
				dbName,
			)
			mustExec(t, ctx, conn, reservedFailureSQL)
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare issue26685_text_reserved_error") }()
			mustExec(t, ctx, conn, "set @names = 'names-before', @character_set_client = 'client-before', @character_set_connection = 'connection-before', @character_set_results = 'results-before', @reserved_input = 'mutated'")
			_, err = conn.ExecContext(ctx, "execute issue26685_text_reserved_error using @reserved_input")
			require.ErrorContains(t, err, "Subquery returns more than 1 row")
			var names string
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(@names as char)").Scan(&names))
			require.Equal(t, "names-before", names)
			client, connection, results := queryIssue26685CharsetVars(t, ctx, conn)
			require.Equal(t, "client-before", client)
			require.Equal(t, "connection-before", connection)
			require.Equal(t, "results-before", results)

			mustExec(t, ctx, conn, "prepare issue26685_text_system from 'set @target_a = ?, transaction_isolation = ?'")
			defer func() { _, _ = conn.ExecContext(ctx, "deallocate prepare issue26685_text_system") }()
			mustExec(t, ctx, conn, "set @input_a = 7, @input_b = 'INVALID', @target_a = 88")
			beforeIsolation := queryIssue26685Isolation(t, ctx, conn)
			_, err = conn.ExecContext(ctx, "execute issue26685_text_system using @input_a, @input_b")
			require.Error(t, err)
			a, _ = queryIssue26685Vars(t, ctx, conn)
			require.Equal(t, "88", a)
			require.Equal(t, beforeIsolation, queryIssue26685Isolation(t, ctx, conn))
			require.ErrorContains(t, err, "prepared multi-assignment SET supports user variables only")
			var one int
			require.NoError(t, conn.QueryRowContext(ctx, "select 1").Scan(&one))
			require.Equal(t, 1, one)
		})

		t.Run("binary protocol", func(t *testing.T) {
			mustExec(t, ctx, conn, "set @target_a = 99, @target_b = 'before'")
			stmt, err := conn.PrepareContext(ctx, "set @target_a = ? + 1, @target_b = concat(?, '-binary')")
			require.NoError(t, err)
			defer stmt.Close()

			_, err = stmt.ExecContext(ctx, int64(41), "first")
			require.NoError(t, err)
			a, b := queryIssue26685Vars(t, ctx, conn)
			require.Equal(t, "42", a)
			require.Equal(t, "first-binary", b)

			_, err = stmt.ExecContext(ctx, int64(9), nil)
			require.NoError(t, err)
			a, b = queryIssue26685Vars(t, ctx, conn)
			require.Equal(t, "10", a)
			require.Equal(t, "NULL", b)

			mustExec(t, ctx, conn, "set @visibility_a = 5, @visibility_b = 0")
			visibilityStmt, err := conn.PrepareContext(ctx,
				"set @visibility_a = ?, @visibility_b = @visibility_a + 1")
			require.NoError(t, err)
			defer visibilityStmt.Close()
			_, err = visibilityStmt.ExecContext(ctx, int64(41))
			require.NoError(t, err)
			var visibilityA, visibilityB string
			require.NoError(t, conn.QueryRowContext(ctx,
				"select cast(@visibility_a as char), cast(@visibility_b as char)").Scan(&visibilityA, &visibilityB))
			require.Equal(t, "41", visibilityA)
			require.Equal(t, "42", visibilityB)

			mustExec(t, ctx, conn, "set @self_ref = 5")
			selfStmt, err := conn.PrepareContext(ctx, "set @self_ref = ?, @self_ref = @self_ref + 1")
			require.NoError(t, err)
			defer selfStmt.Close()
			_, err = selfStmt.ExecContext(ctx, int64(41))
			require.NoError(t, err)
			var selfRef string
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(@self_ref as char)").Scan(&selfRef))
			require.Equal(t, "42", selfRef)

			repeatedFailureSQL := fmt.Sprintf(
				"set @self_ref = ?, @self_ref = @self_ref + 1, @target_b = (select v from `%s`.multi_set_values)",
				dbName)
			repeatedFailureStmt, err := conn.PrepareContext(ctx, repeatedFailureSQL)
			require.NoError(t, err)
			defer repeatedFailureStmt.Close()
			mustExec(t, ctx, conn, "set @self_ref = 5, @target_b = 'unchanged'")
			_, err = repeatedFailureStmt.ExecContext(ctx, int64(41))
			require.ErrorContains(t, err, "Subquery returns more than 1 row")
			require.NoError(t, conn.QueryRowContext(ctx,
				"select cast(@self_ref as char), cast(@target_b as char)").Scan(&selfRef, &visibilityB))
			require.Equal(t, "5", selfRef)
			require.Equal(t, "unchanged", visibilityB)

			failingSQL := fmt.Sprintf(
				"set @target_a = ?, @target_b = (select v from `%s`.multi_set_values)", dbName)
			failingStmt, err := conn.PrepareContext(ctx, failingSQL)
			require.NoError(t, err)
			defer failingStmt.Close()
			mustExec(t, ctx, conn, "set @target_a = 88, @target_b = 'unchanged'")
			_, err = failingStmt.ExecContext(ctx, int64(7))
			require.ErrorContains(t, err, "Subquery returns more than 1 row")
			a, b = queryIssue26685Vars(t, ctx, conn)
			require.Equal(t, "88", a)
			require.Equal(t, "unchanged", b)

			reservedFailureSQL := fmt.Sprintf(
				"set @names = ?, @target_b = (select v from `%s`.multi_set_values)", dbName)
			reservedFailureStmt, err := conn.PrepareContext(ctx, reservedFailureSQL)
			require.NoError(t, err)
			defer reservedFailureStmt.Close()
			mustExec(t, ctx, conn, "set @names = 'binary-names-before', @character_set_client = 'binary-client-before', @character_set_connection = 'binary-connection-before', @character_set_results = 'binary-results-before'")
			_, err = reservedFailureStmt.ExecContext(ctx, "binary-mutated")
			require.ErrorContains(t, err, "Subquery returns more than 1 row")
			var names string
			require.NoError(t, conn.QueryRowContext(ctx, "select cast(@names as char)").Scan(&names))
			require.Equal(t, "binary-names-before", names)
			client, connection, results := queryIssue26685CharsetVars(t, ctx, conn)
			require.Equal(t, "binary-client-before", client)
			require.Equal(t, "binary-connection-before", connection)
			require.Equal(t, "binary-results-before", results)

			mixedStmt, err := conn.PrepareContext(ctx,
				"set @target_a = ?, transaction_isolation = ?")
			require.NoError(t, err)
			defer mixedStmt.Close()
			mustExec(t, ctx, conn, "set @target_a = 88")
			beforeIsolation := queryIssue26685Isolation(t, ctx, conn)
			singleSystemStmt, err := conn.PrepareContext(ctx, "set transaction_isolation = ?")
			require.NoError(t, err)
			defer singleSystemStmt.Close()
			_, err = singleSystemStmt.ExecContext(ctx, "READ-COMMITTED")
			require.NoError(t, err)
			require.Equal(t, "READ-COMMITTED", queryIssue26685Isolation(t, ctx, conn))
			_, err = singleSystemStmt.ExecContext(ctx, beforeIsolation)
			require.NoError(t, err)
			require.Equal(t, beforeIsolation, queryIssue26685Isolation(t, ctx, conn))

			_, err = mixedStmt.ExecContext(ctx, int64(7), "INVALID")
			require.Error(t, err)
			a, _ = queryIssue26685Vars(t, ctx, conn)
			require.Equal(t, "88", a)
			require.Equal(t, beforeIsolation, queryIssue26685Isolation(t, ctx, conn))
			require.ErrorContains(t, err, "prepared multi-assignment SET supports user variables only")
		})
	})
}

func queryIssue26685Vars(t *testing.T, ctx context.Context, conn *sql.Conn) (string, string) {
	t.Helper()
	var a, b sql.NullString
	require.NoError(t, conn.QueryRowContext(ctx,
		"select cast(@target_a as char), cast(@target_b as char)").Scan(&a, &b))
	if !a.Valid {
		a.String = "NULL"
	}
	if !b.Valid {
		b.String = "NULL"
	}
	return a.String, b.String
}

func queryIssue26685Isolation(t *testing.T, ctx context.Context, conn *sql.Conn) string {
	t.Helper()
	var isolation string
	require.NoError(t, conn.QueryRowContext(ctx, "select @@transaction_isolation").Scan(&isolation))
	return isolation
}

func queryIssue26685CharsetVars(t *testing.T, ctx context.Context, conn *sql.Conn) (string, string, string) {
	t.Helper()
	var client, connection, results string
	require.NoError(t, conn.QueryRowContext(ctx,
		"select cast(@character_set_client as char), cast(@character_set_connection as char), cast(@character_set_results as char)").Scan(
		&client, &connection, &results))
	return client, connection, results
}
