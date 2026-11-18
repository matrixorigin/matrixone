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

package sqlintegration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestParserReportedIssues32542And32606SQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()

		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)

		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		exec := func(t *testing.T, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}

		readOneInt := func(t *testing.T, query string) (int, []string) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, query)
			require.NoError(t, err, query)
			defer rows.Close()

			columns, err := rows.Columns()
			require.NoError(t, err, query)
			require.Len(t, columns, 1, query)

			require.True(t, rows.Next(), query)
			var value int
			require.NoError(t, rows.Scan(&value), query)
			require.False(t, rows.Next(), query)
			require.NoError(t, rows.Err(), query)

			return value, columns
		}

		rejectSyntax := func(t *testing.T, statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.Error(t, err, statement)

			var mysqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &mysqlErr, statement)
			require.EqualValues(t, 1064, mysqlErr.Number, statement)

			var recovered int
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT 42").Scan(&recovered), "same connection after rejected statement: %s", statement)
			require.Equal(t, 42, recovered)
		}

		requireDatabaseAbsent := func(t *testing.T, name string) {
			t.Helper()
			var count int
			err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?", name).Scan(&count)
			require.NoError(t, err, name)
			require.Zero(t, count, "owned database name must not preexist: %s", name)
		}

		const endDB = "end_owner_32542"
		const withDB = "with_body_32606"

		requireDatabaseAbsent(t, endDB)
		requireDatabaseAbsent(t, withDB)

		exec(t, "CREATE DATABASE "+endDB)
		defer cleanupSQLIntegration(t, cn, "DROP DATABASE IF EXISTS "+endDB)

		exec(t, "CREATE DATABASE "+withDB)
		defer cleanupSQLIntegration(t, cn, "DROP DATABASE IF EXISTS "+withDB)

		if !t.Run("issue_32542_bare_end_varbinary_column_owner", func(t *testing.T) {
			t.Log("target_hit=issue_32542; actual_operation=create_bare_END_column; oracle=varbinary(16)_schema_and_exact_nonempty_value")

			exec(t, "CREATE TABLE "+endDB+".end_t (id INT, end VARBINARY(16))")
			exec(t, "INSERT INTO "+endDB+".end_t (id, end) VALUES (1, X'000102030405060708090A0B0C0D0E0F')")

			rows, err := conn.QueryContext(ctx, "SELECT "+endDB+".end_t.end FROM "+endDB+".end_t WHERE id = 1")
			require.NoError(t, err)
			defer rows.Close()

			columns, err := rows.Columns()
			require.NoError(t, err)
			require.Equal(t, []string{"end"}, columns)

			require.True(t, rows.Next())
			var endValue []byte
			require.NoError(t, rows.Scan(&endValue))
			require.Equal(t, []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f}, endValue)
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())

			var dataType string
			var length int
			require.NoError(t, conn.QueryRowContext(
				ctx,
				"SELECT data_type, character_maximum_length FROM information_schema.columns WHERE table_schema = ? AND table_name = 'end_t' AND column_name = 'end'",
				endDB,
			).Scan(&dataType, &length))
			require.Equal(t, "varbinary", dataType)
			require.Equal(t, 16, length)
		}) {
			return
		}

		if !t.Run("issue_32542_ctas_table_options_before_as_select", func(t *testing.T) {
			t.Log("target_hit=issue_32542; actual_operation=CTAS_options_before_AS_select; oracle=one_row_and_same_metadata_as_plain_twin")

			exec(t, "CREATE TABLE "+endDB+".ct_twin charset utf8mb4 AS SELECT 1 AS a")
			exec(t, "CREATE TABLE "+endDB+".plain_twin (a INT) charset utf8mb4")
			exec(t, "INSERT INTO "+endDB+".plain_twin (a) VALUES (1)")
			exec(t, "CREATE TABLE "+endDB+".ctas_no_option AS SELECT 1 AS a")

			ctasValue, ctasColumns := readOneInt(t, "SELECT a FROM "+endDB+".ct_twin")
			plainValue, plainColumns := readOneInt(t, "SELECT a FROM "+endDB+".plain_twin")
			noOptionValue, noOptionColumns := readOneInt(t, "SELECT a FROM "+endDB+".ctas_no_option")
			require.Equal(t, 1, ctasValue)
			require.Equal(t, 1, plainValue)
			require.Equal(t, 1, noOptionValue)
			require.Equal(t, plainColumns, ctasColumns)
			require.Equal(t, plainColumns, noOptionColumns)

			var ctasCollation, ctasType string
			var plainCollation, noOptionType string
			require.NoError(t, conn.QueryRowContext(
				ctx,
				"SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ct_twin'",
				endDB,
			).Scan(&ctasCollation))
			require.NoError(t, conn.QueryRowContext(
				ctx,
				"SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'plain_twin'",
				endDB,
			).Scan(&plainCollation))
			require.NoError(t, conn.QueryRowContext(
				ctx,
				"SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ct_twin' AND COLUMN_NAME = 'a'",
				endDB,
			).Scan(&ctasType))
			require.NoError(t, conn.QueryRowContext(
				ctx,
				"SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'ctas_no_option' AND COLUMN_NAME = 'a'",
				endDB,
			).Scan(&noOptionType))
			require.Equal(t, plainCollation, ctasCollation)
			require.Equal(t, noOptionType, ctasType)
		}) {
			return
		}

		if !t.Run("issue_32606_literal_cte_paren_query_body_and_plain_twin", func(t *testing.T) {
			t.Log("target_hit=issue_32606; actual_operation=paren_query_body_and_plain_twin; oracle=one_row_and_column_a")

			parenValue, parenColumns := readOneInt(t, "WITH cte_body AS (SELECT 1 AS a) (SELECT * FROM cte_body)")
			plainValue, plainColumns := readOneInt(t, "WITH cte_body AS (SELECT 1 AS a) SELECT * FROM cte_body")
			require.Equal(t, 1, parenValue)
			require.Equal(t, 1, plainValue)
			require.Equal(t, plainColumns, parenColumns)
			require.Equal(t, "a", parenColumns[0])
		}) {
			return
		}

		if !t.Run("issue_32606_table_backed_cte_paren_query_body_and_plain_twin", func(t *testing.T) {
			t.Log("target_hit=issue_32606; actual_operation=table_backed_cte_paren_query_body_and_plain_twin; oracle=one_row_and_column_a")

			exec(t, "CREATE TABLE "+withDB+".cte_source (a INT)")
			exec(t, "INSERT INTO "+withDB+".cte_source (a) VALUES (5)")

			parenValue, parenColumns := readOneInt(t, "WITH cte_body AS (SELECT a FROM "+withDB+".cte_source) (SELECT * FROM cte_body)")
			plainValue, plainColumns := readOneInt(t, "WITH cte_body AS (SELECT a FROM "+withDB+".cte_source) SELECT * FROM cte_body")
			require.Equal(t, 5, parenValue)
			require.Equal(t, 5, plainValue)
			require.Equal(t, plainColumns, parenColumns)
			require.Equal(t, "a", parenColumns[0])
		}) {
			return
		}

		if !t.Run("issue_32542_and_32606_malformed_syntax_rejected_on_same_connection", func(t *testing.T) {
			t.Log("target_hit=issues_32542_and_32606; actual_operation=invalid_syntax_then_SELECT_42_write_recovery; oracle=typed_MySQL_1064_and_restored_select_on_same_connection")

			rejectSyntax(t, "CREATE TABLE "+endDB+".bad_ctas AS SELECT 1 AS a charset utf8mb4")
			rejectSyntax(t, "WITH cte_body AS (SELECT 1 AS a) ()")
		}) {
			return
		}
	})
}
