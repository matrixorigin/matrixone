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
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

func TestIssue27088PreparedDecimalCommonType(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		mustExec(t, ctx, conn, fmt.Sprintf("create database `%s`", dbName))
		mustExec(t, ctx, conn, fmt.Sprintf("use `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
		}()

		mustExec(t, ctx, conn, `create table common_type (
			id int primary key,
			d decimal(38,10),
			low_bound decimal(38,10),
			high_bound decimal(38,10)
		)`)
		mustExec(t, ctx, conn, `insert into common_type values
			(1, 9007199254740992.0000000000, 9007199254740992.0000000000, 9007199254740992.0000000000),
			(2, 9007199254740992.0000000002, 9007199254740992.0000000000, 9007199254740992.0000000002),
			(3, 9007199254740992.0000000003, 9007199254740992.0000000003, 9007199254740992.0000000004),
			(4, null, null, null)`)

		assertIDs := func(t *testing.T, rows *sql.Rows, queryErr error, want ...int) {
			t.Helper()
			require.NoError(t, queryErr)
			defer rows.Close()
			var got []int
			for rows.Next() {
				var id int
				require.NoError(t, rows.Scan(&id))
				got = append(got, id)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, want, got)
		}

		t.Run("COM_STMT exact comparison and list", func(t *testing.T) {
			equality, prepareErr := conn.PrepareContext(ctx,
				"select id from common_type where d = ? order by id")
			require.NoError(t, prepareErr)
			defer equality.Close()
			rows, queryErr := equality.QueryContext(ctx, "9007199254740992.0000000002tail")
			assertIDs(t, rows, queryErr, 2)
			require.NoError(t, rows.Err())

			inList, prepareErr := conn.PrepareContext(ctx,
				"select id from common_type where d in (?, ?) order by id")
			require.NoError(t, prepareErr)
			defer inList.Close()
			rows, queryErr = inList.QueryContext(ctx,
				"9007199254740992.0000000000first", "9007199254740992.0000000003second")
			assertIDs(t, rows, queryErr, 1, 3)
			require.NoError(t, rows.Err())
		})

		t.Run("COM_STMT compensated exponent remains exact", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table exponent_exact(id int primary key, d decimal(65,0))")
			mustExec(t, ctx, conn, `insert into exponent_exact values
				(1, 10000000000000000000000000000000000000000000000000000000000000000),
				(2, 10000000000000000000000000000000000000000000000000000000000000001)`)
			stmt, prepareErr := conn.PrepareContext(ctx,
				"select id from exponent_exact where d = ? order by id")
			require.NoError(t, prepareErr)
			defer stmt.Close()
			rows, queryErr := stmt.QueryContext(ctx, "0.000000000000000000000000000000000001e100")
			assertIDs(t, rows, queryErr, 1)
			require.NoError(t, rows.Err())
		})

		t.Run("COM_STMT row dependent between", func(t *testing.T) {
			stmt, prepareErr := conn.PrepareContext(ctx,
				"select id from common_type where ? between low_bound and high_bound order by id")
			require.NoError(t, prepareErr)
			defer stmt.Close()
			rows, queryErr := stmt.QueryContext(ctx, "9007199254740992.0000000001tail")
			assertIDs(t, rows, queryErr, 2)
			require.NoError(t, rows.Err())
		})

		t.Run("common value functions preserve context", func(t *testing.T) {
			common, prepareErr := conn.PrepareContext(ctx,
				"select coalesce(?, d) from common_type where id = 1")
			require.NoError(t, prepareErr)
			defer common.Close()
			var decimalValue string
			require.NoError(t, common.QueryRowContext(ctx, "12.5tail").Scan(&decimalValue))
			require.Equal(t, "12.5000000000", decimalValue)

			nested, prepareErr := conn.PrepareContext(ctx, `select id from common_type
				where coalesce(?, d) = cast('9007199254740992.0000000002' as decimal(38,10))
				order by id`)
			require.NoError(t, prepareErr)
			defer nested.Close()
			rows, queryErr := nested.QueryContext(ctx, "9007199254740992.0000000001tail")
			assertIDs(t, rows, queryErr)
			require.NoError(t, rows.Err())
			rows, queryErr = nested.QueryContext(ctx, nil)
			assertIDs(t, rows, queryErr, 2)
			require.NoError(t, rows.Err())

			stringsOnly, prepareErr := conn.PrepareContext(ctx, "select greatest(?, ?)")
			require.NoError(t, prepareErr)
			defer stringsOnly.Close()
			var stringValue string
			require.NoError(t, stringsOnly.QueryRowContext(ctx, "10", "2").Scan(&stringValue))
			require.Equal(t, "2", stringValue)
		})

		t.Run("SQL PREPARE uses the same prefix domain", func(t *testing.T) {
			mustExec(t, ctx, conn,
				"prepare issue27088_sql from 'select id from common_type where d = ? order by id'")
			defer func() { _, _ = conn.ExecContext(context.Background(), "deallocate prepare issue27088_sql") }()
			mustExec(t, ctx, conn, "set @issue27088_value = '9007199254740992.0000000002tail'")
			rows, queryErr := conn.QueryContext(ctx, "execute issue27088_sql using @issue27088_value")
			assertIDs(t, rows, queryErr, 2)
			require.NoError(t, rows.Err())

			mustExec(t, ctx, conn, `prepare issue27088_nested_sql from 'select id from common_type
				where coalesce(?, d) = cast(''9007199254740992.0000000002'' as decimal(38,10))
				order by id'`)
			defer func() {
				_, _ = conn.ExecContext(context.Background(), "deallocate prepare issue27088_nested_sql")
			}()
			mustExec(t, ctx, conn,
				"set @issue27088_nested = cast('9007199254740992.0000000001' as decimal(38,10))")
			rows, queryErr = conn.QueryContext(ctx, "execute issue27088_nested_sql using @issue27088_nested")
			assertIDs(t, rows, queryErr)
			require.NoError(t, rows.Err())
			mustExec(t, ctx, conn, "set @issue27088_nested = null")
			rows, queryErr = conn.QueryContext(ctx, "execute issue27088_nested_sql using @issue27088_nested")
			assertIDs(t, rows, queryErr, 2)
			require.NoError(t, rows.Err())
		})

		t.Run("SQL EXECUTE specializes CTAS query", func(t *testing.T) {
			mustExec(t, ctx, conn, `prepare issue27088_ctas from
				'create table ctas_result as select id from common_type where d = ?'`)
			defer func() { _, _ = conn.ExecContext(context.Background(), "deallocate prepare issue27088_ctas") }()
			mustExec(t, ctx, conn, "set @issue27088_ctas = '9007199254740992.0000000002tail'")
			mustExec(t, ctx, conn, "execute issue27088_ctas using @issue27088_ctas")
			rows, queryErr := conn.QueryContext(ctx, "select id from ctas_result order by id")
			assertIDs(t, rows, queryErr, 2)
			require.NoError(t, rows.Err())
		})

		t.Run("SQL EXECUTE specializes SET expression", func(t *testing.T) {
			mustExec(t, ctx, conn, `prepare issue27088_set from
				'set @issue27088_out = coalesce(?, cast(1 as decimal(38,10)))'`)
			defer func() { _, _ = conn.ExecContext(context.Background(), "deallocate prepare issue27088_set") }()
			mustExec(t, ctx, conn, "set @issue27088_set_value = '12.5tail'")
			mustExec(t, ctx, conn, "execute issue27088_set using @issue27088_set_value")
			var value string
			require.NoError(t, conn.QueryRowContext(ctx, "select @issue27088_out").Scan(&value))
			require.Equal(t, "12.5000000000", value)
		})

		t.Run("SQL EXECUTE specializes integer comparison for decimal variable", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table execute_integer(id int primary key, vi int, key idx_i(vi))")
			mustExec(t, ctx, conn, "insert into execute_integer values (1, 9), (2, 10), (3, null)")
			mustExec(t, ctx, conn,
				"prepare issue27088_integer from 'select id from execute_integer where vi = ? order by id'")
			defer func() { _, _ = conn.ExecContext(context.Background(), "deallocate prepare issue27088_integer") }()
			mustExec(t, ctx, conn, "set @issue27088_integer = 9.0")
			rows, queryErr := conn.QueryContext(ctx, "execute issue27088_integer using @issue27088_integer")
			assertIDs(t, rows, queryErr, 1)
			require.NoError(t, rows.Err())
		})

		t.Run("SQL EXECUTE preserves legacy float and null domains", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table execute_control(id int primary key, f float)")
			mustExec(t, ctx, conn, "insert into execute_control values (1, 1.2345678)")

			mustExec(t, ctx, conn,
				"prepare issue27088_float from 'select id from execute_control where f = ? order by id'")
			defer func() { _, _ = conn.ExecContext(context.Background(), "deallocate prepare issue27088_float") }()
			mustExec(t, ctx, conn, "set @issue27088_float = 1.2345678")
			rows, queryErr := conn.QueryContext(ctx, "execute issue27088_float using @issue27088_float")
			assertIDs(t, rows, queryErr, 1)
			require.NoError(t, rows.Err())

			mustExec(t, ctx, conn,
				"prepare issue27088_count from 'select count(?) from execute_control'")
			defer func() { _, _ = conn.ExecContext(context.Background(), "deallocate prepare issue27088_count") }()
			mustExec(t, ctx, conn, "set @issue27088_count = 'x'")
			var count int
			require.NoError(t, conn.QueryRowContext(ctx,
				"execute issue27088_count using @issue27088_count").Scan(&count))
			require.Equal(t, 1, count)
			mustExec(t, ctx, conn, "set @issue27088_count = null")
			require.NoError(t, conn.QueryRowContext(ctx,
				"execute issue27088_count using @issue27088_count").Scan(&count))
			require.Equal(t, 0, count)
		})

		t.Run("same type UUID constant casts remain bindable", func(t *testing.T) {
			mustExec(t, ctx, conn, "create table uuid_control(u uuid primary key)")
			mustExec(t, ctx, conn, `insert into uuid_control values
				('00000000-0000-0000-0000-000000000001'),
				('00000000-0000-0000-0000-000000000002'),
				('00000000-0000-0000-0000-000000000003'),
				('00000000-0000-0000-0000-000000000004')`)

			assertUUIDs := func(rows *sql.Rows, queryErr error, want ...string) {
				t.Helper()
				require.NoError(t, queryErr)
				defer rows.Close()
				var got []string
				for rows.Next() {
					var value string
					require.NoError(t, rows.Scan(&value))
					got = append(got, value)
				}
				require.NoError(t, rows.Err())
				require.Equal(t, want, got)
			}

			rows, queryErr := conn.QueryContext(ctx, `select u from uuid_control
				where u = cast('00000000-0000-0000-0000-000000000001' as uuid)
				   or u in (cast('00000000-0000-0000-0000-000000000002' as uuid),
				            cast('00000000-0000-0000-0000-000000000003' as uuid)) order by u`)
			assertUUIDs(rows, queryErr,
				"00000000-0000-0000-0000-000000000001",
				"00000000-0000-0000-0000-000000000002",
				"00000000-0000-0000-0000-000000000003")
			require.NoError(t, rows.Err())

			rows, queryErr = conn.QueryContext(ctx, `select u from uuid_control
				where u between cast('00000000-0000-0000-0000-000000000002' as uuid)
				            and cast('00000000-0000-0000-0000-000000000003' as uuid)
				   or u >= cast('00000000-0000-0000-0000-000000000004' as uuid) order by u`)
			assertUUIDs(rows, queryErr,
				"00000000-0000-0000-0000-000000000002",
				"00000000-0000-0000-0000-000000000003",
				"00000000-0000-0000-0000-000000000004")
			require.NoError(t, rows.Err())
		})
	})
}
