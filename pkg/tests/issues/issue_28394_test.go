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
	"fmt"
	"hash/crc32"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue28394CRC32AcceptsScalarExpressions(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)

		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()

		dbName := testutils.GetDatabaseName(t)
		exec := func(tb testing.TB, statement string) {
			tb.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoErrorf(tb, err, "exec failed: %s", statement)
		}
		exec(t, fmt.Sprintf("create database `%s`", dbName))
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, err := conn.ExecContext(cleanupCtx, fmt.Sprintf("drop database if exists `%s`", dbName))
			require.NoError(t, err)
		}()
		exec(t, fmt.Sprintf("use `%s`", dbName))

		const crc123 int64 = 2286445522
		for _, tc := range []struct {
			name       string
			expression string
			want       int64
			null       bool
		}{
			{name: "zero", expression: "0", want: 4108050209},
			{name: "positive integer", expression: "123", want: crc123},
			{name: "negative integer", expression: "-123", want: 68489791},
			{name: "decimal", expression: "cast(1.50 as decimal(4,2))", want: 3756579112},
			{name: "double", expression: "cast(1.5 as double)", want: 2270993338},
			{name: "small double", expression: "cast(1.234567890123456e-12 as double)", want: int64(crc32.ChecksumIEEE([]byte("0.000000000001234567890123456")))},
			{name: "boolean", expression: "true", want: 2212294583},
			{name: "binary string", expression: "_binary'123'", want: crc123},
			{name: "null", expression: "null", null: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var got sql.NullInt64
				err := conn.QueryRowContext(ctx, "select crc32("+tc.expression+")").Scan(&got)
				require.NoError(t, err)
				if tc.null {
					require.False(t, got.Valid)
					return
				}
				require.True(t, got.Valid)
				require.Equal(t, tc.want, got.Int64)
			})
		}

		exec(t, "create table crc32_scalars (i bigint, d decimal(8,2), f double, b boolean, s varchar(20))")
		exec(t, "insert into crc32_scalars values (123, 1.50, 1.5, true, '123')")
		var gotI, gotD, gotF, gotB, gotS int64
		require.NoError(t, conn.QueryRowContext(ctx,
			"select crc32(i), crc32(d), crc32(f), crc32(b), crc32(s) from crc32_scalars").Scan(
			&gotI, &gotD, &gotF, &gotB, &gotS))
		require.Equal(t, crc123, gotI)
		require.Equal(t, int64(3756579112), gotD)
		require.Equal(t, int64(2270993338), gotF)
		require.Equal(t, int64(2212294583), gotB)
		require.Equal(t, crc123, gotS)

		exec(t, "create table crc32_generated (i bigint, c bigint generated always as (crc32(i)) stored)")
		exec(t, "insert into crc32_generated(i) values (123)")
		var generated int64
		require.NoError(t, conn.QueryRowContext(ctx, "select c from crc32_generated").Scan(&generated))
		require.Equal(t, crc123, generated)
		exec(t, "update crc32_generated set i = -123")
		require.NoError(t, conn.QueryRowContext(ctx, "select c from crc32_generated").Scan(&generated))
		require.Equal(t, int64(68489791), generated)

		stmt, err := conn.PrepareContext(ctx, "select crc32(?)")
		require.NoError(t, err)
		defer stmt.Close()
		var prepared int64
		require.NoError(t, stmt.QueryRowContext(ctx, int64(123)).Scan(&prepared))
		require.Equal(t, crc123, prepared)
		var preparedNull sql.NullInt64
		require.NoError(t, stmt.QueryRowContext(ctx, nil).Scan(&preparedNull))
		require.False(t, preparedNull.Valid)
		require.NoError(t, stmt.QueryRowContext(ctx, int64(-123)).Scan(&prepared))
		require.Equal(t, int64(68489791), prepared)

		var binaryPrepared int64
		require.NoError(t, stmt.QueryRowContext(ctx, []byte{0, 0xff, '1'}).Scan(&binaryPrepared))
		require.Equal(t, int64(1035674714), binaryPrepared)
	})
}
