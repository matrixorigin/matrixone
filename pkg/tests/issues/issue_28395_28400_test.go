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
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue28395And28400StringIntegerConversions(t *testing.T) {
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

		t.Run("space integer and decimal counts", func(t *testing.T) {
			var got8000, got8001, got10000, got65535, got65536, got100000 int64
			err := conn.QueryRowContext(ctx,
				"select length(space(8000)), length(space(8001)), length(space(10000)), length(space(65535)), length(space(65536)), length(space(100000))",
			).Scan(&got8000, &got8001, &got10000, &got65535, &got65536, &got100000)
			require.NoError(t, err)
			require.Equal(t, int64(8000), got8000)
			require.Equal(t, int64(8001), got8001)
			require.Equal(t, int64(10000), got10000)
			require.Equal(t, int64(65535), got65535)
			require.Equal(t, int64(65536), got65536)
			require.Equal(t, int64(100000), got100000)

			var got14, got15, got19, gotNegative int64
			err = conn.QueryRowContext(ctx,
				"select length(space(cast(1.4 as decimal(4,1)))), length(space(cast(1.5 as decimal(4,1)))), length(space(cast(1.9 as decimal(4,1)))), length(space(cast(-1.5 as decimal(4,1))))",
			).Scan(&got14, &got15, &got19, &gotNegative)
			require.NoError(t, err)
			require.Equal(t, int64(1), got14)
			require.Equal(t, int64(2), got15)
			require.Equal(t, int64(2), got19)
			require.Zero(t, gotNegative)
		})

		t.Run("space large output metadata and materialization", func(t *testing.T) {
			func() {
				rows, err := conn.QueryContext(ctx, "select space(100000)")
				require.NoError(t, err)
				defer rows.Close()
				columns, err := rows.ColumnTypes()
				require.NoError(t, err)
				require.Len(t, columns, 1)
				require.Equal(t, "LONGTEXT", columns[0].DatabaseTypeName())
				require.True(t, rows.Next())
				var value string
				require.NoError(t, rows.Scan(&value))
				require.Len(t, value, 100000)
				require.Empty(t, strings.Trim(value, " "))
				require.False(t, rows.Next())
				require.NoError(t, rows.Err())
			}()
			exec(t, "create table space_large_result as select space(100000) as s")
			defer exec(t, "drop table space_large_result")
			var length, rounded int64
			err := conn.QueryRowContext(ctx, "select length(s), length(space(cast(65535.5 as decimal(8,1)))) from space_large_result").Scan(&length, &rounded)
			require.NoError(t, err)
			require.Equal(t, int64(100000), length)
			require.Equal(t, int64(65536), rounded)
		})

		for _, protocol := range []string{"sql", "binary"} {
			t.Run("space prepared reuse "+protocol, func(t *testing.T) {
				var stmt *sql.Stmt
				if protocol == "sql" {
					exec(t, "prepare space_length_stmt from 'select length(space(?))'")
					defer exec(t, "deallocate prepare space_length_stmt")
				} else {
					var err error
					stmt, err = conn.PrepareContext(ctx, "select length(space(?))")
					require.NoError(t, err)
					defer stmt.Close()
				}
				for _, value := range []any{int64(65535), int64(65536), int64(100000), int64(-1), nil, int64(67108865), int64(3)} {
					var got sql.NullInt64
					var err error
					if protocol == "sql" {
						if value == nil {
							exec(t, "set @space_count = null")
						} else {
							exec(t, fmt.Sprintf("set @space_count = %d", value.(int64)))
						}
						err = conn.QueryRowContext(ctx, "execute space_length_stmt using @space_count").Scan(&got)
					} else {
						err = stmt.QueryRowContext(ctx, value).Scan(&got)
					}
					if value == int64(67108865) {
						require.ErrorContains(t, err, "the space count is greater than max allowed value 67108864")
						continue
					}
					require.NoError(t, err)
					if value == nil {
						require.False(t, got.Valid)
					} else {
						require.Equal(t, sql.NullInt64{Int64: max(value.(int64), 0), Valid: true}, got)
					}
				}
			})
		}

		exec(t, "create table issue28400_space_inputs (id int primary key, n bigint)")
		exec(t, "insert into issue28400_space_inputs values (1, -1), (2, 0), (3, 1), (4, 8001), (5, 10000), (6, null), (7, 65536), (8, 100000)")
		rows, err := conn.QueryContext(ctx,
			"select id, length(space(n)) from issue28400_space_inputs order by id",
		)
		require.NoError(t, err)
		defer rows.Close()
		spaceWant := []struct {
			id     int
			length sql.NullInt64
		}{
			{id: 1, length: sql.NullInt64{Int64: 0, Valid: true}},
			{id: 2, length: sql.NullInt64{Int64: 0, Valid: true}},
			{id: 3, length: sql.NullInt64{Int64: 1, Valid: true}},
			{id: 4, length: sql.NullInt64{Int64: 8001, Valid: true}},
			{id: 5, length: sql.NullInt64{Int64: 10000, Valid: true}},
			{id: 6, length: sql.NullInt64{}},
			{id: 7, length: sql.NullInt64{Int64: 65536, Valid: true}},
			{id: 8, length: sql.NullInt64{Int64: 100000, Valid: true}},
		}
		for _, want := range spaceWant {
			require.True(t, rows.Next())
			var id int
			var length sql.NullInt64
			require.NoError(t, rows.Scan(&id, &length))
			require.Equal(t, want.id, id)
			require.Equal(t, want.length, length)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())

		t.Run("sha2 string hash lengths", func(t *testing.T) {
			const sha256Digest = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
			const sha224Digest = "ea09ae9cc6768c50fcee903ed054556e5bfc8347907f12598aa24193"
			const sha384Digest = "59e1748777448c69de6b800d7a33bbfb9ff1b463e44354c3553bcdb9c666fa90125a3c79f90397bdf5f6a13de828684f"
			const sha512Digest = "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"

			exec(t, "create table issue28395_sha2_inputs (id int primary key, hash_length varchar(64))")
			exec(t, "insert into issue28395_sha2_inputs values (1, '0'), (2, '224'), (3, '256tail'), (4, '384'), (5, '512'), (6, 'abc'), (7, ''), (8, '-256tail'), (9, null)")
			rows, err := conn.QueryContext(ctx,
				"select id, sha2('hello', hash_length) from issue28395_sha2_inputs order by id",
			)
			require.NoError(t, err)
			defer rows.Close()
			for id := 1; id <= 9; id++ {
				require.True(t, rows.Next())
				var gotID int
				var digest sql.NullString
				require.NoError(t, rows.Scan(&gotID, &digest))
				require.Equal(t, id, gotID)
				switch id {
				case 1, 3, 6, 7:
					require.True(t, digest.Valid)
					require.Equal(t, sha256Digest, digest.String)
				case 2:
					require.True(t, digest.Valid)
					require.Equal(t, sha224Digest, digest.String)
				case 4:
					require.True(t, digest.Valid)
					require.Equal(t, sha384Digest, digest.String)
				case 5:
					require.True(t, digest.Valid)
					require.Equal(t, sha512Digest, digest.String)
				case 8, 9:
					require.False(t, digest.Valid)
				}
			}
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())

			stmt, err := conn.PrepareContext(ctx, "select sha2('hello', ?)")
			require.NoError(t, err)
			defer stmt.Close()
			var boundDigest sql.NullString
			err = stmt.QueryRowContext(ctx, "256tail").Scan(&boundDigest)
			require.NoError(t, err)
			require.True(t, boundDigest.Valid)
			require.Equal(t, sha256Digest, boundDigest.String)

			var binaryDigest sql.NullString
			err = conn.QueryRowContext(ctx,
				"select sha2(_binary x'ff00', _binary x'3235367461696c')",
			).Scan(&binaryDigest)
			require.NoError(t, err)
			require.True(t, binaryDigest.Valid)
			require.Equal(t, "ea5dbf9596d187e9500f23e9a680109475341cf4e81f7e043f7d97152c10772f", binaryDigest.String)
		})

		exec(t, "drop table issue28400_space_inputs")
		exec(t, "drop table issue28395_sha2_inputs")
	})
}
