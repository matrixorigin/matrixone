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
)

// TestIssue26725PreparedBit64SignedLong exercises the same binary prepared
// statement path used by the legacy flink-cdc sink, which binds BIT(64)
// payloads with PreparedStatement.setLong().
func TestIssue26725PreparedBit64SignedLong(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf(
			"dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", port))
		require.NoError(t, err)
		defer db.Close()

		const dbName = "issue_26725_prepared_bit64"
		execSQLMaybe(t, ctx, db, "drop database if exists "+dbName)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			execSQLMaybe(t, cleanupCtx, db, "drop database if exists "+dbName)
		}()
		execSQLRequire(t, ctx, db, "create database "+dbName)
		execSQLRequire(t, ctx, db,
			"create table "+dbName+".t64(id bigint primary key, b bit(64))")

		stmt, err := db.PrepareContext(ctx,
			"insert into "+dbName+".t64(id, b) values (?, ?), (?, ?), (?, ?)")
		require.NoError(t, err)
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx,
			int64(1), int64(-6109877384019645241),
			int64(2), int64(-1),
			int64(3), int64(5))
		require.NoError(t, err)

		rows, err := db.QueryContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range []string{"12336866689689906375", "18446744073709551615", "5"} {
			require.True(t, rows.Next())
			var actual string
			require.NoError(t, rows.Scan(&actual))
			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())

		stringStmt, err := db.PrepareContext(ctx,
			"insert into "+dbName+".t64(id, b) values (?, ?)")
		require.NoError(t, err)
		defer stringStmt.Close()
		_, err = stringStmt.ExecContext(ctx, int64(4), "5")
		require.NoError(t, err)
		var stringValue string
		require.NoError(t, db.QueryRowContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 where id = 4").Scan(&stringValue))
		require.Equal(t, "53", stringValue)
		_, err = stringStmt.ExecContext(ctx, int64(5), "-6109877384019645241")
		require.ErrorContains(t, err, "data out of range")

		execSQLRequire(t, ctx, db,
			"create table "+dbName+".t63(id bigint primary key, b bit(63))")
		narrowStmt, err := db.PrepareContext(ctx,
			"insert into "+dbName+".t63(id, b) values (?, ?)")
		require.NoError(t, err)
		defer narrowStmt.Close()
		_, err = narrowStmt.ExecContext(ctx, int64(1), int64(-1))
		require.ErrorContains(t, err, "data out of range")
	})
}
