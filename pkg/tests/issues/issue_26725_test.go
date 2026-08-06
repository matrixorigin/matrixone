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
	"math"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
)

// TestIssue26725PreparedBit64Numeric exercises the same binary prepared
// statement path used by numeric client bindings, including the legacy
// flink-cdc sink's PreparedStatement.setLong() BIT(64) payloads.
func TestIssue26725PreparedBit64Numeric(t *testing.T) {
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
			"insert into "+dbName+".t64(id, b) values (?, ?), (?, ?), (?, ?), (?, ?), (?, ?)")
		require.NoError(t, err)
		defer stmt.Close()
		_, err = stmt.ExecContext(ctx,
			int64(1), int64(-6109877384019645241),
			int64(2), int64(-1),
			int64(3), int64(5),
			int64(4), uint64(5),
			int64(5), uint64(math.MaxUint64))
		require.NoError(t, err)

		rows, err := db.QueryContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range []string{
			"12336866689689906375", "18446744073709551615", "5", "5", "18446744073709551615",
		} {
			require.True(t, rows.Next())
			var actual string
			require.NoError(t, rows.Scan(&actual))
			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())

		singleStmt, err := db.PrepareContext(ctx,
			"insert into "+dbName+".t64(id, b) values (?, ?)")
		require.NoError(t, err)
		defer singleStmt.Close()
		_, err = singleStmt.ExecContext(ctx, int64(6), "5")
		require.NoError(t, err)
		var stringValue string
		require.NoError(t, db.QueryRowContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 where id = 6").Scan(&stringValue))
		require.Equal(t, "53", stringValue)
		_, err = singleStmt.ExecContext(ctx, int64(7), "-6109877384019645241")
		require.ErrorContains(t, err, "data out of range")

		// Rebinding the same statement as DOUBLE must refresh string provenance
		// and use numeric rather than ASCII byte semantics.
		_, err = singleStmt.ExecContext(ctx, int64(8), float64(5))
		require.NoError(t, err)
		var floatValue string
		require.NoError(t, db.QueryRowContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 where id = 8").Scan(&floatValue))
		require.Equal(t, "5", floatValue)

		// SQL PREPARE/EXECUTE must preserve the numeric type of user variables too.
		execSQLRequire(t, ctx, db,
			"prepare issue26725_sql from 'insert into "+dbName+".t64(id, b) values (?, ?)'")
		execSQLRequire(t, ctx, db, "set @issue26725_id = 9, @issue26725_bit = 5.0")
		execSQLRequire(t, ctx, db,
			"execute issue26725_sql using @issue26725_id, @issue26725_bit")
		var sqlPrepareValue string
		require.NoError(t, db.QueryRowContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 where id = 9").Scan(&sqlPrepareValue))
		require.Equal(t, "5", sqlPrepareValue)

		execSQLRequire(t, ctx, db, "set @issue26725_id = 10, @issue26725_bit = 5")
		execSQLRequire(t, ctx, db,
			"execute issue26725_sql using @issue26725_id, @issue26725_bit")
		execSQLRequire(t, ctx, db, "set @issue26725_id = 11, @issue26725_bit = '5'")
		execSQLRequire(t, ctx, db,
			"execute issue26725_sql using @issue26725_id, @issue26725_bit")
		execSQLRequire(t, ctx, db, "set @issue26725_id = 12, @issue26725_bit = true")
		execSQLRequire(t, ctx, db,
			"execute issue26725_sql using @issue26725_id, @issue26725_bit")
		execSQLRequire(t, ctx, db, "set @issue26725_id = 13, @issue26725_bit = b'101'")
		execSQLRequire(t, ctx, db,
			"execute issue26725_sql using @issue26725_id, @issue26725_bit")
		rows, err = db.QueryContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 where id between 10 and 13 order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range []string{"5", "53", "1", "5"} {
			require.True(t, rows.Next())
			var actual string
			require.NoError(t, rows.Scan(&actual))
			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		execSQLRequire(t, ctx, db, "deallocate prepare issue26725_sql")

		// Direct user-variable expressions use a different executor from
		// EXECUTE USING and must preserve the same source conversion semantics.
		execSQLRequire(t, ctx, db, "set @issue26725_id = 14, @issue26725_bit = 5.0")
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t64(id, b) values (@issue26725_id, @issue26725_bit)")
		execSQLRequire(t, ctx, db, "set @issue26725_id = 15, @issue26725_bit = '5'")
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t64(id, b) values (@issue26725_id, @issue26725_bit)")
		execSQLRequire(t, ctx, db, "set @issue26725_id = 16, @issue26725_bit = true")
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t64(id, b) values (@issue26725_id, @issue26725_bit)")
		rows, err = db.QueryContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 where id between 14 and 16 order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range []string{"5", "53", "1"} {
			require.True(t, rows.Next())
			var actual string
			require.NoError(t, rows.Scan(&actual))
			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		// Prepared multi-assignment SET uses an evaluate/apply transaction in the
		// frontend. Each assignment must retain its own source category across that
		// staging step, including when the same statement is rebound to text.
		setStmt, err := db.PrepareContext(ctx, "set @issue26725_id = ?, @issue26725_bit = ?")
		require.NoError(t, err)
		defer setStmt.Close()
		_, err = setStmt.ExecContext(ctx, int64(17), float64(5))
		require.NoError(t, err)
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t64(id, b) values (@issue26725_id, @issue26725_bit)")
		rollbackStmt, err := db.PrepareContext(ctx,
			"set @issue26725_bit = ?, @issue26725_failure = "+
				"(select id from "+dbName+".t64 where id in (1, 2))")
		require.NoError(t, err)
		defer rollbackStmt.Close()
		_, err = rollbackStmt.ExecContext(ctx, "5")
		require.Error(t, err)
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t64(id, b) values (19, @issue26725_bit)")
		_, err = setStmt.ExecContext(ctx, int64(18), "5")
		require.NoError(t, err)
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t64(id, b) values (@issue26725_id, @issue26725_bit)")
		rows, err = db.QueryContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 where id between 17 and 19 order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range []string{"5", "53", "5"} {
			require.True(t, rows.Next())
			var actual string
			require.NoError(t, rows.Scan(&actual))
			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		execSQLRequire(t, ctx, db,
			"create table "+dbName+".t63(id bigint primary key, b bit(63))")
		narrowStmt, err := db.PrepareContext(ctx,
			"insert into "+dbName+".t63(id, b) values (?, ?)")
		require.NoError(t, err)
		defer narrowStmt.Close()
		_, err = narrowStmt.ExecContext(ctx, int64(1), int64(-1))
		require.ErrorContains(t, err, "data out of range")

		execSQLRequire(t, ctx, db,
			"create table "+dbName+".t3(id bigint primary key, b bit(3))")
		ignoreStmt, err := db.PrepareContext(ctx,
			"insert ignore into "+dbName+".t3(id, b) values (?, ?), (?, ?)")
		require.NoError(t, err)
		defer ignoreStmt.Close()
		_, err = ignoreStmt.ExecContext(ctx,
			int64(1), int64(8),
			int64(2), int64(-1))
		require.NoError(t, err)

		ignoreRows, err := db.QueryContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t3 order by id")
		require.NoError(t, err)
		defer ignoreRows.Close()
		for _, expected := range []string{"7", "0"} {
			require.True(t, ignoreRows.Next())
			var actual string
			require.NoError(t, ignoreRows.Scan(&actual))
			require.Equal(t, expected, actual)
		}
		require.False(t, ignoreRows.Next())
		require.NoError(t, ignoreRows.Err())
		require.NoError(t, ignoreRows.Close())
	})
}
