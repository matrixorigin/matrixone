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

		// Transparent SELECT/derived-table shapes must not erase provenance, while
		// an explicit cast to CHAR must deliberately switch back to byte semantics.
		derivedStmt, err := db.PrepareContext(ctx,
			"insert into "+dbName+".t64(id, b) select ?, x from (select ? as x) d")
		require.NoError(t, err)
		defer derivedStmt.Close()
		_, err = derivedStmt.ExecContext(ctx, int64(20), float64(5))
		require.NoError(t, err)
		_, err = derivedStmt.ExecContext(ctx, int64(21), "5")
		require.NoError(t, err)
		execSQLRequire(t, ctx, db, "set @issue26725_id = 22, @issue26725_bit = 5.0")
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t64(id, b) select id, x from "+
				"(select @issue26725_id as id, @issue26725_bit as x) d")
		execSQLRequire(t, ctx, db, "set @issue26725_id = 23, @issue26725_bit = '5'")
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t64(id, b) select id, x from "+
				"(select @issue26725_id as id, @issue26725_bit as x) d")
		castSetStmt, err := db.PrepareContext(ctx,
			"set @issue26725_id = ?, @issue26725_bit = cast(? as char)")
		require.NoError(t, err)
		defer castSetStmt.Close()
		_, err = castSetStmt.ExecContext(ctx, int64(24), float64(5))
		require.NoError(t, err)
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t64(id, b) values (@issue26725_id, @issue26725_bit)")
		rows, err = db.QueryContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 where id between 20 and 24 order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range []string{"5", "53", "5", "53", "53"} {
			require.True(t, rows.Next())
			var actual string
			require.NoError(t, rows.Scan(&actual))
			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())

		// Aggregates materialize a new result vector. Aggregates that return an
		// unchanged input value must preserve numeric-vs-string source semantics
		// across that boundary for both protocol and SQL prepared statements.
		materializedStmt, err := db.PrepareContext(ctx,
			"insert into "+dbName+".t64(id, b) select ?, min(?) from "+dbName+".t64")
		require.NoError(t, err)
		defer materializedStmt.Close()
		_, err = materializedStmt.ExecContext(ctx, int64(25), float64(5))
		require.NoError(t, err)
		_, err = materializedStmt.ExecContext(ctx, int64(26), "5")
		require.NoError(t, err)
		windowStmt, err := db.PrepareContext(ctx,
			"insert into "+dbName+".t64(id, b) select ?, min(?) over() from "+dbName+".t64 limit 1")
		require.NoError(t, err)
		defer windowStmt.Close()
		_, err = windowStmt.ExecContext(ctx, int64(27), float64(5))
		require.NoError(t, err)
		_, err = windowStmt.ExecContext(ctx, int64(28), "5")
		require.NoError(t, err)

		execSQLRequire(t, ctx, db,
			"prepare issue26725_agg from 'insert into "+dbName+
				".t64(id, b) select ?, min(?) from "+dbName+".t64'")
		execSQLRequire(t, ctx, db, "set @issue26725_id = 29, @issue26725_bit = 5.0")
		execSQLRequire(t, ctx, db,
			"execute issue26725_agg using @issue26725_id, @issue26725_bit")
		execSQLRequire(t, ctx, db, "set @issue26725_id = 30, @issue26725_bit = '5'")
		execSQLRequire(t, ctx, db,
			"execute issue26725_agg using @issue26725_id, @issue26725_bit")
		execSQLRequire(t, ctx, db, "deallocate prepare issue26725_agg")
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t64(id, b) select 31, min(5.0) from "+dbName+".t64")
		execSQLRequire(t, ctx, db,
			"insert into "+dbName+".t64(id, b) select 32, min('5') from "+dbName+".t64")

		rows, err = db.QueryContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 where id between 25 and 32 order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range []string{"5", "53", "5", "53", "5", "53", "5", "53"} {
			require.True(t, rows.Next())
			var actual string
			require.NoError(t, rows.Scan(&actual))
			require.Equal(t, expected, actual)
		}
		require.False(t, rows.Next())
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		// Stored-procedure locals are string-backed at runtime for DECIMAL and
		// YEAR, so EXECUTE USING must use the declaration from the same scope.
		execSQLRequire(t, ctx, db,
			"create procedure "+dbName+".issue26725_local_types() 'begin "+
				"declare d decimal(10,2) default 5.00; "+
				"declare y year default 2024; "+
				"declare s varchar(8) default ''5''; "+
				"prepare issue26725_local from ''insert into "+dbName+
				".t64(id, b) values (33, ?), (34, ?), (35, ?)''; "+
				"execute issue26725_local using @d, @y, @s; "+
				"deallocate prepare issue26725_local; end'")
		execSQLRequire(t, ctx, db, "call "+dbName+".issue26725_local_types()")
		rows, err = db.QueryContext(ctx,
			"select cast(b as unsigned) from "+dbName+".t64 where id between 33 and 35 order by id")
		require.NoError(t, err)
		defer rows.Close()
		for _, expected := range []string{"5", "2024", "53"} {
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
