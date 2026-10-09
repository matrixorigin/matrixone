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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestBigDataBulkWriteAndPrimaryKeyCopyPreserveRows(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		var cnServices [2]cnservice.Service
		open := func(index int) *sql.DB {
			cn, err := cluster.GetCNService(index)
			require.NoError(t, err)
			cnServices[index] = cn.RawService().(cnservice.Service)
			db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		creator, writer := open(0), open(1)
		// Each CN may start at its latest applied logtail rather than wall time.
		// Order the next cross-CN consumer after the producer's commit, including
		// COPY DDL and its replacement table. SQL completion alone is not a
		// visibility barrier for a different CN.
		waitCrossCN := func(t *testing.T, producer, consumer int) {
			t.Helper()
			frontier := cnServices[producer].GetTxnClient().GetLatestCommitTS()
			require.False(t, frontier.IsEmpty())
			waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
			defer waitCancel()
			snapshot, err := cnServices[consumer].GetTxnClient().WaitLogTailAppliedAt(waitCtx, frontier)
			require.NoError(t, err)
			require.True(t, frontier.Less(snapshot))
		}
		database := strings.ToLower(testutils.GetDatabaseName(t))
		execSQLRequire(t, ctx, creator, "create database `"+database+"`")
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cleanupCancel()
			execSQLRequire(t, cleanupCtx, creator, "drop database if exists `"+database+"`")
		})
		table := "`" + database + "`.`bulk_rows`"
		execSQLRequire(t, ctx, creator, "create table "+table+` (
			id bigint unsigned not null,
			inline_value varchar(23), area_value varchar(24),
			inline_vector vecf32(4), area_vector vecf64(3))`)

		// 32768 rows cross the default 10000-ID cache boundary with real
		// consumed rows. CN1 has no table cache from CREATE and must resolve
		// hidden fake-PK ownership through the committed catalog before inserting.
		// The fixture reuses the existing cluster and has no time-based oracle.
		const rows = 32768
		const idSum = uint64(rows * (rows + 1) / 2)
		verify := func(t *testing.T, reader *sql.DB, extraRows int) {
			t.Helper()
			var count, distinct, nulls, badInline, badArea, nullVectors int64
			var total, minID, maxID uint64
			var inlineDistance, areaDistance float64
			require.NoError(t, reader.QueryRowContext(ctx, `select
				count(*), count(distinct id), sum(id), min(id), max(id),
				count(case when inline_value is null then 1 end),
				count(case when (id % 2 = 0 and inline_value is not null)
					or (id % 2 = 1 and (inline_value is null or inline_value <> repeat('i',23))) then 1 end),
				count(case when area_value is null or area_value <> repeat('a',24) then 1 end),
				count(case when inline_vector is null or area_vector is null then 1 end),
				max(l2_distance(inline_vector,'[1,2,3,4]')),
				max(l2_distance(area_vector,'[1,2,3]')) from `+table).Scan(
				&count, &distinct, &total, &minID, &maxID, &nulls,
				&badInline, &badArea, &nullVectors, &inlineDistance, &areaDistance))
			require.Equal(t, int64(rows+extraRows), count)
			require.Equal(t, int64(rows), distinct)
			require.Equal(t, idSum+uint64(extraRows), total)
			require.Equal(t, uint64(1), minID)
			require.Equal(t, uint64(rows), maxID)
			require.Equal(t, int64(rows/2), nulls)
			require.Zero(t, badInline)
			require.Zero(t, badArea)
			require.Zero(t, nullVectors)
			require.Zero(t, inlineDistance)
			require.Zero(t, areaDistance)
		}
		primaryKeys := func(t *testing.T, want int) {
			t.Helper()
			var count int
			require.NoError(t, creator.QueryRowContext(ctx,
				"select count(*) from information_schema.statistics where table_schema = ? and table_name = 'bulk_rows' and index_name = 'PRIMARY'",
				database).Scan(&count))
			require.Equal(t, want, count)
		}
		if !t.Run("cold CN bulk insert", func(t *testing.T) {
			waitCrossCN(t, 0, 1)
			execSQLRequire(t, ctx, writer, fmt.Sprintf(`insert into %s
				select cast(result as bigint unsigned), if(result %% 2 = 0, null, repeat('i',23)),
				repeat('a',24), '[1,2,3,4]', '[1,2,3]' from generate_series(1,%d) g`, table, rows))
			waitCrossCN(t, 1, 0)
			verify(t, creator, 0)
			primaryKeys(t, 0)
		}) {
			return
		}
		if !t.Run("add primary key copies payloads", func(t *testing.T) {
			execSQLRequire(t, ctx, creator, "alter table "+table+" add primary key(id)")
			waitCrossCN(t, 0, 1)
			verify(t, writer, 0)
			primaryKeys(t, 1)
		}) {
			return
		}
		if !t.Run("drop primary key creates fresh hidden IDs", func(t *testing.T) {
			execSQLRequire(t, ctx, creator, "alter table "+table+" drop primary key")
			waitCrossCN(t, 0, 1)
			verify(t, writer, 0)
			primaryKeys(t, 0)
		}) {
			return
		}
		if !t.Run("duplicate copy rejects publication and remains writable", func(t *testing.T) {
			duplicate := "insert into " + table + " select * from " + table + " where id=1 limit 1"
			execSQLRequire(t, ctx, writer, duplicate)
			waitCrossCN(t, 1, 0)
			_, err := creator.ExecContext(ctx, "alter table "+table+" add primary key(id)")
			issue289RequireMySQLError(t, err, 1062)
			verify(t, writer, 1)
			primaryKeys(t, 0)
			// A second duplicate remains legal only if the failed COPY did not
			// publish its PRIMARY constraint or retire the source allocator.
			execSQLRequire(t, ctx, writer, duplicate)
			waitCrossCN(t, 1, 0)
			verify(t, creator, 2)
		}) {
			return
		}

		t.Run("visible fake PK name remains a user auto increment", func(t *testing.T) {
			userTable := "`" + database + "`.`visible_fakepk`"
			column := "`" + catalog.FakePrimaryKeyColName + "`"
			execSQLRequire(t, ctx, creator, "create table "+userTable+" ("+column+" bigint unsigned auto_increment primary key, payload varchar(24))")
			waitCrossCN(t, 0, 1)
			inserted, err := writer.ExecContext(ctx, "insert into "+userTable+" (payload) values ('one'),('two'),('three')")
			require.NoError(t, err)
			affected, err := inserted.RowsAffected()
			require.NoError(t, err)
			require.Equal(t, int64(3), affected)
			waitCrossCN(t, 1, 0)
			var count, distinct int
			var minID, maxID, total uint64
			require.NoError(t, creator.QueryRowContext(ctx,
				"select count(*), count(distinct "+column+"), min("+column+"), max("+column+"), sum("+column+") from "+userTable).Scan(&count, &distinct, &minID, &maxID, &total))
			require.Equal(t, 3, count)
			require.Equal(t, 3, distinct)
			require.Positive(t, minID)
			// The DDL CN can already own a default reservation. Do not require
			// another CN to start at one or erase existing public cache gaps.
			require.Equal(t, minID+2, maxID)
			require.Equal(t, 3*minID+3, total)
			var columns, hidden int
			require.NoError(t, creator.QueryRowContext(ctx, `select count(*),
				count(case when c.att_is_hidden = 1 then 1 end)
				from mo_catalog.mo_columns c join mo_catalog.mo_tables b on c.att_relname_id = b.rel_id
				where b.reldatabase = ? and b.relname = 'visible_fakepk' and c.attname = ?`,
				database, catalog.FakePrimaryKeyColName).Scan(&columns, &hidden))
			require.Equal(t, 1, columns)
			require.Zero(t, hidden)
		})
	})
}
