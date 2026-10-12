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
		services := make([]cnservice.Service, 2)
		open := func(index int) *sql.DB {
			cn, err := cluster.GetCNService(index)
			require.NoError(t, err)
			service, ok := cn.RawService().(cnservice.Service)
			require.Truef(t, ok, "CN%d must expose its transaction service", index)
			services[index] = service
			db, err := sql.Open("mysql", issue27487DSN(cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			return db
		}
		creator, writer := open(0), open(1)
		// A remote commit does not order this CN's next autocommit snapshot.
		// Advance its logtail without resolving the table, preserving the cold cache.
		waitCrossCN := func(t *testing.T, source, target int) {
			t.Helper()
			frontier := services[source].GetTxnClient().GetLatestCommitTS()
			require.Falsef(t, frontier.IsEmpty(), "CN%d must publish a commit before CN%d observes it", source, target)
			waitCtx, waitCancel := context.WithTimeout(ctx, 10*time.Second)
			defer waitCancel()
			snapshot, err := services[target].GetTxnClient().WaitLogTailAppliedAt(waitCtx, frontier)
			t.Logf("CN%d -> CN%d: writer frontier=%s, reader admitted snapshot=%s, wait error=%v",
				source, target, frontier.DebugString(), snapshot.DebugString(), err)
			require.NoError(t, err)
			require.Truef(t, frontier.Less(snapshot), "CN%d snapshot %s must advance past CN%d frontier %s",
				target, snapshot.DebugString(), source, frontier.DebugString())
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
			var total, minID, maxID sql.Null[uint64]
			var inlineDistance, areaDistance sql.NullFloat64
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
			readerIndex, peer := 0, writer
			if reader == writer {
				readerIndex, peer = 1, creator
			}
			t.Logf("CN%d aggregates: count=%d distinct=%d sum=%+v min=%+v max=%+v nulls=%d badInline=%d badArea=%d nullVectors=%d inlineDistance=%+v areaDistance=%+v",
				readerIndex, count, distinct, total, minID, maxID, nulls, badInline, badArea, nullVectors, inlineDistance, areaDistance)
			if count != int64(rows+extraRows) || distinct != int64(rows) || !total.Valid || !minID.Valid || !maxID.Valid {
				// A peer observation is diagnostic only, never a retry of the failed reader.
				var peerCount, peerDistinct int64
				var peerTotal, peerMin, peerMax sql.Null[uint64]
				err := peer.QueryRowContext(ctx, "select count(*), count(distinct id), sum(id), min(id), max(id) from "+table).
					Scan(&peerCount, &peerDistinct, &peerTotal, &peerMin, &peerMax)
				t.Logf("CN%d visibility control: count=%d distinct=%d sum=%+v min=%+v max=%+v error=%v",
					1-readerIndex, peerCount, peerDistinct, peerTotal, peerMin, peerMax, err)
			}
			require.Equal(t, int64(rows+extraRows), count)
			require.Equal(t, int64(rows), distinct)
			require.Equal(t, sql.Null[uint64]{V: idSum + uint64(extraRows), Valid: true}, total)
			require.Equal(t, sql.Null[uint64]{V: 1, Valid: true}, minID)
			require.Equal(t, sql.Null[uint64]{V: rows, Valid: true}, maxID)
			require.Equal(t, int64(rows/2), nulls)
			require.Zero(t, badInline)
			require.Zero(t, badArea)
			require.Zero(t, nullVectors)
			require.Equal(t, sql.NullFloat64{Valid: true}, inlineDistance)
			require.Equal(t, sql.NullFloat64{Valid: true}, areaDistance)
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
			execSQLRequire(t, ctx, writer, "insert into "+userTable+" (payload) values ('one'),('two'),('three')")
			waitCrossCN(t, 1, 0)
			var count, distinct int
			var minID, maxID, total sql.Null[uint64]
			require.NoError(t, creator.QueryRowContext(ctx,
				"select count(*), count(distinct "+column+"), min("+column+"), max("+column+"), sum("+column+") from "+userTable).Scan(&count, &distinct, &minID, &maxID, &total))
			t.Logf("CN0 visible fake-PK aggregates: count=%d distinct=%d min=%+v max=%+v sum=%+v",
				count, distinct, minID, maxID, total)
			require.Equal(t, 3, count)
			require.Equal(t, 3, distinct)
			require.True(t, minID.Valid, "visible fake-PK minimum must not be NULL")
			require.Positive(t, minID.V)
			// The DDL CN can already own a default reservation. Do not require
			// another CN to start at one or erase existing public cache gaps.
			require.Equal(t, sql.Null[uint64]{V: minID.V + 2, Valid: true}, maxID)
			require.Equal(t, sql.Null[uint64]{V: 3*minID.V + 3, Valid: true}, total)
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
