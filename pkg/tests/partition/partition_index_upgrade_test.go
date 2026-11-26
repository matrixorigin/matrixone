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

package partition

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// TestPartitionIndexOfflineCopy exercises the documented recovery for the
// legacy layout where CREATE INDEX installed an index only on partition leaves.
func TestPartitionIndexOfflineCopy(t *testing.T) {
	require.NoError(t, runPartitionClusterTest(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		open := func(i int) *sql.DB {
			t.Helper()
			cn, err := c.GetCNService(i)
			require.NoError(t, err)
			db, err := sql.Open("mysql", fmt.Sprintf("root:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			return db
		}
		db0, db1, db2 := open(0), open(1), open(2)
		defer db0.Close()
		defer db1.Close()
		defer db2.Close()
		writer, err := db0.Conn(ctx)
		require.NoError(t, err)
		defer writer.Close()

		// Listener readiness does not guarantee that the writer has discovered
		// every CN. Establish fixture membership before testing the barrier.
		cn0, err := c.GetCNService(0)
		require.NoError(t, err)
		view := clusterservice.GetMOCluster(cn0.ServiceID())
		refresher, ok := view.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		wantCNs := make(map[string]bool)
		for i := 0; i < 3; i++ {
			cn, err := c.GetCNService(i)
			require.NoError(t, err)
			wantCNs[cn.ServiceID()] = true
		}
		require.Eventually(t, func() bool {
			refreshCtx, stop := context.WithTimeout(ctx, 2*time.Second)
			defer stop()
			if err := refresher.Refresh(refreshCtx); err != nil {
				return false
			}
			gotCNs := make(map[string]bool)
			view.GetCNService(clusterservice.NewSelector(), func(cn metadata.CNService) bool {
				gotCNs[cn.ServiceID] = cn.QueryAddress != ""
				return true
			})
			return maps.Equal(wantCNs, gotCNs)
		}, 30*time.Second, 100*time.Millisecond, "all fixture CNs must be discoverable")

		name := strings.ToLower(testutils.GetDatabaseName(t))
		quote := func(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
		exec := func(conn *sql.Conn, query string) sql.Result {
			t.Helper()
			result, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
			return result
		}
		syncCommit := func() {
			t.Helper()
			var result string
			require.NoError(t, writer.QueryRowContext(ctx, "select mo_ctl('cn','syncCommit','')").Scan(&result))
			require.Contains(t, result, "sync 3 cn services")
			require.NotContains(t, result, "commit ts to 0-0")
		}
		exec(writer, "create database "+quote(name))
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 30*time.Second)
			defer stop()
			_, _ = writer.ExecContext(cleanup, "drop table if exists "+quote(name)+"."+quote("%!%p2%!%bad_old"))
			_, err := writer.ExecContext(cleanup, "drop database "+quote(name))
			require.NoError(t, err)
		}()
		exec(writer, "use "+quote(name))

		id := func(conn *sql.Conn, table string) uint64 {
			t.Helper()
			var result uint64
			err := conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?", name, table).Scan(&result)
			require.NoError(t, err)
			return result
		}
		checkNames := func(conn *sql.Conn, expected map[string]uint64) {
			t.Helper()
			rows, err := conn.QueryContext(ctx, `select relname, rel_id from mo_catalog.mo_tables
where reldatabase=? and relname in ('legacy','legacy_backup','replacement')`, name)
			require.NoError(t, err)
			defer rows.Close()
			actual := make(map[string]uint64)
			for rows.Next() {
				var table string
				var tableID uint64
				require.NoError(t, rows.Scan(&table, &tableID))
				require.NotContains(t, actual, table)
				actual[table] = tableID
			}
			require.NoError(t, rows.Err())
			require.Equal(t, expected, actual)
		}
		index := func(conn *sql.Conn, table, partition, key string) string {
			t.Helper()
			var result string
			err := conn.QueryRowContext(ctx, `select distinct i.index_table_name
from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on t.rel_id=i.table_id
left join mo_catalog.mo_partition_tables p on p.partition_id=t.rel_id
where t.reldatabase=? and i.name=? and ((?='' and t.relname=?) or
  (p.partition_name=? and p.primary_table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname=?)))`,
				name, key, partition, table, partition, name, table).Scan(&result)
			require.NoError(t, err, table, partition, key)
			return result
		}
		physicalKey := func(conn *sql.Conn, table string, owner int) string {
			t.Helper()
			var result string
			err := conn.QueryRowContext(ctx, "select hex(__mo_index_idx_col) from "+quote(table)+" where __mo_index_pri_col=?", owner).Scan(&result)
			require.NoError(t, err)
			return result
		}
		count := func(conn *sql.Conn, query string) int {
			t.Helper()
			var result int
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&result), query)
			return result
		}

		partition2 := " partition by range(id) (partition p0 values less than (10), partition p1 values less than (20))"
		columns := "id int primary key,e enum('','red','green'),s set('','a','b'),n int"
		values := "(1,'invalid',0,100),(2,'',1,200),(3,'red',2,300),(4,null,null,null),(11,'green',4,400)"
		exec(writer, "create table legacy ("+columns+",key sn(n))"+partition2)
		exec(writer, "insert ignore into legacy values "+values)
		exec(writer, "create index ie on legacy(e,s)")
		require.Zero(t, count(writer, `select count(*) from mo_catalog.mo_indexes where table_id=`+fmt.Sprint(id(writer, "legacy"))+` and name='ie'`))
		for _, p := range []string{"p0", "p1"} {
			require.NotEmpty(t, index(writer, "legacy", p, "ie"))
		}

		exec(writer, "create table replacement ("+columns+",key sn(n),key ie(e,s))"+partition2)
		// An empty SET member and an empty SET have the same display value.
		// Copy the bitmap so both values, and NULL, remain distinguishable.
		exec(writer, "insert into replacement(id,e,s,n) select id,e,s+0,n from legacy use index()")
		require.Zero(t, count(writer, `select count(*) from legacy as o use index()
left join replacement as n use index() on o.id=n.id
where n.id is null or not (o.e+0 <=> n.e+0) or not (o.s+0 <=> n.s+0) or not (o.n <=> n.n)`))
		require.Zero(t, count(writer, `select count(*) from replacement as n use index()
left join legacy as o use index() on o.id=n.id where o.id is null`))
		exec(writer, "create table reference_t ("+columns+",key sn(n),key ie(e,s))")
		exec(writer, "insert ignore into reference_t values "+values+",(21,'red',1,500)")
		for _, item := range []struct {
			part  string
			owner int
		}{{"p0", 1}, {"p0", 2}, {"p0", 3}, {"p0", 4}, {"p1", 11}} {
			for _, key := range []string{"ie", "sn"} {
				require.Equal(t, physicalKey(writer, index(writer, "reference_t", "", key), item.owner),
					physicalKey(writer, index(writer, "replacement", item.part, key), item.owner))
			}
		}
		oldID, newID := id(writer, "legacy"), id(writer, "replacement")
		exec(writer, "rename table legacy to legacy_backup, replacement to legacy")
		// Independent CNs may use older snapshots until their commit watermark
		// is synchronized. Check the complete mapping once on every CN.
		syncCommit()
		reader, err := db1.Conn(ctx)
		require.NoError(t, err)
		defer reader.Close()
		reader2, err := db2.Conn(ctx)
		require.NoError(t, err)
		defer reader2.Close()
		exec(reader, "use "+quote(name))
		exec(reader2, "use "+quote(name))
		for _, conn := range []*sql.Conn{writer, reader, reader2} {
			checkNames(conn, map[string]uint64{"legacy": newID, "legacy_backup": oldID})
			require.Equal(t, 5, count(conn, "select count(*) from legacy use index()"))
			require.Equal(t, physicalKey(writer, index(writer, "reference_t", "", "ie"), 2),
				physicalKey(conn, index(conn, "legacy", "p0", "ie"), 2))
		}
		result := exec(reader, "delete from legacy where id=2")
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, affected)
		syncCommit()
		require.Equal(t, 1, count(writer, "select count(*) from legacy_backup use index() where id=2"))
		for _, key := range []string{"ie", "sn"} {
			idx := index(writer, "legacy", "p0", key)
			require.Zero(t, count(writer, "select count(*) from "+quote(idx)+" where __mo_index_pri_col=2"))
			require.Equal(t, physicalKey(writer, index(writer, "reference_t", "", key), 1), physicalKey(writer, idx, 1))
		}

		exec(writer, "alter table legacy add partition (partition p2 values less than (30))")
		syncCommit()
		exec(reader, "insert into legacy values (21,'red',1,500)")
		syncCommit()
		for _, key := range []string{"ie", "sn"} {
			require.Equal(t, physicalKey(writer, index(writer, "reference_t", "", key), 21),
				physicalKey(writer, index(writer, "legacy", "p2", key), 21))
		}
		result = exec(reader, "delete from legacy where id=21")
		affected, err = result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, affected)
		syncCommit()
		for _, key := range []string{"ie", "sn"} {
			require.Zero(t, count(writer, "select count(*) from "+quote(index(writer, "legacy", "p2", key))+" where __mo_index_pri_col=21"))
		}

		// Fail the second rename after its first two leaf renames. The first
		// table rename and both tables' catalog metadata must roll back too.
		exec(writer, "create table bad_old (id int primary key)"+partition2)
		exec(writer, "create table bad_new (id int primary key) partition by range(id) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than (30))")
		exec(writer, "create table "+quote("%!%p2%!%bad_old")+" (id int primary key)")
		exec(writer, "insert into bad_old values (1)")
		exec(writer, "insert into bad_new values (11),(21)")
		badOldID, badNewID := id(writer, "bad_old"), id(writer, "bad_new")
		_, err = writer.ExecContext(ctx, "rename table bad_old to bad_backup, bad_new to bad_old")
		var duplicate *mysql.MySQLError
		require.ErrorAs(t, err, &duplicate)
		require.EqualValues(t, 1050, duplicate.Number)
		syncCommit()
		require.Equal(t, badOldID, id(reader, "bad_old"))
		require.Equal(t, badNewID, id(reader, "bad_new"))
		require.Equal(t, 1, count(reader, "select count(*) from bad_old use index()"))
		require.Equal(t, 2, count(reader, "select count(*) from bad_new use index()"))
		require.Zero(t, count(reader, "select count(*) from mo_catalog.mo_tables where reldatabase='"+name+"' and relname='bad_backup'"))
		for _, item := range []struct {
			table string
			want  int
		}{{"bad_old", 2}, {"bad_new", 3}} {
			require.Equal(t, item.want, count(reader, "select count(*) from mo_catalog.mo_partition_tables where primary_table_id="+fmt.Sprint(id(reader, item.table))))
		}
	}))
}
