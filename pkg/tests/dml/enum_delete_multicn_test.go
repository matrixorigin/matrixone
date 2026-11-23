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

package dml

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Exercise the independent index readers on actual remote CNs. Each index has
// a duplicate physical row for one owner, as can occur in released data.
func TestEnumDeleteMultiCN(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		name := strings.ToLower(testutils.GetDatabaseName(t))
		exec := func(query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		exec("create database `" + name + "`")
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := db.ExecContext(cleanup, "drop database `"+name+"`")
			require.NoError(t, err)
		}()
		exec("use `" + name + "`")
		exec("create table t(id int primary key, e enum('red','green'), n int, key inum(n), unique key uk(e,n))")
		exec("insert into t select result,'red',result from generate_series(1,24576) g")
		exec("alter table t add key ie(e)")
		exec("insert into t select result,'green',result from generate_series(24577,49152) g")

		indexName := func(index string) string {
			t.Helper()
			var hidden string
			err := conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name=? and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t') limit 1", index, name).Scan(&hidden)
			require.NoError(t, err)
			return "`" + strings.ReplaceAll(hidden, "`", "``") + "`"
		}
		ie, inum, uk := indexName("ie"), indexName("inum"), indexName("uk")
		for _, hidden := range []string{ie, inum} {
			exec("insert into " + hidden + " (__mo_index_idx_col,__mo_index_pri_col) select __mo_index_idx_col,__mo_index_pri_col from " + hidden + " where __mo_index_pri_col=1")
		}
		count := func(query string) int {
			t.Helper()
			var n int
			require.NoError(t, conn.QueryRowContext(ctx, query).Scan(&n))
			return n
		}
		for _, hidden := range []string{ie, inum} {
			require.Equal(t, 2, count("select count(*) from "+hidden+" where __mo_index_pri_col=1"), hidden)
		}
		for _, table := range []string{"t", ie, inum, uk} {
			exec("select mo_ctl('dn','flush','" + name + "." + strings.Trim(table, "`") + "')")
		}
		exec("analyze table t")

		oldForce := plan.GetForceScanOnMultiCN()
		plan.SetForceScanOnMultiCN(true)
		defer plan.SetForceScanOnMultiCN(oldForce)
		cluster := clusterservice.GetMOCluster(cn.ServiceID())
		refresher, ok := cluster.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		require.Eventually(t, func() bool {
			if refresher.Refresh(ctx) != nil {
				return false
			}
			count := 0
			cluster.GetCNService(clusterservice.NewSelector(), func(metadata.CNService) bool { count++; return true })
			return count == 3
		}, 30*time.Second, 100*time.Millisecond)

		rows, err := conn.QueryContext(ctx, "explain phyplan analyze delete from t where id between 1 and 10000")
		require.NoError(t, err)
		defer rows.Close()
		var lines []string
		for rows.Next() {
			var line string
			require.NoError(t, rows.Scan(&line))
			lines = append(lines, line)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		physical := strings.Join(lines, "\n")
		addresses := make(map[string]bool)
		for _, match := range regexp.MustCompile(`addr:([^,]+)`).FindAllStringSubmatch(physical, -1) {
			addresses[match[1]] = true
		}
		t.Logf("executed physical plan:\n%s", physical)
		require.GreaterOrEqual(t, len(addresses), 2, "DELETE must execute on multiple CNs")
		require.Regexp(t, `AffectedRows:\s*10000\b`, physical)
		for _, hidden := range []string{ie, inum} {
			pattern := `(?m)Scope \d+ \(Magic: Remote, addr:([^,]+),[^\n]*\n\s*DataSource: [^\n]*\.` + regexp.QuoteMeta(strings.Trim(hidden, "`")) + `\[`
			scans := make(map[string]bool)
			for _, match := range regexp.MustCompile(pattern).FindAllStringSubmatch(physical, -1) {
				scans[match[1]] = true
			}
			require.GreaterOrEqual(t, len(scans), 2, "ordinary index scan must be scheduled across CNs: %s", hidden)
			require.Equal(t, 0, count("select count(*) from "+hidden+" where __mo_index_pri_col=1"), hidden)
		}
		for _, table := range []string{"t", ie, inum, uk} {
			require.Equal(t, 39152, count("select count(*) from "+table), table)
		}
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer tx.Rollback()
		result, err := tx.ExecContext(ctx, "delete from t where id between 10001 and 20000")
		require.NoError(t, err)
		affected, err := result.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 10000, affected)
		for _, table := range []string{"t", ie, inum, uk} {
			var n int
			require.NoError(t, tx.QueryRowContext(ctx, "select count(*) from "+table).Scan(&n))
			require.Equal(t, 29152, n, table)
		}
		require.NoError(t, tx.Rollback())
		for _, table := range []string{"t", ie, inum, uk} {
			require.Equal(t, 39152, count("select count(*) from "+table), table)
		}
	})
}
