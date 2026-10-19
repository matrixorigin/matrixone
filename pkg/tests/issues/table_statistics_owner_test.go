// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTableStatisticsOwnerPolicySQL(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		conns := make([]*sql.Conn, 2)
		services := make([]cnservice.Service, 2)
		controls := make([]interface{ HandleMoTableStatsCtl(string) string }, 2)
		for i := range conns {
			cn, err := c.GetCNService(i)
			require.NoError(t, err)
			services[i] = cn.RawService().(cnservice.Service)
			controls[i] = services[i].GetEngine().(interface{ HandleMoTableStatsCtl(string) string })
			var move, old, force bool
			_, err = fmt.Sscanf(controls[i].HandleMoTableStatsCtl("echo_current_setting:true"), "move_on(%t), use_old_impl(%t), force_update(%t)", &move, &old, &force)
			require.NoError(t, err)
			defer func(i int, move, old, force bool) {
				controls[i].HandleMoTableStatsCtl(fmt.Sprintf("move_on:%t", move))
				controls[i].HandleMoTableStatsCtl(fmt.Sprintf("use_old_impl:%t", old))
				controls[i].HandleMoTableStatsCtl(fmt.Sprintf("force_update:%t", force))
			}(i, move, old, force)
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer db.Close()
			conns[i], err = db.Conn(ctx)
			require.NoError(t, err)
			defer conns[i].Close()
			_, err = conns[i].ExecContext(ctx, "set mo_table_stats.use_old_impl='no'")
			require.NoError(t, err)
			controls[i].HandleMoTableStatsCtl("force_update:false")
		}
		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		_, err := conns[0].ExecContext(ctx, "create database `"+dbName+"`")
		require.NoError(t, err)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_, err := conns[0].ExecContext(cleanup, "drop database `"+dbName+"`")
			assert.NoError(t, err)
		}()
		_, err = conns[0].ExecContext(ctx, "create table `"+dbName+"`.t (id int primary key)")
		require.NoError(t, err)
		_, err = conns[0].ExecContext(ctx, "insert into `"+dbName+"`.t values (1),(2),(3)")
		require.NoError(t, err)
		frontier := services[0].GetTxnClient().GetLatestCommitTS()
		_, err = services[1].GetTxnClient().WaitLogTailAppliedAt(ctx, frontier)
		require.NoError(t, err)
		query := fmt.Sprintf("select mo_table_rows('%s','t'), mo_table_size('%s','t')", dbName, dbName)
		for _, oldOwner := range []int{0, 1} {
			controls[oldOwner].HandleMoTableStatsCtl("use_old_impl:true")
			controls[1-oldOwner].HandleMoTableStatsCtl("use_old_impl:false")
			var rows, size int64
			require.NoError(t, conns[oldOwner].QueryRowContext(ctx, query).Scan(&rows, &size))
			require.Equal(t, int64(3), rows)
			require.Positive(t, size)
			var otherRows, otherSize int64
			require.NoError(t, conns[1-oldOwner].QueryRowContext(ctx, query).Scan(&otherRows, &otherSize))
			require.GreaterOrEqual(t, otherRows, int64(0))
			require.GreaterOrEqual(t, otherSize, int64(0))
			require.NoError(t, conns[oldOwner].QueryRowContext(ctx, query).Scan(&rows, &size))
			require.Equal(t, int64(3), rows, "querying the new-mode CN must not change the old-mode CN policy")
			_, err = conns[1-oldOwner].ExecContext(ctx, "set mo_table_stats.use_old_impl='yes'")
			require.NoError(t, err)
			require.NoError(t, conns[1-oldOwner].QueryRowContext(ctx, query).Scan(&otherRows, &otherSize))
			require.Equal(t, rows, otherRows)
			require.Equal(t, size, otherSize)
			_, err = conns[1-oldOwner].ExecContext(ctx, "set mo_table_stats.use_old_impl='no'")
			require.NoError(t, err)

		}
	})
}

// The catalog projection resolves AUTO_INCREMENT first; index-size lookups
// then reuse the same transaction after the projection's vectors are released.
// The deterministic buffer-reuse oracle lives in disttae's ownership test.
func TestShowTableStatusWithIndexes(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		dbName := strings.ToLower(testutils.GetDatabaseName(t))
		_, err = conn.ExecContext(ctx, "create database `"+dbName+"`")
		require.NoError(t, err)
		defer func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanup, "rollback")
			assert.NoError(t, err)
			_, err = conn.ExecContext(cleanup, "drop database `"+dbName+"`")
			assert.NoError(t, err)
			var remaining int
			assert.NoError(t, conn.QueryRowContext(cleanup, "select count(*) from mo_catalog.mo_database where datname = ?", dbName).Scan(&remaining))
			assert.Zero(t, remaining)
		}()
		for _, statement := range []string{
			"use `" + dbName + "`",
			"create table indexed_auto_increment_table(id bigint primary key auto_increment, v int, key idx_v(v)) auto_increment=10 auto_id_cache=1",
			"create table ordinary_indexed_table(id bigint primary key, v int, unique key idx_v(v))",
			"begin",
		} {
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err)
		}
		// Repeat in one transaction so the cache must remain usable across SQL
		// statement cleanup. Two empty tables suffice; no load/stress fixture.
		for round := 0; round < 2; round++ {
			func() {
				rows, err := conn.QueryContext(ctx, "show table status")
				require.NoError(t, err)
				defer rows.Close()
				columns, err := rows.Columns()
				require.NoError(t, err)
				for i := range columns {
					columns[i] = strings.ToLower(columns[i])
				}
				require.Subset(t, columns, []string{"name", "auto_increment", "index_length"})
				observed := make(map[string]int64)
				for rows.Next() {
					var name string
					var next, indexSize int64
					dest := make([]any, len(columns))
					for i, col := range columns {
						dest[i] = new(any)
						switch col {
						case "name":
							dest[i] = &name
						case "auto_increment":
							dest[i] = &next
						case "index_length":
							dest[i] = &indexSize
						}
					}
					require.NoError(t, rows.Scan(dest...))
					require.GreaterOrEqual(t, indexSize, int64(0))
					require.NotContains(t, observed, name)
					observed[name] = next
				}
				require.NoError(t, rows.Err())
				require.Equal(t, map[string]int64{"indexed_auto_increment_table": 10, "ordinary_indexed_table": 0}, observed)
			}()
		}
		_, err = conn.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		var next uint64
		require.NoError(t, conn.QueryRowContext(ctx, "select internal_auto_increment(database(), 'indexed_auto_increment_table')").Scan(&next))
		require.Equal(t, uint64(10), next, "SHOW must not allocate IDs")
	})
}
