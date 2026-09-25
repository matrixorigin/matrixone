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

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue29226VectorScanAcrossCoordinators(t *testing.T) {
	embed.RunBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		dbs := make([]*sql.DB, 2)
		addrs := make([]string, 2)
		for i := range dbs {
			cn, err := cluster.GetCNService(i)
			require.NoError(t, err)
			inventory := clusterservice.GetMOCluster(cn.ServiceID())
			require.Eventually(t, func() bool {
				inventory.ForceRefresh(true)
				count := 0
				inventory.GetCNService(clusterservice.NewSelector(), func(service metadata.CNService) bool {
					count++
					if service.ServiceID == cn.ServiceID() {
						addrs[i] = service.PipelineServiceAddress
					}
					return true
				})
				return count == 2
			}, 20*time.Second, 100*time.Millisecond, "both CNs must be discoverable")
			cfg := cn.GetServiceConfig()
			require.NotEmpty(t, addrs[i])
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cfg.CN.Frontend.Port))
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)
			dbs[i] = db
		}
		name := strings.ToLower(testutils.GetDatabaseName(t))
		execSQLRequire(t, ctx, dbs[0], "create database `"+name+"`")
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			execSQLRequire(t, cleanup, dbs[0], "drop database if exists `"+name+"`")
		}()
		for _, db := range dbs {
			for _, statement := range []string{
				"use `" + name + "`", "set experimental_ivf_index=1",
				"set ivf_preload_entries=0", "set probe_limit=1",
				"set session optimizer_hints='execType=2'",
			} {
				execSQLRequire(t, ctx, db, statement)
			}
		}
		for _, statement := range []string{
			"create table docs(id bigint primary key, v vecf32(2))",
			"insert into docs values (1,'[1,0]'),(2,'[2,0]'),(3,'[3,0]'),(4,'[4,0]')",
			"create index idx using ivfflat on docs(v) lists=1 op_type 'vector_l2_ops'",
		} {
			execSQLRequire(t, ctx, dbs[0], statement)
		}
		var entries string
		require.NoError(t, dbs[0].QueryRowContext(ctx, `select distinct i.index_table_name
			from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id
			where t.reldatabase=? and t.relname='docs' and i.name='idx' and i.algo_table_type='entries'`, name).Scan(&entries))
		flush := "select mo_ctl('dn','flush','" + name + "." + entries + "')"
		execSQLRequire(t, ctx, dbs[0], flush)
		execSQLRequire(t, ctx, dbs[0], "insert into docs values (5,'[5,0]')")
		execSQLRequire(t, ctx, dbs[0], flush)
		var count float64
		var objects int64
		require.NoError(t, dbs[0].QueryRowContext(ctx, fmt.Sprintf(
			"select table_cnt, accurate_object_number from table_stats('%s.%s','refresh','full') g", name, entries)).Scan(&count, &objects))
		require.Equal(t, float64(5), count)
		require.GreaterOrEqual(t, objects, int64(2))
		// Keep committed in-memory data alongside the explicitly flushed objects.
		execSQLRequire(t, ctx, dbs[0], "insert into docs values (6,'[6,0]')")
		query := "select id from docs order by l2_distance(v,'[0,0]') limit 10"
		for _, ingress := range []int{0, 1, 0} {
			db := dbs[ingress]
			logical, err := testutils.QueryTextResult(ctx, db, "explain "+query)
			require.NoError(t, err)
			require.Contains(t, logical.Text, "Vector Index Scan")
			require.Equal(t, []int64{1, 2, 3, 4, 5, 6}, queryInt64Rows(t, ctx, db, query))
			physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
			require.NoError(t, err)
			require.Contains(t, strings.ToLower(physical.ColumnName), "multicn")
			require.Contains(t, physical.Text, addrs[1-ingress], "the peer must execute the scan")
			t.Logf("ingress=%d executed plan:\n%s", ingress, physical.Text)
		}
		// A writable workspace must remain local regardless of the coordinator's bucket.
		for _, tc := range []struct {
			ingress int
			commit  bool
		}{{0, false}, {1, false}, {1, true}} {
			db := dbs[tc.ingress]
			func() {
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				for _, statement := range []string{
					"insert into docs values (7,'[7,0]')", "update docs set v='[8,0]' where id=1", "delete from docs where id=2",
				} {
					_, err := tx.ExecContext(ctx, statement)
					require.NoError(t, err)
				}
				rows, err := tx.QueryContext(ctx, query)
				require.NoError(t, err)
				defer rows.Close()
				var got []int64
				for rows.Next() {
					var id int64
					require.NoError(t, rows.Scan(&id))
					got = append(got, id)
				}
				require.NoError(t, rows.Err())
				require.Equal(t, []int64{3, 4, 5, 6, 7, 1}, got)
				if tc.commit {
					require.NoError(t, tx.Commit())
				}
			}()
			want := []int64{1, 2, 3, 4, 5, 6}
			if tc.commit {
				want = []int64{3, 4, 5, 6, 7, 1}
			}
			for _, observer := range dbs {
				require.Equal(t, want, queryInt64Rows(t, ctx, observer, query))
			}
		}
	})
}
