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
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

// Reuse the shared three-CN fixture. 65536 unique values are needed to cross
// the unchanged 64000 distinct-state shuffle threshold; this is a topology
// boundary test, not a timing assertion or a large ClickBench reproduction.
func TestDistinctBackportMultiCN(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		const name = "distinct_backport_42"
		execSQLRequire(t, ctx, db, "create database "+name)
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			_, err := db.ExecContext(cleanup, "drop database "+name)
			require.NoError(t, err)
		}()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		exec := func(q string) { t.Helper(); _, err := conn.ExecContext(ctx, q); require.NoError(t, err, q) }
		exec("use " + name)
		exec("create table t (g int, k bigint)")
		exec("insert into t select mod(result,128), result from generate_series(1,65536) s")
		exec("select mo_ctl('dn','flush','" + name + ".t')")
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
		}, 30*time.Second, 100*time.Millisecond, "all three CNs must be visible before the distributed plan is compiled")
		query := "select g, count(distinct k), sum(k), count(k), avg(k) from t group by g order by g"
		read := func() []string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, query)
			require.NoError(t, err)
			defer rows.Close()
			var result []string
			for rows.Next() {
				var g, distinct, sum, count int64
				var average float64
				require.NoError(t, rows.Scan(&g, &distinct, &sum, &count, &average))
				require.Equal(t, int64(512), distinct)
				require.Equal(t, int64(512), count)
				expected := int64(128*511*512/2) + 512*g
				if g == 0 {
					expected = 128 * 512 * 513 / 2
				}
				require.Equal(t, expected, sum)
				require.InDelta(t, float64(expected)/512, average, 1e-9)
				result = append(result, fmt.Sprint(g, "/", distinct, "/", sum, "/", count, "/", average))
			}
			require.NoError(t, rows.Err())
			require.Len(t, result, 128)
			return result
		}
		var oldHints string
		require.NoError(t, conn.QueryRowContext(ctx, "select @@session.optimizer_hints").Scan(&oldHints))
		defer func() {
			cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			literal := strings.ReplaceAll(strings.ReplaceAll(oldHints, `\`, `\\`), "'", "''")
			_, err := conn.ExecContext(cleanup, "set session optimizer_hints = '"+literal+"'")
			require.NoError(t, err)
		}()
		exec("set session optimizer_hints='determineShuffle=1'")
		control := read()
		exec("set session optimizer_hints=''")
		require.Equal(t, control, read(), "optimized default-DOP rows must match the no-shuffle control")
		// Runtime evidence: EXPLAIN PHYPLAN ANALYZE executes the distributed graph.
		explain := func(q string) string {
			t.Helper()
			rows, err := conn.QueryContext(ctx, q)
			require.NoError(t, err)
			defer rows.Close()
			var lines []string
			for rows.Next() {
				var line string
				require.NoError(t, rows.Scan(&line))
				lines = append(lines, line)
			}
			require.NoError(t, rows.Err())
			return strings.Join(lines, "\n")
		}
		physical := explain("explain phyplan analyze " + query)
		t.Log(physical)
		require.Contains(t, strings.ToLower(physical), "shuffle")
		addresses := make(map[string]bool)
		for _, match := range regexp.MustCompile(`addr:([^,]+)`).FindAllStringSubmatch(physical, -1) {
			addresses[match[1]] = true
		}
		require.GreaterOrEqual(t, len(addresses), 2, "the executed graph must span multiple CN addresses")
		require.Regexp(t, `shuffle CallNum:[1-9]`, physical, "an executed shuffle must report calls")
		exec("set session max_dop=1")
		require.Equal(t, control, read(), "one worker per CN must preserve logical ownership")
		func() {
			rows, err := conn.QueryContext(ctx, "select r.g, count(distinct l.k) from t l left join (select result as g from generate_series(0,31) s) r on l.g=r.g group by r.g order by r.g")
			require.NoError(t, err)
			defer rows.Close()
			groups, nullGroups := 0, 0
			for rows.Next() {
				var key sql.NullInt64
				var count int64
				require.NoError(t, rows.Scan(&key, &count))
				if key.Valid {
					require.Equal(t, int64(512), count)
				} else {
					nullGroups++
					require.Equal(t, int64(96*512), count)
				}
				groups++
			}
			require.NoError(t, rows.Err())
			require.Equal(t, 33, groups)
			require.Equal(t, 1, nullGroups)
		}()
		// NULL/duplicate and composite-key controls share the same table.
		exec("set session max_dop=4")
		exec("insert into t values(1,1),(1,1),(0,null)")
		var distinct, tuple int64
		require.NoError(t, conn.QueryRowContext(ctx, "select count(distinct k), count(distinct k,g) from t").Scan(&distinct, &tuple))
		require.Equal(t, int64(65536), distinct)
		require.Equal(t, distinct, tuple)
		// Non-COUNT DISTINCT retains the original single-stage contract.
		var sum int64
		require.NoError(t, conn.QueryRowContext(ctx, "select sum(distinct k) from t").Scan(&sum))
		require.Equal(t, int64(65536*65537/2), sum)
	})
}
