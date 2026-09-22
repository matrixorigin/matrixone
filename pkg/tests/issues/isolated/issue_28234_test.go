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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestIssue28234ExceptAllCluster(t *testing.T) {
	releaseSharedSingleCNCluster(t)
	for _, cnCount := range []int{1, 2} {
		t.Run(fmt.Sprintf("cn=%d", cnCount), func(t *testing.T) {
			cluster, err := embed.StartTestCluster(embed.WithCNCount(cnCount))
			if cluster != nil {
				t.Cleanup(func() { require.NoError(t, cluster.Close()) })
			}
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			cn, err := cluster.GetCNService(0)
			require.NoError(t, err)
			inventory := clusterservice.GetMOCluster(cn.ServiceID())
			refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
			require.True(t, ok)
			var peerAddr string
			require.Eventually(t, func() bool {
				if refresher.Refresh(ctx) != nil {
					return false
				}
				count := 0
				inventory.GetCNService(clusterservice.NewSelector(), func(service metadata.CNService) bool {
					count++
					if service.ServiceID != cn.ServiceID() {
						peerAddr = service.PipelineServiceAddress
					}
					return true
				})
				return count == cnCount
			}, 30*time.Second, 100*time.Millisecond)
			db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false", cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			defer db.Close()
			db.SetMaxOpenConns(1)
			exec := func(query string) {
				t.Helper()
				_, err := db.ExecContext(ctx, query)
				require.NoError(t, err, query)
			}
			exec("create database except_all_cluster")
			exec("use except_all_cluster")
			// Persist two left blocks so multi-CN scheduling has actual remote
			// input, not merely a MULTICN label on local execution.
			const n = objectio.BlockMaxRows * 2
			exec("create table l(k int)")
			exec("create table r(k int)")
			exec(fmt.Sprintf("insert into l select result %% 4 from generate_series(0,%d) g", n-1))
			exec(fmt.Sprintf("insert into r select result %% 4 from generate_series(0,%d) g", n/2-1))
			exec("insert into l values(null),(null)")
			exec("insert into r values(null)")
			for _, table := range []string{"l", "r"} {
				exec("select mo_ctl('dn','flush','except_all_cluster." + table + "')")
			}
			if cnCount == 2 {
				// This fixture is private and closed below: no shared work-state
				// is mutated. Exclude ingress from placement so the subtraction
				// owner itself, not just an input scan, must execute remotely.
				updater, ok := inventory.(clusterservice.CNWorkStateUpdaterWithContext)
				require.True(t, ok)
				require.NoError(t, updater.DebugUpdateCNWorkStateWithContext(ctx, cn.ServiceID(), int(metadata.WorkState_Draining)))
				require.Eventually(t, func() bool {
					if refresher.Refresh(ctx) != nil {
						return false
					}
					count := 0
					local := false
					inventory.GetCNService(clusterservice.NewSelector(), func(service metadata.CNService) bool {
						count++
						local = local || service.ServiceID == cn.ServiceID()
						return true
					})
					return count == 1 && !local
				}, 30*time.Second, 100*time.Millisecond)
			}
			oldForce := plan.GetForceScanOnMultiCN()
			defer plan.SetForceScanOnMultiCN(oldForce)
			plan.SetForceScanOnMultiCN(cnCount == 2)
			for _, tc := range []struct {
				name, set          string
				rows, nonnull, sum int64
			}{
				{"except", "select k from l except all select k from r", n/2 + 1, n / 2, n * 3 / 4},
				{"minus", "select k from l minus all select k from r", n/2 + 1, n / 2, n * 3 / 4},
				{"nested_left", "(select k from l except all select k from r) except all select k from r", 0, 0, 0},
				{"nested_right", "select k from l except all (select k from r except all select k from l)", n + 2, n, n * 3 / 2},
				{"remote_ancestor", "(select k from l except all select k from r) intersect all select k from l", n/2 + 1, n / 2, n * 3 / 4},
				{"distinct_intersect_ancestor", "(select k from l except all select k from r) intersect select k from l", 5, 4, 6},
				{"distinct_minus_ancestor", "(select k from l except all select k from r) except select k from r", 0, 0, 0},
				{"empty_left", "select k from l where k<0 except all select k from r", 0, 0, 0},
				{"empty_right", "select k from l except all select k from r where k<0", n + 2, n, n * 3 / 2},
			} {
				t.Run(tc.name, func(t *testing.T) {
					query := "select count(*),count(k),coalesce(sum(k),0) from (" + tc.set + ") d"
					if cnCount == 2 && (tc.name == "except" || tc.name == "remote_ancestor") {
						physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
						require.NoError(t, err)
						require.NotEmpty(t, peerAddr)
						require.Contains(t, physical.Text, peerAddr)
						require.Contains(t, physical.Text, "minus all")
						t.Logf("executed distributed plan:\n%s", physical.Text)
					}
					var rows, nonnull, sum int64
					require.NoError(t, db.QueryRowContext(ctx, query).Scan(&rows, &nonnull, &sum))
					require.Equal(t, []int64{tc.rows, tc.nonnull, tc.sum}, []int64{rows, nonnull, sum})
				})
			}
			// Assert individual multiplicities, not only total rows and sums.
			rows, err := db.QueryContext(ctx, "select k,count(*) from (select k from l except all select k from r) d group by k order by k")
			require.NoError(t, err)
			defer rows.Close()
			for i := -1; i < 4; i++ {
				require.True(t, rows.Next())
				var key sql.NullInt64
				var count int64
				require.NoError(t, rows.Scan(&key, &count))
				if i == -1 {
					require.False(t, key.Valid)
					require.Equal(t, int64(1), count)
				} else {
					require.Equal(t, sql.NullInt64{Int64: int64(i), Valid: true}, key)
					require.Equal(t, int64(n/8), count)
				}
			}
			require.False(t, rows.Next())
			require.NoError(t, rows.Err())
			require.NoError(t, rows.Close())
			// Early downstream termination must not strand the build/probe
			// producers; the subsequent prepared queries reuse this connection.
			var limited int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from (select k from l except all select k from r limit 1) d").Scan(&limited))
			require.Equal(t, 1, limited)
			stmt, err := db.PrepareContext(ctx, "select count(*) from (select k from l where k=? except all select k from r where k=?) d")
			require.NoError(t, err)
			defer stmt.Close()
			for _, tc := range []struct{ left, right, want int64 }{{1, 1, n / 8}, {1, 2, n / 4}, {2, 2, n / 8}} {
				var count int64
				require.NoError(t, stmt.QueryRowContext(ctx, tc.left, tc.right).Scan(&count))
				require.Equal(t, tc.want, count)
			}
		})
	}
}
