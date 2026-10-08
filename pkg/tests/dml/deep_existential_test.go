// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Reuse the package's two-CN fixture. The process-wide force hook is scoped to
// queries after setup, and this test must not run in parallel with other cases.
func TestDeepExistentialMultiCN(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		cluster := clusterservice.GetMOCluster(cn.ServiceID())
		refresher, ok := cluster.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		var remoteAddr string
		require.Eventually(t, func() bool {
			if refresher.Refresh(ctx) != nil {
				return false
			}
			n := 0
			cluster.GetCNService(clusterservice.NewSelector(), func(service metadata.CNService) bool {
				n++
				if service.ServiceID != cn.ServiceID() {
					remoteAddr = service.PipelineServiceAddress
				}
				return true
			})
			return n == 2
		}, 30*time.Second, 100*time.Millisecond)
		require.NotEmpty(t, remoteAddr)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		name := testutils.GetDatabaseName(t)
		defer cleanupTestDatabases(t, db, name)
		execSQLDB(t, ctx, db, "create database `"+name+"`")
		execSQLDB(t, ctx, db, "use `"+name+"`")
		// Two independent blocks are enough to dispatch work to both CNs. Keep
		// eight nonunique keys so the existential match-group shortcut is still
		// exercised without enumerating I/J witness pairs quadratically.
		const n = objectio.BlockMaxRows * 2
		for _, tab := range []string{"ot", "it", "jt"} {
			execSQLDB(t, ctx, db, "create table "+tab+" (id int, grp int)")
			execSQLDB(t, ctx, db, fmt.Sprintf("insert into %s select result, result%%8 from generate_series(0,%d) g", tab, n-1))
			execSQLDB(t, ctx, db, "insert into "+tab+" values (NULL,NULL)")
		}
		oldForce := plan.GetForceScanOnMultiCN()
		defer plan.SetForceScanOnMultiCN(oldForce)
		plan.SetForceScanOnMultiCN(true)
		and := "exists(select 1 from it i where exists(select 1 from jt j where j.grp=i.grp and j.grp=o.grp))"
		for _, tc := range []struct {
			name, predicate string
			want            int
		}{
			{"semi", and, n},
			{"anti", "not " + and, 1},
			{"anti_gate", "not exists(select 1 from it i where exists(select 1 from jt j where j.grp=i.grp and j.grp=o.grp and o.id=0))", n},
			{"in", "o.grp in(select i.grp from it i where i.grp in(select j.grp from jt j where j.grp=i.grp and j.grp=o.grp))", n},
			{"or", "exists(select 1 from it i where exists(select 1 from jt j where j.grp=i.grp or j.grp=o.grp))", n + 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				query := "select count(*) from ot o where " + tc.predicate
				// Keep one representative execution plan with runtime statistics so
				// this suite still verifies that a distributed plan executes across
				// CNs. The remaining predicates only need plan inspection; ANALYZE
				// would add a full scan before the two result checks below.
				explain := "explain phyplan " + query
				if tc.name == "semi" {
					explain = "explain phyplan analyze " + query
				}
				physical, err := testutils.QueryTextResult(ctx, db, explain)
				require.NoError(t, err)
				require.Contains(t, strings.ToUpper(physical.ColumnName), "MULTICN(")
				// Magic: Remote also names local scan scopes. Require the
				// other CN's pipeline address and a cross-CN receiver, then
				// verify execution consumes all fixture rows below.
				require.Contains(t, physical.Text, remoteAddr)
				require.Contains(t, physical.Text, "cross-cn receiver info:")
				require.NotContains(t, strings.ToLower(physical.Text), "loop join")
				if tc.name == "or" {
					// Keep one independent semantic oracle for the OR rewrite. It
					// need not be repeated for both identical candidate attempts.
					var reference int
					refErr := db.QueryRowContext(ctx, "select count(*) from ot o where ((select count(*)>0 from it i where exists(select 1 from jt j where j.grp=i.grp)) or ((select count(*)>0 from it) and exists(select 1 from jt j where j.grp=o.grp)))").Scan(&reference)
					require.NoError(t, refErr)
					require.Equal(t, tc.want, reference)
				}
				for attempt := range 2 {
					var got int
					err := db.QueryRowContext(ctx, query).Scan(&got)
					if err != nil {
						t.Logf("failed query physical plan: %s", physical.Text)
					}
					require.NoError(t, err, "attempt %d", attempt)
					require.Equal(t, tc.want, got)
				}
			})
		}
		t.Run("cancel streamed output and reuse", func(t *testing.T) {
			func() {
				queryCtx, cancelQuery := context.WithCancel(ctx)
				defer cancelQuery()
				rows, err := db.QueryContext(queryCtx, "select o.id, repeat('x',128) from ot o where "+and)
				require.NoError(t, err)
				defer func() {
					if err := rows.Close(); err != nil {
						require.ErrorIs(t, err, context.Canceled)
					}
					if err := rows.Err(); err != nil {
						require.ErrorIs(t, err, context.Canceled)
					}
				}()
				require.True(t, rows.Next())
				// Cancel only after observing an actual row from the running
				// distributed query, without sleeping or racing query startup.
				cancelQuery()
			}()
			execSQLDB(t, ctx, db, "use `"+name+"`")
			var got int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from ot o where "+and).Scan(&got))
			require.Equal(t, n, got)
		})
	})
}
