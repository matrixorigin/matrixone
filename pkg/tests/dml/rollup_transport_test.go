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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Three persisted rows and the existing shared two-CN fixture distinguish true
// NULL from a ROLLUP sentinel through a real remote sender and receiver.
func TestRollupRemoteGroupingProvenance(t *testing.T) {
	var invalidationErr error
	defer func() {
		if invalidationErr != nil {
			t.Errorf("discarding shared fixture: %v", invalidationErr)
			if err := embed.CloseBaseClusterTests(); err != nil {
				t.Errorf("close invalid fixture: %v", err)
			}
		}
	}()
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		cluster := clusterservice.GetMOCluster(cn.ServiceID())
		inventory, ok := cluster.(cnWorkStateInventory)
		require.True(t, ok)
		refresher, ok := cluster.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		readiness, err := waitForCNReadiness(ctx, cnWorkStatePollInterval, inventory, refresher, cn.ServiceID(), peer.ServiceID())
		require.NoError(t, err)
		db := openRetestSQLDB(t, c)
		defer db.Close()
		const name = "rollup_remote_grouping"
		defer func() {
			if invalidationErr == nil {
				cleanupTestDatabases(t, db, name)
			}
		}()
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "create table src(k int, v int not null)")
		execSQLDB(t, ctx, db, "insert into src values (1,10),(null,5),(2,20)")
		execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".src')")
		oldForce := plan.GetForceScanOnMultiCN()
		defer plan.SetForceScanOnMultiCN(oldForce)
		plan.SetForceScanOnMultiCN(true)
		err = withCNDraining(ctx, inventory, refresher, cn.ServiceID(),
			[]string{cn.ServiceID(), peer.ServiceID()},
			func(err error) { invalidationErr = err }, func() {
				const query = "select k,grouping(k),count(*),sum(v) from src group by k with rollup order by grouping(k),k"
				physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze "+query)
				require.NoError(t, err)
				require.Contains(t, physical.Text, readiness.peerAddr)
				require.Equal(t, [][]string{{"NULL", "0", "1", "5"}, {"1", "0", "1", "10"}, {"2", "0", "1", "20"}, {"NULL", "1", "3", "35"}}, queryStringRows(t, ctx, db, query))
				// NOT NULL keys still acquire ROLLUP NULLs; ranking consumes the
				// combined result rather than restarting at each grouping branch.
				require.Equal(t, [][]string{{"NULL", "35", "1"}, {"20", "20", "2"}, {"10", "10", "3"}, {"5", "5", "4"}}, queryStringRows(t, ctx, db,
					"select v,sum(v),row_number() over(order by sum(v) desc) from src group by v with rollup order by sum(v) desc"))
				require.Equal(t, [][]string{{"NULL", "1", "0", "NULL"}}, queryStringRows(t, ctx, db,
					"select k,grouping(k),count(*),sum(v) from src where v<0 group by k with rollup"))
				require.Empty(t, queryStringRows(t, ctx, db, "select k,count(*) from src where v<0 group by k"))
				require.Equal(t, [][]string{{"1", "35"}}, queryStringRows(t, ctx, db,
					"with rolled as (select k,sum(v) total,grouping(k) g from src group by k with rollup) select count(*),sum(total) from rolled where g=1"))
				require.Equal(t, [][]string{{"<null>", "0", "5"}, {"1", "0", "10"}, {"2", "0", "20"}, {"<null>", "1", "35"}}, queryStringRows(t, ctx, db,
					"select coalesce(cast(k as varchar),'<null>'),grouping(k),sum(v) from src group by k with rollup order by grouping(k),k"))
				require.Equal(t, [][]string{{"NULL", "0", "5"}, {"1", "0", "10"}, {"2", "0", "20"}, {"NULL", "1", "35"}}, queryStringRows(t, ctx, db,
					"select a.k as label,grouping(a.k),sum(a.v) from src a join src b on a.v=b.v group by a.k with rollup order by grouping(a.k),a.k"))
			})
		require.NoError(t, err)
	})
}
