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
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

// Three persisted rows and the existing shared two-CN fixture distinguish true
// NULL from a ROLLUP sentinel through a real remote sender and receiver.
func TestRollupRemoteGroupingProvenance(t *testing.T) {
	var invalidationErr error
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
				require.Empty(t, queryStringRows(t, ctx, db,
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

func TestRollupEmptyInput(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		const name = "rollup_empty_input"
		defer cleanupTestDatabases(t, db, name)
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "create table src(k int,v int)")
		const rolled = "select k,sum(v) s,count(*) c,grouping(k) g from src group by k with rollup"
		for _, test := range []struct{ name, query string }{
			{"empty table", rolled},
			{"constant false", "select k from src where 1=0 group by k with rollup"},
			{"functional spelling", "select k,sum(v) from src group by rollup(k)"},
			{"no user aggregate", "select k,grouping(k) from src group by k with rollup"},
			{"having zero", rolled + " having count(*)=0"},
			{"having OR", rolled + " having count(*)=0 or sum(v)>0"},
			{"window", "select k,sum(v),row_number() over(order by sum(v)) from src group by k with rollup"},
			{"empty SAMPLE with rollup", "select k,sample(v,1 rows) from src group by k with rollup"},
			{"empty SAMPLE functional", "select k,sample(v,1 rows) from src group by rollup(k)"},
			{"alias with window", "select k as x,sum(v) as s,row_number() over(order by sum(v)) as rn from src group by x with rollup"},
			{"derived", "select * from (" + rolled + ") r"},
			{"CTE reused", "with r as (" + rolled + ") select * from r union all select * from r"},
		} {
			t.Run(test.name, func(t *testing.T) {
				require.Empty(t, queryStringRows(t, ctx, db, test.query))
			})
		}
		t.Run("SAMPLE keeps its sampling path", func(t *testing.T) {
			execSQLDB(t, ctx, db, "insert into src values(1,10)")
			t.Cleanup(func() { execSQLDB(t, ctx, db, "delete from src") })
			for _, spelling := range []string{"group by k with rollup", "group by rollup(k)"} {
				for _, sampling := range []string{"1 rows", "100 percent"} {
					query := "select k,sample(v," + sampling + ") from src " + spelling
					require.Equal(t, [][]string{{"1", "10"}, {"1", "10"}}, queryStringRows(t, ctx, db, query))
				}
				require.Empty(t, queryStringRows(t, ctx, db,
					"select k,sample(v,1 rows) from src where false "+spelling))
			}
			require.Equal(t, [][]string{{"1", "10"}}, queryStringRows(t, ctx, db,
				"select k,sample(v,1 rows) from src group by k"))
			require.Equal(t, [][]string{{"1", "10"}, {"1", "10"}}, queryStringRows(t, ctx, db,
				"select k,sample(v,1 rows) from src group by grouping sets ((k),())"))
			_, err := db.ExecContext(ctx, "select k,sample(v,1 rows),count(*) from src group by k with rollup")
			require.ErrorContains(t, err, "cannot fixed non-scalar function and scalar function in the same query")
		})
		require.Equal(t, [][]string{{"99", "99", "99", "0"}}, queryStringRows(t, ctx, db, rolled+" union all select 99,99,99,0"))
		require.Equal(t, [][]string{{"NULL", "0"}}, queryStringRows(t, ctx, db, "select sum(v),count(*) from src"))
		require.Equal(t, [][]string{{"NULL", "NULL", "0", "1"}}, queryStringRows(t, ctx, db,
			"select k,sum(v),count(*),grouping(k) from src group by grouping sets ((k),())"))
		require.Equal(t, [][]string{{"NULL", "0"}}, queryStringRows(t, ctx, db, "select k,count(*) from src group by cube(k)"))
		execSQLDB(t, ctx, db, "create table copied as "+rolled)
		require.Equal(t, [][]string{{"0"}}, queryStringRows(t, ctx, db, "select count(*) from copied"))
		execSQLDB(t, ctx, db, "insert into copied "+rolled)
		require.Equal(t, [][]string{{"0"}}, queryStringRows(t, ctx, db, "select count(*) from copied"))

		// A real NULL row must retain both groups even though COUNT(v) is zero.
		execSQLDB(t, ctx, db, "insert into src values(null,null)")
		require.Equal(t, [][]string{{"NULL", "0", "1", "0"}, {"NULL", "1", "1", "0"}}, queryStringRows(t, ctx, db,
			"select k,grouping(k),count(*),count(v) from src group by k with rollup order by grouping(k)"))
		require.Equal(t, [][]string{{"NULL", "0"}, {"NULL", "0"}}, queryStringRows(t, ctx, db,
			"select k,count(v) from src group by k with rollup having count(v)=0 or count(*)=0"))
		execSQLDB(t, ctx, db, "insert into src values(1,10)")
		require.Equal(t, [][]string{{"1", "10"}, {"NULL", "10"}}, queryStringRows(t, ctx, db,
			"select k,sum(v) s from src group by k with rollup having s>0 order by grouping(k),k"))

		// Reuse the same prepared plan across empty -> nonempty -> empty input.
		stmt, err := db.PrepareContext(ctx, "select count(*) from (select k from src where v>? group by k with rollup) r")
		require.NoError(t, err)
		defer stmt.Close()
		for _, test := range []struct{ cutoff, rows int }{{20, 0}, {0, 2}, {20, 0}} {
			var rows int
			require.NoError(t, stmt.QueryRowContext(ctx, test.cutoff).Scan(&rows))
			require.Equal(t, test.rows, rows)
		}
		t.Run("shared computation rollback", func(t *testing.T) {
			cn, err := c.GetCNService(0)
			require.NoError(t, err)
			rt := runtime.ServiceRuntime(cn.ServiceID())
			old, existed := rt.GetGlobalVariables("optimizer_hints")
			rt.SetGlobalVariables("optimizer_hints", "sharedComputation=1")
			defer func() {
				if existed {
					rt.SetGlobalVariables("optimizer_hints", old)
				} else {
					rt.CompareAndDeleteGlobalVariables("optimizer_hints", "sharedComputation=1")
				}
			}()
			require.Empty(t, queryStringRows(t, ctx, db,
				"select k,count(v),grouping(k) from src where v>20 group by k with rollup"))
			require.Equal(t, [][]string{{"NULL", "0", "0"}, {"1", "1", "0"}, {"NULL", "1", "1"}}, queryStringRows(t, ctx, db,
				"select k,count(v),grouping(k) from src group by k with rollup order by grouping(k),k"))
		})
	})
}
