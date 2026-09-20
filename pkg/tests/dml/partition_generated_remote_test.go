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
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/partitionservice"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestPartitionGeneratedRemoteScan(t *testing.T) {
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
		_, err = waitForCNReadiness(ctx, cnWorkStatePollInterval, inventory, refresher, cn.ServiceID(), peer.ServiceID())
		require.NoError(t, err)
		peerCluster := clusterservice.GetMOCluster(peer.ServiceID())
		_, err = waitForCNReadiness(ctx, cnWorkStatePollInterval, peerCluster.(cnWorkStateInventory), peerCluster.(clusterservice.AuthoritativeRefresher), peer.ServiceID(), cn.ServiceID())
		require.NoError(t, err)
		db := openRetestSQLDB(t, c)
		defer db.Close()
		peerDB := openRetestSQLDBForCN(t, c, 1)
		defer peerDB.Close()
		clients := []*sql.DB{db, peerDB}
		const name = "partition_generated_remote"
		defer cleanupTestDatabases(t, db, name)
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, peerDB, "use "+name)
		execSQLDB(t, ctx, db, `create table src (
   id int, company_id int, amount decimal(10,2), tax_rate decimal(5,2),
   amount_with_tax decimal(10,2) as (amount * (1 + tax_rate)), created_date date
  ) partition by range (year(created_date)) (
   partition p2022 values less than (2023), partition p2023 values less than (2024),
   partition pmax values less than maxvalue)`)
		execSQLDB(t, ctx, db, "insert into src(id,company_id,amount,tax_rate,created_date) values (1,10,100,0.1,'2022-06-01'),(2,20,50,0.2,'2023-06-01'),(3,30,80,0.25,'2024-06-01')")
		oldForce := plan.GetForceScanOnMultiCN()
		defer plan.SetForceScanOnMultiCN(oldForce)
		plan.SetForceScanOnMultiCN(true)
		const projection = "select id,company_id,amount,tax_rate,amount_with_tax,created_date from src"
		const query = projection + " order by id"
		physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan "+query)
		require.NoError(t, err)
		require.Contains(t, strings.ToUpper(physical.ColumnName), "PHYPLAN ON MULTICN(")
		require.Equal(t, 2, strings.Count(physical.Text, "DataSource: "+name+".src"))
		want := [][]string{{"1", "10", "100.00", "0.10", "110.00", "2022-06-01"}, {"2", "20", "50.00", "0.20", "60.00", "2023-06-01"}, {"3", "30", "80.00", "0.25", "100.00", "2024-06-01"}}
		// Default scheduling sorts the same two workers by identity. Querying
		// from both ingress CNs makes every assigned persisted partition remote
		// in one execution, independent of random object IDs, without a third CN.
		checkBoth := func(t *testing.T) {
			t.Helper()
			for _, conn := range clients {
				require.Equal(t, want, queryStringRows(t, ctx, conn, query))
			}
		}
		t.Run("memory", checkBoth)
		flush := func(part string) {
			plan.SetForceScanOnMultiCN(false)
			defer plan.SetForceScanOnMultiCN(true)
			execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+"."+partitionservice.GetPartitionTableName("src", part)+"')")
		}
		flush("p2022")
		t.Run("mixed", checkBoth)
		flush("p2023")
		flush("pmax")
		t.Run("persisted", checkBoth)
		for i, conn := range clients {
			t.Run(fmt.Sprintf("transaction-cn%d", i), func(t *testing.T) {
				stmt, err := conn.PrepareContext(ctx, query)
				require.NoError(t, err)
				defer stmt.Close()
				preparedRows := func() [][]string {
					rows, err := stmt.QueryContext(ctx)
					require.NoError(t, err)
					defer rows.Close()
					var result [][]string
					for rows.Next() {
						values := make([]string, 6)
						require.NoError(t, rows.Scan(&values[0], &values[1], &values[2], &values[3], &values[4], &values[5]))
						result = append(result, values)
					}
					require.NoError(t, rows.Err())
					return result
				}
				require.Equal(t, want, preparedRows())
				execSQLDB(t, ctx, conn, "begin")
				defer func() {
					cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					_, err := conn.ExecContext(cleanupCtx, "rollback")
					require.NoError(t, err)
				}()
				execSQLDB(t, ctx, conn, "delete from src where id=1")
				require.Equal(t, want[1:], preparedRows())
				require.Equal(t, want, queryStringRows(t, ctx, clients[1-i], query), "other session must not see uncommitted deletes")
				execSQLDB(t, ctx, conn, "rollback")
				require.Equal(t, want, preparedRows())
			})
		}
		plan.SetForceScanOnMultiCN(false)
		execSQLDB(t, ctx, db, "update src set amount=120 where id=1")
		plan.SetForceScanOnMultiCN(true)
		want[0][2], want[0][4] = "120.00", "132.00"
		t.Run("updated", checkBoth)
		for _, conn := range clients {
			require.Empty(t, queryStringRows(t, ctx, conn, projection+" where created_date<'2020-01-01'"))
		}
	})
}
