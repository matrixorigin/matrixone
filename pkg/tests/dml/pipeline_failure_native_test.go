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

package dml

import (
	"context"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// A real row-dependent execution error, not an injected wire terminal. Compare
// relational identities and early-stop controls over the public SQL boundary.
func TestRemoteNativeFailureSQLContract(t *testing.T) {
	var invalidationErr error
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		peer, err := c.GetCNService(1)
		require.NoError(t, err)
		inventory, ok := clusterservice.GetMOCluster(cn.ServiceID()).(cnWorkStateInventory)
		require.True(t, ok)
		refresher, ok := clusterservice.GetMOCluster(cn.ServiceID()).(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		ready, err := waitForCNReadiness(ctx, cnWorkStatePollInterval, inventory, refresher, cn.ServiceID(), peer.ServiceID())
		require.NoError(t, err)
		db := openRetestSQLDB(t, c)
		defer db.Close()
		const name = "remote_native_failure"
		defer func() {
			if invalidationErr == nil {
				cleanupTestDatabases(t, db, name)
			}
		}()
		execSQLDB(t, ctx, db, "create database "+name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "create table src(k bigint not null)")
		execSQLDB(t, ctx, db, "insert into src values (1),(2),(3)")
		execSQLDB(t, ctx, db, "select mo_ctl('dn','flush','"+name+".src')")
		previousForce := plan.GetForceScanOnMultiCN()
		defer plan.SetForceScanOnMultiCN(previousForce)
		plan.SetForceScanOnMultiCN(true)
		for _, svc := range []string{cn.ServiceID(), peer.ServiceID()} {
			rt := runtime.ServiceRuntime(svc)
			previous, exists := rt.GetGlobalVariables(runtime.EnablePipelineStreamReuse)
			defer func() {
				if exists {
					rt.SetGlobalVariables(runtime.EnablePipelineStreamReuse, previous)
				} else {
					current, _ := rt.GetGlobalVariables(runtime.EnablePipelineStreamReuse)
					rt.CompareAndDeleteGlobalVariables(runtime.EnablePipelineStreamReuse, current)
				}
			}()
		}
		err = withCNDraining(ctx, inventory, refresher, cn.ServiceID(), []string{cn.ServiceID(), peer.ServiceID()}, func(err error) { invalidationErr = err }, func() {
			for _, reuse := range []bool{true, false} {
				for _, svc := range []string{cn.ServiceID(), peer.ServiceID()} {
					runtime.ServiceRuntime(svc).SetGlobalVariables(runtime.EnablePipelineStreamReuse, reuse)
				}
				physical, err := testutils.QueryTextResult(ctx, db, "explain phyplan analyze select k from src")
				require.NoError(t, err)
				require.Contains(t, physical.Text, ready.peerAddr)
				require.Empty(t, queryStringRows(t, ctx, db, "select k from src except select k from src"))
				require.Equal(t, [][]string{{"1"}, {"2"}, {"3"}}, queryStringRows(t, ctx, db, "select k from src order by k"))
				require.Len(t, queryStringRows(t, ctx, db, "select k from src union all select k from src limit 1"), 1)
				require.Len(t, queryStringRows(t, ctx, db, "select k from src union all select k from src limit 2"), 2)
				require.Empty(t, queryStringRows(t, ctx, db, "select cast(k * 1000 as tinyint) from src where 1=0"))
				for _, sqlText := range []string{
					"select cast(k * 1000 as tinyint) from src",
					"select k from src except select cast(k * 1000 as tinyint) from src",
					"select k from src union all select cast(k * 1000 as tinyint) from src",
				} {
					func() {
						var connBefore uint64
						require.NoError(t, db.QueryRowContext(ctx, "select connection_id()").Scan(&connBefore))
						observedRows := 0
						rows, queryErr := db.QueryContext(ctx, sqlText)
						if queryErr == nil {
							defer rows.Close()
							for rows.Next() {
								observedRows++
								var value int
								require.NoError(t, rows.Scan(&value))
							}
							queryErr = rows.Err()
						}
						var sqlErr *mysql.MySQLError
						require.ErrorAs(t, queryErr, &sqlErr, "reuse=%t sql=%s", reuse, sqlText)
						require.Equal(t, uint16(1690), sqlErr.Number)
						require.Equal(t, [5]byte{'2', '2', '0', '0', '3'}, sqlErr.SQLState)
						t.Logf("reuse=%t rows_before_error=%d sql=%s", reuse, observedRows, sqlText)
						require.NoError(t, ctx.Err())
						require.Equal(t, [][]string{{"6"}}, queryStringRows(t, ctx, db, "select sum(k) from src"))
						var connAfter uint64
						require.NoError(t, db.QueryRowContext(ctx, "select connection_id()").Scan(&connAfter))
						require.Equal(t, connBefore, connAfter, fmt.Sprintf("reuse=%t", reuse))
					}()
				}
			}
		})
		require.NoError(t, err)
	})
}

func TestPreparedFKFailureMetadataQuery(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		db := openRetestSQLDB(t, c)
		defer db.Close()
		const name = "prepared_fk_failure_metadata"
		execSQLDB(t, ctx, db, "create database "+name)
		defer cleanupTestDatabases(t, db, name)
		execSQLDB(t, ctx, db, "use "+name)
		execSQLDB(t, ctx, db, "create table fk_parent (id int primary key)")
		execSQLDB(t, ctx, db, "create table fk_child (pid int)")
		execSQLDB(t, ctx, db, "prepare alter_fk_stmt from 'alter table fk_child add constraint fk_parent_ref foreign key (pid) references fk_parent(id)'")
		defer execSQLDB(t, ctx, db, "deallocate prepare alter_fk_stmt")
		execSQLDB(t, ctx, db, "alter table fk_parent drop primary key")
		_, err := db.ExecContext(ctx, "execute alter_fk_stmt")
		require.Error(t, err)
		for i := 0; i < 2; i++ {
			for _, index := range []int{0, 1} {
				cn, err := c.GetCNService(index)
				require.NoError(t, err)
				rt := runtime.ServiceRuntime(cn.ServiceID())
				previous, exists := rt.GetGlobalVariables(runtime.EnablePipelineStreamReuse)
				defer func() {
					if exists {
						rt.SetGlobalVariables(runtime.EnablePipelineStreamReuse, previous)
					} else {
						current, _ := rt.GetGlobalVariables(runtime.EnablePipelineStreamReuse)
						rt.CompareAndDeleteGlobalVariables(runtime.EnablePipelineStreamReuse, current)
					}
				}()
				rt.SetGlobalVariables(runtime.EnablePipelineStreamReuse, i == 0)
			}

			var count int
			err := db.QueryRowContext(ctx, "select count(*) from information_schema.referential_constraints where constraint_schema = '"+name+"' and constraint_name = 'fk_parent_ref'").Scan(&count)
			require.NoError(t, err, "metadata query iteration=%d", i)
			require.Zero(t, count)
		}
	})
}
