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

package dml

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/schedule"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestPreparedAPRuntimeCacheWorkspaceTransition(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		cluster := clusterservice.GetMOCluster(cn.ServiceID())
		refresher, ok := cluster.(clusterservice.AuthoritativeRefresher)
		require.True(t, ok)
		require.Eventually(t, func() bool {
			if refresher.Refresh(ctx) != nil {
				return false
			}
			count := 0
			cluster.GetCNService(clusterservice.NewSelector(), func(metadata.CNService) bool {
				count++
				return true
			})
			return count == 2
		}, 30*time.Second, 100*time.Millisecond, "both CNs must be ready before testing placement")

		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		db.SetMaxOpenConns(1)
		name := testutils.GetDatabaseName(t)
		defer cleanupTestDatabases(t, db, name)
		execSQLDB(t, ctx, db, "create database `"+name+"`")
		execSQLDB(t, ctx, db, "use `"+name+"`")
		execSQLDB(t, ctx, db, "create table src (id bigint primary key)")
		execSQLDB(t, ctx, db, "insert into src values (1),(2)")

		for _, remoteOnly := range []bool{false, true} {
			t.Run(fmt.Sprintf("remote-only=%t", remoteOnly), func(t *testing.T) {
				if remoteOnly {
					// Change authoritative inventory, not just a cache that its
					// next refresh could silently overwrite. This embedded cluster
					// is serialized and owns both CNs; restore before releasing it.
					defer func() {
						require.NoError(t, cluster.DebugUpdateCNWorkState(cn.ServiceID(), int(metadata.WorkState_Working)))
						restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 10*time.Second)
						defer restoreCancel()
						require.NoError(t, refresher.Refresh(restoreCtx))
					}()
					require.NoError(t, cluster.DebugUpdateCNWorkState(cn.ServiceID(), int(metadata.WorkState_Draining)))
					require.NoError(t, refresher.Refresh(ctx))
				}
				tx, err := db.BeginTx(ctx, nil)
				require.NoError(t, err)
				defer tx.Rollback()
				// The existing test hook selects AP with two committed rows.
				// Never force fixture DDL, transaction writes, or cleanup to AP.
				plan.SetForceScanOnMultiCN(true)
				stmt, err := tx.PrepareContext(ctx, "select sum(id + abs(?)) from src")
				plan.SetForceScanOnMultiCN(false)
				require.NoError(t, err)
				defer stmt.Close()
				query := func(value int64) (int64, error) {
					plan.SetForceScanOnMultiCN(true)
					defer plan.SetForceScanOnMultiCN(false)
					var sum int64
					err := stmt.QueryRowContext(ctx, value).Scan(&sum)
					return sum, err
				}
				sum, err := query(-1)
				require.NoError(t, err)
				require.Equal(t, int64(5), sum)
				_, err = tx.ExecContext(ctx, "insert into src values (3)")
				require.NoError(t, err)
				// The unchanged binary parameter category hits the runtime cache.
				// A writable ingress must participate, or scheduling must fail.
				sum, err = query(-2)
				if remoteOnly {
					require.ErrorContains(t, err, schedule.ReasonCurrentCNDraining)
				} else {
					require.NoError(t, err)
					require.Equal(t, int64(12), sum)
				}
			})
		}
		var sum int64
		require.NoError(t, db.QueryRowContext(ctx, "select sum(id) from src").Scan(&sum))
		require.Equal(t, int64(3), sum, "the uncommitted write must be rolled back")
	})
}
