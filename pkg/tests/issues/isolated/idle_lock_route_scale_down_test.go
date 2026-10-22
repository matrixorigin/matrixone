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
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// This destructive topology needs an isolated fixture. Component tests cover
// the interleavings; this case checks the first SQL DROP after real CN closes,
// with one row instead of the original percentile fixture's ten million rows.
func TestDropDatabaseAfterCNScaleDown(t *testing.T) {
	cluster, err := embed.StartTestCluster(embed.WithCNCount(3), embed.WithPreStart(func(svc embed.ServiceOperator) {
		svc.Adjust(func(cfg *embed.ServiceConfig) {
			switch svc.ServiceType() {
			case metadata.ServiceType_CN:
				cfg.CN.Cluster.RefreshInterval.Duration = 500 * time.Millisecond
				cfg.CN.LockService.KeepRemoteLockDuration.Duration = 100 * time.Millisecond
			case metadata.ServiceType_LOG:
				cfg.LogService.HAKeeperConfig.CNStoreTimeout.Duration = 5 * time.Second
				cfg.LogService.HAKeeperCheckInterval.Duration = 500 * time.Millisecond
			}
		})
	}))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	cns := make([]embed.ServiceOperator, 0, 3)
	dbs := make([]*sql.DB, 0, 3)
	locks := make([]lockservice.LockService, 0, 3)
	for i := range 3 {
		cn, err := cluster.GetCNService(i)
		require.NoError(t, err)
		cns = append(cns, cn)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, db.Close()) })
		require.NoError(t, waitSystemBootstrap(ctx, db))
		dbs = append(dbs, db)
		locks = append(locks, lockservice.GetLockServiceByServiceID(cn.ServiceID()))
		// Warm the same global lifecycle lock on every CN using real DDL.
		warm := fmt.Sprintf("idle_route_warm_%d", i)
		_, err = db.ExecContext(ctx, "create database "+warm)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "drop database "+warm)
		require.NoError(t, err)
	}
	var tableID uint64
	require.NoError(t, dbs[0].QueryRowContext(ctx,
		"select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_feature_registry'").Scan(&tableID))
	bind, err := locks[0].GetLockTableBind(0, tableID)
	require.NoError(t, err)
	require.True(t, bind.Valid)
	owner := -1
	for i, ls := range locks {
		if ls.GetServiceID() == bind.ServiceID {
			owner = i
		}
	}
	require.NotEqual(t, -1, owner)
	survivor := (owner + 1) % len(cns)
	const database = "idle_route_after_scale"
	_, err = dbs[survivor].ExecContext(ctx, "create database "+database)
	require.NoError(t, err)
	_, err = dbs[survivor].ExecContext(ctx, "create table "+database+".t (id int primary key)")
	require.NoError(t, err)
	_, err = dbs[survivor].ExecContext(ctx, "insert into "+database+".t values (1)")
	require.NoError(t, err)
	cached, err := locks[survivor].GetLockTableBind(0, tableID)
	require.NoError(t, err)
	require.Equal(t, bind.ServiceID, cached.ServiceID)
	for i, cn := range cns {
		if i != survivor {
			require.NoError(t, dbs[i].Close())
			require.NoError(t, cn.Close())
		}
	}
	// Observe cache eviction without refreshing it through a SQL probe.
	// GetLockTableBind only reads local state and performs no allocation/RPC.
	require.Eventually(t, func() bool {
		current, err := locks[survivor].GetLockTableBind(0, tableID)
		return err == nil && !current.Valid
	}, time.Minute, 50*time.Millisecond, "idle route must disappear after membership converges")
	// Separately wait for TN's retirement authority. This probe may allocate
	// the new TN binding, but deliberately does not publish a CN route.
	require.Eventually(t, func() bool {
		current, err := locks[survivor].GetLatestLockTableBind(cached)
		return err == nil && current.Valid && current.ServiceID != cached.ServiceID
	}, 30*time.Second, 100*time.Millisecond, "TN must retire the old owner before declaring scale-down recovered")
	current, err := locks[survivor].GetLockTableBind(0, tableID)
	require.NoError(t, err)
	require.False(t, current.Valid, "the readiness probe must leave the CN route cold")
	var rows int
	require.NoError(t, dbs[survivor].QueryRowContext(ctx, "select count(*) from "+database+".t").Scan(&rows))
	require.Equal(t, 1, rows)
	_, err = dbs[survivor].ExecContext(ctx, "drop database "+database)
	require.NoError(t, err, "the first post-scale DROP must succeed without a client retry")
}
