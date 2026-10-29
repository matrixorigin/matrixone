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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This destructive topology needs an isolated fixture. Component tests cover
// the interleavings; this case checks the first SQL DROP after real CN closes,
// with one row instead of the original percentile fixture's ten million rows.
func TestDropDatabaseAfterCNScaleDown(t *testing.T) {
	testDropDatabaseAfterCNScaleDown(t, false)
}

func TestDropDatabaseAfterCatalogCleanupDuringCNScaleDown(t *testing.T) {
	testDropDatabaseAfterCNScaleDown(t, true)
}

func testDropDatabaseAfterCNScaleDown(t *testing.T, cleanupDuringRetirement bool) {
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
	// DROP also locks other catalog tables. One registry route is not a
	// membership barrier for both departing owners, or for the keeper's bounded
	// eviction worklist. Snapshot the routes this survivor actually cached.
	catalogIDs, cachedCatalogBinds := func() ([]uint64, []pb.LockTable) {
		rows, err := dbs[survivor].QueryContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and account_id=0")
		require.NoError(t, err)
		defer rows.Close()
		var ids []uint64
		var binds []pb.LockTable
		for rows.Next() {
			var id uint64
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
			current, err := locks[survivor].GetLockTableBind(0, id)
			require.NoError(t, err)
			if current.Valid && current.ServiceID != locks[survivor].GetServiceID() {
				binds = append(binds, current)
			}
		}
		require.NoError(t, rows.Err())
		return ids, binds
	}()
	require.NotEmpty(t, cachedCatalogBinds)
	if cleanupDuringRetirement {
		origin := (survivor + 1) % len(cns)
		catalogBind, err := locks[origin].GetLockTableBind(0, catalog.MO_TABLES_ID)
		require.NoError(t, err)
		require.True(t, catalogBind.Valid)
		require.Equal(t, locks[owner].GetServiceID(), catalogBind.ServiceID)
		// Retain a real remote lock while its owner retires, then make the
		// second departing CN perform cleanup before that CN itself closes.
		// Cluster cleanup also owns this transaction if an assertion fails.
		txn := []byte("catalog-cleanup-before-origin-close")
		_, err = locks[origin].Lock(ctx, catalog.MO_TABLES_ID, [][]byte{[]byte("catalog-cleanup")}, txn, pb.LockOptions{
			Granularity: pb.Granularity_Row, Mode: pb.LockMode_Shared, Policy: pb.WaitPolicy_Wait,
		})
		require.NoError(t, err)
		require.NoError(t, dbs[owner].Close())
		require.NoError(t, cns[owner].Close())
		// The existing registry probe establishes first-owner retirement
		// without allocating a replacement for mo_tables itself.
		require.Eventually(t, func() bool {
			current, err := locks[survivor].GetLatestLockTableBind(cached)
			return err == nil && current.Valid && current.ServiceID != cached.ServiceID
		}, 30*time.Second, 100*time.Millisecond, "first owner must retire before the second CN cleans up")
		require.NoError(t, locks[origin].Unlock(ctx, txn, timestamp.Timestamp{}))
		afterCleanup, err := locks[origin].GetLockTableBind(0, catalog.MO_TABLES_ID)
		require.NoError(t, err)
		t.Logf("mo_tables route after cleanup: %s", afterCleanup.DebugString())
	}
	for i, cn := range cns {
		if i != survivor && (!cleanupDuringRetirement || i != owner) {
			require.NoError(t, dbs[i].Close())
			require.NoError(t, cn.Close())
		}
	}
	// Observe cache eviction without refreshing it through a SQL probe.
	// GetLockTableBind only reads local state and performs no allocation/RPC.
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		for _, id := range catalogIDs {
			current, err := locks[survivor].GetLockTableBind(0, id)
			if err != nil || (current.Valid && current.ServiceID != locks[survivor].GetServiceID()) {
				collect.Errorf("catalog route table=%d bind=%s expected local owner=%s or no binding, error=%v",
					id, current.DebugString(), locks[survivor].GetServiceID(), err)
				return
			}
		}
	}, time.Minute, 50*time.Millisecond, "all cached catalog routes to both retired CNs must converge")
	// Separately wait for TN's retirement authority. This probe may allocate
	// the new TN binding, but deliberately does not publish a CN route.
	require.Eventually(t, func() bool {
		for _, old := range cachedCatalogBinds {
			current, err := locks[survivor].GetLatestLockTableBind(old)
			if err != nil || !current.Valid || current.ServiceID != locks[survivor].GetServiceID() {
				return false
			}
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "TN must retire both owners before declaring scale-down recovered")
	current, err := locks[survivor].GetLockTableBind(0, tableID)
	require.NoError(t, err)
	require.False(t, current.Valid, "the readiness probe must leave the CN route cold")
	var rows int
	require.NoError(t, dbs[survivor].QueryRowContext(ctx, "select count(*) from "+database+".t").Scan(&rows))
	require.Equal(t, 1, rows)
	_, err = dbs[survivor].ExecContext(ctx, "drop database "+database)
	require.NoError(t, err, "the first post-scale DROP must succeed without a client retry")
}
