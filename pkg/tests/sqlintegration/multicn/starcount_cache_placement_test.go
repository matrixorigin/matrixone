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

package multicn

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/stretchr/testify/require"
)

func TestStarCountSealedBlocksAndCachePlacement(t *testing.T) {
	ctx, cancel := context.WithTimeout(defines.AttachAccountId(t.Context(), 0), 3*time.Minute)
	defer cancel()
	cacheRoot := t.TempDir()
	cluster, err := embed.StartTestCluster(embed.WithCNCount(2), embed.WithTestAdmissionContext(ctx), embed.WithPreStart(func(svc embed.ServiceOperator) {
		if svc.ServiceType() == metadata.ServiceType_TN {
			// Leave the row inserted after the explicit flush in memory
			// even if the test runs under an instrumented scheduler.
			svc.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.TN_please_use_getTNServiceConfig.Ckp.FlushInterval.Duration = time.Hour
			})
		}
		if svc.ServiceType() != metadata.ServiceType_CN {
			return
		}
		svc.Adjust(func(cfg *embed.ServiceConfig) {
			for i := range cfg.FileServices {
				fs := &cfg.FileServices[i]
				if fs.Name == defines.SharedFileServiceName {
					path := filepath.Join(cacheRoot, svc.ServiceID())
					fs.Cache.DiskPath = &path
				}
			}
		})
	}))
	if cluster != nil {
		t.Cleanup(func() { assertStarCountCleanup(t, cluster.Close()) })
	}
	require.NoError(t, err)
	coldCN, err := cluster.GetCNService(0)
	require.NoError(t, err)
	warmCN, err := cluster.GetCNService(1)
	require.NoError(t, err)
	inventory := clusterservice.GetMOCluster(coldCN.ServiceID())
	refresher, ok := inventory.(clusterservice.AuthoritativeRefresher)
	require.True(t, ok)
	require.Eventually(t, func() bool {
		if refresher.Refresh(ctx) != nil {
			return false
		}
		count := 0
		inventory.GetCNService(clusterservice.NewSelector(), func(cn metadata.CNService) bool {
			if cn.PipelineServiceAddress != "" && cn.QueryAddress != "" {
				count++
			}
			return true
		})
		return count == 2
	}, 30*time.Second, 100*time.Millisecond)

	cold, err := openStarCountClient(t, coldCN).Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { assertStarCountCleanup(t, cold.Close()) })
	warm := openStarCountClient(t, warmCN)
	const table = "starcount_cache.rows_to_count"
	const countSQL = "select count(*) from " + table

	exec := func(t *testing.T, q starCountSQL, statement string) {
		t.Helper()
		_, err := q.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	exec(t, warm, "create database starcount_cache")
	exec(t, warm, "create table "+table+"(id bigint primary key)")
	exec(t, warm, "insert into "+table+" values (1),(2),(3),(4)")
	exec(t, warm, "select mo_ctl('dn','flush','"+table+"')")
	exec(t, warm, "delete from "+table+" where id=2")
	exec(t, warm, "select mo_ctl('dn','flush','"+table+"')")
	// This row is committed after the last flush. Its value must come from
	// the ordinary reader alongside the count from the persisted data.
	exec(t, warm, "insert into "+table+" values (5)")
	var dbID, tableID uint64
	require.NoError(t, warm.QueryRowContext(ctx, "select reldatabase_id,rel_id from mo_catalog.mo_tables "+
		"where reldatabase='starcount_cache' and relname='rows_to_count'").Scan(&dbID, &tableID))

	checkRows := func(t *testing.T, q starCountSQL, want ...string) {
		t.Helper()
		// This projects actual IDs and cannot use either COUNT optimization.
		require.Equal(t, want, starCountQuery(t, ctx, q, "select id from "+table+" order by id"))
	}
	checkRows(t, warm, "1", "3", "4", "5")

	warmService := warmCN.RawService().(cnservice.Service)
	coldService := coldCN.RawService().(cnservice.Service)
	frontier := warmService.GetTxnClient().GetLatestCommitTS()
	require.False(t, frontier.IsEmpty())
	_, err = coldService.GetTxnClient().WaitLogTailAppliedAt(ctx, frontier)
	require.NoError(t, err)
	warmEngine := warmService.GetEngine().(*disttae.Engine)
	partition := warmEngine.GetOrCreateLatestPart(ctx, 0, dbID, tableID).Snapshot()
	var tombstones []string
	require.NoError(t, partition.VisitVisibleTombstoneObjects(ctx, types.TimestampToTS(frontier), func(obj objectio.ObjectEntry) {
		tombstones = append(tombstones, obj.ObjectName().String())
	}))
	require.Len(t, tombstones, 1, "the deleted row must be in a committed tombstone object")

	// Prime only the peer's real whole-object cache. Wait for publication
	// of that object, which can complete asynchronously after PrefetchFile.
	warmFS, coldFS := starCountSharedFS(t, warmCN), starCountSharedFS(t, coldCN)
	require.NoError(t, warmFS.PrefetchFile(ctx, tombstones[0]))
	require.Eventually(t, func() bool {
		return fileservice.IsWholeObjectCached(warmFS, tombstones[0])
	}, 15*time.Second, 20*time.Millisecond)
	require.False(t, fileservice.IsWholeObjectCached(coldFS, tombstones[0]))

	type execution struct {
		service string
		sealed  int64
	}
	var observed []execution
	var mu sync.Mutex
	checkCount := func(t *testing.T, q starCountSQL, want int, cn embed.ServiceOperator, sealed int64) {
		t.Helper()
		var count int
		err := q.QueryRowContext(ctx, countSQL).Scan(&count)
		mu.Lock()
		got := observed
		observed = nil
		mu.Unlock()
		require.NoError(t, err)
		require.Equal(t, want, count)
		require.Len(t, got, 1, "accepted sealed partial from the executing CN")
		require.Equal(t, sealed, got[0].sealed)
		if cn != nil {
			require.Equal(t, cn.ServiceID(), got[0].service, "CN accepting the sealed partial")
		}
		t.Logf("count=%d sealed=%d reader=%d executing-CN=%s", count, got[0].sealed, int64(count)-got[0].sealed, got[0].service)
	}
	var defaultCount int
	require.NoError(t, warm.QueryRowContext(ctx, countSQL).Scan(&defaultCount))
	require.Equal(t, 4, defaultCount)

	for _, cn := range []embed.ServiceOperator{coldCN, warmCN} {
		service := cn.ServiceID()
		tc := runtime.MustGetTestingContext(service)
		previous := tc.SetStarCountTestConfig(runtime.StarCountTestConfig{
			TableID: tableID,
			// Exactly one persisted tombstone crosses the test gate.
			TombstoneThreshold: 0,
			// Allow the real RPC to complete on instrumented test runners.
			ProbeTimeout: 5 * time.Second,
			OnSealedRows: func(rows int64) {
				mu.Lock()
				observed = append(observed, execution{service, rows})
				mu.Unlock()
			},
		})
		t.Cleanup(func() { tc.SetStarCountTestConfig(previous) })
	}

	t.Run("cold coordinator selects and runs on warm peer", func(t *testing.T) {
		// The callback runs on the CN accepting the sealed partial result.
		// Three sealed rows plus one remaining reader row make four.
		checkCount(t, cold, 4, warmCN, 3)
	})
	t.Run("hot coordinator keeps count local", func(t *testing.T) {
		checkCount(t, warm, 4, warmCN, 3)
	})
	t.Run("transaction deletes sealed rows and reads its insert", func(t *testing.T) {
		tx, err := cold.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer func() {
			if err := tx.Rollback(); err != sql.ErrTxDone {
				assertStarCountCleanup(t, err)
			}
		}()
		exec(t, tx, "delete from "+table+" where id in (1,3)")
		exec(t, tx, "insert into "+table+" values (6)")
		checkRows(t, tx, "4", "5", "6")
		// One sealed row survives both committed and transaction deletes.
		// The reader contributes the committed and uncommitted inserts.
		checkCount(t, tx, 3, coldCN, 1)
		checkRows(t, warm, "1", "3", "4", "5")
		checkCount(t, warm, 4, warmCN, 3)

		require.NoError(t, tx.Rollback())
		checkRows(t, cold, "1", "3", "4", "5")
		checkCount(t, cold, 4, nil, 3)
	})
}

type starCountSQL interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func openStarCountClient(t *testing.T, cn embed.ServiceOperator) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { assertStarCountCleanup(t, db.Close()) })
	return db
}

func starCountQuery(t *testing.T, ctx context.Context, q starCountSQL, statement string) []string {
	t.Helper()
	rows, err := q.QueryContext(ctx, statement)
	require.NoError(t, err, statement)
	defer rows.Close()
	var result []string
	for rows.Next() {
		var value string
		require.NoError(t, rows.Scan(&value), statement)
		result = append(result, value)
	}
	require.NoError(t, rows.Err(), statement)
	return result
}

func starCountSharedFS(t *testing.T, cn embed.ServiceOperator) fileservice.FileService {
	t.Helper()
	value, ok := runtime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables("parameter-unit")
	require.True(t, ok)
	fs, err := fileservice.Get[fileservice.FileService](value.(*config.ParameterUnit).FileService, defines.SharedFileServiceName)
	require.NoError(t, err)
	return fs
}

func assertStarCountCleanup(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("clean up starcount fixture: %v", err)
	}
}
