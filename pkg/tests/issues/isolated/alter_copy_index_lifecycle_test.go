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

package isolated

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/embed"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	cagracompile "github.com/matrixorigin/matrixone/pkg/vectorindex/cagra/plugin/compile"
	ivfpqcompile "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfpq/plugin/compile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This private CN is required for fault injection and executor handoff. The
// ordinary COPY SQL cases use BVT; no second cluster is needed for these phases.
func TestAlterCopyIndexLifecycle(t *testing.T) {
	path, err := os.MkdirTemp("", "copy-ann-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(path)) })
	cluster, err := embed.NewCluster(embed.WithTesting(), embed.WithDataPath(path), embed.WithCNCount(1))
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)
	require.NoError(t, cluster.Start())
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=45s&writeTimeout=45s", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	db.SetMaxOpenConns(3)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	var runtime *iscp.ISCPTaskExecutor
	require.Eventually(t, func() bool {
		var ok bool
		runtime, ok = iscp.GetExecutorRuntime(cn.ServiceID())
		return ok && runtime.IsRunning()
	}, 90*time.Second, 100*time.Millisecond)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	exec := func(t *testing.T, q string) {
		t.Helper()
		_, err := conn.ExecContext(ctx, q)
		require.NoError(t, err, q)
	}
	exec(t, "create database copy_hnsw_lifecycle")
	exec(t, "use copy_hnsw_lifecycle")
	exec(t, "set experimental_hnsw_index=1")
	t.Cleanup(func() {
		c, stop := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer stop()
		_, err := db.ExecContext(c, "drop database if exists copy_hnsw_lifecycle")
		require.NoError(t, err)
	})
	jobSQL := "select job_state,watermark,job_status from mo_catalog.mo_iscp_log where drop_at is null and job_name='index_hv' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='copy_hnsw_lifecycle' and relname='h')"
	readJob := func() (int8, string, iscp.JobStatus, error) {
		var state int8
		var watermark, text string
		var status iscp.JobStatus
		err := db.QueryRowContext(ctx, jobSQL).Scan(&state, &watermark, &text)
		if err == nil {
			err = json.Unmarshal([]byte(text), &status)
		}
		return state, watermark, status, err
	}
	waitComplete := func(t *testing.T) {
		t.Helper()
		require.Eventually(t, func() bool {
			state, wm, s, err := readJob()
			return err == nil && state == iscp.ISCPJobState_Completed && wm != "0-0" && s.LSN > 0 && s.ErrorCode == 0
		}, 60*time.Second, 100*time.Millisecond)
	}
	// Wait until the durable index includes the completed write before the first
	// index read. A read before this point can cache an older model for minutes.
	waitThrough := func(t *testing.T, after time.Time) {
		t.Helper()
		target := types.BuildTS(after.UnixNano(), 0)
		require.EventuallyWithT(t, func(check *assert.CollectT) {
			state, wm, status, err := readJob()
			if !assert.NoError(check, err) {
				return
			}
			assert.Equal(check, int8(iscp.ISCPJobState_Completed), state)
			assert.Zero(check, status.ErrorCode)
			watermark := types.StringToTS(wm)
			assert.True(check, watermark.GE(&target), "watermark %s has not reached %s", wm, target.ToString())
		}, 60*time.Second, 100*time.Millisecond)
	}
	nearest := func(t *testing.T, want int64) {
		t.Helper()
		require.EventuallyWithT(t, func(check *assert.CollectT) {
			var id int64
			err := conn.QueryRowContext(ctx, "select id from h order by l2_distance(v,'[0,0,0]') limit 1").Scan(&id)
			if assert.NoError(check, err) {
				assert.Equal(check, want, id)
			}
		}, 45*time.Second, 100*time.Millisecond)
	}
	create := func(t *testing.T) {
		exec(t, "create table h(a int,id bigint generated always as(a*2) stored,v vecf32(3),primary key(id))")
		exec(t, "insert into h(a,v) values(1,'[1,2,3]'),(2,'[9,9,9]')")
		exec(t, "create index hv using hnsw on h(v) op_type 'vector_l2_ops'")
	}
	t.Run("synchronous build and post-copy writes", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanupCtx, "rollback")
			require.NoError(t, err)
			_, err = conn.ExecContext(cleanupCtx, "drop table if exists copy_hnsw_lifecycle.h")
			require.NoError(t, err)
		})

		create(t)
		exec(t, "alter table h algorithm=copy,change a a bigint")
		waitComplete(t)
		exec(t, "insert into h(a,v) values(3,'[0,0,0]')")
		exec(t, "delete from h where a=3")
		exec(t, "update h set v='[0,0,0]' where a=2")
		waitThrough(t, time.Now())
		nearest(t, 4)
		exec(t, "drop table h")
	})
	t.Run("failed snapshot replaces deleted rows on retry", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanupCtx, "rollback")
			require.NoError(t, err)
			_, err = conn.ExecContext(cleanupCtx, "drop table if exists copy_hnsw_lifecycle.h")
			require.NoError(t, err)
		})

		create(t)
		fault.Enable()
		defer fault.Disable()
		require.NoError(t, fault.AddFaultPoint(ctx, objectio.FJ_ISCPIndexWatermarkErr, ":::", "return", 0, "", false))
		defer fault.RemoveFaultPoint(ctx, objectio.FJ_ISCPIndexWatermarkErr)
		exec(t, "alter table h algorithm=copy,change a a bigint")
		require.Eventually(t, func() bool {
			state, wm, s, err := readJob()
			return err == nil && state == iscp.ISCPJobState_Completed && s.ErrorCode != 0 && wm == "0-0"
		}, 60*time.Second, 100*time.Millisecond, "failed Save+progress transaction must not advance the cursor")
		exec(t, "delete from h where a=2")
		exec(t, "insert into h(a,v) values(3,'[0,0,0]')")
		written := time.Now()
		_, err := fault.RemoveFaultPoint(ctx, objectio.FJ_ISCPIndexWatermarkErr)
		require.NoError(t, err)
		waitComplete(t)
		waitThrough(t, written)
		nearest(t, 6)
		rows, err := conn.QueryContext(ctx, "select id from h order by l2_distance(v,'[0,0,0]') limit 10")
		require.NoError(t, err)
		defer func() { require.NoError(t, rows.Close()) }()
		var ids []int64
		for rows.Next() {
			var id int64
			require.NoError(t, rows.Scan(&id))
			ids = append(ids, id)
		}
		require.NoError(t, rows.Err())
		require.Equal(t, []int64{6, 2}, ids)
		exec(t, "drop table h")
	})
	t.Run("empty snapshot and runner handoff", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanupCtx, "rollback")
			require.NoError(t, err)
			_, err = conn.ExecContext(cleanupCtx, "drop table if exists copy_hnsw_lifecycle.h")
			require.NoError(t, err)
		})

		exec(t, "create table h(a int,id bigint generated always as(a*2) stored,v vecf32(3),primary key(id))")
		exec(t, "create index hv using hnsw on h(v) op_type 'vector_l2_ops'")
		fault.Enable()
		defer fault.Disable()
		waitKey := objectio.FJ_ISCPCancelLongHnswBeforeSave + ":hv"
		require.NoError(t, fault.AddFaultPoint(ctx, waitKey, ":::", "wait", 0, "", false))
		defer fault.RemoveFaultPoint(ctx, waitKey)
		require.NoError(t, fault.AddFaultPoint(ctx, "copy-hnsw-waiters", ":::", "getwaiters", 0, waitKey, false))
		defer fault.RemoveFaultPoint(ctx, "copy-hnsw-waiters")
		exec(t, "alter table h algorithm=copy,change a a bigint")
		require.Eventually(t, func() bool { n, _, ok := fault.TriggerFault("copy-hnsw-waiters"); return ok && n > 0 }, 60*time.Second, 100*time.Millisecond)
		stopped := make(chan struct{})
		go func() { runtime.Stop(); close(stopped) }()
		select {
		case <-stopped:
		case <-ctx.Done():
			t.Fatal("canceled snapshot did not release the old executor")
		}
		_, err := fault.RemoveFaultPoint(ctx, waitKey)
		require.NoError(t, err)
		require.NoError(t, runtime.Start())
		waitComplete(t)
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from h").Scan(&count))
		require.Zero(t, count)
		exec(t, "insert into h(a,v) values(1,'[0,0,0]')")
		waitThrough(t, time.Now())
		nearest(t, 2)
		exec(t, "drop table h")
	})
	t.Run("explicit copy rollback", func(t *testing.T) {
		t.Cleanup(func() {
			cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
			defer stop()
			_, err := conn.ExecContext(cleanupCtx, "rollback")
			require.NoError(t, err)
			_, err = conn.ExecContext(cleanupCtx, "drop table if exists copy_hnsw_lifecycle.h")
			require.NoError(t, err)
		})

		create(t)
		exec(t, "begin")
		exec(t, "alter table h algorithm=copy,change a a bigint")
		exec(t, "rollback")
		exec(t, "insert into h(a,v) values(3,'[0,0,0]')")
		waitThrough(t, time.Now())
		nearest(t, 6)
		exec(t, "drop table h")
	})
	t.Run("cuVS generated clear preserves physical table identities", func(t *testing.T) {
		// Exercise the actual providers' SQL through the CPU SQL planner. The native
		// model build remains a GPU CI requirement; this test checks its table fence.
		exec(t, "create table clear_meta(id int primary key)")
		exec(t, "create table clear_storage(id int primary key)")
		defer exec(t, "drop table clear_meta,clear_storage")
		var optimization int
		require.NoError(t, conn.QueryRowContext(ctx, "select @@delete_opt_to_truncate").Scan(&optimization))
		require.Equal(t, 1, optimization)
		relationID := func(name string) uint64 {
			var id uint64
			require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='copy_hnsw_lifecycle' and relname=?", name).Scan(&id))
			return id
		}
		meta, storage := relationID("clear_meta"), relationID("clear_storage")
		providers := []struct {
			hook          compileplugin.SourceSnapshotInitializer
			meta, storage string
		}{{cagracompile.Hooks{}, catalog.Cagra_TblType_Metadata, catalog.Cagra_TblType_Storage}, {ivfpqcompile.Hooks{}, catalog.Ivfpq_TblType_Metadata, catalog.Ivfpq_TblType_Storage}}
		providerCtx := sourceSnapshotCompileContext{database: "copy_hnsw_lifecycle", table: &plan.TableDef{Name: "source", Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"}}}
		for _, provider := range providers {
			exec(t, "insert into clear_meta values(1)")
			exec(t, "insert into clear_storage values(1)")
			stmts, err := provider.hook.SourceSnapshotInitSQL(providerCtx, map[string]*plan.IndexDef{
				provider.meta:    {IndexTableName: "clear_meta", Parts: []string{"v"}},
				provider.storage: {IndexTableName: "clear_storage", Parts: []string{"v"}, IndexAlgoParams: `{"op_type":"vector_l2_ops"}`},
			})
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(stmts), 3)
			exec(t, "begin")
			exec(t, stmts[0])
			exec(t, stmts[1])
			require.Equal(t, meta, relationID("clear_meta"))
			require.Equal(t, storage, relationID("clear_storage"))
			exec(t, "commit")
			require.Equal(t, meta, relationID("clear_meta"))
			require.Equal(t, storage, relationID("clear_storage"))
			var rows int
			require.NoError(t, conn.QueryRowContext(ctx, "select (select count(*) from clear_meta)+(select count(*) from clear_storage)").Scan(&rows))
			require.Zero(t, rows)
		}
	})
	t.Run("failed CPU initializer retains Init stage and retries model build", func(t *testing.T) {
		exec(t, "create table init_ivf(id bigint primary key,v vecf32(3))")
		defer exec(t, "drop table init_ivf")
		exec(t, "insert into init_ivf values(1,'[1,2,3]'),(2,'[7,8,9]')")
		fault.Enable()
		defer fault.Disable()
		require.NoError(t, fault.AddFaultPoint(ctx, objectio.FJ_ISCPIndexWatermarkErr, ":::", "return", 0, "", false))
		defer fault.RemoveFaultPoint(ctx, objectio.FJ_ISCPIndexWatermarkErr)
		exec(t, "create index iv using ivfflat on init_ivf(v) lists=1 op_type 'vector_l2_ops' async")
		query := "select job_state,watermark,job_status from mo_catalog.mo_iscp_log where drop_at is null and job_name='index_iv' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='copy_hnsw_lifecycle' and relname='init_ivf')"
		var failedLSN uint64
		require.Eventually(t, func() bool {
			var state int8
			var wm, text string
			var status iscp.JobStatus
			if db.QueryRowContext(ctx, query).Scan(&state, &wm, &text) != nil || json.Unmarshal([]byte(text), &status) != nil {
				return false
			}
			if state != iscp.ISCPJobState_Completed || status.ErrorCode == 0 {
				return false
			}
			require.Equal(t, "0-0", wm)
			require.Equal(t, iscp.JobStage_Init, status.Stage, "rolled-back completion must retry its initializer")
			failedLSN = status.LSN
			return true
		}, 60*time.Second, 100*time.Millisecond)
		var entries string
		require.NoError(t, conn.QueryRowContext(ctx, "select index_table_name from mo_catalog.mo_indexes where name='iv' and algo_table_type='entries' and table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='copy_hnsw_lifecycle' and relname='init_ivf')").Scan(&entries))
		var count int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from `"+entries+"`").Scan(&count))
		require.Zero(t, count, "failed initializer output must roll back with its progress")
		_, err = fault.RemoveFaultPoint(ctx, objectio.FJ_ISCPIndexWatermarkErr)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			var state int8
			var wm, text string
			var status iscp.JobStatus
			return db.QueryRowContext(ctx, query).Scan(&state, &wm, &text) == nil && json.Unmarshal([]byte(text), &status) == nil && state == iscp.ISCPJobState_Completed && status.ErrorCode == 0 && status.Stage == iscp.JobStage_Running && status.LSN > failedLSN && wm != "0-0"
		}, 60*time.Second, 100*time.Millisecond)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from `"+entries+"`").Scan(&count))
		require.Equal(t, 2, count)
		exec(t, "insert into init_ivf values(3,'[0,0,0]')")
		require.Eventually(t, func() bool {
			var id int64
			return conn.QueryRowContext(ctx, "select id from init_ivf order by l2_distance(v,'[0,0,0]') limit 1").Scan(&id) == nil && id == 3
		}, 45*time.Second, 100*time.Millisecond)
	})

}

// Only these compilation inputs are used by the two existing source providers.
// Unexpected calls fail through the embedded interface rather than silently
// supplying a second fake compiler implementation.
type sourceSnapshotCompileContext struct {
	compileplugin.CompileContext
	database string
	table    *plan.TableDef
}

func (c sourceSnapshotCompileContext) QryDatabase() string              { return c.database }
func (c sourceSnapshotCompileContext) OriginalTableDef() *plan.TableDef { return c.table }
func (c sourceSnapshotCompileContext) ResolveVariable(string, bool, bool) (any, error) {
	return int64(1), nil
}
