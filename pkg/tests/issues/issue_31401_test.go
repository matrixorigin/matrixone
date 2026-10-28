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

package issues

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type issue31401CopyObserver struct {
	executor.SQLExecutor
	copies   atomic.Int32
	retired  atomic.Int32
	replicas sync.Map
	// Optional barriers/failure injection operate at existing nested SQL calls.
	afterCopy        func(context.Context, int32) error
	beforeRetire     func() error
	beforeRecreate   func() error
	beforeSourceDrop func() error
	afterRetire      func(context.Context) error
}

func TestIssue31401AlterCopyRebuildTransaction(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		for _, scenario := range []string{"commit", "rollback", "borrowed", "retire_error", "copy_error", "cancel_rebuild", "no_change", "plugin", "lineage", "late_lineage", "fk_grant", "late_lineage_error", "create_error", "publish_retry",
			"scalar_commit", "scalar_rollback", "scalar_borrowed", "scalar_retire_error", "scalar_copy_error", "scalar_cancel_rebuild", "scalar_no_change", "scalar_lineage", "scalar_late_lineage", "scalar_late_lineage_error", "scalar_create_error", "scalar_publish_retry"} {
			t.Run(scenario, func(t *testing.T) {
				mode := strings.TrimPrefix(scenario, "scalar_")
				scalar := strings.HasPrefix(scenario, "scalar_")
				ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
				defer cancel()
				name := "issue31401_txn_" + scenario
				resetIssue277xxDatabase(t, ctx, db, name)
				defer func() {
					clean, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					_, _ = db.ExecContext(clean, "drop database if exists "+name)
				}()
				lineageMode := mode == "lineage" || mode == "late_lineage"
				extra := ""
				if mode == "fk_grant" {
					execSQLRequire(t, ctx, db, "create table "+name+".parent(id int primary key)")
					execSQLRequire(t, ctx, db, "insert into "+name+".parent values(1)")
					extra = ",pid int default 1,constraint fk foreign key(pid) references " + name + ".parent(id)"
				}
				var savedFulltextSetting int
				if mode == "plugin" {
					savedFulltextSetting = queryIssue277xxInt(t, ctx, db, "select @@experimental_fulltext2_index")
					execSQLRequire(t, ctx, db, "set experimental_fulltext2_index=1")
					extra = ",body text default 'needle',FULLTEXT2 ft(body)"
				}
				indexes := ",unique key uq(v),key idx_v(v)"
				if scalar {
					indexes = ""
				}
				execSQLRequire(t, ctx, db, "create table "+name+".t(id int primary key,v int"+indexes+extra+")")
				execSQLRequire(t, ctx, db, "insert into "+name+".t(id,v) values(1,10),(2,20),(3,30),(6,60)")
				if mode == "lineage" {
					execSQLRequire(t, ctx, db, "data branch create table "+name+".branch from "+name+".t")
				}
				if mode == "fk_grant" {
					execSQLRequire(t, ctx, db, "create role "+name)
					defer func() {
						cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
						defer stop()
						_, _ = db.ExecContext(cleanup, "drop role if exists "+name)
					}()
					execSQLRequire(t, ctx, db, "grant select on table "+name+".t to "+name)
				}

				conn, err := db.Conn(ctx)
				require.NoError(t, err)
				defer conn.Close()
				if mode == "plugin" {
					defer func() {
						cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
						defer stop()
						_, _ = conn.ExecContext(cleanup, fmt.Sprintf("set experimental_fulltext2_index=%d", savedFulltextSetting))
					}()
				}
				defer func() { _, _ = conn.ExecContext(context.WithoutCancel(ctx), "rollback") }()
				var connectionID uint32
				require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
				prepared, proceed, rebuilding := make(chan struct{}), make(chan struct{}), make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(proceed) }) }
				defer release()
				observer := issue31401Observe(t, cn.ServiceID())
				var sourceTableID, sourceJobID uint64
				var taskPreserved atomic.Bool
				if mode == "plugin" {
					require.NoError(t, db.QueryRowContext(ctx, "select table_id,job_id from mo_catalog.mo_iscp_log where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t') and job_name='index_ft' and drop_at is null", name).Scan(&sourceTableID, &sourceJobID))
					// The daemon can run on any CN in the shared fixture. Fence all
					// candidates, including not-yet-started executor generations.
					key := iscp.NewJobRuntimeKey(0, sourceTableID, "index_ft", sourceJobID)
					var fenced []string
					defer func() {
						for _, sid := range fenced {
							iscp.RemoveCNJobFence(sid, key)
						}
					}()
					cluster.ForeachServices(func(service embed.ServiceOperator) bool {
						if service.ServiceType() != metadata.ServiceType_CN {
							return true
						}
						sid := service.ServiceID()
						iscp.InstallCNJobFence(sid, key, 5*time.Minute)
						fenced = append(fenced, sid)
						if running, ok := iscp.GetExecutorRuntime(sid); ok {
							t.Logf("draining source job on %s", sid)
							require.NoError(t, running.CancelAndDrainJobConsumer(ctx, 0, sourceTableID, "index_ft", sourceJobID))
						}
						return true
					})
					observer.afterRetire = func(checkCtx context.Context) error {
						var count int
						err := db.QueryRowContext(checkCtx, "select count(*) from mo_catalog.mo_iscp_log where table_id=? and job_id=? and drop_at is null", sourceTableID, sourceJobID).Scan(&count)
						if err != nil {
							return err
						}
						if count != 1 {
							return fmt.Errorf("source job changed during private DROP: %d", count)
						}
						taskPreserved.Store(true)
						return nil
					}
				}
				observer.afterCopy = func(callCtx context.Context, n int32) error {
					if n == 1 {
						close(prepared)
						select {
						case <-proceed:
							return nil
						case <-callCtx.Done():
							return callCtx.Err()
						}
					}
					if n == 2 && mode == "copy_error" {
						return errors.New("injected second copy failure")
					}
					if n == 2 && mode == "cancel_rebuild" {
						close(rebuilding)
						<-callCtx.Done()
						return callCtx.Err()
					}
					return nil
				}
				if mode == "retire_error" {
					observer.beforeRetire = func() error { return errors.New("injected replica retirement failure") }
				}
				if mode == "create_error" {
					observer.beforeRecreate = func() error { return errors.New("injected replica recreation failure") }
				}
				if mode == "publish_retry" {
					observer.beforeSourceDrop = func() error { return moerr.NewTxnNeedRetryNoCtx() }
				}
				before := []string{"update " + name + ".t set v=11 where id=1", "delete from " + name + ".t where id=2", "insert into " + name + ".t(id,v) values(4,40)"}
				alterSQL := "alter table " + name + ".t add column d int default 7"
				if mode == "late_lineage_error" {
					execSQLRequire(t, ctx, db, "create table "+name+".guard(id int primary key)")
					before = []string{"insert into " + name + ".guard values(1)"}
				}

				if mode != "borrowed" && !lineageMode {
					_, err = conn.ExecContext(ctx, "begin")
					require.NoError(t, err)
					for _, query := range before {
						_, err = conn.ExecContext(ctx, query)
						require.NoError(t, err)
					}
				}
				done := make(chan error, 1)
				joined := false
				defer func() {
					release()
					cancel()
					if !joined {
						<-done
					}
				}()
				go func() {
					if mode == "borrowed" {
						done <- cn.RawService().(cnservice.Service).GetSQLExecutor().ExecTxn(ctx, func(tx executor.TxnExecutor) error {
							for _, query := range append(before, alterSQL) {
								res, err := tx.Exec(query, executor.StatementOption{})
								if err != nil {
									return err
								}
								res.Close()
							}
							return nil
						}, executor.Options{}.WithAccountID(0).WithDatabase(name).WithTxnMode(txn.TxnMode_Pessimistic).WithTxnIsolation(txn.TxnIsolation_RC))
						return
					}
					_, err := conn.ExecContext(ctx, alterSQL)
					done <- err
				}()
				select {
				case <-prepared:
				case err := <-done:
					joined = true
					t.Fatalf("ALTER did not prepare: %v", err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if mode == "late_lineage" || mode == "late_lineage_error" {
					execSQLRequire(t, ctx, db, "data branch create table "+name+".branch from "+name+".t")
				}
				if mode != "no_change" {
					for _, query := range []string{"insert into " + name + ".t(id,v) values(5,50)", "update " + name + ".t set v=31 where id=3", "delete from " + name + ".t where id=6"} {
						execSQLRequire(t, ctx, db, query)
					}
				}
				release()
				if mode == "cancel_rebuild" {
					select {
					case <-rebuilding:
					case err := <-done:
						joined = true
						t.Fatalf("ALTER did not rebuild: %v", err)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					_, err = db.ExecContext(ctx, fmt.Sprintf("kill query %d", connectionID))
					require.NoError(t, err)
				}
				err = <-done
				joined = true
				failed := mode == "publish_retry" || mode == "create_error" || mode == "retire_error" || mode == "copy_error" || mode == "cancel_rebuild" || mode == "late_lineage_error"
				if failed {
					requireIssue277xxMySQLError(t, err, 20601)
					require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_columns where att_database=? and att_relname='t' and attname='d'", name))
					require.Equal(t, [][3]int{{1, 10, 0}, {2, 20, 0}, {3, 31, 0}, {5, 50, 0}}, issue31401Rows(t, ctx, db, "select id,v,0 from "+name+".t order by id"), "fatal recovery must roll back caller-local writes and preserve other committed DML")
				} else {
					require.NoError(t, err)
					if mode != "borrowed" && !lineageMode {
						rows, err := conn.QueryContext(ctx, "select id,v,d from "+name+".t order by id")
						require.NoError(t, err)
						func() {
							defer rows.Close()
							var got [][3]int
							for rows.Next() {
								var row [3]int
								require.NoError(t, rows.Scan(&row[0], &row[1], &row[2]))
								got = append(got, row)
							}
							require.NoError(t, rows.Err())
							want := [][3]int{{1, 11, 7}, {3, 31, 7}, {4, 40, 7}, {5, 50, 7}}
							if mode == "no_change" {
								want = [][3]int{{1, 11, 7}, {3, 30, 7}, {4, 40, 7}, {6, 60, 7}}
							}
							require.Equal(t, want, got)
						}()
						terminal := "commit"
						if mode == "rollback" {
							terminal = "rollback"
						}
						_, err = conn.ExecContext(ctx, terminal)
						require.NoError(t, err)
					}
					if mode == "rollback" {
						require.Equal(t, [][3]int{{1, 10, 0}, {2, 20, 0}, {3, 31, 0}, {5, 50, 0}}, issue31401Rows(t, ctx, db, "select id,v,0 from "+name+".t order by id"))
					} else if lineageMode {
						require.Equal(t, [][3]int{{1, 10, 7}, {2, 20, 7}, {3, 31, 7}, {5, 50, 7}}, issue31401Rows(t, ctx, db, "select id,v,d from "+name+".t order by id"))
						require.Equal(t, [][3]int{{1, 10, 0}, {2, 20, 0}, {3, 30, 0}, {6, 60, 0}}, issue31401Rows(t, ctx, db, "select id,v,0 from "+name+".branch order by id"))
						execSQLRequire(t, ctx, db, "data branch create table "+name+".next_branch from "+name+".t")
						require.Equal(t, [][3]int{{1, 10, 7}, {2, 20, 7}, {3, 31, 7}, {5, 50, 7}}, issue31401Rows(t, ctx, db, "select id,v,d from "+name+".next_branch order by id"))
					} else if mode != "no_change" {
						indexHint := " force index(idx_v)"
						if scalar {
							indexHint = ""
						}
						require.Equal(t, [][3]int{{1, 11, 7}, {3, 31, 7}, {4, 40, 7}, {5, 50, 7}}, issue31401Rows(t, ctx, db, "select id,v,d from "+name+".t"+indexHint+" where v>=11 order by id"))
					}
				}
				wantCopies := int32(2)
				if mode == "no_change" || mode == "retire_error" || mode == "late_lineage_error" || mode == "create_error" {
					wantCopies = 1
				}
				require.Equal(t, wantCopies, observer.copies.Load())
				wantRetired := int32(1)
				if mode == "no_change" || mode == "late_lineage_error" {
					wantRetired = 0
				}
				require.Equal(t, wantRetired, observer.retired.Load())
				if mode == "fk_grant" {
					require.Equal(t, 1, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_role_privs where role_name=? and privilege_name='select'", name))
					_, invalid := db.ExecContext(ctx, "insert into "+name+".t(id,v,pid) values(9,90,999)")
					requireIssue277xxForeignKeyError(t, invalid)
				}
				if mode == "late_lineage_error" {
					require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from "+name+".guard"))
				}
				observer.beforeRetire = nil
				observer.beforeRecreate = nil
				observer.beforeSourceDrop = nil
				if mode == "plugin" {
					require.True(t, taskPreserved.Load())
					require.Equal(t, 0, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_iscp_log where table_id=? and job_id=? and drop_at is null", sourceTableID, sourceJobID))
					require.Equal(t, 1, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_iscp_log where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t') and job_name='index_ft' and drop_at is null", name))
				}
				if failed {
					execSQLRequire(t, ctx, db, alterSQL)
				}
				require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like 't_copy_%'", name))
			})
		}
	})
}

func (o *issue31401CopyObserver) Exec(ctx context.Context, q string, opts executor.Options) (executor.Result, error) {
	copyOpt := opts.StatementOption().AlterCopyDedupOpt()
	copySQL := copyOpt != nil && strings.HasPrefix(strings.ToLower(q), "insert into")
	if copySQL {
		o.replicas.Store(copyOpt.TargetTableName, struct{}{})
	}
	retiring := false
	if strings.HasPrefix(q, "drop table ") {
		o.replicas.Range(func(name, _ any) bool {
			retiring = retiring || strings.Contains(q, "`"+name.(string)+"`")
			return !retiring
		})
	}
	if retiring {
		o.retired.Add(1)
		if o.beforeRetire != nil {
			if err := o.beforeRetire(); err != nil {
				return executor.Result{}, err
			}
		}
	}
	if o.beforeRecreate != nil && o.retired.Load() == 1 && strings.HasPrefix(strings.ToLower(q), "create table ") {
		matched := false
		o.replicas.Range(func(name, _ any) bool {
			matched = strings.Contains(q, name.(string))
			return !matched
		})
		if matched {
			if err := o.beforeRecreate(); err != nil {
				return executor.Result{}, err
			}
		}
	}
	if o.beforeSourceDrop != nil && !retiring && strings.HasPrefix(q, "drop table ") && strings.HasSuffix(q, ".`t`") {
		if err := o.beforeSourceDrop(); err != nil {
			return executor.Result{}, err
		}
	}
	result, err := o.SQLExecutor.Exec(ctx, q, opts)
	if err == nil && retiring && o.afterRetire != nil {
		if err = o.afterRetire(ctx); err != nil {
			result.Close()
			return executor.Result{}, err
		}
	}
	if err == nil && copySQL {
		n := o.copies.Add(1)
		if o.afterCopy != nil {
			if err = o.afterCopy(ctx, n); err != nil {
				result.Close()
				return executor.Result{}, err
			}
		}
	}
	return result, err
}

func issue31401Observe(t *testing.T, sid string) *issue31401CopyObserver {
	t.Helper()
	rt := moruntime.ServiceRuntime(sid)
	original, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	require.True(t, ok)
	o := &issue31401CopyObserver{SQLExecutor: original.(executor.SQLExecutor)}
	rt.SetGlobalVariables(moruntime.InternalSQLExecutor, o)
	t.Cleanup(func() { rt.SetGlobalVariables(moruntime.InternalSQLExecutor, original) })
	return o
}

func issue31401Rows(t *testing.T, ctx context.Context, db *sql.DB, q string) [][3]int {
	t.Helper()
	rows, err := db.QueryContext(ctx, q)
	require.NoError(t, err)
	defer rows.Close()
	var got [][3]int
	for rows.Next() {
		var row [3]int
		require.NoError(t, rows.Scan(&row[0], &row[1], &row[2]))
		got = append(got, row)
	}
	require.NoError(t, rows.Err())
	return got
}

func TestIssue31401AlterCopyWaitsForLoad(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		t.Logf("embedded owner pid=%d config=%s/cn-0.toml data=%s", os.Getpid(), cn.GetServiceConfig().DataDir, cn.GetServiceConfig().DataDir)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		for _, tc := range []struct {
			name, definition, suffix string
			rollback, cancel         bool
		}{
			{name: "no_pk", definition: "id int,v int"},
			{name: "pk_indexes", definition: "id int primary key,v int,unique key uq(v),key idx_v(v)"},
			{name: "partition_allocator", definition: "id int auto_increment primary key,v int,key idx_v(v)", suffix: " auto_id_cache=1 partition by hash(id) partitions 2"},
			{name: "load_rollback", definition: "id int primary key,v int", rollback: true},
			{name: "cancel_wait", definition: "id int primary key,v int", cancel: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
				defer cancel()
				database := "issue31401_" + tc.name
				resetIssue277xxDatabase(t, ctx, db, database)
				defer func() {
					clean, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					_, _ = db.ExecContext(clean, "drop database if exists "+database)
				}()
				execSQLRequire(t, ctx, db, "create table "+database+".t("+tc.definition+")"+tc.suffix)
				var sourceID uint64
				require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='t'", database).Scan(&sourceID))
				csv := filepath.Join(t.TempDir(), "two.csv")
				require.NoError(t, os.WriteFile(csv, []byte("1,10\n2,20\n"), 0600))
				load, err := db.Conn(ctx)
				require.NoError(t, err)
				defer load.Close()
				alter, err := db.Conn(ctx)
				require.NoError(t, err)
				defer alter.Close()
				connID := func(c *sql.Conn) uint32 {
					var id uint32
					require.NoError(t, c.QueryRowContext(ctx, "select connection_id()").Scan(&id))
					return id
				}
				loadID, alterID := connID(load), connID(alter)
				sm := cn.RawService().(interface {
					SessionMgr() *queryservice.SessionManager
				}).SessionMgr()
				belongs := func(actor []byte, id uint32) bool {
					return issue29715TxnBelongsToConnection(sm.GetAllSessions(), cn.ServiceID(), id, actor)
				}
				if tc.rollback {
					_, err = load.ExecContext(ctx, "begin")
					require.NoError(t, err)
					defer load.ExecContext(context.WithoutCancel(ctx), "rollback")
				}
				locked, release := make(chan struct{}), make(chan struct{})
				var first atomic.Bool
				var releaseOnce sync.Once
				unblock := func() { releaseOnce.Do(func() { close(release) }) }
				defer unblock()
				testingCtx := moruntime.MustGetTestingContext(cn.ServiceID())
				before, adjust := testingCtx.GetBeforeLockFunc(), testingCtx.GetAdjustLockResultFunc()
				testingCtx.SetBeforeLockFunc(func([]byte, uint64) {})
				defer testingCtx.SetBeforeLockFunc(before)
				testingCtx.SetAdjustLockResultFunc(func(actor []byte, id uint64, _ *pblock.Result) {
					if id == catalog.MO_TABLES_ID && belongs(actor, loadID) && first.CompareAndSwap(false, true) {
						close(locked)
						select {
						case <-release:
						case <-ctx.Done():
						}
					}
				})
				defer testingCtx.SetAdjustLockResultFunc(adjust)
				queued := make(chan struct{}, 1)
				restore := lockservice.SetWaiterEnqueuedHookForTest(func(id uint64, actor []byte, _ [][]byte) {
					if id == catalog.MO_TABLES_ID && belongs(actor, alterID) {
						select {
						case queued <- struct{}{}:
						default:
						}
					}
				})
				defer restore()
				observer := issue31401Observe(t, cn.ServiceID())
				loadDone, alterDone := make(chan error, 1), make(chan error, 1)
				alterCtx, stopAlter := context.WithCancel(ctx)
				defer stopAlter()
				loadJoined, alterStarted, alterJoined := false, false, false
				defer func() {
					unblock()
					stopAlter()
					cancel()
					if !loadJoined {
						<-loadDone
					}
					if alterStarted && !alterJoined {
						<-alterDone
					}
				}()
				go func() {
					_, e := load.ExecContext(ctx, fmt.Sprintf("load data infile '%s' into table %s.t", csv, database))
					if e == nil && tc.rollback {
						_, e = load.ExecContext(ctx, "rollback")
					}
					loadDone <- e
				}()
				select {
				case <-locked:
				case e := <-loadDone:
					loadJoined = true
					t.Fatalf("LOAD missed barrier: %v", e)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				alterStarted = true
				go func() {
					_, e := alter.ExecContext(alterCtx, "alter table "+database+".t add column d int default 7")
					alterDone <- e
				}()
				select {
				case <-queued:
				case e := <-alterDone:
					alterJoined = true
					t.Fatalf("ALTER bypassed LOAD: %v", e)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				if tc.cancel {
					_, err = db.ExecContext(ctx, fmt.Sprintf("kill query %d", alterID))
					require.NoError(t, err)
					err = <-alterDone
					alterJoined = true
					require.Error(t, err)
				}
				unblock()
				err = <-loadDone
				loadJoined = true
				require.NoError(t, err)
				if !alterJoined {
					err = <-alterDone
					alterJoined = true
					require.NoError(t, err, "the first ALTER must finish without external retry")
				}
				wantCopies, wantRetired := int32(2), int32(1)
				if tc.rollback || tc.cancel {
					wantCopies, wantRetired = 1, 0
				}
				require.Equal(t, wantCopies, observer.copies.Load())
				require.Equal(t, wantRetired, observer.retired.Load())
				if tc.cancel {
					execSQLRequire(t, ctx, db, "alter table "+database+".t add column d int default 7")
				}
				var want [][3]int
				if !tc.rollback {
					want = [][3]int{{1, 10, 7}, {2, 20, 7}}
				}
				require.Equal(t, want, issue31401Rows(t, ctx, db, "select id,v,d from "+database+".t order by id"))
				if strings.Contains(tc.definition, "idx_v") {
					require.Equal(t, [][3]int{{2, 20, 7}}, issue31401Rows(t, ctx, db, "select id,v,d from "+database+".t force index(idx_v) where v=20"))
				}
				if tc.name == "partition_allocator" {
					execSQLRequire(t, ctx, db, "insert into "+database+".t(v) values(30)")
					require.Equal(t, [][3]int{{3, 30, 7}}, issue31401Rows(t, ctx, db, "select id,v,d from "+database+".t where v=30"))
				}
				require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like 't_copy_%'", database))
			})
		}
	})
}
