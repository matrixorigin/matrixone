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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/stretchr/testify/require"
)

func TestIssue31401ScalarRefreshProgress(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		t.Logf("embedded owner pid=%d config=%s/cn-0.toml data=%s", os.Getpid(), cn.GetServiceConfig().DataDir, cn.GetServiceConfig().DataDir)
		sys := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer sys.Close()
		for _, mode := range []string{"ordinary", "pk", "allocator", "prepared", "defaults", "other_account", "broad_index", "initial_snapshot", "late_snapshot", "initial_pitr", "late_pitr", "late_branch", "tenant_snapshot", "upgrade_conflict", "initial_fk", "late_fk"} {
			t.Run(mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
				defer cancel()
				db := sys
				if mode == "other_account" || mode == "tenant_snapshot" {
					execSQLRequire(t, ctx, sys, "create account issue31401_scalar admin_name 'admin' identified by '111'")
					defer func() {
						clean, stop := context.WithTimeout(context.Background(), 20*time.Second)
						defer stop()
						execSQLMaybe(t, clean, sys, "drop account if exists issue31401_scalar")
					}()
					db, err = sql.Open("mysql", fmt.Sprintf("issue31401_scalar#admin#accountadmin:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
					require.NoError(t, err)
					defer db.Close()
				}
				const source, peer = "issue31401_progress", "issue31401_peer"
				resetIssue277xxDatabase(t, ctx, db, source)
				resetIssue277xxDatabase(t, ctx, sys, peer)
				defer func() {
					clean, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					execSQLMaybe(t, clean, db, "drop database if exists "+source)
					execSQLMaybe(t, clean, sys, "drop database if exists "+peer)
				}()
				definition := "id int,v int"
				if mode == "pk" || mode == "other_account" || strings.HasSuffix(mode, "fk") || mode == "late_branch" {
					definition = "id int primary key,v int"
				} else if mode == "allocator" {
					definition = "id int auto_increment primary key,v int"
				} else if mode == "broad_index" {
					definition = "id int,v int,key idx_v(v)"
				} else if mode == "defaults" {
					definition = "id int not null,v int not null,body varchar(12) not null default 'needle',created timestamp not null default current_timestamp,changed timestamp default current_timestamp on update current_timestamp"
				}
				tableOptions := ""
				if mode == "allocator" {
					tableOptions = " auto_id_cache=1"
				}
				execSQLRequire(t, ctx, db, "create table "+source+".t("+definition+")"+tableOptions)
				execSQLRequire(t, ctx, db, "insert into "+source+".t(id,v) values(1,10)")
				version := 0
				if mode == "late_fk" {
					version = queryIssue277xxInt(t, ctx, db, "select rel_version from mo_catalog.mo_tables where reldatabase=? and relname='t'", source)
				}
				if mode == "initial_fk" {
					execSQLRequire(t, ctx, db, "create table "+source+".child(pid int,foreign key(pid) references "+source+".t(id))")
				}
				protect := func() {
					if mode == "late_fk" {
						execSQLRequire(t, ctx, db, "create table "+source+".child(pid int,foreign key(pid) references "+source+".t(id))")
					} else if strings.Contains(mode, "pitr") {
						execSQLRequire(t, ctx, db, "create pitr issue31401_protect for table "+source+" t range 1 'h'")
					} else if mode == "late_branch" {
						execSQLRequire(t, ctx, db, "data branch create table "+source+".branch from "+source+".t")
					} else {
						execSQLRequire(t, ctx, db, "create snapshot issue31401_protect for table "+source+" t")
					}
				}
				if strings.Contains(mode, "snapshot") || mode == "upgrade_conflict" || strings.Contains(mode, "pitr") {
					defer func() {
						clean, stop := context.WithTimeout(context.Background(), 10*time.Second)
						defer stop()
						kind := "snapshot"
						if strings.Contains(mode, "pitr") {
							kind = "pitr"
						}
						execSQLMaybe(t, clean, db, "drop "+kind+" if exists issue31401_protect")
					}()
				}
				if strings.HasPrefix(mode, "initial_") && mode != "initial_fk" || mode == "tenant_snapshot" {
					protect()
				}
				execSQLRequire(t, ctx, sys, "create table "+peer+".t(id int)")
				execSQLRequire(t, ctx, sys, "insert into "+peer+".t values(1)")
				var registryID uint64
				require.NoError(t, sys.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_feature_registry'").Scan(&registryID))
				conn, e := sys.Conn(ctx)
				require.NoError(t, e)
				defer conn.Close()
				defer func() { _, _ = conn.ExecContext(context.Background(), "rollback") }()
				alterSQL := "alter table " + source + ".t add column d int default 7"
				var preparedAlter *sql.Stmt
				if mode == "prepared" {
					statement, prepareErr := db.PrepareContext(ctx, alterSQL)
					require.NoError(t, prepareErr)
					defer statement.Close()
					preparedAlter = statement
				}
				var connectionID uint32
				require.NoError(t, conn.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
				prepared, rebuilding, proceed, publish := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
				var prepareOnce, publishOnce sync.Once
				releasePrepare := func() { prepareOnce.Do(func() { close(proceed) }) }
				releasePublish := func() { publishOnce.Do(func() { close(publish) }) }
				defer releasePrepare()
				defer releasePublish()
				observer := issue31401Observe(t, cn.ServiceID())
				observer.afterCopy = func(callCtx context.Context, n int32) error {
					var release <-chan struct{}
					if n == 1 {
						close(prepared)
						release = proceed
					} else if n == 2 {
						close(rebuilding)
						release = publish
					} else {
						return fmt.Errorf("unexpected COPY generation %d", n)
					}
					select {
					case <-release:
						return nil
					case <-callCtx.Done():
						return callCtx.Err()
					}
				}
				alterDone, peerDone := make(chan error, 1), make(chan error, 1)
				alterJoined, peerStarted, peerJoined := false, false, false
				defer func() {
					releasePrepare()
					releasePublish()
					cancel()
					if !alterJoined {
						<-alterDone
					}
					if peerStarted && !peerJoined {
						<-peerDone
					}
				}()
				go func() {
					var e error
					if preparedAlter != nil {
						_, e = preparedAlter.ExecContext(ctx)
					} else {
						_, e = db.ExecContext(ctx, alterSQL)
					}
					alterDone <- e
				}()
				wait := func(barrier <-chan struct{}) {
					select {
					case <-barrier:
					case e := <-alterDone:
						alterJoined = true
						t.Fatalf("ALTER missed preparation barrier: %v", e)
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				wait(prepared)
				if strings.HasPrefix(mode, "late_") || mode == "upgrade_conflict" {
					protect()
				}
				if mode == "late_fk" {
					require.Greater(t, queryIssue277xxInt(t, ctx, db, "select rel_version from mo_catalog.mo_tables where reldatabase=? and relname='t'", source), version,
						"incoming FK changes the source definition, so this is a terminal definition race")
				}
				if mode == "upgrade_conflict" {
					_, e = conn.ExecContext(ctx, "begin")
					require.NoError(t, e)
					_, e = conn.ExecContext(ctx, "drop table "+peer+".t")
					require.NoError(t, e)
				}
				execSQLRequire(t, ctx, db, "insert into "+source+".t(id,v) values(2,20)")
				var defaults [3]string
				if mode == "defaults" {
					require.NoError(t, db.QueryRowContext(ctx, "select body,created,changed from "+source+".t where id=1").Scan(&defaults[0], &defaults[1], &defaults[2]))
				}
				releasePrepare()
				if mode == "upgrade_conflict" || mode == "late_fk" {
					select {
					case e = <-alterDone:
						alterJoined = true
						if mode == "upgrade_conflict" {
							requireIssue277xxMySQLError(t, e, moerr.ErrLockConflict)
						} else {
							requireIssue277xxMySQLError(t, e, moerr.ErrTxnWriteConflict)
						}
					case <-rebuilding:
						t.Fatal("conflicting G promotion reached a second COPY")
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
					require.EqualValues(t, 1, observer.copies.Load())
					require.Zero(t, observer.retired.Load())
					require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_columns where att_database=? and att_relname='t' and attname='d'", source))
					require.Equal(t, [][3]int{{1, 10, 0}, {2, 20, 0}}, issue31401Rows(t, ctx, db, "select id,v,0 from "+source+".t order by id"))
					if mode == "late_fk" {
						_, invalid := db.ExecContext(ctx, "insert into "+source+".child values(999)")
						requireIssue277xxForeignKeyError(t, invalid)
					}
					_, e = conn.ExecContext(ctx, "rollback")
					require.NoError(t, e)
					execSQLRequire(t, ctx, sys, "truncate table "+peer+".t")
					return
				}
				wait(rebuilding)
				sm := cn.RawService().(interface {
					SessionMgr() *queryservice.SessionManager
				}).SessionMgr()
				queued := make(chan struct{}, 1)
				restore := lockservice.SetWaiterEnqueuedHookForTest(func(id uint64, actor []byte, _ [][]byte) {
					if id == registryID && issue29715TxnBelongsToConnection(sm.GetAllSessions(), cn.ServiceID(), connectionID, actor) {
						select {
						case queued <- struct{}{}:
						default:
						}
					}
				})
				defer restore()
				peerStarted = true
				go func() {
					_, e := conn.ExecContext(ctx, "truncate table "+peer+".t")
					peerDone <- e
				}()
				expectBroad := mode == "broad_index" || strings.HasPrefix(mode, "initial_") || strings.HasPrefix(mode, "late_") || mode == "tenant_snapshot"
				select {
				case <-queued:
					require.True(t, expectBroad, "unrelated scalar TRUNCATE waited on G during refresh")
				case e := <-peerDone:
					peerJoined = true
					require.NoError(t, e)
					require.False(t, expectBroad)
					require.Zero(t, queryIssue277xxInt(t, ctx, sys, "select count(*) from "+peer+".t"))
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				releasePublish()
				err = <-alterDone
				alterJoined = true
				require.NoError(t, err)
				if !peerJoined {
					err = <-peerDone
					peerJoined = true
					require.NoError(t, err)
				}
				require.EqualValues(t, 2, observer.copies.Load())
				require.EqualValues(t, 1, observer.retired.Load())
				require.Equal(t, [][3]int{{1, 10, 7}, {2, 20, 7}}, issue31401Rows(t, ctx, db, "select id,v,d from "+source+".t order by id"))
				if mode == "defaults" {
					var got [3]string
					require.NoError(t, db.QueryRowContext(ctx, "select body,created,changed from "+source+".t where id=1").Scan(&got[0], &got[1], &got[2]))
					require.Equal(t, defaults, got, "COPY must preserve stored strings/timestamps and ON UPDATE values")
				}
				if strings.Contains(mode, "snapshot") {
					require.Equal(t, 1, queryIssue277xxInt(t, ctx, db, "select count(*) from "+source+".t{snapshot='issue31401_protect'}"))
				}
				if mode == "late_branch" {
					require.Equal(t, 1, queryIssue277xxInt(t, ctx, db, "select count(*) from "+source+".branch"))
				}
				if strings.HasSuffix(mode, "fk") {
					_, invalid := db.ExecContext(ctx, "insert into "+source+".child values(999)")
					requireIssue277xxForeignKeyError(t, invalid)
				}
				if mode == "allocator" {
					execSQLRequire(t, ctx, db, "insert into "+source+".t(v) values(30)")
					require.Equal(t, [][3]int{{3, 30, 7}}, issue31401Rows(t, ctx, db, "select id,v,d from "+source+".t where v=30"))
				}
				require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like 't_copy_%'", source))
			})
		}
	})
}

func TestIssue31401ScalarRetainedLocksFastFail(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db := openIssue277xxDB(t, cn.GetServiceConfig().CN.Frontend.Port)
		defer db.Close()
		for _, mode := range []string{"component", "name"} {
			t.Run(mode, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
				defer cancel()
				const database = "issue31401_retained"
				resetIssue277xxDatabase(t, ctx, db, database)
				defer func() {
					clean, stop := context.WithTimeout(context.Background(), 10*time.Second)
					defer stop()
					execSQLMaybe(t, clean, db, "drop database if exists "+database)
				}()
				for _, table := range []string{"a", "b"} {
					execSQLRequire(t, ctx, db, "create table "+database+"."+table+"(id int primary key,v int)")
					execSQLRequire(t, ctx, db, "insert into "+database+"."+table+" values(1,10)")
				}
				a, err := db.Conn(ctx)
				require.NoError(t, err)
				defer a.Close()
				b, err := db.Conn(ctx)
				require.NoError(t, err)
				defer b.Close()
				defer func() { _, _ = a.ExecContext(context.Background(), "rollback") }()
				defer func() { _, _ = b.ExecContext(context.Background(), "rollback") }()
				var aid, bid uint32
				require.NoError(t, a.QueryRowContext(ctx, "select connection_id()").Scan(&aid))
				require.NoError(t, b.QueryRowContext(ctx, "select connection_id()").Scan(&bid))
				var aTable, componentTable uint64
				require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase=? and relname='a'", database).Scan(&aTable))
				require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_branch_metadata'").Scan(&componentTable))
				_, err = a.ExecContext(ctx, "begin")
				require.NoError(t, err)
				_, err = a.ExecContext(ctx, "update "+database+".a set v=11 where id=1")
				require.NoError(t, err)
				_, err = b.ExecContext(ctx, "begin")
				require.NoError(t, err)
				if mode == "name" {
					_, err = b.ExecContext(ctx, "update "+database+".b set v=20 where id=1")
					require.NoError(t, err)
				}
				sm := cn.RawService().(interface {
					SessionMgr() *queryservice.SessionManager
				}).SessionMgr()
				blocked, reverseWait := make(chan struct{}, 1), make(chan struct{}, 1)
				restore := lockservice.SetWaiterEnqueuedHookForTest(func(id uint64, actor []byte, _ [][]byte) {
					peerKey, reverseKey := uint64(catalog.MO_TABLES_ID), componentTable
					if mode == "name" {
						peerKey, reverseKey = aTable, uint64(catalog.MO_TABLES_ID)
					}
					if id == peerKey && issue29715TxnBelongsToConnection(sm.GetAllSessions(), cn.ServiceID(), bid, actor) {
						select {
						case blocked <- struct{}{}:
						default:
						}
					}
					if id == reverseKey && issue29715TxnBelongsToConnection(sm.GetAllSessions(), cn.ServiceID(), aid, actor) {
						select {
						case reverseWait <- struct{}{}:
						default:
						}
					}
				})
				defer restore()
				observer := issue31401Observe(t, cn.ServiceID())
				alterDone, peerDone := make(chan error, 1), make(chan error, 1)
				alterCtx, stopAlter := context.WithCancel(ctx)
				defer stopAlter()
				alterStarted, alterJoined, peerJoined := false, false, false
				defer func() {
					stopAlter()
					cancel()
					if alterStarted && !alterJoined {
						<-alterDone
					}
					_, _ = a.ExecContext(context.Background(), "rollback")
					if !peerJoined {
						<-peerDone
					}
				}()
				go func() {
					query := "drop table " + database + ".a," + database + ".b"
					if mode == "name" {
						query = "update " + database + ".a set v=30 where id=1"
					}
					_, e := b.ExecContext(ctx, query)
					peerDone <- e
				}()
				select {
				case <-blocked:
				case e := <-peerDone:
					peerJoined = true
					t.Fatalf("peer missed retained-lock barrier: %v", e)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				alterStarted = true
				go func() {
					_, e := a.ExecContext(alterCtx, "alter table "+database+".b add column d int default 7")
					alterDone <- e
				}()
				select {
				case e := <-alterDone:
					alterJoined = true
					requireIssue277xxMySQLError(t, e, moerr.ErrLockConflict)
				case <-reverseWait:
					t.Fatal("retained caller locks must FastFail instead of queuing behind their waiter")
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				// The ALTER's terminal error must release A before any manual rollback.
				select {
				case e := <-peerDone:
					peerJoined = true
					require.NoError(t, e)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				_, err = b.ExecContext(ctx, "rollback")
				require.NoError(t, err)
				require.EqualValues(t, 1, observer.copies.Load())
				require.Zero(t, observer.retired.Load())
				require.Equal(t, 10, queryIssue277xxInt(t, ctx, db, "select v from "+database+".a"))
				require.Equal(t, 10, queryIssue277xxInt(t, ctx, db, "select v from "+database+".b"))
				require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_columns where att_database=? and att_relname='b' and attname='d'", database))
				require.Zero(t, queryIssue277xxInt(t, ctx, db, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname like 'b_copy_%'", database))
			})
		}
	})
}
