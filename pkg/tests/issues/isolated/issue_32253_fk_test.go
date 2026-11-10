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
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/iscp"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/stretchr/testify/require"
)

func TestPR32253ForeignKeyAddedAfterAlterPlanning(t *testing.T) {
	path, err := os.MkdirTemp("", "copy-fk-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(path)) })
	cluster, err := embed.NewCluster(embed.WithTesting(), embed.WithDataPath(path), embed.WithCNCount(1))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	require.NoError(t, cluster.Start())
	cn, err := cluster.GetCNService(0)
	require.NoError(t, err)
	rt := moruntime.ServiceRuntime(cn.ServiceID())
	oldMode, hadMode := rt.GetGlobalVariables(moruntime.TxnMode)
	oldIsolation, hadIsolation := rt.GetGlobalVariables(moruntime.TxnIsolation)
	rt.SetGlobalVariables(moruntime.TxnMode, pbtxn.TxnMode_Pessimistic)
	rt.SetGlobalVariables(moruntime.TxnIsolation, pbtxn.TxnIsolation_RC)
	t.Cleanup(func() {
		if hadMode {
			rt.SetGlobalVariables(moruntime.TxnMode, oldMode)
		}
		if hadIsolation {
			rt.SetGlobalVariables(moruntime.TxnIsolation, oldIsolation)
		}
	})
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	db.SetMaxOpenConns(3)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	require.Eventually(t, func() bool { e, ok := iscp.GetExecutorRuntime(cn.ServiceID()); return ok && e.IsRunning() }, 120*time.Second, 250*time.Millisecond)
	provider, ok := cn.RawService().(interface {
		SessionMgr() *queryservice.SessionManager
	})
	require.True(t, ok)
	manager := provider.SessionMgr()
	require.NotNil(t, manager)
	for _, tc := range []struct {
		name         string
		physicalLock bool
		safeChange   bool
	}{
		{name: "before catalog lock"},
		{name: "before physical table lock", physicalLock: true},
		{name: "safe change after child creation", physicalLock: true, safeChange: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			physicalLock := tc.physicalLock
			safeChange := tc.safeChange
			exec := func(q string) { t.Helper(); _, e := db.ExecContext(ctx, q); require.NoError(t, e, q) }
			exec("create database issue32253_fk_race")
			t.Cleanup(func() {
				cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
				defer stop()
				_, e := db.ExecContext(cleanupCtx, "drop database if exists issue32253_fk_race")
				require.NoError(t, e)
			})
			exec("create table issue32253_fk_race.p (a decimal(5,2), id decimal(6,2) generated always as (a*2) stored, primary key(id))")
			exec("insert into issue32253_fk_race.p(a) values (1.21)")
			alterConn, err := db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, alterConn.Close()) })
			var connectionID uint32
			require.NoError(t, alterConn.QueryRowContext(ctx, "select connection_id()").Scan(&connectionID))
			lockTableID := uint64(catalog.MO_TABLES_ID)
			if physicalLock {
				require.NoError(t, db.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where reldatabase='issue32253_fk_race' and relname='p'").Scan(&lockTableID))
			}
			paused, release := make(chan struct{}), make(chan struct{})
			var first atomic.Bool
			var once sync.Once
			tc := moruntime.MustGetTestingContext(cn.ServiceID())
			tc.SetBeforeLockFunc(func(txnID []byte, tableID uint64) {
				if tableID == lockTableID && issue32253TxnBelongsToConnection(manager.GetAllSessions(), cn.ServiceID(), connectionID, txnID) && first.CompareAndSwap(false, true) {
					close(paused)
					select {
					case <-release:
					case <-ctx.Done():
					}
				}
			})
			tc.SetAdjustLockResultFunc(func([]byte, uint64, *pblock.Result) {})
			done := make(chan error, 1)
			joined := false
			t.Cleanup(func() {
				once.Do(func() { close(release) })
				tc.SetBeforeLockFunc(nil)
				tc.SetAdjustLockResultFunc(nil)
				if !joined {
					select {
					case <-done:
					case <-time.After(15 * time.Second):
						t.Error("ALTER did not exit during cleanup")
					}
				}
			})
			alterSQL := "alter table issue32253_fk_race.p algorithm=copy, change a a decimal(5,1)"
			if safeChange {
				alterSQL = "alter table issue32253_fk_race.p algorithm=copy, add column extra int"
			}
			go func() {
				_, e := alterConn.ExecContext(ctx, alterSQL)
				done <- e
			}()
			select {
			case <-paused:
				t.Log("FK_RACE_PLANNED_ALTER_PAUSED")
			case <-ctx.Done():
				t.Fatal("ALTER did not reach parent metadata lock")
			}
			exec("create table issue32253_fk_race.c (id int primary key, pid decimal(6,2), foreign key(pid) references issue32253_fk_race.p(id))")
			t.Log("FK_RACE_CHILD_COMMITTED")
			once.Do(func() { close(release) })
			select {
			case alterErr := <-done:
				joined = true
				if safeChange {
					require.NoError(t, alterErr)
				} else {
					var mysqlErr *mysql.MySQLError
					require.ErrorAs(t, alterErr, &mysqlErr)
					require.Equal(t, uint16(1833), mysqlErr.Number)
				}
			case <-ctx.Done():
				t.Fatal("ALTER did not finish")
			}
			exec("insert into issue32253_fk_race.c select 1,id from issue32253_fk_race.p")
			var orphans int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from issue32253_fk_race.c c left join issue32253_fk_race.p p on c.pid=p.id where p.id is null").Scan(&orphans))
			t.Logf("FK_RACE_ORPHANS %d", orphans)
			require.Zero(t, orphans)
			_, err = db.ExecContext(ctx, "insert into issue32253_fk_race.c values (2, 9.99)")
			require.Error(t, err)
			_, err = db.ExecContext(ctx, "drop table issue32253_fk_race.p")
			require.Error(t, err)
		})
	}
}

// Read only the transaction identity, not mutable statement status fields.
func issue32253TxnBelongsToConnection(sessions []queryservice.Session, sid string, connectionID uint32, txnID []byte) bool {
	for _, session := range sessions {
		identity, ok := session.(interface {
			GetTxnId() uuid.UUID
			GetConnectionID() uint32
			GetService() string
		})
		if ok {
			id := identity.GetTxnId()
			if id != (uuid.UUID{}) && bytes.Equal(id[:], txnID) && identity.GetService() == sid && identity.GetConnectionID() == connectionID {
				return true
			}
		}
	}
	return false
}
