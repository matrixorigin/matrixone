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

package issues

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/stretchr/testify/require"
)

func TestIssue28317ViewSnapshotGateOrderSQL(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		cn0, err := cluster.GetCNService(0)
		require.NoError(t, err)
		cn1, err := cluster.GetCNService(1)
		require.NoError(t, err)
		db0, err := sql.Open("mysql", issue27487DSN(cn0.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db0.Close()
		db1, err := sql.Open("mysql", issue27487DSN(cn1.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db1.Close()
		services := issue27487LockServices(cluster)
		require.NotEmpty(t, services)
		const dbA, dbB = "issue_28317_a", "issue_28317_b"
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, sql := range []string{"drop database if exists `" + dbA + "`", "drop database if exists `" + dbB + "`"} {
				if _, err := db0.ExecContext(cleanupCtx, sql); err != nil {
					t.Errorf("cleanup %s: %v", sql, err)
				}
			}
		}()
		for _, sql := range []string{
			"drop database if exists `" + dbA + "`", "drop database if exists `" + dbB + "`",
			"create database `" + dbA + "`", "create database `" + dbB + "`",
			"create table `" + dbB + "`.`existing` (id int primary key, v int)",
			"insert into `" + dbB + "`.`existing` values (1, 1)",
		} {
			execSQLRequire(t, ctx, db0, sql)
		}
		var snapshotTableID uint64
		require.NoError(t, db0.QueryRowContext(ctx,
			"select rel_id from mo_catalog.mo_tables where reldatabase='mo_catalog' and relname='mo_feature_registry'").Scan(&snapshotTableID))
		for _, tc := range []struct {
			name, mode string
			want       []string
		}{
			{"cancel-waiter-and-rollback-holder", "cancel", []string{"int", "bigint"}},
			{"rollback-holder", "rollback", []string{"bigint"}},
			{"drop-and-commit", "drop", []string{"bigint"}},
		} {
			t.Run(tc.name, func(t *testing.T) {
				execSQLRequire(t, ctx, db0, "drop table if exists `"+dbA+"`.`ephemeral`")
				execSQLRequire(t, ctx, db0, "alter table `"+dbB+"`.`existing` modify column v int")
				runIssue28317(t, ctx, db0, db1, services, snapshotTableID, dbA, dbB, tc.mode, tc.want)
			})
		}
		t.Run("ordinary tenant SNAPSHOT and PITR restore", func(t *testing.T) {
			runIssue28317OrdinaryTenantRestores(t, ctx, db0, cn0.GetServiceConfig().CN.Frontend.Port)
		})
	})
}

func runIssue28317OrdinaryTenantRestores(t *testing.T, parent context.Context, sysDB *sql.DB, port int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()

	const (
		accountName  = "issue_28317_tenant"
		databaseName = "issue_28317_tenant_db"
		snapshotName = "issue_28317_tenant_snapshot"
		pitrName     = "issue_28317_tenant_pitr"
	)

	execSQLMaybe(t, ctx, sysDB, "drop snapshot if exists "+snapshotName)
	execSQLMaybe(t, ctx, sysDB, "drop account if exists `"+accountName+"`")
	execSQLRequire(t, ctx, sysDB,
		"create account `"+accountName+"` admin_name 'admin' identified by '111'")

	openTenant := func() *sql.DB {
		tenantDB, err := sql.Open("mysql", fmt.Sprintf(
			"%s#admin#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, port,
		))
		require.NoError(t, err)
		return tenantDB
	}
	tenantDB := openTenant()
	defer tenantDB.Close()
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		execSQLMaybe(t, cleanupCtx, tenantDB, "drop pitr if exists "+pitrName)
		execSQLMaybe(t, cleanupCtx, tenantDB, "drop snapshot if exists "+snapshotName)
		execSQLMaybe(t, cleanupCtx, sysDB, "drop account if exists `"+accountName+"`")
	}()

	execSQLRequire(t, ctx, tenantDB, "create database `"+databaseName+"`")
	execSQLRequire(t, ctx, tenantDB,
		"create table `"+databaseName+"`.t (id int primary key)")
	execSQLRequire(t, ctx, tenantDB,
		"insert into `"+databaseName+"`.t values (1)")

	execSQLRequire(t, ctx, tenantDB,
		"create snapshot "+snapshotName+" for account")
	execSQLRequire(t, ctx, tenantDB,
		"insert into `"+databaseName+"`.t values (2)")
	execSQLRequire(t, ctx, tenantDB,
		"restore account `"+accountName+"`{snapshot='"+snapshotName+"'}")

	afterSnapshot := openTenant()
	defer afterSnapshot.Close()
	var rows int
	require.NoError(t, afterSnapshot.QueryRowContext(ctx,
		"select count(*) from `"+databaseName+"`.t").Scan(&rows))
	require.Equal(t, 1, rows)

	execSQLRequire(t, ctx, afterSnapshot,
		"create pitr "+pitrName+" for account range 1 'h'")
	var restoreAt string
	require.NoError(t, afterSnapshot.QueryRowContext(ctx,
		"select date_format(current_timestamp(6), '%Y-%m-%d %H:%i:%s.%f')").Scan(&restoreAt))
	execSQLRequire(t, ctx, afterSnapshot,
		"restore from pitr "+pitrName+" '"+restoreAt+"'")

	afterPitr := openTenant()
	defer afterPitr.Close()
	require.NoError(t, afterPitr.QueryRowContext(ctx,
		"select count(*) from `"+databaseName+"`.t").Scan(&rows))
	require.Equal(t, 1, rows)
}

func runIssue28317(t *testing.T, parent context.Context, db0, db1 *sql.DB, services []lockservice.LockService, snapshotID uint64, dbA, dbB, mode string, wantTypes []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	a, err := db0.Conn(ctx)
	require.NoError(t, err)
	defer a.Close()
	b, err := db1.Conn(ctx)
	require.NoError(t, err)
	defer b.Close()
	require.Eventually(t, func() bool {
		var n int
		return b.QueryRowContext(ctx, "select count(*) from `"+dbB+"`.`existing`").Scan(&n) == nil && n == 1
	}, 20*time.Second, 10*time.Millisecond, "second CN did not see seed data")

	_, err = a.ExecContext(ctx, "begin")
	require.NoError(t, err)
	holderDone := false
	defer func() {
		if !holderDone {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
			if _, err := a.ExecContext(cleanupCtx, "rollback"); err != nil {
				t.Errorf("holder rollback cleanup: %v", err)
			}
			cleanupCancel()
		}
	}()
	for _, sql := range []string{
		"create table `" + dbA + "`.`ephemeral` (id int primary key)",
		"insert into `" + dbA + "`.`ephemeral` values (1), (2), (3)",
	} {
		_, err = a.ExecContext(ctx, sql)
		require.NoError(t, err, sql)
	}
	beforeSnapshot, beforeView := issue28317Waiters(services, snapshotID)
	alterCtx, cancelAlter := context.WithTimeout(ctx, 60*time.Second)
	done := make(chan error, 1)
	go func() {
		_, err := b.ExecContext(alterCtx, "alter table `"+dbB+"`.`existing` modify column v bigint")
		done <- err
	}()
	alterDone := false
	defer func() {
		cancelAlter()
		if !alterDone {
			select {
			case <-done:
				alterDone = true
			case <-time.After(15 * time.Second):
				t.Errorf("ALTER cleanup join timed out")
			}
		}
	}()

	var gate string
	require.Eventually(t, func() bool {
		snapshot, view := issue28317Waiters(services, snapshotID)
		if snapshot > beforeSnapshot {
			gate = "SNAPSHOT"
		} else if view > beforeView {
			gate = "View"
		} else {
			return false
		}
		return true
	}, 30*time.Second, 10*time.Millisecond, "ALTER did not enter a known gate")
	t.Logf("issue28317 barrier reached at %s gate", gate)

	switch mode {
	case "cancel":
		cancelAlter()
		alterErr := issue28317Wait(t, done, "canceled ALTER")
		// Cancellation can race a waiter that has just acquired the gate. Both a
		// cancellation error and successful completion are valid; the state
		// assertions below require either the old or committed schema to converge.
		t.Logf("canceled ALTER returned: %v; server-side completion is verified from catalog state", alterErr)
		alterDone = true
		_, err = a.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		holderDone = true
	case "rollback":
		_, err = a.ExecContext(ctx, "rollback")
		require.NoError(t, err)
		holderDone = true
		require.NoError(t, issue28317Wait(t, done, "ALTER after rollback"))
		alterDone = true
	case "drop":
		dropCtx, dropCancel := context.WithTimeout(ctx, 60*time.Second)
		_, err = a.ExecContext(dropCtx, "drop table `"+dbA+"`.`ephemeral`")
		dropCancel()
		require.NoError(t, err, "DROP exposed the old lock inversion")
		_, err = a.ExecContext(ctx, "commit")
		require.NoError(t, err)
		holderDone = true
		require.NoError(t, issue28317Wait(t, done, "ALTER after DROP"))
		alterDone = true
	}
	issue28317State(t, ctx, []*sql.DB{db0, db1}, dbA, dbB, wantTypes)
}

func issue28317Waiters(services []lockservice.LockService, snapshotID uint64) (snapshot, view int) {
	for _, service := range services {
		service.IterLocks(func(tableID uint64, keys [][]byte, lock lockservice.Lock) bool {
			isSnapshot := tableID == snapshotID
			isView := tableID == catalog.MO_TABLES_ID && issue28317HasKey(keys, []byte("mo_view_refresh"))
			if !isSnapshot && !isView {
				return true
			}
			lock.IterWaiters(func(pblock.WaitTxn) bool {
				if isSnapshot {
					snapshot++
				} else {
					view++
				}
				return true
			})
			return true
		})
	}
	return
}

func issue28317HasKey(keys [][]byte, needle []byte) bool {
	for _, key := range keys {
		if bytes.Contains(key, needle) {
			return true
		}
	}
	return false
}

func issue28317Wait(t *testing.T, done <-chan error, op string) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(60 * time.Second):
		t.Fatalf("%s did not return", op)
		return context.DeadlineExceeded
	}
}

func issue28317State(t *testing.T, ctx context.Context, dbs []*sql.DB, dbA, dbB string, wantTypes []string) {
	t.Helper()
	for _, db := range dbs {
		require.Eventually(t, func() bool {
			var tables, columns, rows int
			var dataType string
			if db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase=? and relname='ephemeral'", dbA).Scan(&tables) != nil || tables != 0 {
				return false
			}
			if db.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_columns where att_database=? and att_relname='ephemeral'", dbA).Scan(&columns) != nil || columns != 0 {
				return false
			}
			if db.QueryRowContext(ctx, "select data_type from information_schema.columns where table_schema=? and table_name='existing' and column_name='v'", dbB).Scan(&dataType) != nil {
				return false
			}
			if db.QueryRowContext(ctx, "select count(*) from `"+dbB+"`.`existing` where id=1 and v=1").Scan(&rows) != nil {
				return false
			}
			for _, wantType := range wantTypes {
				if strings.ToLower(dataType) == wantType && rows == 1 {
					return true
				}
			}
			return false
		}, 20*time.Second, 10*time.Millisecond, "schema/data did not converge")
	}
}
