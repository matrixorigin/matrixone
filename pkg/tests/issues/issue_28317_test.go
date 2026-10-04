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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	pblock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/stretchr/testify/require"
)

func TestIssue28317DisjointDDLAndTenantRestore(t *testing.T) {
	runAuthenticatedClusterTest(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
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
		const dbA, dbB = "issue_28317_a", "issue_28317_b"
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			for _, q := range []string{"drop database if exists `" + dbA + "`", "drop database if exists `" + dbB + "`"} {
				if _, err := db0.ExecContext(cleanupCtx, q); err != nil {
					t.Errorf("cleanup %s: %v", q, err)
				}
			}
		}()
		for _, q := range []string{
			"drop database if exists `" + dbA + "`", "drop database if exists `" + dbB + "`",
			"create database `" + dbA + "`", "create database `" + dbB + "`",
			"create table `" + dbB + "`.`existing` (id int primary key, v int)",
			"insert into `" + dbB + "`.`existing` values (1, 1)",
		} {
			execSQLRequire(t, ctx, db0, q)
		}
		// Disabled View refresh no longer writes its global marker. A local
		// COPY ALTER must finish while an unrelated CREATE is uncommitted.
		a, err := db0.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer a.Rollback()
		_, err = a.ExecContext(ctx, "create table `"+dbA+"`.`ephemeral` (id int primary key)")
		require.NoError(t, err)
		alterCtx, cancelAlter := context.WithTimeout(ctx, 10*time.Second)
		_, err = db1.ExecContext(alterCtx, "alter table `"+dbB+"`.`existing` modify column v bigint")
		cancelAlter()
		require.NoError(t, err, "unrelated COPY ALTER waited for the old global View marker")
		require.NoError(t, a.Rollback())
		issue28317State(t, ctx, []*sql.DB{db0, db1}, dbA, dbB, []string{"bigint"})
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
