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

package sqlintegration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// Reuse the shared SQL cluster; only the named databases and snapshot belong
// to this case. This proves epoch identities, not all account/catalog restores.
func TestDatabaseInventoryCloneAndSnapshotRestore(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop snapshot if exists nr_inventory_restore_s0",
			"drop database if exists nr_inventory_restore_abort", "drop database if exists nr_inventory_restore_clone", "drop database if exists nr_inventory_restore")
		f := rebaseSQLFixture{t, ctx, db, "nr_inventory_restore_"}
		f.exec("create database nr_inventory_restore")
		f.exec("create table nr_inventory_restore.t(id int primary key,v int)")
		f.exec("insert into nr_inventory_restore.t values(1,10)")
		sourceID := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_restore'")
		service := cn.RawService().(cnservice.Service)
		internal := service.GetSQLExecutor()
		opts := executor.Options{}.WithAccountID(0).WithTxnMode(pbtxn.TxnMode_Pessimistic).
			WithTxnIsolation(pbtxn.TxnIsolation_RC).WithWaitCommittedLogApplied()
		setEpoch := func(t *testing.T, epoch int64) {
			t.Helper()
			// A high, valid committed counter distinguishes the live target
			// from a historical counter, without generating a long DDL history.
			result, err := internal.Exec(ctx, fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=%d where account_id=0 and database_id=%d", epoch, sourceID), opts)
			require.NoError(t, err)
			defer result.Close()
			require.EqualValues(t, 1, result.AffectedRows)
		}
		setEpoch(t, 99)
		f.exec("create snapshot nr_inventory_restore_s0 for database nr_inventory_restore")

		if !t.Run("clone initializes an independent identity and rolls back", func(t *testing.T) {
			f := rebaseSQLFixture{t, ctx, db, "nr_inventory_restore_"}
			f.exec("data branch create database nr_inventory_restore_clone from nr_inventory_restore")
			cloneID := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_restore_clone'")
			require.Positive(t, cloneID)
			require.NotEqual(t, sourceID, cloneID)
			cloneEpoch := f.scalar(fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", cloneID))
			require.Positive(t, cloneEpoch)
			require.NotEqualValues(t, 99, cloneEpoch, "a logical clone must not copy the source inventory counter")
			require.Equal(t, []string{"1:10"}, f.rows("nr_inventory_restore_clone", "t"))
			owner, err := db.Conn(ctx)
			require.NoError(t, err)
			defer owner.Close()
			defer func() {
				cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				defer stop()
				_, _ = owner.ExecContext(cleanupCtx, "rollback")
			}()
			for _, query := range []string{"set session transaction isolation level read committed", "begin", "data branch create database nr_inventory_restore_abort from nr_inventory_restore"} {
				_, err = owner.ExecContext(ctx, query)
				require.NoError(t, err, query)
			}
			var abortedID int64
			require.NoError(t, owner.QueryRowContext(ctx, "select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_restore_abort'").Scan(&abortedID))
			require.Positive(t, abortedID)
			var staged int64
			require.NoError(t, owner.QueryRowContext(ctx, fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d and epoch>0", abortedID)).Scan(&staged))
			require.EqualValues(t, 1, staged)
			_, err = owner.ExecContext(ctx, "rollback")
			require.NoError(t, err)
			require.Zero(t, f.scalar("select count(*) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_restore_abort'"))
			require.Zero(t, f.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", abortedID)))
			require.EqualValues(t, 99, f.scalar(fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", sourceID)))
			require.Equal(t, []string{"1:10"}, f.rows("nr_inventory_restore", "t"))
		}) {
			return
		}

		if !t.Run("table restore preserves database identity and advances its live epoch", func(t *testing.T) {
			f := rebaseSQLFixture{t, ctx, db, "nr_inventory_restore_"}
			f.exec("update nr_inventory_restore.t set v=20 where id=1")
			f.exec("create table nr_inventory_restore.kept(id int primary key)")
			f.exec("insert into nr_inventory_restore.kept values(7)")
			setEpoch(t, 123)
			f.exec("restore table nr_inventory_restore.t {snapshot='nr_inventory_restore_s0'}")
			require.Equal(t, sourceID, f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_restore'"))
			epoch := f.scalar(fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", sourceID))
			require.Greater(t, epoch, int64(123), "restore may not reset a live database to the historical epoch")
			require.Equal(t, []string{"1:10"}, f.rows("nr_inventory_restore", "t"))
			require.EqualValues(t, 1, f.scalar("select count(*) from nr_inventory_restore.kept where id=7"))
			inventory, ok := service.GetEngine().(interface {
				ReserveDatabaseInventory(context.Context, string, uint64, uint64, client.TxnOperator) error
			})
			require.True(t, ok)
			reservedCtx := defines.AttachAccountId(ctx, 0)
			err := internal.ExecTxn(reservedCtx, func(tx executor.TxnExecutor) error {
				return inventory.ReserveDatabaseInventory(reservedCtx, "nr_inventory_restore", uint64(sourceID), 123, tx.Txn())
			}, opts)
			// The owning internal executor joins callback and rollback errors.
			var conflict *moerr.Error
			require.True(t, errors.As(err, &conflict), "expected a typed definition conflict: %v", err)
			require.True(t, moerr.IsMoErrCode(conflict, moerr.ErrTxnNeedRetryWithDefChanged), "a token fixed before table restore must fail with a definition conflict: %v", err)
		}) {
			return
		}

		t.Run("database restore replaces identity without restoring historical inventory", func(t *testing.T) {
			f := rebaseSQLFixture{t, ctx, db, "nr_inventory_restore_"}
			f.exec("update nr_inventory_restore.t set v=30 where id=1")
			f.exec("restore database nr_inventory_restore {snapshot='nr_inventory_restore_s0'}")
			restoredID := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_restore'")
			require.Positive(t, restoredID)
			require.NotEqual(t, sourceID, restoredID)
			require.Zero(t, f.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", sourceID)))
			epoch := f.scalar(fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", restoredID))
			require.Positive(t, epoch)
			require.NotEqualValues(t, 99, epoch)
			require.Equal(t, []string{"1:10"}, f.rows("nr_inventory_restore", "t"))
			require.Equal(t, []string{"1:10"}, f.rows("nr_inventory_restore_clone", "t"))
		})
	})
}

// Account Restore rebuilds routine catalogs only after retiring/recreating user
// databases in its owner. Use one private tenant, not destructive SYS Restore.
func TestDatabaseInventoryAccountRoutineRestore(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		port := cn.GetServiceConfig().CN.Frontend.Port
		sys, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer sys.Close()
		const account = "nr_inventory_account_restore"
		const snapshot = "nr_inventory_account_routines_s0"
		defer cleanupSQLIntegration(t, cn, "drop snapshot if exists "+snapshot, "drop account if exists "+account)
		system := rebaseSQLFixture{t, ctx, sys, "nr_inventory_account_"}
		system.exec("create account " + account + " admin_name 'root' identified by '111'")
		accountID := system.scalar("select cast(account_id as signed) from mo_catalog.mo_account where account_name='" + account + "'")
		require.Positive(t, accountID)
		openTenant := func() *sql.DB {
			t.Helper()
			db, err := sql.Open("mysql", fmt.Sprintf("%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", account, port))
			require.NoError(t, err)
			return db
		}
		tenant := openTenant()
		defer tenant.Close()
		f := rebaseSQLFixture{t, ctx, tenant, "nr_inventory_account_"}
		f.exec("create database app")
		f.exec("create table app.payload(id int primary key,v int)")
		f.exec("insert into app.payload values(1,10)")
		f.exec("create function app.inventory_fn(x int) returns int language sql as '$1+1'")
		f.exec("create procedure app.inventory_proc() 'begin select 1; end'")
		identity := fmt.Sprintf("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=%d and datname='app'", accountID)
		oldID := f.scalar(identity)
		service := cn.RawService().(cnservice.Service)
		opts := executor.Options{}.WithAccountID(uint32(accountID)).WithTxnMode(pbtxn.TxnMode_Pessimistic).
			WithTxnIsolation(pbtxn.TxnIsolation_RC).WithWaitCommittedLogApplied()
		result, err := service.GetSQLExecutor().Exec(ctx, fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=99 where account_id=%d and database_id=%d", accountID, oldID), opts)
		require.NoError(t, err)
		func() {
			defer result.Close()
			require.EqualValues(t, 1, result.AffectedRows)
		}()
		system.exec("create snapshot " + snapshot + " for account " + account)
		require.EqualValues(t, 1, system.scalar("select count(*) from mo_catalog.mo_snapshots where sname='"+snapshot+"'"), "the account Restore caller must see its snapshot")
		f.exec("update app.payload set v=20 where id=1")
		f.exec("drop function app.inventory_fn(int)")
		f.exec("drop procedure app.inventory_proc")
		f.exec("create database post_snapshot")
		f.exec("create table post_snapshot.payload(id int primary key,v int)")
		f.exec("insert into post_snapshot.payload values(2,99)")
		lateID := f.scalar(fmt.Sprintf("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=%d and datname='post_snapshot'", accountID))
		oldEpoch := f.scalar(fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=%d and database_id=%d", accountID, oldID))
		require.Greater(t, oldEpoch, int64(99))
		// Do not retain a session through replacement of its authorization catalogs.
		require.NoError(t, tenant.Close())
		system.exec("restore account " + account + " {snapshot='" + snapshot + "'}")
		restored := openTenant()
		defer restored.Close()
		current := rebaseSQLFixture{t, ctx, restored, "nr_inventory_account_"}
		newID := current.scalar(identity)
		require.Positive(t, newID)
		require.NotEqual(t, oldID, newID)
		newEpochQuery := fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=%d and database_id=%d", accountID, newID)
		newEpoch := current.scalar(newEpochQuery)
		require.Positive(t, newEpoch)
		require.NotEqualValues(t, 99, newEpoch, "historical inventory is not bulk-copied over the target owner")
		require.Zero(t, current.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=%d and database_id in (%d,%d)", accountID, oldID, lateID)))
		require.Zero(t, current.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database where account_id=%d and datname='post_snapshot'", accountID)))
		require.Equal(t, []string{"1:10"}, current.rows("app", "payload"))
		for _, table := range []string{"mo_user_defined_function", "mo_stored_procedure"} {
			require.EqualValues(t, 1, current.scalar("select count(*) from mo_catalog."+table+" where db='app'"))
		}
		func() {
			reader, err := restored.Conn(ctx)
			require.NoError(t, err)
			defer reader.Close()
			_, err = reader.ExecContext(ctx, "use app")
			require.NoError(t, err)
			var value int64
			require.NoError(t, reader.QueryRowContext(ctx, "select inventory_fn(41)").Scan(&value))
			require.EqualValues(t, 42, value)
			require.NoError(t, reader.QueryRowContext(ctx, "call inventory_proc()").Scan(&value))
			require.EqualValues(t, 1, value)
		}()
		inventory, ok := service.GetEngine().(interface {
			ReserveDatabaseInventory(context.Context, string, uint64, uint64, client.TxnOperator) error
		})
		require.True(t, ok)
		err = service.GetSQLExecutor().ExecTxn(defines.AttachAccountId(ctx, uint32(accountID)), func(tx executor.TxnExecutor) error {
			return inventory.ReserveDatabaseInventory(defines.AttachAccountId(ctx, uint32(accountID)), "app", uint64(oldID), uint64(oldEpoch), tx.Txn())
		}, opts)
		var conflict *moerr.Error
		require.True(t, errors.As(err, &conflict), "expected a typed stale database identity: %v", err)
		require.True(t, moerr.IsMoErrCode(conflict, moerr.ErrTxnNeedRetryWithDefChanged), "%v", err)
		// Snapshot catalogs are caller scoped. A SYS-owned account snapshot
		// verifies the SYS restore route; use this tenant's own fresh snapshot
		// to reach its forbidden-catalog check, not a cross-owner lookup error.
		const refusalSnapshot = "nr_inventory_account_refusal_s1"
		current.exec("create snapshot " + refusalSnapshot + " for account")
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer stop()
			_, err := restored.ExecContext(cleanupCtx, "drop snapshot if exists "+refusalSnapshot)
			require.NoError(t, err)
		}()
		require.EqualValues(t, 1, current.scalar("select count(*) from mo_catalog.mo_snapshots where sname='"+refusalSnapshot+"'"))
		_, err = restored.ExecContext(ctx, "restore table mo_catalog.mo_user_defined_function {snapshot='"+refusalSnapshot+"'}")
		require.ErrorContains(t, err, "can't restore db: mo_catalog")
		require.Equal(t, newEpoch, current.scalar(newEpochQuery), "rejected catalog-table Restore has no user-database mutation")
		require.Equal(t, []string{"1:10"}, current.rows("app", "payload"))
	})
}
