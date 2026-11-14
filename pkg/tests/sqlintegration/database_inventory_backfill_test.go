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

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func stageDatabaseInventoryUpgrade(ctx context.Context, accountID int32, tx executor.TxnExecutor) error {
	_, err := versions.UpgradeDatabaseInventory(ctx, accountID, versions.DefaultCatalogUpgradeObjectBytes,
		func(_ string, _ uint64, _ bool, work func(executor.TxnExecutor) error) (bool, error) {
			return true, work(tx)
		})
	return err
}

func TestDatabaseInventoryBackfillSQL(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists nr_inventory_backfill_keep", "drop database if exists nr_inventory_backfill_missing")
		f := rebaseSQLFixture{t, ctx, db, "nr_inventory_backfill_"}
		f.exec("create database nr_inventory_backfill_missing")
		f.exec("create database nr_inventory_backfill_keep")
		missing := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_backfill_missing'")
		keep := f.scalar("select cast(dat_id as signed) from mo_catalog.mo_database where account_id=0 and datname='nr_inventory_backfill_keep'")
		require.Positive(t, missing)
		require.Greater(t, keep, missing)
		internal := cn.RawService().(cnservice.Service).GetSQLExecutor()
		opts := executor.Options{}.WithAccountID(0).WithTxnMode(pbtxn.TxnMode_Pessimistic).
			WithTxnIsolation(pbtxn.TxnIsolation_RC).WithWaitCommittedLogApplied()
		// Restore only this fixture's missing identity before normal DROP cleanup,
		// including when a failed page/rollback leaves inventory absent.
		defer func() {
			cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer stop()
			query := fmt.Sprintf("insert into mo_catalog.mo_database_inventory(account_id,database_id,epoch) select d.account_id,d.dat_id,1 from mo_catalog.mo_database d where d.account_id=0 and d.dat_id=%d and not exists (select 1 from mo_catalog.mo_database_inventory i where i.account_id=0 and i.database_id=%d)", missing, missing)
			result, err := internal.Exec(cleanupCtx, query, opts)
			if err != nil {
				t.Errorf("backfill fixture inventory restoration: %v", err)
				return
			}
			result.Close()
		}()
		for _, query := range []string{
			fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=99 where account_id=0 and database_id=%d", keep),
			fmt.Sprintf("delete from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", missing),
		} {
			result, err := internal.Exec(ctx, query, opts)
			require.NoError(t, err)
			affected := result.AffectedRows
			result.Close()
			require.EqualValues(t, 1, affected)
		}
		countMissing := fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", missing)
		readKeep := fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", keep)
		var originalVersion string
		require.NoError(t, db.QueryRowContext(ctx, "select create_version from mo_catalog.mo_account where account_id=0").Scan(&originalVersion))
		rollback := errors.New("rollback the completed inventory page")
		err = internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			if err := stageDatabaseInventoryUpgrade(ctx, 0, tx); err != nil {
				return err
			}
			next, more, err := versions.BackfillDatabaseInventoryPage(ctx, tx, 0, uint64(missing-1))
			if err != nil {
				return err
			}
			if next < uint64(keep) || more {
				return fmt.Errorf("unexpected backfill page cursor %d, more=%t", next, more)
			}
			return rollback
		}, opts)
		require.ErrorIs(t, err, rollback)
		require.Zero(t, f.scalar(countMissing), "a successful page in a rolled-back owner must not persist")
		require.EqualValues(t, 99, f.scalar(readKeep))
		for replay := range 2 {
			err = internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				if replay == 1 {
					return stageDatabaseInventoryUpgrade(ctx, 0, tx)
				}
				_, _, err := versions.BackfillDatabaseInventoryPage(ctx, tx, 0, uint64(missing-1))
				return err
			}, opts)
			require.NoError(t, err)
			require.EqualValues(t, 1, f.scalar(countMissing))
			require.EqualValues(t, 1, f.scalar(fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id=%d", missing)))
			require.EqualValues(t, 99, f.scalar(readKeep), "backfill replay must preserve an existing advanced counter")
			require.EqualValues(t, 2, f.scalar(fmt.Sprintf("select count(*) from mo_catalog.mo_database_inventory where account_id=0 and database_id in (%d,%d)", missing, keep)))
		}
		// Exercise real table creation without destroying the shared fixture:
		// dropping the old table, migration DDL and backfill share one owner,
		// then the sentinel rolls everything back to the old advanced counter.
		err = internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			result, err := tx.Exec("drop table mo_catalog.mo_database_inventory", executor.StatementOption{})
			if err != nil {
				return err
			}
			result.Close()
			if err := stageDatabaseInventoryUpgrade(ctx, 0, tx); err != nil {
				return err
			}
			query := fmt.Sprintf("select cast(epoch as signed) from mo_catalog.mo_database_inventory where account_id=0 and database_id in (%d,%d) order by database_id", missing, keep)
			result, err = tx.Exec(query, executor.StatementOption{})
			if err != nil {
				return err
			}
			defer result.Close()
			var epochs []int64
			result.ReadRows(func(rows int, cols []*vector.Vector) bool {
				for i := 0; i < rows; i++ {
					epochs = append(epochs, vector.GetFixedAtWithTypeCheck[int64](cols[0], i))
				}
				return true
			})
			if len(epochs) != 2 || epochs[0] != 1 || epochs[1] != 1 {
				return fmt.Errorf("new inventory table has unexpected fixture epochs: %v", epochs)
			}
			return rollback
		}, opts)
		require.ErrorIs(t, err, rollback)
		require.EqualValues(t, 1, f.scalar(countMissing))
		require.EqualValues(t, 99, f.scalar(readKeep), "catalog DDL must obey the same rollback owner")
		var currentVersion string
		require.NoError(t, db.QueryRowContext(ctx, "select create_version from mo_catalog.mo_account where account_id=0").Scan(&currentVersion))
		require.Equal(t, originalVersion, currentVersion, "only the service completion owner may publish readiness")
	})
}
