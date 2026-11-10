// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real persisted old-shape and service-owner recovery, not old-binary/crash proof.
// Neither tenant logs in before the migration, so no ready-cache entry is forged.
func TestDatabaseInventoryRegisteredTenantUpgrade(t *testing.T) {
	runSQLIntegration(t, func(c embed.Cluster) {
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		sys, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer sys.Close()
		svc := cn.RawService().(cnservice.Service)
		internal := svc.GetSQLExecutor()
		boot := svc.GetBootstrapService()
		require.Equal(t, "4.0.7", boot.GetFinalVersion())
		opts := executor.Options{}.WithAccountID(0).WithDatabase("mo_catalog").
			WithTxnMode(pbtxn.TxnMode_Pessimistic).WithTxnIsolation(pbtxn.TxnIsolation_RC).
			WithWaitCommittedLogApplied()
		for _, afterFill := range []bool{false, true} {
			if !t.Run(fmt.Sprint(afterFill), func(t *testing.T) {
				account := "nr_inventory_upgrade_empty"
				if afterFill {
					account = "nr_inventory_upgrade_filled"
				}
				_, err := sys.ExecContext(ctx, "create account "+account+" admin_name 'root' identified by '111'")
				require.NoError(t, err)
				var accountID uint32
				defer func() {
					cleanupCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
					defer stop()
					if accountID != 0 {
						// Restore only this synthetic fixture before ordinary account
						// retirement, including if the migration oracle failed.
						if err := internal.ExecTxn(cleanupCtx, func(tx executor.TxnExecutor) error {
							return stageDatabaseInventoryUpgrade(cleanupCtx, int32(accountID), tx)
						}, opts); err != nil {
							t.Errorf("inventory upgrade fixture restoration failed: %v", err)
							return
						}
					}
					_, err := sys.ExecContext(cleanupCtx, "drop account "+account)
					require.NoError(t, err)
				}()
				require.NoError(t, sys.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name=?", account).Scan(&accountID))
				require.Positive(t, accountID)
				execute := func(query string) {
					t.Helper()
					result, err := internal.Exec(ctx, query, opts.WithAccountID(accountID))
					require.NoError(t, err, query)
					result.Close()
				}
				readUint := func(query string) uint64 {
					t.Helper()
					result, err := internal.Exec(ctx, query, opts.WithAccountID(accountID))
					require.NoError(t, err, query)
					defer result.Close()
					require.Len(t, result.Batches, 1)
					require.Equal(t, 1, result.Batches[0].RowCount())
					return vector.GetFixedAtWithTypeCheck[uint64](result.Batches[0].Vecs[0], 0)
				}
				var ids [2]uint64
				for i, database := range []string{"legacy_a", "legacy_b"} {
					execute("create database " + database)
					execute("create table " + database + ".payload(id int primary key,v int)")
					execute(fmt.Sprintf("insert into %s.payload values(1,%d)", database, 11+i))
					ids[i] = readUint(fmt.Sprintf("select cast(dat_id as bigint unsigned) from mo_catalog.mo_database where account_id=%d and datname='%s'", accountID, database))
				}
				require.NotEqual(t, ids[0], ids[1])
				require.NoError(t, internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
					result, err := tx.Exec("drop table mo_catalog.mo_database_inventory", versions.UpgradeStatementOption(accountID))
					if err != nil {
						return err
					}
					result.Close()
					result, err = tx.Exec(fmt.Sprintf("update mo_catalog.mo_account set create_version='4.0.6' where account_id=%d", accountID), executor.StatementOption{})
					if err != nil {
						return err
					}
					defer result.Close()
					if result.AffectedRows != 1 {
						return errors.New("fixture account version update did not affect one row")
					}
					return nil
				}, opts))
				if !afterFill {
					rollback := errors.New("rollback the supplied registered upgrade owner")
					err := internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
						changed, err := boot.MaybeUpgradeTenant(defines.AttachAccountId(ctx, 0),
							func() (int32, string, error) { return int32(accountID), "4.0.6", nil }, tx.Txn())
						if err != nil {
							return err
						}
						if !changed {
							return errors.New("registered supplied upgrade was not staged")
						}
						version, err := versions.GetTenantCreateVersionForUpdate(int32(accountID), tx)
						if err != nil {
							return err
						}
						if version != "4.0.7" {
							return fmt.Errorf("supplied upgrade did not stage readiness: %s", version)
						}
						return rollback
					}, opts)
					require.ErrorIs(t, err, rollback)
					var version string
					require.NoError(t, sys.QueryRowContext(ctx, "select create_version from mo_catalog.mo_account where account_id=?", accountID).Scan(&version))
					require.Equal(t, "4.0.6", version)
					require.Zero(t, readUint(fmt.Sprintf("select cast(count(*) as bigint unsigned) from mo_catalog.mo_tables where account_id=%d and reldatabase='mo_catalog' and relname='mo_database_inventory'", accountID)))
				}
				interrupted := errors.New("stop after confirmed component commit before readiness")
				operations := 0
				_, err = versions.UpgradeDatabaseInventory(ctx, int32(accountID), versions.DefaultCatalogUpgradeObjectBytes,
					func(_ string, _ uint64, _ bool, work func(executor.TxnExecutor) error) (bool, error) {
						err := internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
							version, err := versions.GetTenantCreateVersionForUpdate(int32(accountID), tx)
							if err != nil {
								return err
							}
							if version != "4.0.6" {
								return fmt.Errorf("fixture account unexpectedly ready: %s", version)
							}
							return work(tx)
						}, opts)
						if err != nil {
							return false, err
						}
						operations++
						if !afterFill || operations == 2 {
							return false, interrupted
						}
						return true, nil
					})
				require.ErrorIs(t, err, interrupted)
				var persisted string
				require.NoError(t, sys.QueryRowContext(ctx, "select create_version from mo_catalog.mo_account where account_id=?", accountID).Scan(&persisted))
				require.Equal(t, "4.0.6", persisted, "committed component work must not publish readiness")
				count := fmt.Sprintf("select cast(count(*) as bigint unsigned) from mo_catalog.mo_database_inventory where account_id=%d and database_id in (%d,%d)", accountID, ids[0], ids[1])
				wantEpoch := uint64(1)
				if afterFill {
					require.EqualValues(t, 2, readUint(count))
					execute(fmt.Sprintf("update mo_catalog.mo_database_inventory set epoch=99 where account_id=%d and database_id=%d", accountID, ids[0]))
					wantEpoch = 99
				} else {
					require.Zero(t, readUint(count))
				}
				if afterFill {
					_, err := sys.ExecContext(ctx, "upgrade account '"+account+"'")
					require.NoError(t, err)
					// Public UPGRADE ACCOUNT admits an asynchronous task. Its
					// committed readiness, not the command response, is the oracle.
					require.EventuallyWithT(t, func(check *assert.CollectT) {
						var current string
						require.NoError(check, sys.QueryRowContext(ctx, "select create_version from mo_catalog.mo_account where account_id=?", accountID).Scan(&current))
						require.Equal(check, "4.0.7", current)
					}, 20*time.Second, 10*time.Millisecond)
				} else {
					changed, err := boot.MaybeUpgradeTenant(defines.AttachAccountId(ctx, 0),
						func() (int32, string, error) { return int32(accountID), "4.0.6", nil }, nil)
					require.NoError(t, err)
					require.True(t, changed)
				}
				require.NoError(t, sys.QueryRowContext(ctx, "select create_version from mo_catalog.mo_account where account_id=?", accountID).Scan(&persisted))
				require.Equal(t, "4.0.7", persisted)
				require.EqualValues(t, 2, readUint(count))
				require.Equal(t, wantEpoch, readUint(fmt.Sprintf("select epoch from mo_catalog.mo_database_inventory where account_id=%d and database_id=%d", accountID, ids[0])))
				for i, database := range []string{"legacy_a", "legacy_b"} {
					require.Equal(t, ids[i], readUint(fmt.Sprintf("select cast(dat_id as bigint unsigned) from mo_catalog.mo_database where account_id=%d and datname='%s'", accountID, database)))
					func() {
						result, err := internal.Exec(ctx, "select id,v from "+database+".payload order by id", opts.WithAccountID(accountID))
						require.NoError(t, err)
						defer result.Close()
						var rows [][2]int32
						result.ReadRows(func(n int, cols []*vector.Vector) bool {
							for j := 0; j < n; j++ {
								rows = append(rows, [2]int32{vector.GetFixedAtWithTypeCheck[int32](cols[0], j), vector.GetFixedAtWithTypeCheck[int32](cols[1], j)})
							}
							return true
						})
						require.Equal(t, [][2]int32{{1, int32(11 + i)}}, rows)
					}()
				}
				changed, err := boot.MaybeUpgradeTenant(defines.AttachAccountId(ctx, 0),
					func() (int32, string, error) { return int32(accountID), "4.0.6", nil }, nil)
				require.NoError(t, err)
				require.False(t, changed)
				require.Equal(t, wantEpoch, readUint(fmt.Sprintf("select epoch from mo_catalog.mo_database_inventory where account_id=%d and database_id=%d", accountID, ids[0])))
			}) {
				return
			}
		}
	})
}
