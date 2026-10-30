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

package embed

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_6"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/defines"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type failViewReplacementTxn struct {
	executor.TxnExecutor
	failure error
}

func (tx failViewReplacementTxn) Exec(sql string, opts executor.StatementOption) (executor.Result, error) {
	if strings.HasPrefix(sql, "CREATE OR REPLACE VIEW") {
		return executor.Result{}, tx.failure
	}
	return tx.TxnExecutor.Exec(sql, opts)
}

// One tenant on the shared fixture covers actual logical-ID propagation,
// table/view replacement, atomic failures and the orphan-maintenance consumer.
func TestCatalogViewReplacementPreservesGrants(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		sys := recoverySQLClient(t, cn)
		defer sys.Close()
		const account = "view_identity_upgrade"
		_, err = sys.ExecContext(ctx, "create account "+account+" admin_name 'root' identified by '111'")
		require.NoError(t, err)
		defer func() {
			_, err := sys.ExecContext(context.Background(), "drop account "+account)
			require.NoError(t, err)
		}()
		var accountID uint32
		require.NoError(t, sys.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name=?", account).Scan(&accountID))
		tenant := recoverySQLClientWithUser(t, cn, account+"#root")
		defer tenant.Close()
		exec := cn.RawService().(cnservice.Service).GetSQLExecutor()
		internal := context.WithValue(ctx, defines.InternalExecutorKey{}, true)
		opts := executor.Options{}.WithAccountID(0).WithDatabase("mo_catalog").WithWaitCommittedLogApplied().WithTxnMode(txnpb.TxnMode_Pessimistic).WithTxnIsolation(txnpb.TxnIsolation_RC)
		const object = "KEY_COLUMN_USAGE"
		for _, kind := range []string{"table", "view"} {
			t.Run(kind, func(t *testing.T) {
				require.NoError(t, exec.ExecTxn(internal, func(tx executor.TxnExecutor) error {
					for _, sql := range []string{"drop view if exists information_schema.KEY_COLUMN_USAGE", "create " + kind + " information_schema.KEY_COLUMN_USAGE " + map[string]string{"table": "(n int)", "view": "as select 1 as n"}[kind]} {
						res, err := tx.Exec(sql, versions.UpgradeStatementOption(accountID))
						if err != nil {
							return err
						}
						res.Close()
					}
					return nil
				}, opts))
				for _, sql := range []string{"create role metadata_reader", "create role metadata_grantor", "create role metadata_sink", "create user metadata_delegate identified by '111'", "grant metadata_grantor to metadata_delegate", "grant select on table information_schema.KEY_COLUMN_USAGE to metadata_reader", "grant select on table information_schema.KEY_COLUMN_USAGE to metadata_grantor with grant option"} {
					_, err := tenant.ExecContext(ctx, sql)
					require.NoError(t, err)
				}
				defer func() {
					_, err := tenant.ExecContext(context.Background(), "drop user metadata_delegate")
					require.NoError(t, err)
					for _, role := range []string{"metadata_reader", "metadata_grantor", "metadata_sink"} {
						_, err := tenant.ExecContext(context.Background(), "drop role "+role)
						require.NoError(t, err)
					}
				}()
				identity := func() (uint64, uint64, string) {
					t.Helper()
					var physical, logical uint64
					var actualKind string
					require.NoError(t, tenant.QueryRowContext(ctx, "select rel_id,rel_logical_id,relkind from mo_catalog.mo_tables where account_id=current_account_id() and reldatabase='information_schema' and lower(relname)='key_column_usage'").Scan(&physical, &logical, &actualKind))
					return physical, logical, actualKind
				}
				grants := func() []string {
					t.Helper()
					rows, err := tenant.QueryContext(ctx, "select role_name,obj_type,obj_id,privilege_id,privilege_level,with_grant_option,operation_user_id,granted_time from mo_catalog.mo_role_privs where role_name in ('metadata_reader','metadata_grantor') order by role_name,privilege_id,privilege_level")
					require.NoError(t, err)
					defer rows.Close()
					var out []string
					for rows.Next() {
						var values [8]string
						require.NoError(t, rows.Scan(&values[0], &values[1], &values[2], &values[3], &values[4], &values[5], &values[6], &values[7]))
						out = append(out, fmt.Sprint(values))
					}
					require.NoError(t, rows.Err())
					return out
				}
				oldPhysical, oldLogical, oldKind := identity()
				before := grants()
				require.Len(t, before, 2)
				entry := versions.InformationSchemaViewUpgradeEntry(object, "CREATE VIEW information_schema.KEY_COLUMN_USAGE AS select 42 as n", 0)
				if kind == "table" {
					entry.UpgType = versions.RECONCILE_LEGACY_INFORMATION_SCHEMA_VIEW
				}
				failure := errors.New("injected view replacement failure")
				run := func(phase string) error {
					return exec.ExecTxn(internal, func(tx executor.TxnExecutor) error {
						if _, err := versions.GetTenantCreateVersionForUpdate(int32(accountID), tx); err != nil {
							return err
						}
						candidate := tx
						if phase == "before create" {
							candidate = failViewReplacementTxn{TxnExecutor: tx, failure: failure}
						}
						if err := entry.Upgrade(candidate, accountID); err != nil {
							return err
						}
						if phase == "before commit" {
							return failure
						}
						return nil
					}, opts)
				}
				for _, phase := range []string{"before create", "before commit"} {
					require.ErrorIs(t, run(phase), failure)
					physical, logical, actualKind := identity()
					require.Equal(t, oldPhysical, physical)
					require.Equal(t, oldLogical, logical)
					require.Equal(t, oldKind, actualKind)
					require.Equal(t, before, grants())
				}
				require.NoError(t, run("commit"))
				physical, logical, actualKind := identity()
				require.NotEqual(t, oldPhysical, physical)
				require.Equal(t, oldLogical, logical)
				require.Equal(t, "v", actualKind)
				require.Equal(t, before, grants())
				require.NoError(t, run("replay"))
				replayed, _, _ := identity()
				require.Equal(t, physical, replayed)
				var value int
				require.NoError(t, tenant.QueryRowContext(ctx, "select n from information_schema.KEY_COLUMN_USAGE").Scan(&value))
				require.Equal(t, 42, value)
				scan := v4_0_6.OrphanPrivilegeScan{}
				for {
					var next v4_0_6.OrphanPrivilegeScan
					var done bool
					require.NoError(t, exec.ExecTxn(internal, func(tx executor.TxnExecutor) error {
						var err error
						next, done, err = v4_0_6.MaintainOrphanObjectPrivilegesPage(tx, accountID, scan)
						return err
					}, opts))
					scan = next
					if done {
						break
					}
				}
				require.Equal(t, before, grants(), "maintenance must retain every exact grant after reconciliation")
				// SELECT on information_schema is public. Exercise retained grant
				// authority through a non-admin principal rather than using SELECT
				// accessibility as a privilege oracle.
				delegate := recoverySQLClientWithUser(t, cn, account+"#metadata_delegate#metadata_grantor")
				defer delegate.Close()
				_, err = delegate.ExecContext(ctx, "grant select on table information_schema.KEY_COLUMN_USAGE to metadata_sink")
				require.NoError(t, err)
				_, err = delegate.ExecContext(ctx, "grant insert on table information_schema.KEY_COLUMN_USAGE to metadata_sink")
				require.Error(t, err)
				_, err = delegate.ExecContext(ctx, "grant select on table information_schema.STATISTICS to metadata_sink")
				require.Error(t, err)
			})
		}
	})
}

func TestCallerOwnedTenantUpgradeRollsBackAndRetries(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		sys := recoverySQLClient(t, cn)
		defer sys.Close()
		const account = "caller_owned_upgrade"
		_, err = sys.ExecContext(ctx, "create account "+account+" admin_name 'root' identified by '111'")
		require.NoError(t, err)
		defer func() {
			_, err := sys.ExecContext(context.Background(), "drop account "+account)
			require.NoError(t, err)
		}()
		var accountID uint32
		require.NoError(t, sys.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name=?", account).Scan(&accountID))
		svc := cn.RawService().(cnservice.Service)
		exec := svc.GetSQLExecutor()
		final := svc.GetBootstrapService().GetFinalVersion()
		internal := context.WithValue(ctx, defines.InternalExecutorKey{}, true)
		opts := executor.Options{}.WithAccountID(0).WithDatabase("mo_catalog").WithWaitCommittedLogApplied().WithTxnMode(txnpb.TxnMode_Pessimistic).WithTxnIsolation(txnpb.TxnIsolation_RC)
		versionSQL := fmt.Sprintf("select create_version from mo_catalog.mo_account where account_id=%d", accountID)
		definitionSQL := fmt.Sprintf("select rel_createsql from mo_catalog.mo_tables where account_id=%d and reldatabase='information_schema' and lower(relname)='key_column_usage'", accountID)
		require.NoError(t, exec.ExecTxn(internal, func(tx executor.TxnExecutor) error {
			for _, stmt := range []struct {
				sql string
				id  uint32
			}{
				{"create or replace view information_schema.KEY_COLUMN_USAGE as select 17 as pending_upgrade", accountID},
				{fmt.Sprintf("update mo_catalog.mo_account set create_version='4.0.6' where account_id=%d", accountID), 0},
			} {
				res, err := tx.Exec(stmt.sql, versions.UpgradeStatementOption(stmt.id))
				if err != nil {
					return err
				}
				res.Close()
			}
			return nil
		}, opts))
		var oldDefinition string
		require.NoError(t, sys.QueryRowContext(ctx, definitionSQL).Scan(&oldDefinition))
		abort := errors.New("caller rolls back after migration")
		for _, rollback := range []bool{true, false} {
			observed := make(map[string]string)
			var didUpgrade bool
			err := exec.ExecTxn(internal, func(tx executor.TxnExecutor) error {
				upgraded, err := svc.GetBootstrapService().MaybeUpgradeTenant(internal,
					func() (int32, string, error) { return int32(accountID), "4.0.6", nil }, tx.Txn())
				if err != nil {
					return err
				}
				didUpgrade = upgraded
				for _, query := range []string{versionSQL, definitionSQL} {
					res, err := tx.Exec(query, versions.UpgradeStatementOption(0))
					if err != nil {
						return err
					}
					if len(res.Batches) != 1 || res.Batches[0].RowCount() != 1 {
						res.Close()
						return fmt.Errorf("expected one catalog row for %s", query)
					}
					observed[query] = res.Batches[0].Vecs[0].GetStringAt(0)
					res.Close()
				}
				if rollback {
					return abort
				}
				return nil
			}, opts)
			if rollback {
				require.ErrorIs(t, err, abort)
			} else {
				require.NoError(t, err)
			}
			require.True(t, didUpgrade, "a rolled-back migration must not publish completion to the CN cache")
			// The walk continues past the paged 4.0.7 stage to the cluster's
			// final registered version (#31489 publishes declared catalog
			// metadata through the renumbered final tenant hop).
			require.Equal(t, final, observed[versionSQL])
			require.NotEqual(t, oldDefinition, observed[definitionSQL], "the final tenant walker must replace the old view")
			var version, definition string
			require.NoError(t, sys.QueryRowContext(ctx, versionSQL).Scan(&version))
			require.NoError(t, sys.QueryRowContext(ctx, definitionSQL).Scan(&definition))
			if rollback {
				require.Equal(t, "4.0.6", version)
				require.Equal(t, oldDefinition, definition, "no migration operation may commit outside the caller transaction")
			} else {
				require.Equal(t, final, version)
				require.NotEqual(t, oldDefinition, definition)
			}
		}
	})
}
