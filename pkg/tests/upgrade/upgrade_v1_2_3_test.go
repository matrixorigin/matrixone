// Copyright 2024 Matrix Origin
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

package upgrade

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v1_2_3"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_0"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func Test_UpgradeEntry(t *testing.T) {
	c, err := embed.StartTestCluster(embed.WithCNCount(1))
	if c != nil {
		t.Cleanup(func() { require.NoError(t, c.Close()) })
	}
	require.NoError(t, err)

	svc, err := c.GetCNService(0)
	require.NoError(t, err)

	exec := testutils.GetSQLExecutor(svc)
	require.NotNil(t, exec)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	err = exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		err = v1_2_3.Handler.HandleClusterUpgrade(ctx, txn)
		require.NoError(t, err)

		err = v1_2_3.Handler.HandleTenantUpgrade(ctx, 0, txn)
		require.NoError(t, err)

		return nil
	}, executor.Options{}.WithWaitCommittedLogApplied())
	require.NoError(t, err)

	t.Run("catalog upgrade without lineage prerequisites", func(t *testing.T) {
		// Reuse the isolated cluster to restore the relevant v3.0.4 catalog
		// shape. No user tables or large historical-data fixture are needed.
		opts := executor.Options{}.WithWaitCommittedLogApplied()
		for _, sql := range []string{
			"alter table mo_catalog.mo_pitr drop column kind",
			"alter table mo_catalog.mo_snapshots drop column kind",
			"drop table mo_catalog.mo_feature_registry",
		} {
			res, err := exec.Exec(ctx, sql, opts)
			res.Close()
			require.NoError(t, err, sql)
		}
		res, err := exec.Exec(ctx, "insert into mo_catalog.mo_snapshots(snapshot_id,sname,ts,level,account_name,database_name,table_name,obj_id) values('00000000-0000-0000-0000-000000000001','upgrade_preserve_snapshot',42,'table','sys','old_db','old_table',43)", opts)
		res.Close()
		require.NoError(t, err)

		// A marked statement must not mark subsequent statements in the same
		// transaction. Ordinary ALTER still requires the lineage catalogs.
		err = exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			res, err := txn.Exec("select 1", executor.StatementOption{}.WithCatalogUpgrade())
			res.Close()
			if err != nil {
				return err
			}
			res, err = txn.Exec("alter table mo_catalog.mo_pitr add column kind varchar(32) default 'user'", executor.StatementOption{})
			res.Close()
			return err
		}, opts)
		var missingTable *moerr.Error
		require.ErrorAs(t, err, &missingTable)
		require.Equal(t, moerr.ErrNoSuchTable, missingTable.ErrorCode())

		upgrade := func(txn executor.TxnExecutor) error {
			if err := v4_0_0.Handler.HandleClusterUpgrade(ctx, txn); err != nil {
				return err
			}
			return v4_0_0.Handler.HandleTenantUpgrade(ctx, 0, txn)
		}
		injectedErr := errors.New("rollback catalog upgrade")
		err = exec.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			if err := upgrade(txn); err != nil {
				return err
			}
			return injectedErr
		}, opts)
		require.ErrorIs(t, err, injectedErr)
		res, err = exec.Exec(ctx, "show tables from mo_catalog like 'mo_feature_registry'", opts)
		rows := 0
		for _, bat := range res.Batches {
			rows += bat.RowCount()
		}
		res.Close()
		require.NoError(t, err)
		require.Zero(t, rows, "failed upgrade must roll back its DDL")

		// Run twice to cover a retry after committed cluster/tenant progress.
		for range 2 {
			err := exec.ExecTxn(ctx, upgrade, opts)
			require.NoError(t, err)
		}

		for _, sql := range []string{
			"select feature_code from mo_catalog.mo_feature_registry where feature_code in ('SNAPSHOT', 'BRANCH')",
			"select attname from mo_catalog.mo_columns where account_id = 0 and att_database = 'mo_catalog' and att_relname in ('mo_pitr', 'mo_snapshots') and attname = 'kind'",
		} {
			func() {
				res, err := exec.Exec(ctx, sql, opts)
				if err != nil {
					res.Close()
					require.NoError(t, err, sql)
				}
				defer res.Close()
				rows := 0
				for _, bat := range res.Batches {
					rows += bat.RowCount()
				}
				require.Equal(t, 2, rows, sql)
			}()
		}
		res, err = exec.Exec(ctx, "select sname from mo_catalog.mo_snapshots where snapshot_id = '00000000-0000-0000-0000-000000000001' and sname = 'upgrade_preserve_snapshot' and ts = 42 and level = 'table' and account_name = 'sys' and database_name = 'old_db' and table_name = 'old_table' and obj_id = 43 and kind = 'user'", opts)
		if err != nil {
			res.Close()
			require.NoError(t, err)
		}
		defer res.Close()
		require.Len(t, res.Batches, 1)
		require.Equal(t, 1, res.Batches[0].RowCount())
	})
}
