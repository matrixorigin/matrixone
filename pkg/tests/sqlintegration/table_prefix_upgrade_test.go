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

package sqlintegration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

type tablesUpgradeExecutor struct {
	executor.SQLExecutor
	failure error
	creates int
}
type tablesUpgradeTxn struct {
	executor.TxnExecutor
	owner *tablesUpgradeExecutor
}

func (e *tablesUpgradeExecutor) ExecTxn(ctx context.Context, body func(executor.TxnExecutor) error, opts executor.Options) error {
	return e.SQLExecutor.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		return body(&tablesUpgradeTxn{TxnExecutor: tx, owner: e})
	}, opts)
}
func (tx *tablesUpgradeTxn) Exec(statement string, opts executor.StatementOption) (executor.Result, error) {
	if statement == sysview.InformationSchemaTablesV118DDL {
		if tx.owner.failure != nil {
			return executor.Result{}, tx.owner.failure
		}
		tx.owner.creates++
	}
	return tx.TxnExecutor.Exec(statement, opts)
}

func TestTablePrefixMetadataUpgrade(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		svc := cn.RawService().(cnservice.Service)
		internal := svc.GetSQLExecutor()
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		require.NoError(t, testutils.WaitSystemBootstrap(ctx, db))
		const account = "table_prefix_upgrade"
		defer cleanupSQLIntegration(t, cn, "drop account if exists "+account)
		execSQLRequire(t, ctx, db, "create account "+account+" admin_name 'root' identified by '111'")
		var accountID int32
		require.NoError(t, db.QueryRowContext(ctx, "select account_id from mo_catalog.mo_account where account_name=?", account).Scan(&accountID))
		tenant, err := sql.Open("mysql", fmt.Sprintf(account+"#root#accountadmin:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer tenant.Close()
		execSQLRequire(t, ctx, tenant, "create database prefix_upgrade")
		execSQLRequire(t, ctx, tenant, "create table prefix_upgrade.demo_index_data (id int primary key, v int, key idx_v(v))")
		count := func() int {
			t.Helper()
			var n int
			require.NoError(t, tenant.QueryRowContext(ctx, "select count(*) from information_schema.tables where table_schema='prefix_upgrade' and table_name='demo_index_data'").Scan(&n))
			return n
		}
		require.Equal(t, 1, count(), "new tenant uses corrected view")

		execSQLRequire(t, ctx, tenant, "use prefix_upgrade")
		execSQLRequire(t, ctx, tenant, "create table plain(id int)")
		execSQLRequire(t, ctx, tenant, "create table x1mo2index3foo(id int)")
		execSQLRequire(t, ctx, tenant, "create table __mo_indexx_data(id int)")
		names := func(statement string) []string {
			t.Helper()
			rows, err := tenant.QueryContext(ctx, statement)
			require.NoError(t, err, statement)
			defer rows.Close()
			cols, err := rows.Columns()
			require.NoError(t, err)
			var result []string
			for rows.Next() {
				vals := make([]sql.RawBytes, len(cols))
				ptrs := make([]any, len(cols))
				for i := range vals {
					ptrs[i] = &vals[i]
				}
				require.NoError(t, rows.Scan(ptrs...))
				result = append(result, string(vals[0]))
			}
			require.NoError(t, rows.Err())
			return result
		}
		expected := []string{"__mo_indexx_data", "demo_index_data", "plain", "x1mo2index3foo"}
		require.ElementsMatch(t, expected, names("show tables"))
		require.ElementsMatch(t, expected, names("show full tables"))
		require.ElementsMatch(t, expected, names("show table status"))
		require.ElementsMatch(t, expected, names("select table_name from information_schema.tables where table_schema='prefix_upgrade'"))
		require.ElementsMatch(t, []string{"__mo_indexx_data", "demo_index_data", "x1mo2index3foo"}, names("show tables like '__mo_index_%'"))
		require.ElementsMatch(t, []string{"demo_index_data"}, names("show tables where Tables_in_prefix_upgrade = 'demo_index_data'"))
		var number int
		require.NoError(t, tenant.QueryRowContext(ctx, "show table_number").Scan(&number))
		require.Equal(t, 4, number)
		var indexes int
		require.NoError(t, tenant.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_indexes i join mo_catalog.mo_tables t on i.table_id=t.rel_id join mo_catalog.mo_tables backing on backing.reldatabase=t.reldatabase and backing.relname=i.index_table_name and backing.account_id=t.account_id where t.reldatabase='prefix_upgrade' and t.relname='demo_index_data' and i.name='idx_v' and backing.relkind='i'").Scan(&indexes))
		require.Positive(t, indexes, "the fixture must have a real internal index")

		for _, statement := range []string{
			"create role prefix_reader",
			"create user prefix_user identified by '111' default role prefix_reader",
			"grant connect on account * to prefix_reader",
			"grant select on table prefix_upgrade.demo_index_data to prefix_reader",
		} {
			execSQLRequire(t, ctx, tenant, statement)
		}
		restricted, err := sql.Open("mysql", fmt.Sprintf(account+"#prefix_user#prefix_reader:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer restricted.Close()
		restrictedNames := func(statement string) []string {
			t.Helper()
			rows, err := restricted.QueryContext(ctx, statement)
			return readMetadataTableNames(t, rows, err)
		}
		require.Equal(t, []string{"demo_index_data"}, restrictedNames("show tables from prefix_upgrade"))
		require.Equal(t, []string{"demo_index_data"}, restrictedNames("select table_name from information_schema.tables where table_schema='prefix_upgrade'"))

		const corrected = "not startswith(tbl.relname, '__mo_index_')"
		require.Equal(t, 2, strings.Count(sysview.InformationSchemaTablesV118DDL, corrected))
		oldDDL := strings.ReplaceAll(sysview.InformationSchemaTablesV118DDL, corrected, "tbl.relname not like '__mo_index_%'")
		require.NoError(t, internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{"DROP VIEW IF EXISTS information_schema.TABLES;", oldDDL} {
				result, err := tx.Exec(statement, versions.UpgradeStatementOption(uint32(accountID)))
				if err != nil {
					return err
				}
				result.Close()
			}
			return nil
		}, executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied()))
		result, err := internal.Exec(ctx, fmt.Sprintf("update mo_catalog.mo_account set create_version='4.0.17' where account_id=%d", accountID), executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied())
		require.NoError(t, err)
		result.Close()
		require.Zero(t, count(), "historical view reproduces the defect")
		version := func() string {
			t.Helper()
			var value string
			require.NoError(t, db.QueryRowContext(ctx, "select create_version from mo_catalog.mo_account where account_id=?", accountID).Scan(&value))
			return value
		}
		owner := &tablesUpgradeExecutor{SQLExecutor: internal, failure: errors.New("injected view creation failure")}
		upgrader := bootstrap.NewService(cn.ServiceID(), nil, svc.GetClock(), svc.GetTxnClient(), owner)
		defer func() { require.NoError(t, upgrader.Close()) }()
		upgrade := func() (bool, error) {
			return upgrader.MaybeUpgradeTenant(ctx, func() (int32, string, error) { return accountID, "", nil }, nil)
		}
		_, err = upgrade()
		require.ErrorIs(t, err, owner.failure)
		require.Equal(t, "4.0.17", version())
		require.Zero(t, count(), "failed create must roll back the drop")
		owner.failure = nil
		upgraded, err := upgrade()
		require.NoError(t, err)
		require.True(t, upgraded)
		require.Equal(t, "4.0.18", version())
		require.Equal(t, 1, count())
		require.Equal(t, 1, owner.creates)
		upgraded, err = upgrade()
		require.NoError(t, err)
		require.False(t, upgraded)
		require.Equal(t, 1, owner.creates)

		freshOwner := &tablesUpgradeExecutor{SQLExecutor: internal}
		freshUpgrader := bootstrap.NewService(cn.ServiceID(), nil, svc.GetClock(), svc.GetTxnClient(), freshOwner)
		defer func() { require.NoError(t, freshUpgrader.Close()) }()
		upgraded, err = freshUpgrader.MaybeUpgradeTenant(ctx, func() (int32, string, error) { return accountID, "", nil }, nil)
		require.NoError(t, err)
		require.False(t, upgraded)
		require.Zero(t, freshOwner.creates, "persisted completion must survive a new upgrader")
		require.Equal(t, []string{"demo_index_data"}, restrictedNames("select table_name from information_schema.tables where table_schema='prefix_upgrade'"))
	})
}
