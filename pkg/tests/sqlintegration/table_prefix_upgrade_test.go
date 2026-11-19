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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/sqlquote"
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

// Frozen TABLES input from release v4.2.7, commit
// 260678bace0177678255089a23d0fe7d54cfc745. Its formatting is immaterial to
// the persisted view, but its source, columns and predicates are retained here.
const releasedV427TablesDDL = `CREATE VIEW information_schema.TABLES AS SELECT
'def' AS TABLE_CATALOG, reldatabase AS TABLE_SCHEMA, relname AS TABLE_NAME,
(case when relkind = 'v' and (reldatabase='mo_catalog' or reldatabase='information_schema') then 'SYSTEM VIEW'
when relkind = 'v' then 'VIEW' when relkind = 'e' then 'EXTERNAL TABLE'
when relkind = 'r' then 'BASE TABLE' else 'INTERNAL TABLE' end) AS TABLE_TYPE,
if(relkind = 'r','Tae',NULL) AS ENGINE, if(relkind = 'v',NULL,10) AS VERSION,
'Compressed' AS ROW_FORMAT, if(relkind = 'v', NULL, 0) AS TABLE_ROWS,
if(relkind = 'v', NULL, 0) AS AVG_ROW_LENGTH, if(relkind = 'v', NULL, 0) AS DATA_LENGTH,
if(relkind = 'v', NULL, 0) AS MAX_DATA_LENGTH, if(relkind = 'v', NULL, 0) AS INDEX_LENGTH,
if(relkind = 'v', NULL, 0) AS DATA_FREE,
if(relkind = 'v', NULL, internal_auto_increment(reldatabase, relname)) AS ` + "`AUTO_INCREMENT`" + `,
created_time AS CREATE_TIME, if(relkind = 'v', NULL, created_time) AS UPDATE_TIME,
if(relkind = 'v', NULL, created_time) AS CHECK_TIME, 'utf8mb4_0900_ai_ci' AS TABLE_COLLATION,
if(relkind = 'v', NULL, 0) AS CHECKSUM,
if(relkind = 'v', NULL, if(partitioned = 0, '', cast('partitioned' as varchar(256)))) AS CREATE_OPTIONS,
cast(rel_comment as text) AS TABLE_COMMENT FROM mo_catalog.mo_tables tbl
WHERE tbl.account_id = current_account_id() and tbl.relname not like '__mo_index_%' and
not (tbl.relkind = 'temporary_table' or mo_is_legacy_temporary_table(coalesce(tbl.relkind, ''), coalesce(tbl.relname, ''), coalesce(tbl.reldatabase, ''), coalesce(tbl.rel_createsql, ''), coalesce(tbl.extra_info, '')) or (coalesce(tbl.relkind, '') not in ('r', 'v', 'e', 'm', 's', 'cluster', 'partition', 'S') and regexp_like(tbl.relname, '^__mo_tmp_[0-9a-f]{32}_'))) and
tbl.relname != '__mo_account_lock' and tbl.relkind != 'partition'`

func (e *tablesUpgradeExecutor) ExecTxn(ctx context.Context, body func(executor.TxnExecutor) error, opts executor.Options) error {
	return e.SQLExecutor.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		return body(&tablesUpgradeTxn{TxnExecutor: tx, owner: e})
	}, opts)
}
func (tx *tablesUpgradeTxn) Exec(statement string, opts executor.StatementOption) (executor.Result, error) {
	result, err := tx.TxnExecutor.Exec(statement, opts)
	if err != nil || statement != versions.InformationSchemaTablesUpgradeEntry().UpgSql {
		return result, err
	}
	if tx.owner.failure != nil {
		result.Close()
		return executor.Result{}, tx.owner.failure
	}
	tx.owner.creates++
	return result, nil
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
		installView := func(name, ddl string) {
			t.Helper()
			require.NoError(t, internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				for _, statement := range []string{"DROP VIEW IF EXISTS information_schema." + name, ddl} {
					result, err := tx.Exec(statement, versions.UpgradeStatementOption(uint32(accountID)))
					if err != nil {
						return err
					}
					result.Close()
				}
				return nil
			}, executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied()))
		}
		installView("TABLES", oldDDL)
		require.Zero(t, count(), "historical view reproduces the defect")
		entry := versions.InformationSchemaTablesUpgradeEntry()
		owner := &tablesUpgradeExecutor{SQLExecutor: internal, failure: errors.New("injected view replacement failure")}
		repair := func(e *tablesUpgradeExecutor) error {
			return e.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				return entry.UpgradeAfterProtocolCheck(tx, uint32(accountID))
			}, executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied())
		}
		require.ErrorIs(t, repair(owner), owner.failure)
		require.Zero(t, count(), "failed replacement must leave the old view intact")
		owner.failure = nil
		require.NoError(t, repair(owner))
		require.Equal(t, 1, count())
		require.Equal(t, 1, owner.creates)
		require.NoError(t, repair(owner))
		require.Equal(t, 1, owner.creates, "current persisted view must not be replaced again")
		freshOwner := &tablesUpgradeExecutor{SQLExecutor: internal}
		require.NoError(t, repair(freshOwner))
		require.Zero(t, freshOwner.creates, "a fresh executor must see the persisted view")
		require.Equal(t, []string{"demo_index_data"}, restrictedNames("select table_name from information_schema.tables where table_schema='prefix_upgrade'"))

		// A supported released deployment enters through the actual v4_0_7
		// tenant handler. Failed replacement rolls back, and a fresh retry and
		// a subsequent duplicate invocation leave one current definition.
		installView("TABLES", releasedV427TablesDDL)
		// The v4.2.8 COLUMNS source is unchanged from this captured v4.2.6
		// release definition; exercise it through the pending tenant handler.
		data, err := os.ReadFile("../../sql/plan/testdata/view_metadata_legacy_v4_2_6.json")
		require.NoError(t, err)
		var fixture struct {
			Columns string `json:"system_columns_definition"`
		}
		require.NoError(t, json.Unmarshal(data, &fixture))
		var releasedColumns struct{ Stmt string }
		require.NoError(t, json.Unmarshal([]byte(fixture.Columns), &releasedColumns))
		require.NotEmpty(t, releasedColumns.Stmt)
		installView("COLUMNS", releasedColumns.Stmt)
		columnsDefinition := func() string {
			t.Helper()
			var definition string
			require.NoError(t, internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				exists, actual, err := versions.CheckInformationSchemaViewDefinition(tx, uint32(accountID), "COLUMNS")
				if err != nil {
					return err
				}
				if !exists {
					return errors.New("COLUMNS view is missing")
				}
				definition = actual
				return nil
			}, executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied()))
			return definition
		}
		oldColumns := columnsDefinition()
		require.Zero(t, count(), "the pinned release retains the old wildcard predicate")
		releasedOwner := &tablesUpgradeExecutor{SQLExecutor: internal, failure: errors.New("injected released-view replacement failure")}
		runTenantUpgrade := func(e *tablesUpgradeExecutor) error {
			return e.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
				return v4_0_7.Handler.HandleTenantUpgrade(ctx, accountID, tx)
			}, executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied())
		}
		require.ErrorIs(t, runTenantUpgrade(releasedOwner), releasedOwner.failure)
		require.Zero(t, count(), "the released view survives a failed transaction")
		require.Equal(t, oldColumns, columnsDefinition(), "COLUMNS replacement must roll back with TABLES")
		releasedOwner.failure = nil
		require.NoError(t, runTenantUpgrade(releasedOwner))
		require.Equal(t, 1, count())
		require.True(t, sysview.InformationSchemaViewDefinitionMatches(columnsDefinition(), sysview.InformationSchemaColumnsDDL))
		var visibleColumns int
		require.NoError(t, restricted.QueryRowContext(ctx, "select count(*) from information_schema.columns where table_schema='prefix_upgrade' and table_name='demo_index_data'").Scan(&visibleColumns))
		require.Equal(t, 2, visibleColumns)
		require.Equal(t, 1, releasedOwner.creates)
		require.NoError(t, runTenantUpgrade(releasedOwner))
		require.Equal(t, 1, releasedOwner.creates)
		require.Equal(t, []string{"demo_index_data"}, restrictedNames("select table_name from information_schema.tables where table_schema='prefix_upgrade'"))

		t.Run("released_snapshot", func(t *testing.T) {
			testReleasedTablesSnapshot(t, cn, db, internal)
		})
	})
}

func testReleasedTablesSnapshot(t *testing.T, cn embed.ServiceOperator, db *sql.DB, internal executor.SQLExecutor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	defer cleanupSQLIntegration(t, cn,
		"drop snapshot if exists prefix_current_snap", "drop snapshot if exists prefix_release_snap",
		"drop user if exists prefix_snapshot_user", "drop role if exists prefix_snapshot_role",
		"drop database if exists prefix_snapshot_db")
	admin, err := db.Conn(ctx)
	require.NoError(t, err)
	defer admin.Close()
	exec := func(statement string) {
		t.Helper()
		_, err := admin.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	for _, statement := range []string{
		"create database prefix_snapshot_db",
		"set experimental_fulltext_index=1",
		"create table prefix_snapshot_db.docs(id bigint primary key,body text,fulltext index ft(body))",
		"create role prefix_snapshot_role",
		"create user prefix_snapshot_user identified by '111' default role prefix_snapshot_role",
		"grant connect on account * to prefix_snapshot_role",
		"grant all on database prefix_snapshot_db to prefix_snapshot_role",
		"grant all on table prefix_snapshot_db.* to prefix_snapshot_role",
	} {
		exec(statement)
	}
	var child string
	require.NoError(t, admin.QueryRowContext(ctx, "select distinct index_table_name from mo_catalog.mo_indexes where table_id=(select rel_id from mo_catalog.mo_tables where reldatabase='prefix_snapshot_db' and relname='docs') and index_table_name is not null and index_table_name<>'' limit 1").Scan(&child))
	exec("alter table prefix_snapshot_db." + sqlquote.Ident(child) + " rename to renamed_index")
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, err := admin.ExecContext(cleanupCtx, "alter table prefix_snapshot_db.renamed_index rename to "+sqlquote.Ident(child))
		require.NoError(t, err)
	}()
	replace := func(ctx context.Context, ddl string) error {
		return internal.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
			for _, statement := range []string{"DROP VIEW IF EXISTS information_schema.TABLES", ddl} {
				result, err := tx.Exec(statement, versions.UpgradeStatementOption(0))
				if err != nil {
					return err
				}
				result.Close()
			}
			return nil
		}, executor.Options{}.WithAccountID(0).WithWaitCommittedLogApplied())
	}
	require.NoError(t, replace(ctx, releasedV427TablesDDL))
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		require.NoError(t, replace(cleanupCtx, sysview.InformationSchemaTablesCurrentDDL))
	}()
	exec("create view prefix_snapshot_db.tables_view as select table_name from information_schema.tables where table_schema='prefix_snapshot_db'")
	exec("grant select on view prefix_snapshot_db.tables_view to prefix_snapshot_role")
	exec("create snapshot prefix_release_snap for account")
	require.NoError(t, replace(ctx, sysview.InformationSchemaTablesCurrentDDL))
	exec("create snapshot prefix_current_snap for account")
	reader, err := sql.Open("mysql", fmt.Sprintf("sys#prefix_snapshot_user#prefix_snapshot_role:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	defer reader.Close()
	for _, query := range []string{
		"select table_name from information_schema.tables {snapshot='prefix_release_snap'} where table_schema='prefix_snapshot_db' and table_name in ('renamed_index','docs')",
		"select table_name from prefix_snapshot_db.tables_view {snapshot='prefix_release_snap'} where table_name in ('renamed_index','docs')",
		"select table_name from information_schema.tables {snapshot='prefix_current_snap'} where table_schema='prefix_snapshot_db' and table_name in ('renamed_index','docs')",
	} {
		rows, err := reader.QueryContext(ctx, query)
		require.Equal(t, []string{"docs"}, readMetadataTableNames(t, rows, err), query)
	}
}
