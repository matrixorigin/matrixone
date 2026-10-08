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

package upgrade

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_12"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/matrixorigin/matrixone/pkg/udf"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// Reuse the upgrade package's single-CN fixture. A late tenant must be repaired
// on its first real login, even when the cluster has already completed upgrade.
func TestV4012LoginRepairsLegacyRoutineCatalog(t *testing.T) {
	embed.RunSingleCNBaseClusterTests(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		systemDB, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer systemDB.Close()
		const account = "routine_repair_4012"
		_, err = systemDB.ExecContext(ctx, "create account "+account+" ADMIN_NAME 'root' IDENTIFIED BY '111'")
		require.NoError(t, err)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cleanupCancel()
			_, err := systemDB.ExecContext(cleanupCtx, "drop account "+account)
			require.NoError(t, err)
			var remaining int
			require.NoError(t, systemDB.QueryRowContext(cleanupCtx,
				"select count(*) from mo_catalog.mo_account where account_name = '"+account+"'").Scan(&remaining))
			require.Zero(t, remaining)
		}()
		var accountID uint32
		require.NoError(t, systemDB.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name = '"+account+"'").Scan(&accountID))
		sqlExecutor := testutils.GetSQLExecutor(cn)
		// Model an old writer through a sys-owned transaction, without tenant
		// authentication populating the bootstrap service's checked-tenant cache.
		require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			for _, statement := range []string{
				"create database routine_repair",
				`insert into mo_catalog.mo_user_defined_function(name,owner,args,arg_types,rettype,body,language,db,definer,type,security_type) values ('legacy_plus_seven',2,'[{"name":"x","type":"bigint"}]','["bigint"]','bigint','$1 + 7','sql','routine_repair','root','SQL','INVOKER')`,
				"drop table mo_catalog.mo_function_revisions",
			} {
				res, err := txn.Exec(statement, versions.UpgradeStatementOption(accountID))
				if err != nil {
					return err
				}
				res.Close()
			}
			return versions.UpgradeTenantVersion(int32(accountID), "4.0.11", txn)
		}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).
			WithAccountID(catalog.System_Account).WithWaitCommittedLogApplied()))
		var functionID uint64
		checkCatalog := func(wantVersion string, wantTable bool) {
			t.Helper()
			var version string
			require.NoError(t, systemDB.QueryRowContext(ctx,
				fmt.Sprintf("select create_version from mo_catalog.mo_account where account_id = %d", accountID)).Scan(&version))
			require.Equal(t, wantVersion, version)
			require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
				exists, err := versions.CheckTableDefinition(txn, accountID, catalog.MO_CATALOG, "mo_function_revisions")
				if err != nil {
					return err
				}
				require.Equal(t, wantTable, exists)
				res, err := txn.Exec("select function_id,body,active_revision,namespace_version from mo_catalog.mo_user_defined_function where name = 'legacy_plus_seven'", versions.UpgradeStatementOption(accountID))
				if err != nil {
					return err
				}
				defer res.Close()
				require.Equal(t, 1, len(res.Batches))
				batch := res.Batches[0]
				require.Equal(t, 1, batch.RowCount())
				id := uint64(vector.GetFixedAtWithTypeCheck[int32](batch.Vecs[0], 0))
				if functionID == 0 {
					functionID = id
				}
				require.Equal(t, functionID, id)
				require.Equal(t, "$1 + 7", batch.Vecs[1].GetStringAt(0))
				require.Equal(t, uint64(0), vector.GetFixedAtWithTypeCheck[uint64](batch.Vecs[2], 0))
				require.Equal(t, uint64(0), vector.GetFixedAtWithTypeCheck[uint64](batch.Vecs[3], 0))
				return nil
			}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithAccountID(catalog.System_Account)))
		}
		checkCatalog("4.0.11", false)
		rt := moruntime.ServiceRuntime(cn.ServiceID())
		original, present := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
		require.True(t, present)
		require.GreaterOrEqual(t, original.(int64), udf.SharedRoutineRevisionProtocolVersion)
		defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		tenantDSN := fmt.Sprintf("%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", account, port)
		func() {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, udf.SharedRoutineRevisionProtocolVersion-1)
			defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
			db, err := sql.Open("mysql", tenantDSN)
			require.NoError(t, err)
			defer db.Close()
			require.ErrorContains(t, db.PingContext(ctx), "upgrade requires all CNs to support protocol version 107")
		}()
		checkCatalog("4.0.11", false)
		tenantDB, err := sql.Open("mysql", tenantDSN)
		require.NoError(t, err)
		defer tenantDB.Close()
		checkResult := func() {
			t.Helper()
			_, err := tenantDB.ExecContext(ctx, "use routine_repair")
			require.NoError(t, err)
			var result int64
			require.NoError(t, tenantDB.QueryRowContext(ctx, "select legacy_plus_seven(5)").Scan(&result))
			require.Equal(t, int64(12), result)
		}
		checkResult()
		checkCatalog("4.0.12", true)
		// Explicit replay reaches the handler; a cached second login would not
		// prove that its DDL is idempotent against the persisted catalog.
		for range 2 {
			var mutations int
			require.NoError(t, sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
				return v4_0_12.Handler.HandleTenantUpgrade(ctx, int32(accountID),
					&routineRepairTxn{TxnExecutor: txn, mutations: &mutations})
			}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).
				WithAccountID(catalog.System_Account).WithWaitCommittedLogApplied()))
			require.Zero(t, mutations)
			checkCatalog("4.0.12", true)
			checkResult()
		}
	})
}

type routineRepairTxn struct {
	executor.TxnExecutor
	mutations *int
}

func (txn *routineRepairTxn) Exec(statement string, opts executor.StatementOption) (executor.Result, error) {
	res, err := txn.TxnExecutor.Exec(statement, opts)
	lower := strings.ToLower(strings.TrimSpace(statement))
	if err == nil && (strings.HasPrefix(lower, "alter table") || strings.HasPrefix(lower, "create table") || strings.HasPrefix(lower, "create unique index")) {
		(*txn.mutations)++
	}
	return res, err
}
