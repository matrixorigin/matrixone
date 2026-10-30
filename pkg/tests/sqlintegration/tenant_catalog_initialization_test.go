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
	"fmt"
	"strings"
	"testing"
	"time"

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
	"github.com/stretchr/testify/require"
)

func TestCreateAccountCatalogInitialization(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		port := cn.GetServiceConfig().CN.Frontend.Port
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer db.Close()
		require.NoError(t, testutils.WaitSystemBootstrap(ctx, db))
		sys, err := db.Conn(ctx)
		require.NoError(t, err)
		defer sys.Close()

		const account = "tenant_catalog_initialization"
		defer cleanupSQLIntegration(t, cn, "drop account if exists "+account)
		original, ok := moruntime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables(moruntime.MOProtocolVersion)
		require.True(t, ok)
		originalVersion, ok := original.(int64)
		require.True(t, ok)
		require.GreaterOrEqual(t, originalVersion, int64(defines.MORPCVersion126))
		setProtocol := func(version int64) string {
			return fmt.Sprintf("select mo_ctl('cn','setprotocolversion','%s:%d')", cn.ServiceID(), version)
		}
		// Restore capability before account cleanup, including after FailNow.
		defer func() {
			restoreCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			_, err := sys.ExecContext(restoreCtx, setProtocol(originalVersion))
			require.NoError(t, err)
		}()

		_, err = sys.ExecContext(ctx, setProtocol(defines.MORPCVersion125))
		require.NoError(t, err)
		create := "create account " + account + " admin_name 'root' identified by '111'"
		_, err = sys.ExecContext(ctx, create)
		require.ErrorContains(t, err, "protocol version 126")
		var count int
		require.NoError(t, sys.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_account where account_name=?", account).Scan(&count))
		require.Zero(t, count, "failed initialization must roll back the account version row")

		_, err = sys.ExecContext(ctx, setProtocol(originalVersion))
		require.NoError(t, err)
		_, err = sys.ExecContext(ctx, create)
		require.NoError(t, err, "the same account name must be reusable after recovery")
		var version string
		require.NoError(t, sys.QueryRowContext(ctx, "select create_version from mo_catalog.mo_account where account_name=?", account).Scan(&version))
		require.Equal(t, "4.0.18", version)

		tenant, err := sql.Open("mysql", fmt.Sprintf(account+"#root#accountadmin:111@tcp(127.0.0.1:%d)/", port))
		require.NoError(t, err)
		defer tenant.Close()
		execSQLRequire(t, ctx, tenant, "create database catalog_init collate utf8mb4_bin")
		execSQLRequire(t, ctx, tenant, "create table catalog_init.t(id int primary key, raw varbinary(8))")
		execSQLRequire(t, ctx, tenant, "create view catalog_init.v as select id from catalog_init.t with local check option")
		var updateNull, checkNull, charsetNull, collationNull int
		var tableCollation string
		require.NoError(t, tenant.QueryRowContext(ctx, "select update_time is null, check_time is null, table_collation from information_schema.tables where table_schema='catalog_init' and table_name='t'").Scan(&updateNull, &checkNull, &tableCollation))
		require.Equal(t, 1, updateNull)
		require.Equal(t, 1, checkNull)
		require.Equal(t, "utf8mb4_bin", tableCollation)
		require.NoError(t, tenant.QueryRowContext(ctx, "select character_set_name is null, collation_name is null from information_schema.columns where table_schema='catalog_init' and table_name='t' and column_name='raw'").Scan(&charsetNull, &collationNull))
		require.Equal(t, 1, charsetNull)
		require.Equal(t, 1, collationNull)
		var definition, checkOption, updatable string
		require.NoError(t, tenant.QueryRowContext(ctx, "select view_definition, check_option, is_updatable from information_schema.views where table_schema='catalog_init' and table_name='v'").Scan(&definition, &checkOption, &updatable))
		require.True(t, strings.HasPrefix(strings.ToLower(definition), "select "), definition)
		require.Equal(t, "LOCAL", checkOption)
		require.Equal(t, "NO", updatable)
	})
}
