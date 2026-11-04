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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
	"github.com/stretchr/testify/require"
)

func TestTableCollationViewProtocolSQL(t *testing.T) {
	runSQLIntegration(t, func(cluster embed.Cluster) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		cn, err := cluster.GetCNService(0)
		require.NoError(t, err)
		db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn.GetServiceConfig().CN.Frontend.Port))
		require.NoError(t, err)
		defer db.Close()
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer conn.Close()
		defer cleanupSQLIntegration(t, cn, "drop database if exists c03_function_protocol")
		exec := func(statement string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, statement)
			require.NoError(t, err, statement)
		}
		exec("create database c03_function_protocol")
		exec("use c03_function_protocol")
		original, ok := moruntime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables(moruntime.MOProtocolVersion)
		require.True(t, ok)
		originalVersion, ok := original.(int64)
		require.True(t, ok)
		internalValue, ok := moruntime.ServiceRuntime(cn.ServiceID()).GetGlobalVariables(moruntime.InternalSQLExecutor)
		require.True(t, ok)
		internal, ok := internalValue.(executor.SQLExecutor)
		require.True(t, ok)
		rpc := func(version int64) string {
			return fmt.Sprintf("select mo_ctl('cn','setprotocolversion','%s:%d')", cn.ServiceID(), version)
		}
		// Restore before dropping the schema and before closing the connection, even
		// when an assertion fails after lowering the advertised capability.
		defer func() {
			restoreCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			_, err := conn.ExecContext(restoreCtx, rpc(originalVersion))
			require.NoError(t, err, "restore the original CN protocol")
			require.NoError(t, bootstrap.InitSystemViews(restoreCtx, internal), "restore the shared TABLES definition")
		}()
		exec(rpc(113))
		exec("create view legacy_control as select 1 as c")
		_, err = conn.ExecContext(ctx, "create view blocked_function as select internal_table_collation('') as c")
		require.ErrorContains(t, err, "protocol version 118")
		var published int
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='c03_function_protocol' and relname='blocked_function'").Scan(&published))
		require.Zero(t, published, "rejection must precede catalog publication")
		var control int
		require.NoError(t, conn.QueryRowContext(ctx, "select c from legacy_control").Scan(&control))
		require.Equal(t, 1, control)
		exec(rpc(118))
		exec("create view admitted_function as select internal_table_collation('') as c")
		var collation string
		require.NoError(t, conn.QueryRowContext(ctx, "select c from admitted_function").Scan(&collation))
		require.Equal(t, "utf8mb4_general_ci", collation)
		exec("prepare c03_persisted_view from \"create view prepared_function as select internal_table_collation('') as c\"")
		defer func() {
			closeCtx, done := context.WithTimeout(context.Background(), 30*time.Second)
			defer done()
			_, err := conn.ExecContext(closeCtx, "deallocate prepare c03_persisted_view")
			require.NoError(t, err)
		}()
		viewID := func() uint64 {
			t.Helper()
			var id uint64
			require.NoError(t, conn.QueryRowContext(ctx, "select rel_id from mo_catalog.mo_tables where account_id=0 and reldatabase='information_schema' and lower(relname)='tables'").Scan(&id))
			return id
		}
		before := viewID()
		exec(rpc(113))
		_, err = conn.ExecContext(ctx, "execute c03_persisted_view")
		require.ErrorContains(t, err, "protocol version 118", "a prepared author must not reuse an earlier capability observation")
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from mo_catalog.mo_tables where reldatabase='c03_function_protocol' and relname='prepared_function'").Scan(&published))
		require.Zero(t, published)
		require.NoError(t, bootstrap.InitSystemViews(ctx, internal), "a matching definition needs no new authoring")
		require.Equal(t, before, viewID(), "a matching TABLES view must not be replaced")

		// Install an actual pre-v118 definition so reconciliation must author
		// the new function. A current definition correctly skips this DDL.
		legacyDDL := strings.Replace(sysview.InformationSchemaTablesDDL, "CREATE VIEW ", "CREATE OR REPLACE VIEW ", 1)
		result, err := internal.Exec(ctx, legacyDDL, executor.Options{}.
			WithAccountID(catalog.System_Account).WithWaitCommittedLogApplied().
			WithTxnMode(txn.TxnMode_Pessimistic).WithTxnIsolation(txn.TxnIsolation_RC))
		require.NoError(t, err)
		result.Close()
		before = viewID()
		err = bootstrap.InitSystemViews(ctx, internal)
		require.ErrorContains(t, err, "protocol version 118")
		require.Equal(t, before, viewID(), "rejected replacement must preserve the old view")
		var definition string
		require.NoError(t, conn.QueryRowContext(ctx, "select rel_createsql from mo_catalog.mo_tables where account_id=0 and reldatabase='information_schema' and lower(relname)='tables'").Scan(&definition))
		require.Equal(t, legacyDDL, definition, "rejected authoring must preserve the old definition")
		_, err = conn.ExecContext(ctx, "select c from admitted_function where 1=1")
		require.ErrorContains(t, err, "protocol version 118", "persisted readers must reapply the capability floor")
		exec(rpc(118))
		require.NoError(t, bootstrap.InitSystemViews(ctx, internal), "reconcile after a rejected initialization")
		before = viewID()
		require.NoError(t, bootstrap.InitSystemViews(ctx, internal), "replay after successful replacement")
		require.Equal(t, before, viewID(), "replay must not issue another replacement")
		exec("execute c03_persisted_view")
		require.NoError(t, conn.QueryRowContext(ctx, "select c from prepared_function").Scan(&collation))
		require.Equal(t, "utf8mb4_general_ci", collation)
		require.NoError(t, conn.QueryRowContext(ctx, "select count(*) from information_schema.TABLES where TABLE_SCHEMA='c03_function_protocol'").Scan(&published))
		require.Equal(t, 3, published, "the v118 system view, legacy view and direct/prepared authors are usable")
		require.NoError(t, conn.QueryRowContext(ctx, "select c from admitted_function where 2=2").Scan(&collation))
		require.Equal(t, "utf8mb4_general_ci", collation)
	})
}
