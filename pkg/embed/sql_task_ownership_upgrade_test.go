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
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/bootstrap"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_8"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

// Stage the released 4.0.7 schema on the current engine, then restart through
// the actual bootstrap route. This is not an old-binary or mixed-writer test.
func TestSQLTaskOwnershipUpgradeFromFrozenCatalog(t *testing.T) {
	c := newColumnsUpgradeCluster(t, t.Context(), []bootstrap.VersionHandle{v4_0_7.Handler})
	start, cleanup := newRecoveryClusterStarterWithBudget(t.Context(), c.Start, time.Now, 2*time.Minute)
	t.Cleanup(cleanup)
	ctx, err := start()
	require.NoError(t, err)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	db := recoverySQLClient(t, cn)
	defer db.Close()
	db.SetMaxOpenConns(1)
	require.Eventually(t, func() bool {
		rows, err := db.QueryContext(ctx, "show tasks")
		if err != nil {
			return false
		}
		defer rows.Close()
		for rows.Next() {
		}
		return rows.Err() == nil
	}, 30*time.Second, 100*time.Millisecond)
	for _, statement := range []string{
		"create database task_upgrade",
		"use task_upgrade",
		"create task existing_task as begin select 1; end",
		"execute task existing_task",
	} {
		_, err = db.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}
	var before int
	require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_task.sql_task_run where task_name='existing_task' and status='SUCCESS'").Scan(&before))
	require.Equal(t, 1, before)

	// The task table user is created by this private cluster; credentials are
	// read from its HAKeeper and never logged or taken from another environment.
	hakeeper, err := logservice.NewCNHAKeeperClient(ctx, cn.GetServiceConfig().CN.UUID, cn.GetServiceConfig().HAKeeperClient)
	require.NoError(t, err)
	state, err := hakeeper.GetClusterState(ctx)
	require.NoError(t, err)
	require.NoError(t, hakeeper.Close())
	config := mysql.NewConfig()
	config.User, config.Passwd = state.TaskTableUser.Username, state.TaskTableUser.Password
	config.Net, config.Addr, config.DBName = "tcp", fmt.Sprintf("127.0.0.1:%d", cn.GetServiceConfig().CN.Frontend.Port), "mo_task"
	connector, err := mysql.NewConnector(config)
	require.NoError(t, err)
	taskDB := sql.OpenDB(connector)
	defer taskDB.Close()
	for _, statement := range []string{
		"alter table mo_task.sql_task_run drop index idx_task_running",
		"alter table mo_task.sql_task drop column schedule_generation",
		"alter table mo_task.sys_async_task drop column sql_task_run_id",
	} {
		_, err = taskDB.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}

	// A partial or externally modified catalog must not be silently accepted.
	// Exercise the real upgrade handler and its transaction before staging the
	// clean predecessor for restart.
	applyUpgrade := func() error {
		return cn.RawService().(cnservice.Service).GetSQLExecutor().ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			return v4_0_8.Handler.HandleClusterUpgrade(ctx, txn)
		}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
	}
	_, err = taskDB.ExecContext(ctx, "alter table mo_task.sql_task add column schedule_generation bigint not null default 2")
	require.NoError(t, err)
	require.ErrorContains(t, applyUpgrade(), "incompatible definition")
	_, err = taskDB.ExecContext(ctx, "alter table mo_task.sql_task drop column schedule_generation")
	require.NoError(t, err)
	_, err = taskDB.ExecContext(ctx, "create index idx_task_running on mo_task.sql_task_run(account_id, task_name, status)")
	require.NoError(t, err)
	require.ErrorContains(t, applyUpgrade(), "unexpected columns")
	_, err = taskDB.ExecContext(ctx, "alter table mo_task.sql_task_run drop index idx_task_running")
	require.NoError(t, err)
	require.NoError(t, taskDB.Close())
	require.NoError(t, db.Close())
	require.NoError(t, c.Stop())

	setRecoveryCNHandles(c, []bootstrap.VersionHandle{v4_0_7.Handler, v4_0_8.Handler}, true)
	for generation := 0; generation < 2; generation++ {
		ctx, err = start()
		require.NoError(t, err)
		cn, err = c.GetCNService(0)
		require.NoError(t, err)
		final, steps := readRecoveryCatalog(t, ctx, cn, v4_0_8.Handler.Metadata())
		require.Equal(t, versions.StateReady, final.State)
		require.Len(t, steps, 1)
		require.Equal(t, "4.0.7", steps[0].FromVersion)
		require.Equal(t, "4.0.8", steps[0].ToVersion)
		require.Equal(t, versions.StateReady, steps[0].State)
		require.NoError(t, applyUpgrade(), "replaying a completed migration must be idempotent")

		db = recoverySQLClient(t, cn)
		db.SetMaxOpenConns(1)
		_, err = db.ExecContext(ctx, "use task_upgrade")
		require.NoError(t, err)
		var count int
		require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_task.sql_task_run where task_name='existing_task' and status='SUCCESS'").Scan(&count))
		require.Equal(t, 1+generation, count, "restart must preserve history without repeating the SQL body")
		if generation == 0 {
			_, err = db.ExecContext(ctx, "execute task existing_task")
			require.NoError(t, err)
		}
		require.NoError(t, db.Close())
		require.NoError(t, c.Stop())
	}
}

func TestSQLTaskOwnershipUpgradeCrossesFrozenCatalog(t *testing.T) {
	c, start := stageReleasedMetadataPredecessor(t, 3*time.Minute)
	setRecoveryCNHandles(c, append(columns46ShapeChain(), v4_0_7.Handler, v4_0_8.Handler), true)
	ctx, err := start()
	require.NoError(t, err)
	cn, err := c.GetCNService(0)
	require.NoError(t, err)
	final, steps := readRecoveryCatalog(t, ctx, cn, v4_0_8.Handler.Metadata())
	require.Equal(t, versions.StateReady, final.State)
	require.Len(t, steps, 2)
	require.Equal(t, "4.0.7", steps[0].ToVersion)
	require.Equal(t, "4.0.8", steps[1].ToVersion)
	for _, step := range steps {
		require.Equal(t, versions.StateReady, step.State)
	}
	requireDecoratedColumns(t, ctx, cn)
	require.NoError(t, c.Stop())
}
