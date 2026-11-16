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
	"sync"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions/v4_0_7"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type upgradeTaskStorageFactory struct {
	store   taskservice.TaskStorage
	created chan struct{}
	once    sync.Once
}

func (f *upgradeTaskStorageFactory) Create(string) (taskservice.TaskStorage, error) {
	f.once.Do(func() { close(f.created) })
	return f.store, nil
}

// Stage the released 4.0.6 task schema on the current engine. This exercises
// the real upgrade and restart path, but is not a test of an old binary.
func TestSQLTaskOwnershipUpgradeFromReleasedCatalog(t *testing.T) {
	require.NoError(t, CloseBaseClusterTests())
	require.NoError(t, CloseSingleCNBaseClusterTests())
	c := newColumnsUpgradeCluster(t, t.Context(), columns46ShapeChain())
	factory := &upgradeTaskStorageFactory{store: taskservice.NewMemTaskStorage(), created: make(chan struct{})}
	var logOperator *operator
	c.ForeachServices(func(svc ServiceOperator) bool {
		if svc.ServiceType() == metadata.ServiceType_LOG {
			logOperator = svc.(*operator)
			logOperator.testingLogOptions = []logservice.Option{logservice.WithTaskStorageFactory(factory)}
		}
		return true
	})
	require.NotNil(t, logOperator)
	start, cleanup := newRecoveryClusterStarterWithBudget(t.Context(), c.Start, time.Now, 3*time.Minute)
	t.Cleanup(cleanup)
	ctx, err := start()
	require.NoError(t, err)
	select {
	case <-factory.created:
	case <-ctx.Done():
		t.Fatal("staging scheduler did not use the isolated task store")
	}
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

	// Stop the CN's real task writers before changing their table shapes. The
	// HAKeeper scheduler uses the isolated store until the staging cluster stops.
	cnService := cn.RawService().(cnservice.Service)
	var runner taskservice.TaskRunner
	require.Eventually(t, func() bool {
		runner = cnService.GetTaskRunner()
		return runner != nil
	}, 30*time.Second, 100*time.Millisecond)
	require.NoError(t, runner.Stop())
	taskService, ok := cnService.GetTaskService()
	require.True(t, ok)
	require.NoError(t, taskService.Close())

	// Obtain the private cluster's task-table user; never use external credentials.
	hakeeper, err := logservice.NewCNHAKeeperClient(ctx, cn.GetServiceConfig().CN.UUID, cn.GetServiceConfig().HAKeeperClient)
	require.NoError(t, err)
	defer hakeeper.Close()
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
	columns := func(db *sql.DB, table string) []string {
		t.Helper()
		rows, err := db.QueryContext(ctx, "select * from mo_task."+table+" limit 0")
		require.NoError(t, err)
		defer rows.Close()
		names, err := rows.Columns()
		require.NoError(t, err)
		require.NoError(t, rows.Err())
		return names
	}
	freshAsyncColumns := columns(taskDB, "sys_async_task")
	freshTaskColumns := columns(taskDB, "sql_task")

	// Preserve a historical live run as well as the successful history. The
	// new owner must not silently rerun it after the upgrade.
	_, err = taskDB.ExecContext(ctx, "insert into mo_task.sql_task_run(task_id, task_name, account_id, status, trigger_type, runner_cn) select task_id, task_name, account_id, 'RUNNING', 'SCHEDULED', 'released-cn' from mo_task.sql_task where task_name='existing_task'")
	require.NoError(t, err)
	var oldRunID uint64
	require.NoError(t, taskDB.QueryRowContext(ctx, "select run_id from mo_task.sql_task_run where task_name='existing_task' and status='RUNNING'").Scan(&oldRunID))

	// The staged marker alone still uses current fresh DDL. Remove new fields
	// and indexes explicitly to model the task tables in released 4.2.8.
	for _, statement := range []string{
		"alter table mo_task.sql_task_run drop index idx_task_running",
		"alter table mo_task.sql_task_run drop index idx_account_id",
		"alter table mo_task.sql_task drop index idx_account_id",
		"alter table mo_task.sys_async_task drop index idx_task_parent_id",
		"alter table mo_task.sql_task drop column schedule_generation",
		"alter table mo_task.sys_async_task drop column sql_task_run_id",
	} {
		_, err = taskDB.ExecContext(ctx, statement)
		require.NoError(t, err, statement)
	}

	// An incompatible pre-existing object must fail rather than be accepted.
	applyEntry := func(index int) error {
		return cn.RawService().(cnservice.Service).GetSQLExecutor().ExecTxn(ctx, func(txn executor.TxnExecutor) error {
			_, err := v4_0_7.Handler.UpgradeClusterEntryPage(ctx, index, txn, 64<<20)
			return err
		}, executor.Options{}.WithDatabase(catalog.MO_CATALOG).WithWaitCommittedLogApplied())
	}
	ownershipStart := v4_0_7.Handler.ClusterUpgradeEntryCount() - 3
	_, err = taskDB.ExecContext(ctx, "alter table mo_task.sql_task add column schedule_generation bigint not null default 2")
	require.NoError(t, err)
	require.ErrorContains(t, applyEntry(ownershipStart+1), "incompatible definition")
	_, err = taskDB.ExecContext(ctx, "alter table mo_task.sql_task drop column schedule_generation")
	require.NoError(t, err)
	_, err = taskDB.ExecContext(ctx, "create index idx_task_running on mo_task.sql_task_run(account_id, task_name, status)")
	require.NoError(t, err)
	require.ErrorContains(t, applyEntry(ownershipStart+2), "unexpected columns")
	_, err = taskDB.ExecContext(ctx, "alter table mo_task.sql_task_run drop index idx_task_running")
	require.NoError(t, err)
	// Stage one committed ownership DDL before crossing. The upgrade must
	// recognize the existing column and apply the remaining entries.
	require.NoError(t, applyEntry(ownershipStart))
	require.NoError(t, taskDB.Close())
	require.NoError(t, db.Close())
	require.NoError(t, c.Stop())
	logOperator.testingLogOptions = nil

	setRecoveryCNHandles(c, append(columns46ShapeChain(), v4_0_7.Handler), true)
	for generation := 0; generation < 2; generation++ {
		ctx, err = start()
		require.NoError(t, err)
		cn, err = c.GetCNService(0)
		require.NoError(t, err)
		final, steps := readRecoveryCatalog(t, ctx, cn, v4_0_7.Handler.Metadata())
		require.Equal(t, versions.StateReady, final.State)
		require.Len(t, steps, 1)
		require.Equal(t, "4.0.6", steps[0].FromVersion)
		require.Equal(t, "4.0.7", steps[0].ToVersion)
		require.Equal(t, versions.StateReady, steps[0].State)
		for index := ownershipStart; index < v4_0_7.Handler.ClusterUpgradeEntryCount(); index++ {
			require.NoError(t, applyEntry(index), "replaying a completed migration must be idempotent")
		}

		func() {
			db := recoverySQLClient(t, cn)
			defer db.Close()
			db.SetMaxOpenConns(1)
			_, err = db.ExecContext(ctx, "use task_upgrade")
			require.NoError(t, err)
			taskDB := sql.OpenDB(connector)
			defer taskDB.Close()
			require.Equal(t, freshAsyncColumns, columns(taskDB, "sys_async_task"))
			require.Equal(t, freshTaskColumns, columns(taskDB, "sql_task"))
			var asyncColumnCount int
			require.NoError(t, taskDB.QueryRowContext(ctx, "show column_number from sys_async_task").Scan(&asyncColumnCount))
			require.Equal(t, 15, asyncColumnCount)
			var count int
			require.NoError(t, db.QueryRowContext(ctx, "select count(*) from mo_task.sql_task_run where task_name='existing_task' and status='SUCCESS'").Scan(&count))
			require.Equal(t, 1+generation, count, "restart must preserve history without repeating the SQL body")
			if generation == 0 {
				_, err = db.ExecContext(ctx, "execute task existing_task")
				require.ErrorContains(t, err, "already running")
				_, err = db.ExecContext(ctx, fmt.Sprintf("alter task existing_task reset runs %d force", oldRunID))
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, "alter task existing_task resume")
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, "execute task existing_task")
				require.NoError(t, err)
			}
		}()
		require.NoError(t, c.Stop())
	}
}
