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

package isolated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIssue27719DropAccountCleansSQLTaskLifecycle(t *testing.T) {
	cluster, err := embed.StartTestCluster(
		embed.WithCNCount(2),
		embed.WithPreStart(func(service embed.ServiceOperator) {
			if service.ServiceType() == metadata.ServiceType_CN {
				service.Adjust(func(config *embed.ServiceConfig) {
					config.CN.Frontend.SkipCheckUser = false
				})
			}
		}),
	)
	if cluster != nil {
		t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	}
	require.NoError(t, err)

	cn1, err := cluster.GetCNService(0)
	require.NoError(t, err)
	cn2, err := cluster.GetCNService(1)
	require.NoError(t, err)
	cn1Port := cn1.GetServiceConfig().CN.Frontend.Port
	cn2Port := cn2.GetServiceConfig().CN.Frontend.Port

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	sysDB := openIssue27719DB(t, ctx, fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn1Port))
	defer sysDB.Close()
	sysDBCN2 := openIssue27719DB(t, ctx, fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn2Port))
	defer sysDBCN2.Close()
	require.NoError(t, waitSystemBootstrap(ctx, sysDB))
	requireIssue27719Exec(t, ctx, sysDB, "set role moadmin")
	requireIssue27719Exec(t, ctx, sysDBCN2, "set role moadmin")

	accountName := fmt.Sprintf("issue27719_%d", time.Now().UnixNano())
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = sysDB.ExecContext(cleanupCtx, "drop account if exists `"+accountName+"`")
	}()
	requireIssue27719Exec(t, ctx, sysDB, fmt.Sprintf(
		"create account `%s` admin_name 'root' identified by '111'", accountName))

	var accountID uint32
	require.NoError(t, sysDB.QueryRowContext(ctx,
		"select account_id from mo_catalog.mo_account where account_name = ?", accountName,
	).Scan(&accountID))
	require.NotZero(t, accountID)

	tenantRoot := openIssue27719DB(t, ctx, fmt.Sprintf(
		"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, cn2Port))
	defer tenantRoot.Close()
	databaseName := "issue27719_tasks"
	requireIssue27719Exec(t, ctx, tenantRoot, "create database `"+databaseName+"`")
	tenantDB := openIssue27719DB(t, ctx, fmt.Sprintf(
		"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/%s", accountName, cn2Port, databaseName))
	defer tenantDB.Close()
	tenantDB.SetMaxOpenConns(8)
	requireIssue27719Exec(t, ctx, tenantDB, "create table sink(v int)")

	createStatements := []string{
		"create task never_run schedule '0 0 0 1 1 *' timezone 'UTC' as begin select 1; end",
		"create task completed_task as begin insert into sink values (1); end",
		"create task failed_task as begin insert into no_such_table values (1); end",
		"create task skipped_task when (0) as begin insert into sink values (2); end",
		"create task timeout_task timeout '1s' as begin insert into sink select sleep(2) + 3; end",
		"create task suspended_task schedule '*/1 * * * * *' timezone 'UTC' as begin insert into sink values (6); end",
		"create task repeating_task schedule '*/1 * * * * *' timezone 'UTC' as begin insert into sink values (4); end",
		"create task running_task as begin insert into sink select sleep(8) + 5; end",
	}
	for _, statement := range createStatements {
		requireIssue27719EventuallyExec(t, ctx, tenantDB, statement)
	}
	requireIssue27719Exec(t, ctx, tenantDB, "alter task suspended_task suspend")
	requireIssue27719Exec(t, ctx, tenantDB, "execute task completed_task")
	_, err = tenantDB.ExecContext(ctx, "execute task failed_task")
	require.Error(t, err)
	requireIssue27719Exec(t, ctx, tenantDB, "execute task skipped_task")
	_, err = tenantDB.ExecContext(ctx, "execute task timeout_task")
	require.Error(t, err)

	taskIDs := queryIssue27719TaskIDs(t, ctx, sysDB, accountID)
	require.Len(t, taskIDs, len(createStatements))
	repeatingTaskID := queryIssue27719TaskID(t, ctx, sysDB, accountID, "repeating_task")
	require.Eventually(t, func() bool {
		return queryIssue27719Count(t, ctx, sysDB,
			"select count(*) from mo_task.sys_async_task where task_parent_id = ?",
			fmt.Sprintf("sql-task:%d", repeatingTaskID)) > 0
	}, 20*time.Second, 100*time.Millisecond, "repeating task was not scheduled")

	runningDone := make(chan error, 1)
	go func() {
		_, executeErr := tenantDB.ExecContext(ctx, "execute task running_task")
		runningDone <- executeErr
	}()
	require.Eventually(t, func() bool {
		return queryIssue27719Count(t, ctx, sysDB,
			"select count(*) from mo_task.sql_task_run where account_id = ? and task_name = 'running_task' and status = 'RUNNING'",
			accountID) == 1
	}, 20*time.Second, 50*time.Millisecond, "running task did not start on CN2")

	// Keep creating tasks on CN2 while CN1 drops the account. Tasks that commit
	// before account cleanup must be deleted; tasks that reach storage after the
	// account-row lock must fail instead of committing orphan definitions.
	var raceWG sync.WaitGroup
	raceWG.Add(1)
	go func() {
		defer raceWG.Done()
		for i := 0; i < 32; i++ {
			_, _ = tenantDB.ExecContext(ctx, fmt.Sprintf(
				"create task race_task_%d as begin select %d; end", i, i))
		}
	}()

	requireIssue27719Exec(t, ctx, sysDB, "drop account `"+accountName+"`")
	raceWG.Wait()
	select {
	case <-runningDone:
	case <-time.After(20 * time.Second):
		t.Fatal("running SQL task did not converge after DROP ACCOUNT")
	}

	requireIssue27719AccountTaskResidue(t, ctx, sysDB, accountID, taskIDs, 0)

	// Every CN refreshes its SQL-task cache independently. Wait beyond the
	// fetch interval and prove stale cron jobs cannot recreate async work.
	time.Sleep(12 * time.Second)
	requireIssue27719AccountTaskResidue(t, ctx, sysDB, accountID, taskIDs, 0)
	require.NoError(t, tenantDB.Close())
	require.NoError(t, tenantRoot.Close())

	// Reusing the account name gets a new account ID and can create tasks; the
	// deleted generation neither blocks nor aliases the new tenant.
	requireIssue27719Exec(t, ctx, sysDB, fmt.Sprintf(
		"create account `%s` admin_name 'root' identified by '111'", accountName))
	var recreatedAccountID uint32
	require.NoError(t, sysDB.QueryRowContext(ctx,
		"select account_id from mo_catalog.mo_account where account_name = ?", accountName,
	).Scan(&recreatedAccountID))
	require.NotEqual(t, accountID, recreatedAccountID)
	require.Eventually(t, func() bool {
		var cn2AccountID uint32
		err := sysDBCN2.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name = ?", accountName,
		).Scan(&cn2AccountID)
		return err == nil && cn2AccountID == recreatedAccountID
	}, 10*time.Second, 50*time.Millisecond, "recreated account generation was not visible on CN2")

	recreatedRoot := openIssue27719AccountGenerationDB(t, ctx, fmt.Sprintf(
		"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, cn2Port), recreatedAccountID)
	defer recreatedRoot.Close()
	requireIssue27719Exec(t, ctx, recreatedRoot, "create database `"+databaseName+"`")
	requireIssue27719Exec(t, ctx, recreatedRoot, "use `"+databaseName+"`")
	requireIssue27719EventuallyExec(t, ctx, recreatedRoot,
		"create task never_run schedule '0 0 0 1 1 *' timezone 'UTC' as begin select 1; end")
	require.Eventually(t, func() bool {
		return queryIssue27719Count(t, ctx, sysDB,
			"select count(*) from mo_task.sql_task where account_id = ? and task_name = 'never_run'",
			recreatedAccountID) == 1
	}, 10*time.Second, 50*time.Millisecond, "recreated account task was not visible across CNs")
}

func openIssue27719DB(t *testing.T, ctx context.Context, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	require.NoError(t, db.PingContext(ctx))
	return db
}

func openIssue27719AccountGenerationDB(
	t *testing.T,
	ctx context.Context,
	dsn string,
	wantAccountID uint32,
) *sql.DB {
	t.Helper()
	var (
		db      *sql.DB
		lastErr error
	)
	require.Eventually(t, func() bool {
		candidate, err := sql.Open("mysql", dsn)
		if err != nil {
			lastErr = err
			return false
		}
		if err = candidate.PingContext(ctx); err != nil {
			lastErr = err
			_ = candidate.Close()
			return false
		}
		var accountID uint32
		if err = candidate.QueryRowContext(ctx, "select current_account_id()").Scan(&accountID); err != nil {
			lastErr = err
			_ = candidate.Close()
			return false
		}
		if accountID != wantAccountID {
			lastErr = fmt.Errorf("connected to account generation %d, want %d", accountID, wantAccountID)
			_ = candidate.Close()
			return false
		}
		db = candidate
		lastErr = nil
		return true
	}, 10*time.Second, 100*time.Millisecond, "recreated account authentication did not converge")
	require.NoError(t, lastErr)
	require.NotNil(t, db)
	return db
}

func requireIssue27719Exec(t *testing.T, ctx context.Context, db *sql.DB, statement string) {
	t.Helper()
	_, err := db.ExecContext(ctx, statement)
	require.NoErrorf(t, err, "exec failed: %s", statement)
}

func requireIssue27719EventuallyExec(t *testing.T, ctx context.Context, db *sql.DB, statement string) {
	t.Helper()
	var lastErr error
	require.Eventually(t, func() bool {
		_, lastErr = db.ExecContext(ctx, statement)
		return lastErr == nil || !strings.Contains(lastErr.Error(), "task service not ready yet")
	}, 10*time.Second, 100*time.Millisecond, "task service did not become ready")
	require.NoErrorf(t, lastErr, "exec failed: %s", statement)
}

func queryIssue27719Count(t *testing.T, ctx context.Context, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var count int
	require.NoError(t, db.QueryRowContext(ctx, query, args...).Scan(&count))
	return count
}

func queryIssue27719TaskID(t *testing.T, ctx context.Context, db *sql.DB, accountID uint32, taskName string) uint64 {
	t.Helper()
	var taskID uint64
	require.NoError(t, db.QueryRowContext(ctx,
		"select task_id from mo_task.sql_task where account_id = ? and task_name = ?", accountID, taskName,
	).Scan(&taskID))
	return taskID
}

func queryIssue27719TaskIDs(t *testing.T, ctx context.Context, db *sql.DB, accountID uint32) []uint64 {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		"select task_id from mo_task.sql_task where account_id = ? order by task_id", accountID)
	require.NoError(t, err)
	defer rows.Close()

	var taskIDs []uint64
	for rows.Next() {
		var taskID uint64
		require.NoError(t, rows.Scan(&taskID))
		taskIDs = append(taskIDs, taskID)
	}
	require.NoError(t, rows.Err())
	return taskIDs
}

func requireIssue27719AccountTaskResidue(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	accountID uint32,
	taskIDs []uint64,
	want int,
) {
	t.Helper()
	assert.Equal(t, want, queryIssue27719Count(t, ctx, db,
		"select count(*) from mo_catalog.mo_account where account_id = ?", accountID), "account rows")
	assert.Equal(t, want, queryIssue27719Count(t, ctx, db,
		"select count(*) from mo_task.sql_task where account_id = ?", accountID), "SQL task definitions")
	assert.Equal(t, want, queryIssue27719Count(t, ctx, db,
		"select count(*) from mo_task.sql_task_run where account_id = ?", accountID), "SQL task runs")

	parents := make([]string, len(taskIDs))
	args := make([]any, len(taskIDs))
	for i, taskID := range taskIDs {
		parents[i] = "?"
		args[i] = fmt.Sprintf("sql-task:%d", taskID)
	}
	if len(args) > 0 {
		assert.Equal(t, want, queryIssue27719Count(t, ctx, db,
			"select count(*) from mo_task.sys_async_task where task_parent_id in ("+strings.Join(parents, ",")+")",
			args...), "scheduled async tasks")
	}
}
