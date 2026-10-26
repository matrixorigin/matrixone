// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/stretchr/testify/require"
)

// TestIssue31303TaskNameInjectionCannotCrossTenant is the public-SQL
// regression for issue #31303: a tenant user must not be able to read
// (SHOW TASK RUNS FOR), delete (DROP TASK) or execute (EXECUTE TASK) another
// tenant's SQL tasks by wrapping an injected predicate into the task name.
// The scenario runs against a real embedded cluster through the MySQL wire
// protocol, so it exercises the same statement → parser → handler →
// task storage path as user traffic. All waits are bounded readiness polls
// on observable state; no fixed sleeps.
func TestIssue31303TaskNameInjectionCannotCrossTenant(t *testing.T) {
	cluster, err := embed.StartTestCluster(
		embed.WithCNCount(1),
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
	cn1Port := cn1.GetServiceConfig().CN.Frontend.Port

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	sysDB := openIssue27719DB(t, ctx, fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/", cn1Port))
	defer sysDB.Close()
	require.NoError(t, waitSystemBootstrap(ctx, sysDB))
	readyCtx, readyCancel := context.WithTimeout(ctx, 30*time.Second)
	defer readyCancel()
	require.NoError(t, waitSQLTaskReady(readyCtx, sysDB))

	// sys account owns the secret task.
	const (
		sysTaskName  = "sys_31303_task"
		sysDBName    = "issue_31303_sys"
		accountName  = "issue31303_acct"
		tenantDBName = "issue_31303_tenant"
		tenantTask   = "tenant_31303_own"
	)
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		_, _ = sysDB.ExecContext(cleanupCtx, "drop account if exists `"+accountName+"`")
		_, _ = sysDB.ExecContext(cleanupCtx, "drop database if exists "+sysDBName)
	}()

	execSQLRequire(t, ctx, sysDB, "create database "+sysDBName)
	sysTaskDB := openIssue27719DB(t, ctx, fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/%s", cn1Port, sysDBName))
	defer sysTaskDB.Close()
	execSQLRequire(t, ctx, sysTaskDB, "create table sys_31303_sink(v int)")
	execSQLRequire(t, ctx, sysTaskDB,
		fmt.Sprintf("create task %s as begin insert into sys_31303_sink values (1); end", sysTaskName))
	execSQLRequire(t, ctx, sysTaskDB, "execute task "+sysTaskName)
	sysTaskCount := func() int {
		return queryIssue27719Count(t, ctx, sysDB,
			"select count(*) from mo_task.sql_task where account_id = 0 and task_name = ?", sysTaskName)
	}
	sysRunCount := func() int {
		return queryIssue27719Count(t, ctx, sysDB,
			"select count(*) from mo_task.sql_task_run where account_id = 0 and task_name = ?", sysTaskName)
	}
	require.Eventually(t, func() bool { return sysTaskCount() == 1 },
		20*time.Second, 100*time.Millisecond, "sys task must be visible in task storage")
	require.Eventually(t, func() bool { return sysRunCount() == 1 },
		20*time.Second, 100*time.Millisecond, "sys task manual run must be recorded")

	// Tenant account under attack.
	execSQLRequire(t, ctx, sysDB, fmt.Sprintf(
		"create account `%s` admin_name 'root' identified by '111'", accountName))
	var tenantAccountID uint32
	require.NoError(t, sysDB.QueryRowContext(ctx,
		"select account_id from mo_catalog.mo_account where account_name = ?", accountName,
	).Scan(&tenantAccountID))
	require.NotZero(t, tenantAccountID)

	tenantRoot := openIssue27719DB(t, ctx, fmt.Sprintf(
		"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/", accountName, cn1Port))
	defer tenantRoot.Close()
	execSQLRequire(t, ctx, tenantRoot, "create database "+tenantDBName)
	tenantDB := openIssue27719DB(t, ctx, fmt.Sprintf(
		"%s#root#accountadmin:111@tcp(127.0.0.1:%d)/%s", accountName, cn1Port, tenantDBName))
	defer tenantDB.Close()
	execSQLRequire(t, ctx, tenantDB, "create table tenant_31303_sink(v int)")
	execSQLRequire(t, ctx, tenantDB,
		fmt.Sprintf("create task %s as begin insert into tenant_31303_sink values (2); end", tenantTask))
	execSQLRequire(t, ctx, tenantDB, "execute task "+tenantTask)
	require.Eventually(t, func() bool {
		return queryIssue27719Count(t, ctx, sysDB,
			"select count(*) from mo_task.sql_task_run where account_id = ? and task_name = ?",
			tenantAccountID, tenantTask) == 1
	}, 20*time.Second, 100*time.Millisecond, "tenant's own run must be recorded")

	// showTaskRunRows returns the rows of `show task runs for <name>`,
	// i.e. exactly what the tenant can observe.
	showTaskRunRows := func(name string) int {
		rows, err := tenantDB.QueryContext(ctx,
			fmt.Sprintf("show task runs for `%s`", name))
		require.NoError(t, err)
		defer rows.Close()
		count := 0
		for rows.Next() {
			count++
		}
		require.NoError(t, rows.Err())
		return count
	}

	// Attack shapes from the issue. Both resolve the sys account's task on
	// unfixed code, through different mechanisms, and must resolve nothing in
	// the tenant scope on fixed code:
	//   - commentShape terminates the literal and comments out the account_id
	//     conjunct of the storage WHERE clause.
	//   - orShape restructures the predicate with OR so its middle disjunct
	//     (account_id=0 AND task_name='sys_31303_task') selects exactly the
	//     sys task; a payload matching several tasks would be masked by the
	//     handlers' len(tasks) != 1 guard, so orShape is deliberately
	//     single-target.
	attackShapes := []string{
		`x' or task_name='sys_31303_task' -- `,
		`x' or account_id=0 and task_name='sys_31303_task' or '1'='2`,
	}
	for _, shape := range attackShapes {
		// 1) Cross-tenant read must stay impossible: the injection must not
		// resolve the sys account's runs; the tenant's own runs stay visible.
		require.Equal(t, 0, showTaskRunRows(shape),
			"injected SHOW must not leak sys account runs for %q", shape)

		// 2) Cross-tenant delete must stay impossible: the hostile drop finds
		// nothing in the tenant scope and the sys task survives.
		execSQLRequire(t, ctx, tenantDB, fmt.Sprintf("drop task if exists `%s`", shape))
		require.Equal(t, 1, sysTaskCount(),
			"sys task must survive injected drop for %q", shape)

		// 3) Cross-tenant execute must stay impossible: the hostile lookup
		// fails, no additional sys run row appears and the task body's
		// side effect (a sys sink insert) does not run.
		_, err := tenantDB.ExecContext(ctx, fmt.Sprintf("execute task `%s`", shape))
		require.Error(t, err, "injected execute must not find the sys task for %q", shape)
		require.Equal(t, 1, sysRunCount(),
			"no extra sys run may appear from injected execute for %q", shape)
		require.Equal(t, 1,
			queryIssue27719Count(t, ctx, sysDB,
				"select count(*) from "+sysDBName+".sys_31303_sink"),
			"sys sink must be untouched by injected execute for %q", shape)

		// 4) Cross-tenant alter must stay impossible: same lookup boundary.
		_, err = tenantDB.ExecContext(ctx, fmt.Sprintf("alter task `%s` set when (0)", shape))
		require.Error(t, err, "injected alter must not find the sys task for %q", shape)
	}

	// The broad tautology shape from the issue is also an information-disclosure
	// read on unfixed code; it matches several rows, so it is only used for the
	// read (SHOW) path where the row count bound still detects the leak.
	require.Equal(t, 0, showTaskRunRows(`x' or account_id=0 or '1'='1`),
		"OR-tautology injection must not leak sys account runs")
	require.Equal(t, 1, showTaskRunRows(tenantTask), "tenant must still see its own runs")

	// 5) Plain cross-tenant statements must stay scoped too.
	execSQLRequire(t, ctx, tenantDB, "drop task if exists "+sysTaskName)
	_, err = tenantDB.ExecContext(ctx, "execute task "+sysTaskName)
	require.Error(t, err, "plain cross-tenant execute must fail")
	require.Equal(t, 1, sysTaskCount(), "sys task must survive plain cross-tenant drop")
	require.Equal(t, 1, sysRunCount(), "no extra sys run may appear from plain execute")

	// 6) CREATE duplicate check must be tenant scoped: the tenant may create
	// a task whose name collides with the sys account's task name, and the
	// subsequent identical-name drop removes only the tenant's own task.
	execSQLRequire(t, ctx, tenantDB,
		fmt.Sprintf("create task %s as begin insert into tenant_31303_sink values (3); end", sysTaskName))
	execSQLRequire(t, ctx, tenantDB, "drop task if exists "+sysTaskName)
	require.Equal(t, 1, sysTaskCount(), "sys task must survive the tenant's same-name drop")
	require.Equal(t, 0,
		queryIssue27719Count(t, ctx, sysDB,
			"select count(*) from mo_task.sql_task where account_id = ? and task_name = ?",
			tenantAccountID, sysTaskName),
		"tenant's same-name task must be dropped by the tenant drop")

	// 7) ALTER of the tenant's own task keeps working.
	execSQLRequire(t, ctx, tenantDB, "alter task "+tenantTask+" set when (0)")

	// 8) Legal quote/backslash task names round-trip through the public
	// interface: create, locate by exact rendered name (show tasks is the
	// tenant-visible cursor), drop. The doubled-quote/backslash encoding must
	// resolve to the original bytes under the actual storage connection mode.
	quotedName := `it's a\31303 task`
	execSQLRequire(t, ctx, tenantDB,
		fmt.Sprintf("create task `%s` as begin insert into tenant_31303_sink select 1 where false; end", quotedName))
	shown := make(map[string]struct{})
	showTasks, err := tenantDB.QueryContext(ctx, "show tasks")
	require.NoError(t, err)
	for showTasks.Next() {
		var taskName sql.NullString
		require.NoError(t, showTasks.Scan(
			&taskName, new(sql.NullString), new(sql.NullString), new(sql.NullString),
			new(sql.NullString), new(sql.NullString), new(sql.NullString),
			new(sql.NullString), new(sql.NullString)))
		if taskName.Valid {
			shown[taskName.String] = struct{}{}
		}
	}
	require.NoError(t, showTasks.Err())
	showTasks.Close()
	require.Contains(t, shown, quotedName, "quoted/backslash task name must round-trip verbatim")
	execSQLRequire(t, ctx, tenantDB, fmt.Sprintf("drop task if exists `%s`", quotedName))
	require.Equal(t, 0,
		queryIssue27719Count(t, ctx, sysDB,
			"select count(*) from mo_task.sql_task where account_id = ? and task_name = ?",
			tenantAccountID, quotedName),
		"quoted/backslash task must be droppable by its original name")

	// Final invariants: the sys task and its run are untouched, the tenant
	// account remains functional.
	require.Equal(t, 1, sysTaskCount())
	require.Equal(t, 1, sysRunCount())
	require.Equal(t, 1, showTaskRunRows(tenantTask))
	require.Equal(t, 1,
		queryIssue27719Count(t, ctx, sysDB,
			"select count(*) from issue_31303_sys.sys_31303_sink"))
}
