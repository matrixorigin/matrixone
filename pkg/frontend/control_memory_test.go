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

package frontend

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestTaskControlMemoryStatementScope(t *testing.T) {
	for _, tc := range []struct {
		sql, database string
		want          bool
	}{
		{"select task_id from sys_async_task where task_status in (1) order by task_id", "mo_task", true},
		{"select * from mo_task.sys_daemon_task where task_runner='cn'", "other", true},
		{"update sys_daemon_task set last_heartbeat=now() where task_id=1", "mo_task", true},
		{"delete from sys_async_task where task_status=2", "mo_task", true},
		{"select * from sys_cron_task", "mo_task", true},
		{"select * from sys_async_task", "user_db", false},
		{"select * from user_db.sys_async_task", "mo_task", false},
		{"select * from sys_async_task join user_db.data on 1=1", "mo_task", false},
		{"select * from sys_async_task union all select * from sys_async_task", "mo_task", false},
		{"with c as (select * from sys_async_task) select * from c", "mo_task", false},
		{"update sys_daemon_task t join user_db.data d on t.task_id=d.id set t.task_status=1", "mo_task", false},
		{"insert into sys_async_task(task_id) values (1)", "mo_task", false},
		{"select * from user_db.data", "mo_task", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			require.Equal(t, tc.want, isTaskControlMemoryStatement(stmt, tc.database))
		})
	}
}

func TestTaskControlMemoryRequiresServiceIdentity(t *testing.T) {
	const taskUser = "memory-control-task-service"
	SetSpecialUser(taskUser, []byte("test"))
	t.Cleanup(func() {
		specialUsers.Lock()
		delete(specialUsers.users, taskUser)
		specialUsers.Unlock()
	})
	ctrl := gomock.NewController(t)
	ses := newSes(nil, ctrl)
	t.Cleanup(ses.Close)
	ses.SetDatabaseName("mo_task")
	stmt, err := mysql.ParseOne(t.Context(), "select * from sys_async_task", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ses.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, User: rootName})
	require.False(t, taskControlMemoryStatement(t.Context(), ses, stmt), "sys/root is not a task service identity")
	ses.SetTenantInfo(&TenantInfo{Tenant: "tenant", User: taskUser})
	require.False(t, taskControlMemoryStatement(t.Context(), ses, stmt))
	ses.SetTenantInfo(&TenantInfo{Tenant: sysAccountName, User: taskUser})
	require.True(t, taskControlMemoryStatement(t.Context(), ses, stmt))

	// The task storage driver prepares heartbeat updates. Resolve the saved
	// database, not whatever database the session selected before EXECUTE.
	ses.SetDatabaseName("user_db")
	heartbeat, err := mysql.ParseOne(t.Context(), "update sys_daemon_task set last_heartbeat=? where task_id=?", 1)
	require.NoError(t, err)
	require.NoError(t, ses.SetPrepareStmt(t.Context(), "heartbeat", &PrepareStmt{
		Name: "heartbeat", PrepareStmt: heartbeat, defaultDatabase: "mo_task",
	}))
	require.True(t, taskControlMemoryStatement(t.Context(), ses, tree.NewExecute("heartbeat")))
	userQuery, err := mysql.ParseOne(t.Context(), "select * from sys_async_task", 1)
	require.NoError(t, err)
	require.NoError(t, ses.SetPrepareStmt(t.Context(), "user_query", &PrepareStmt{
		Name: "user_query", PrepareStmt: userQuery, defaultDatabase: "user_db",
	}))
	require.False(t, taskControlMemoryStatement(t.Context(), ses, tree.NewExecute("user_query")))
}
