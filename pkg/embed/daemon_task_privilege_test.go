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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDaemonTaskManagementPrivileges(t *testing.T) {
	RunSingleCNBaseClusterTests(t, func(c Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cn, err := c.GetCNService(0)
		require.NoError(t, err)
		service, ok := cn.RawService().(testutil.TaskServiceGetter).GetTaskService()
		require.True(t, ok)
		storage := service.GetStorage()
		open := func(identity string) *sql.Conn {
			t.Helper()
			db, err := sql.Open("mysql", fmt.Sprintf("%s:111@tcp(127.0.0.1:%d)/", identity, cn.GetServiceConfig().CN.Frontend.Port))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			conn, err := db.Conn(ctx)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			return conn
		}
		exec := func(t *testing.T, conn *sql.Conn, query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			require.NoError(t, err, query)
		}
		// Keep a separate control connection so cleanup retains admin authority
		// even if a SET ROLE public assertion fails on the session under test.
		control := open("sys#root#moadmin")
		exec(t, control, "create account daemon_priv_tenant admin_name 'admin' identified by '111'")
		t.Cleanup(func() {
			cleanupCtx, done := context.WithTimeout(context.Background(), 10*time.Second)
			defer done()
			_, err := control.ExecContext(cleanupCtx, "drop account if exists daemon_priv_tenant")
			require.NoError(t, err)
		})
		exec(t, control, "create user daemon_priv_public identified by '111'")
		t.Cleanup(func() {
			cleanupCtx, done := context.WithTimeout(context.Background(), 10*time.Second)
			defer done()
			for _, query := range []string{"drop user if exists daemon_priv_public", "drop role if exists daemon_priv_delegated"} {
				_, err := control.ExecContext(cleanupCtx, query)
				require.NoError(t, err)
			}
		})
		exec(t, control, "create role daemon_priv_delegated")
		exec(t, control, "grant all privileges on account * to daemon_priv_delegated")
		exec(t, control, "grant daemon_priv_delegated to daemon_priv_public")
		sysAdmin := open("sys#root#moadmin")
		sysPublic := open("sys#daemon_priv_public#public")
		delegated := open("sys#daemon_priv_public#daemon_priv_delegated")
		tenantAdmin := open("daemon_priv_tenant#admin#accountadmin")
		exec(t, tenantAdmin, "create user daemon_priv_public identified by '111'")
		tenantPublic := open("daemon_priv_tenant#daemon_priv_public#public")
		var tenantID uint32
		require.NoError(t, control.QueryRowContext(ctx,
			"select account_id from mo_catalog.mo_account where account_name='daemon_priv_tenant'").Scan(&tenantID))
		require.NotZero(t, tenantID)

		read := func(t *testing.T, id uint64) task.DaemonTask {
			t.Helper()
			rows, err := storage.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, id))
			require.NoError(t, err)
			require.Len(t, rows, 1)
			return rows[0]
		}
		create := func(account string, accountID uint32) task.DaemonTask {
			t.Helper()
			metadataID := "daemon-privileges/" + account
			row := task.DaemonTask{
				Metadata:   task.TaskMetadata{ID: metadataID, Executor: task.TaskCode_TestOnly},
				TaskStatus: task.TaskStatus_Paused, CreateAt: time.Now(), UpdateAt: time.Now(),
				Details: &task.Details{Account: account, AccountID: accountID},
			}
			n, err := storage.AddDaemonTask(ctx, row)
			require.NoError(t, err)
			// Daemon storage does not implement the async-task metadata predicate.
			// Resolve the generated ID by the unique metadata key through SQL,
			// including during cleanup if the initial lookup fails.
			var id uint64
			const findID = "select task_id from mo_task.sys_daemon_task where task_metadata_id=?"
			t.Cleanup(func() {
				cleanupCtx, done := context.WithTimeout(context.Background(), 10*time.Second)
				defer done()
				if id == 0 {
					err := control.QueryRowContext(cleanupCtx, findID, metadataID).Scan(&id)
					if errors.Is(err, sql.ErrNoRows) {
						return
					}
					require.NoError(t, err)
				}
				n, err := storage.DeleteDaemonTask(cleanupCtx, taskservice.WithTaskIDCond(taskservice.EQ, id))
				require.NoError(t, err)
				require.Equal(t, 1, n)
			})
			require.Equal(t, 1, n)
			require.NoError(t, control.QueryRowContext(ctx, findID, metadataID).Scan(&id))
			row = read(t, id)
			// AddDaemonTask does not persist claim fields. Set and read them back
			// before making this TestOnly row runnable. A different runner and a
			// heartbeat beyond this test's deadline exclude runner transitions.
			row.TaskRunner = "daemon-privilege-test-owner"
			row.LastRun = time.Now().UTC().Truncate(time.Microsecond)
			row.LastHeartbeat = time.Now().UTC().Add(time.Hour).Truncate(time.Second)
			n, err = storage.UpdateDaemonTask(ctx, []task.DaemonTask{row}, taskservice.WithTaskIDCond(taskservice.EQ, row.ID))
			require.NoError(t, err)
			require.Equal(t, 1, n)
			persisted := read(t, row.ID)
			require.Equal(t, row.TaskRunner, persisted.TaskRunner)
			require.True(t, row.LastHeartbeat.Equal(persisted.LastHeartbeat))
			require.Equal(t, accountID, persisted.AccountID)
			return persisted
		}
		sysRow := create("sys", 0)
		tenantRow := create("daemon_priv_tenant", tenantID)
		deny := func(t *testing.T, conn *sql.Conn, query string) {
			t.Helper()
			_, err := conn.ExecContext(ctx, query)
			var sqlErr *mysql.MySQLError
			require.ErrorAs(t, err, &sqlErr, query)
			require.Equal(t, uint16(20101), sqlErr.Number, query)
			require.Equal(t, "HY000", string(sqlErr.SQLState[:]), query)
			require.Equal(t, "internal error: do not have privilege to execute the statement", sqlErr.Message, query)
		}
		for _, conn := range []*sql.Conn{sysPublic, tenantPublic} {
			var role string
			require.NoError(t, conn.QueryRowContext(ctx, "select current_role()").Scan(&role))
			require.Equal(t, "public", role)
		}
		// mo_task is a sys-only database; the registry SELECT control applies
		// to sys/public. Tenant users instead see an unknown-database error.
		deny(t, sysPublic, "select task_id,task_status from mo_task.sys_daemon_task")
		for _, tc := range []struct {
			verb          string
			before, after task.TaskStatus
		}{
			{"pause", task.TaskStatus_Running, task.TaskStatus_PauseRequested},
			{"resume", task.TaskStatus_Paused, task.TaskStatus_ResumeRequested},
			{"cancel", task.TaskStatus_Running, task.TaskStatus_CancelRequested},
		} {
			t.Run(tc.verb, func(t *testing.T) {
				for _, owner := range []struct {
					row           task.DaemonTask
					admin, public *sql.Conn
				}{
					{sysRow, sysAdmin, sysPublic},
					{tenantRow, tenantAdmin, tenantPublic},
				} {
					row := owner.row
					row.TaskStatus = tc.before
					n, err := storage.UpdateDaemonTask(ctx, []task.DaemonTask{row}, taskservice.WithTaskIDCond(taskservice.EQ, row.ID))
					require.NoError(t, err)
					require.Equal(t, 1, n)
					before := read(t, row.ID)
					query := fmt.Sprintf("%s daemon task %d", tc.verb, row.ID)
					deny(t, owner.public, query)
					require.Equal(t, before, read(t, row.ID), "denied management must not change any stored field")
					deny(t, delegated, query)
					require.Equal(t, before, read(t, row.ID), "AccountAll does not delegate daemon management")
					deny(t, owner.public, tc.verb+" daemon task 0")
					if row.AccountID == 0 {
						_, err := tenantAdmin.ExecContext(ctx, query)
						foreignResult := read(t, row.ID)
						require.ErrorContains(t, err, fmt.Sprintf("task with ID %d not found", row.ID),
							"tenant %d targeting sys task: %s -> %s", tenantID, before.TaskStatus, foreignResult.TaskStatus)
						require.Equal(t, before, foreignResult, "other-tenant admin must not change sys task")
					}
					exec(t, owner.admin, query)
					after := read(t, row.ID)
					require.Equal(t, tc.after, after.TaskStatus)
					require.Equal(t, row.TaskRunner, after.TaskRunner)
					require.True(t, row.LastHeartbeat.Equal(after.LastHeartbeat))
					// Reuse the very same connection after dropping its active admin
					// role. Even idempotent/invalid-state requests must now be denied.
					exec(t, owner.admin, "set role public")
					deny(t, owner.admin, query)
					require.Equal(t, after, read(t, row.ID))
					role := "moadmin"
					if row.AccountID != 0 {
						role = "accountadmin"
					}
					exec(t, owner.admin, "set role "+role)
				}
			})
		}
		_, err = sysAdmin.ExecContext(ctx, "prepare daemon_priv_stmt from 'pause daemon task 0'")
		require.Error(t, err, "daemon PREPARE remains unsupported")
		require.Equal(t, task.TaskStatus_CancelRequested, read(t, sysRow.ID).TaskStatus)
	})
}
