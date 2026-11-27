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
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

func TestCDCManagedRecoveryAndUserRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	path := t.TempDir()
	cluster, err := embed.NewCluster(embed.WithTesting(), embed.WithDataPath(path), embed.WithCNCount(2), embed.WithPreStart(func(s embed.ServiceOperator) {
		if s.ServiceType() == metadata.ServiceType_CN {
			s.Adjust(func(cfg *embed.ServiceConfig) {
				cfg.CN.TaskRunner.FetchInterval.Duration = 100 * time.Millisecond
				cfg.CN.TaskRunner.HeartbeatInterval.Duration = 250 * time.Millisecond
				cfg.CN.TaskRunner.HeartbeatTimeout.Duration = 5 * time.Second
			})
		}
	}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cluster.Close()) })
	require.NoError(t, cluster.Start())
	first, err := cluster.GetCNService(0)
	require.NoError(t, err)
	svc := first.RawService().(cnservice.Service)
	var ts taskservice.TaskService
	require.Eventually(t, func() bool {
		var ok bool
		ts, ok = svc.GetTaskService()
		return ok
	}, 90*time.Second, 100*time.Millisecond)
	db, err := sql.Open("mysql", fmt.Sprintf("dump:111@tcp(127.0.0.1:%d)/?readTimeout=45s&writeTimeout=45s", first.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	const recoverID = "00000000-0000-0000-0000-000000000781"
	const failedID = "00000000-0000-0000-0000-000000000782"
	const invalidID = "00000000-0000-0000-0000-000000000783"
	tables, err := cdc.JsonEncode(cdc.PatternTuples{Pts: []*cdc.PatternTuple{}})
	require.NoError(t, err)
	seed := func(id, name, state, encodedTables string) {
		statement := fmt.Sprintf(`insert into mo_catalog.mo_cdc_task(account_id,task_id,task_name,source_uri,sink_uri,sink_type,sink_password,tables,filters,start_ts,end_ts,no_full,additional_config,state,err_msg) values(0,'%s','%s','','{}','console','','%s', '', '', '', true, '{}', '%s', 'original failure')`, id, name, encodedTables, state)
		result, e := svc.GetSQLExecutor().Exec(ctx, statement, executor.Options{}.WithAccountID(0))
		require.NoError(t, e)
		result.Close()
		details := &task.Details{
			AccountID: 0, Account: "sys", Username: "dump",
			Details: &task.Details_CreateCdc{CreateCdc: &task.CreateCdcDetails{
				TaskId: id, TaskName: name, Accounts: []*task.Account{{Id: 0}},
			}},
		}
		now := time.Now()
		n, e := ts.GetStorage().AddDaemonTask(ctx, task.DaemonTask{
			Metadata: task.TaskMetadata{ID: id, Executor: task.TaskCode_InitCdc},
			TaskType: task.TaskType_CreateCdc, TaskStatus: task.TaskStatus_Running,
			Details: details, CreateAt: now, UpdateAt: now,
		})
		require.NoError(t, e)
		require.Equal(t, 1, n)
	}
	seed(recoverID, "cdc_recovery", cdc.CDCState_Restarting, tables)
	seed(failedID, "cdc_failed", cdc.CDCState_Failed, tables)
	seed(invalidID, "cdc_invalid", cdc.CDCState_Restarting, "invalid-hex")
	find := func(id string) (task.DaemonTask, string, string, error) {
		tasks, e := ts.QueryDaemonTask(ctx, taskservice.WithTaskType(taskservice.EQ, task.TaskType_CreateCdc.String()))
		if e != nil {
			return task.DaemonTask{}, "", "", e
		}
		var row task.DaemonTask
		for _, v := range tasks {
			if v.Metadata.ID == id {
				row = v
				break
			}
		}
		var state, diag string
		e = db.QueryRowContext(ctx, "select state,err_msg from mo_catalog.mo_cdc_task where account_id=0 and task_id=?", id).Scan(&state, &diag)
		return row, state, diag, e
	}
	require.Eventually(t, func() bool {
		row, state, diag, e := find(recoverID)
		return e == nil && row.TaskStatus == task.TaskStatus_Running && row.TaskRunner != "" && state == "running" && diag == ""
	}, 40*time.Second, 100*time.Millisecond, "restarting recovery must reach running")
	require.Eventually(t, func() bool {
		row, state, diag, e := find(failedID)
		return e == nil && row.TaskStatus == task.TaskStatus_Error && state == "failed" && diag == "original failure"
	}, 40*time.Second, 100*time.Millisecond, "terminal failed task must not auto-start")
	require.Eventually(t, func() bool {
		row, state, diag, e := find(invalidID)
		return e == nil && row.TaskStatus == task.TaskStatus_Error && state == "failed" && strings.Contains(diag, "invalid byte")
	}, 40*time.Second, 100*time.Millisecond, "permanent startup errors must stop automatic retry")
	_, err = db.ExecContext(ctx, "resume cdc task cdc_failed 'restart'")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		row, state, diag, e := find(failedID)
		return e == nil && row.TaskStatus == task.TaskStatus_Running && row.TaskRunner != "" && state == "running" && diag == ""
	}, 40*time.Second, 100*time.Millisecond, "explicit restart must recover failed task")
}
