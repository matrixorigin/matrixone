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
package embed

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cnservice"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/logservice"
	logservicepb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/queryservice"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/stretchr/testify/require"
)

type cdcControlResult struct {
	n   int
	err error
}

func testCDCControlConcurrentClaims(t *testing.T, c Cluster) {
	cn0, err := c.GetCNService(0)
	require.NoError(t, err)
	cn1, err := c.GetCNService(1)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	hakeeper, err := logservice.NewCNHAKeeperClient(ctx, cn0.GetServiceConfig().CN.UUID, cn0.GetServiceConfig().HAKeeperClient)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, hakeeper.Close()) })
	state, err := hakeeper.GetClusterState(ctx)
	require.NoError(t, err)
	// The regular task-service address resolver can select either CN. Pin this
	// test's storage to CN0 so the competing transaction is definitely on CN1.
	holder := taskservice.NewTaskServiceHolder(runtime.ServiceRuntime(cn0.ServiceID()), func(context.Context, bool) (string, error) {
		return fmt.Sprintf("127.0.0.1:%d", cn0.GetServiceConfig().CN.Frontend.Port), nil
	})
	t.Cleanup(func() { require.NoError(t, holder.Close()) })
	require.NoError(t, holder.Create(logservicepb.CreateTaskService{
		User: state.TaskTableUser, TaskDatabase: "mo_task",
	}))
	svc, ok := holder.Get()
	require.True(t, ok)
	store := svc.GetStorage()
	exec0 := cn0.RawService().(cnservice.Service).GetSQLExecutor()
	exec1 := cn1.RawService().(cnservice.Service).GetSQLExecutor()
	localLS := lockservice.GetLockServiceByServiceID(cn0.ServiceID())
	require.NotNil(t, localLS)
	ls := lockservice.GetLockServiceByServiceID(cn1.ServiceID())
	require.NotNil(t, ls)
	for _, kind := range []string{"owner_change", "heartbeat"} {
		if !t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			cdcID := uuid.NewString()
			t.Cleanup(func() {
				clean, done := context.WithTimeout(context.Background(), 10*time.Second)
				defer done()
				result, e := exec0.Exec(clean, fmt.Sprintf("delete from mo_catalog.mo_cdc_task where account_id=0 and task_id='%s'", cdcID), executor.Options{}.WithAccountID(catalog.System_Account))
				if e == nil {
					result.Close()
				}
				require.NoError(t, e)
			})
			setup := fmt.Sprintf("insert into mo_catalog.mo_cdc_task (account_id,task_id,task_name,source_uri,sink_uri,tables,state) values (0,'%s','probe-%s','source','sink','db.table','running')", cdcID, cdcID)
			result, e := exec0.Exec(ctx, setup, executor.Options{}.WithAccountID(catalog.System_Account).WithWaitCommittedLogApplied())
			require.NoError(t, e)
			result.Close()
			dt := task.DaemonTask{Metadata: task.TaskMetadata{ID: cdcID}, TaskStatus: task.TaskStatus_Running, CreateAt: time.Now(), UpdateAt: time.Now(), Details: &task.Details{AccountID: 0, Account: "sys", Details: &task.Details_CreateCdc{CreateCdc: &task.CreateCdcDetails{TaskId: cdcID, TaskName: "probe-" + cdcID, Accounts: []*task.Account{{Id: 0, Name: "sys"}}}}}}
			var id uint64
			t.Cleanup(func() {
				clean, done := context.WithTimeout(context.Background(), 10*time.Second)
				defer done()
				if id != 0 {
					_, e := store.DeleteDaemonTask(clean, taskservice.WithTaskIDCond(taskservice.EQ, id))
					require.NoError(t, e)
				} else {
					query := fmt.Sprintf("delete from mo_task.sys_daemon_task where account_id=0 and task_metadata_executor=%d and task_metadata_id='%s'", task.TaskCode_TestOnly, cdcID)
					result, e := exec0.Exec(clean, query, executor.Options{}.WithAccountID(catalog.System_Account))
					if e == nil {
						result.Close()
					}
					require.NoError(t, e)
				}
			})
			n, e := store.AddDaemonTask(ctx, dt)
			require.NoError(t, e)
			require.Equal(t, 1, n)
			all, e := store.QueryDaemonTask(ctx, taskservice.WithTaskExecutorCond(taskservice.EQ, task.TaskCode_TestOnly))
			require.NoError(t, e)
			for _, row := range all {
				if row.Metadata.ID == cdcID {
					dt = row
					id = row.ID
				}
			}
			require.NotZero(t, id)
			dt.TaskRunner = "initial-owner"
			dt.LastRun = time.Now().UTC().Truncate(time.Microsecond)
			dt.LastHeartbeat = time.Now().UTC().Truncate(time.Second)
			n, e = store.UpdateDaemonTask(ctx, []task.DaemonTask{dt}, taskservice.WithTaskIDCond(taskservice.EQ, id))
			require.NoError(t, e)
			require.Equal(t, 1, n)
			ready := make(chan []byte, 1)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			holderDone := make(chan error, 1)
			controlDone := make(chan cdcControlResult, 1)
			holderStarted, holderJoined := false, false
			controlStarted, controlJoined := false, false
			defer func() {
				unblock()
				cancel()
				if holderStarted && !holderJoined {
					<-holderDone
				}
				if controlStarted && !controlJoined {
					<-controlDone
				}
			}()
			holderStarted = true
			go func() {
				holderDone <- exec1.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
					mutation := "last_heartbeat=date_add(last_heartbeat,interval 1 second)"
					if kind == "owner_change" {
						mutation = "task_runner='new-owner',last_run=date_add(last_run,interval 1 microsecond)"
					}
					r, e := tx.Exec(fmt.Sprintf("update mo_task.sys_daemon_task set %s where task_id=%d", mutation, id), executor.StatementOption{})
					if e != nil {
						return e
					}
					r.Close()
					ready <- append([]byte(nil), tx.Txn().Txn().ID...)
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}, executor.Options{}.WithAccountID(catalog.System_Account).WithTxnMode(txn.TxnMode_Pessimistic).WithTxnIsolation(txn.TxnIsolation_RC))
			}()
			var holderID []byte
			select {
			case holderID = <-ready:
			case e := <-holderDone:
				holderJoined = true
				t.Fatalf("holder failed before lock: %v", e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			collectorDone := make(chan uint32, 1)
			controlStarted = true
			go func() {
				n, e := svc.UpdateCDCTask(ctx, task.TaskStatus_PauseRequested, func(ctx context.Context, _ task.TaskStatus, keys map[taskservice.CDCTaskKey]struct{}, db taskservice.SqlExecutor) (int, error) {
					connectionID, e := func() (uint32, error) {
						rows, err := db.QueryContext(ctx, "select connection_id()")
						if err != nil {
							return 0, err
						}
						defer rows.Close()
						if !rows.Next() {
							if err := rows.Err(); err != nil {
								return 0, err
							}
							return 0, fmt.Errorf("connection_id returned no rows")
						}
						var id uint32
						if err := rows.Scan(&id); err != nil {
							return 0, err
						}
						return id, nil
					}()
					if e != nil {
						return 0, e
					}
					r, e := db.ExecContext(ctx, "update mo_catalog.mo_cdc_task set state='pausing' where account_id=0 and task_id=?", cdcID)
					if e != nil {
						return 0, e
					}
					affected, e := r.RowsAffected()
					if e != nil {
						return 0, e
					}
					keys[taskservice.CDCTaskKey{AccountId: 0, TaskId: cdcID}] = struct{}{}
					collectorDone <- connectionID
					return int(affected), nil
				}, taskservice.WithTaskIDCond(taskservice.EQ, id))
				controlDone <- cdcControlResult{n, e}
			}()
			var connectionID uint32
			select {
			case connectionID = <-collectorDone:
			case out := <-controlDone:
				controlJoined = true
				t.Fatalf("control ended before catalog write: %v", out.err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			provider := cn0.RawService().(interface {
				SessionMgr() *queryservice.SessionManager
			})
			require.Eventually(t, func() bool {
				var controlID uuid.UUID
				for _, session := range provider.SessionMgr().GetAllSessions() {
					if identity, ok := session.(interface {
						GetTxnId() uuid.UUID
						GetConnectionID() uint32
					}); ok && identity.GetConnectionID() == connectionID {
						controlID = identity.GetTxnId()
						break
					}
				}
				if controlID == (uuid.UUID{}) {
					return false
				}
				found, waiters, e := ls.GetWaitingList(ctx, holderID)
				if e != nil || !found {
					return false
				}
				for _, waiter := range waiters {
					if bytes.Equal(waiter.TxnID, controlID[:]) && waiter.CreatedOn == localLS.GetServiceID() {
						return true
					}
				}
				return false
			}, 15*time.Second, 20*time.Millisecond, "control must wait for holder row")
			unblock()
			select {
			case e := <-holderDone:
				holderJoined = true
				require.NoError(t, e)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			var out cdcControlResult
			select {
			case out = <-controlDone:
				controlJoined = true
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			stateCount := countInternalRows(t, ctx, cn0, fmt.Sprintf("select count(*) from mo_catalog.mo_cdc_task where account_id=0 and task_id='%s' and state='pausing'", cdcID))
			latest, e := store.QueryDaemonTask(ctx, taskservice.WithTaskIDCond(taskservice.EQ, id))
			require.NoError(t, e)
			require.Len(t, latest, 1)
			require.NoError(t, out.err)
			require.Equal(t, 2, out.n)
			require.EqualValues(t, 1, stateCount)
			require.Equal(t, task.TaskStatus_PauseRequested, latest[0].TaskStatus)
			if kind == "owner_change" {
				require.Equal(t, "new-owner", latest[0].TaskRunner)
				require.True(t, dt.LastRun.Add(time.Microsecond).Equal(latest[0].LastRun))
			} else {
				require.Equal(t, dt.TaskRunner, latest[0].TaskRunner)
				require.True(t, dt.LastRun.Equal(latest[0].LastRun))
				require.True(t, dt.LastHeartbeat.Add(time.Second).Equal(latest[0].LastHeartbeat), "renewed heartbeat must remain")
			}
		}) {
			return
		}
	}
}
