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

package mv

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

type scheduledTasks struct {
	taskservice.TaskService
	batches [][]task.TaskMetadata
}

type scheduledRefreshExecutor struct {
	ie.InternalExecutor
	exec func(context.Context, string, ie.SessionOverrideOptions) error
}

func (e scheduledRefreshExecutor) Exec(ctx context.Context, query string, opts ie.SessionOverrideOptions) error {
	return e.exec(ctx, query, opts)
}

// A queue entry cannot borrow a privileged session, revive an obsolete policy,
// or lose its terminal admission failure. Reuse the existing in-memory catalog
// fixture; no timer, task runner, or SQL service is needed for these boundaries.
func TestMaterializedViewTaskAdmission(t *testing.T) {
	for _, name := range []string{"non-async task", "malformed envelope", "unknown version", "metadata failure", "retired definition", "obsolete epoch", "manual policy", "definer success", "definer failure", "already published failure"} {
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			state := State{Key: Key{7, 99}, Epoch: 2, Interval: 60,
				Definition: catalog.MVDefinition{FormatVersion: 1, Database: "db", Query: "select * from t", OwnerUser: 11, OwnerRole: 12, OwnerName: "owner", Sources: []catalog.MVSource{{AccountID: 7, Database: "db", Table: "t"}}}}
			if name == "manual policy" {
				state.Interval = 0
			}
			if name == "already published failure" {
				state.LastRun = "queued-run"
			}
			fixture := &refreshExecutor{t: t, mp: mp, state: state}
			metadataError := errors.New("metadata unavailable")
			definerError := errors.New("definer permission revoked")
			var writes []string
			loads, calls := 0, 0
			sql := executor.NewMemExecutor(func(query string) (executor.Result, error) {
				if strings.HasPrefix(query, "select database_name,") {
					loads++
					if name == "metadata failure" {
						return executor.Result{}, metadataError
					}
					if name == "retired definition" {
						return executor.Result{}, nil
					}

					return fixture.stateResult(), nil
				}
				if !strings.HasPrefix(query, "select ") {
					writes = append(writes, query)
				}
				return executor.Result{}, nil
			})
			svc := NewService(sql)
			spec := TaskSpec{Version: 1, Key: state.Key, Epoch: 2, RunID: "queued-run"}
			if name == "unknown version" {
				spec.Version = 2
			}
			if name == "obsolete epoch" {
				spec.Epoch = 1
			}
			payload, err := json.Marshal(spec)
			require.NoError(t, err)
			if name == "malformed envelope" {
				payload = []byte("[")
			}
			var queued task.Task = &task.AsyncTask{Metadata: task.TaskMetadata{Context: payload}}
			if name == "non-async task" {
				queued = &task.CronTask{}
			}
			run := svc.TaskExecutor(nil, func() ie.InternalExecutor {
				calls++
				return scheduledRefreshExecutor{exec: func(ctx context.Context, query string, opts ie.SessionOverrideOptions) error {
					account, err := defines.GetAccountId(ctx)
					require.NoError(t, err)
					require.Equal(t, uint32(7), account)
					require.Equal(t, uint32(11), defines.GetUserId(ctx))
					require.Equal(t, uint32(12), defines.GetRoleId(ctx))
					envelope, ok := defines.GetMaterializedViewTask(ctx)
					require.True(t, ok)
					require.Equal(t, defines.MaterializedViewTask{Account: 7, Table: 99, Epoch: 2, RunID: "queued-run"}, envelope)
					require.Equal(t, "refresh materialized view `db`.`stored`", query)
					require.Equal(t, ie.NewOptsBuilder().AccountId(7).UserId(11).DefaultRoleId(12).Username("owner").Database("db").Internal(false).Finish(), opts)
					_, bounded := ctx.Deadline()
					require.True(t, bounded)
					if name != "definer success" {
						return definerError
					}
					return nil
				}}
			})
			err = run(context.Background(), queued)
			switch name {
			case "non-async task", "malformed envelope", "unknown version":
				require.Error(t, err)
				require.Zero(t, loads)
				switch name {
				case "non-async task":
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				case "malformed envelope":
					var syntaxError *json.SyntaxError
					require.ErrorAs(t, err, &syntaxError)
				case "unknown version":
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrNotSupported))
				}
			case "retired definition":
				require.ErrorContains(t, err, "metadata does not exist")
				require.Zero(t, calls)
				require.Empty(t, writes)
			case "metadata failure":
				require.ErrorIs(t, err, metadataError)
			case "definer failure":
				require.ErrorIs(t, err, definerError)
				require.Len(t, writes, 3)
				require.Contains(t, writes[0], "requested_sequence=1")
				require.Contains(t, writes[1], "'FAILED'")
				require.Contains(t, writes[1], "'refresh admission failed'")
				require.NotContains(t, writes[1], definerError.Error(), "persist only sanitized failure diagnostics")
				require.Contains(t, writes[2], "next_due=date_add")
			case "already published failure":
				require.ErrorIs(t, err, definerError)
			default:
				require.NoError(t, err)
			}
			if name != "definer failure" {
				require.Empty(t, writes)
			}
			if name == "definer success" || name == "definer failure" || name == "already published failure" {
				require.Equal(t, 1, calls)
			} else {
				require.Zero(t, calls)
			}
		})
	}
}

func (s *scheduledTasks) CreateBatch(_ context.Context, batch []task.TaskMetadata) error {
	s.batches = append(s.batches, append([]task.TaskMetadata(nil), batch...))
	return nil
}
func TestMaterializedViewReconcileIdentity(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	sql := executor.NewMemExecutor(func(query string) (executor.Result, error) {
		if strings.HasPrefix(query, "select account_id,mv_id,epoch,") {
			require.Contains(t, query, "next_due<=current_timestamp()")
			require.Contains(t, query, "not exists")
			require.Contains(t, query, "limit 128")
			r := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_int64.ToType()}, mp)
			r.NewBatchWithRowCount(2)
			require.NoError(t, executor.AppendFixedRows(r, 0, []uint32{7, 8}))
			require.NoError(t, executor.AppendFixedRows(r, 1, []uint64{99, 99}))
			require.NoError(t, executor.AppendFixedRows(r, 2, []uint64{2, 2}))
			require.NoError(t, executor.AppendFixedRows(r, 3, []int64{123, 123}))
			return r.GetResult(), nil
		}
		require.True(t, strings.HasPrefix(query, "select account_id,mv_id,run_id,") || strings.HasPrefix(query, "select account_id,mv_id from"))
		return executor.Result{}, nil
	})
	service := NewService(sql)
	tasks := &scheduledTasks{}
	require.NoError(t, service.reconcile(context.Background(), tasks))
	require.NoError(t, service.reconcile(context.Background(), tasks))
	require.Len(t, tasks.batches, 2)
	require.Len(t, tasks.batches[0], 2)
	require.Equal(t, tasks.batches[0], tasks.batches[1])
	require.NotEqual(t, tasks.batches[0][0].ID, tasks.batches[0][1].ID)
}

func TestMaterializedViewInterruptedTaskRecovery(t *testing.T) {
	for _, phase := range []string{"success", "scan failure", "reschedule failure", "abort failure", "cancelled page"} {
		t.Run(phase, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			failure := errors.New("interrupted task recovery unavailable")
			var writes []string
			sql := executor.NewMemExecutor(func(query string) (executor.Result, error) {
				if strings.HasPrefix(query, "select account_id,mv_id,run_id,") {
					require.Contains(t, query, "status='RUNNING'")
					require.Contains(t, query, "interval 360 second")
					require.Contains(t, query, "limit 128")
					if phase == "scan failure" {
						return executor.Result{}, failure
					}
					r := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_varchar.ToType(), types.T_uint64.ToType()}, mp)
					r.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(r, 0, []uint32{7}))
					require.NoError(t, executor.AppendFixedRows(r, 1, []uint64{99}))
					require.NoError(t, executor.AppendStringRows(r, 2, []string{"interrupted-run"}))
					require.NoError(t, executor.AppendFixedRows(r, 3, []uint64{2}))
					if phase == "cancelled page" {
						cancel()
					}
					return r.GetResult(), nil
				}
				if strings.HasPrefix(query, "update ") {
					writes = append(writes, query)
					require.Contains(t, query, "account_id=7 and mv_id=99")
					require.Contains(t, query, "run_id='interrupted-run'")
					require.Contains(t, query, "status='RUNNING'")
					if strings.Contains(query, "next_due=") {
						require.Contains(t, query, "epoch=2")
						require.Contains(t, query, "last_run<>'interrupted-run'")
						if phase == "reschedule failure" {
							return executor.Result{}, failure
						}
					} else {
						require.Contains(t, query, "status='ABORTED'")
						if phase == "abort failure" {
							return executor.Result{}, failure
						}
					}
				}
				return executor.Result{}, nil
			})
			tasks := &scheduledTasks{}
			err := NewService(sql).reconcile(ctx, tasks)
			switch phase {
			case "success":
				require.NoError(t, err)
				require.Len(t, writes, 2)
			case "cancelled page":
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, writes)
			default:
				require.ErrorIs(t, err, failure)
			}
			require.Empty(t, tasks.batches, "recovery failure cannot enqueue a replacement refresh")
		})
	}
}

func TestMaterializedViewHistoryRetention(t *testing.T) {
	for _, phase := range []string{"overlapping pages", "key scan failure", "count page failure", "age page failure", "delete failure", "cursor failure", "cancelled page"} {
		t.Run(phase, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			failure := errors.New("history maintenance unavailable")
			pages, deletes, advances := 0, 0, 0
			sql := executor.NewMemExecutor(func(query string) (executor.Result, error) {
				switch {
				case strings.HasPrefix(query, "select account_id,mv_id from"):
					require.Contains(t, query, "limit 16")
					if phase == "key scan failure" {
						return executor.Result{}, failure
					}
					r := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType()}, mp)
					r.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(r, 0, []uint32{7}))
					require.NoError(t, executor.AppendFixedRows(r, 1, []uint64{99}))
					if phase == "cancelled page" {
						cancel()
					}
					return r.GetResult(), nil
				case strings.HasPrefix(query, "select run_id"):
					pages++
					require.Contains(t, query, "account_id=7 and mv_id=99")
					require.Contains(t, query, "status<>'RUNNING'")
					require.Contains(t, query, "limit 128")
					if pages == 1 {
						require.Contains(t, query, "offset 100")
						if phase == "count page failure" {
							return executor.Result{}, failure
						}
					} else {
						require.Contains(t, query, "interval 7 day")
						if phase == "age page failure" {
							return executor.Result{}, failure
						}
					}
					r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
					r.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendStringRows(r, 0, []string{"old-terminal-run"}))
					return r.GetResult(), nil
				case strings.HasPrefix(query, "delete from"):
					deletes++
					require.Contains(t, query, "account_id=7 and mv_id=99")
					require.Contains(t, query, "run_id='old-terminal-run' and status<>'RUNNING'")
					if phase == "delete failure" {
						return executor.Result{}, failure
					}
				case strings.HasPrefix(query, "update "):
					advances++
					require.Contains(t, query, "account_id=7 and mv_id=99")
					require.Contains(t, query, "next_cleanup=date_add")
					if phase == "cursor failure" {
						return executor.Result{}, failure
					}
				default:
					t.Fatalf("unexpected maintenance statement: %s", query)
				}
				return executor.Result{}, nil
			})
			err := NewService(sql).cleanupHistory(ctx)
			switch phase {
			case "overlapping pages":
				require.NoError(t, err)
				require.Equal(t, 2, pages)
				require.Equal(t, 1, deletes, "overlapping retention pages delete each terminal run once")
				require.Equal(t, 1, advances)
			case "cancelled page":
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, pages)
			default:
				require.ErrorIs(t, err, failure)
			}
			if phase != "overlapping pages" && phase != "cursor failure" {
				require.Zero(t, advances, "a failed maintenance page must not advance its cursor")
			}
		})
	}
}
