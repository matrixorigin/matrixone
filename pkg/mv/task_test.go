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
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/stretchr/testify/require"
)

type scheduledTasks struct {
	taskservice.TaskService
	batches [][]task.TaskMetadata
	crons   []task.TaskMetadata
	exprs   []string
	cronErr error
}

type maintenanceTxnExecutor struct {
	executor.SQLExecutor
	execTxn func(context.Context, func(executor.TxnExecutor) error, executor.Options) error
}

func (e *maintenanceTxnExecutor) ExecTxn(ctx context.Context, f func(executor.TxnExecutor) error, opts executor.Options) error {
	return e.execTxn(ctx, f, opts)
}

func TestMaterializedViewMaintenanceLifecycleFence(t *testing.T) {
	for _, phase := range []string{"success", "fence failure", "cancelled fence", "cursor failure"} {
		t.Run(phase, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			failure := errors.New("maintenance lifecycle fence unavailable")
			inTxn, fenced := false, false
			txns, fences, cursors := 0, 0, 0
			query := func(q string) (executor.Result, error) {
				switch {
				case strings.HasPrefix(q, "select account_id,mv_id from"):
					r := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType()}, mp)
					r.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(r, 0, []uint32{7}))
					require.NoError(t, executor.AppendFixedRows(r, 1, []uint64{99}))
					return r.GetResult(), nil
				case strings.HasPrefix(q, "select run_id"):
					return executor.Result{}, nil
				case strings.HasPrefix(q, "select revision from mo_catalog.mo_mv_account_state "):
					require.True(t, inTxn)
					require.Zero(t, cursors)
					require.Equal(t, "select revision from mo_catalog.mo_mv_account_state where account_id=7 for update", q)
					fences++
					if phase == "fence failure" {
						return executor.Result{}, failure
					}
					if phase == "cancelled fence" {
						cancel()
						return executor.Result{}, ctx.Err()
					}
					fenced = true
				case strings.HasPrefix(q, "update mo_catalog.mo_materialized_views set next_cleanup="):
					require.True(t, inTxn && fenced, "cursor and lifecycle fence must share one transaction")
					require.Contains(t, q, "account_id=7 and mv_id=99")
					cursors++
					if phase == "cursor failure" {
						return executor.Result{}, failure
					}
				default:
					t.Fatalf("unexpected maintenance SQL (retired account state must not be inserted): %s", q)
				}
				return executor.Result{}, nil
			}
			sql := &maintenanceTxnExecutor{SQLExecutor: executor.NewMemExecutor(query)}
			sql.execTxn = func(ctx context.Context, f func(executor.TxnExecutor) error, opts executor.Options) error {
				require.Equal(t, txn.TxnMode_Pessimistic, opts.TxnMode())
				require.Equal(t, txn.TxnIsolation_RC, opts.TxnIsolation())
				require.Equal(t, uint32(catalog.System_Account), opts.AccountID())
				txns++
				inTxn = true
				defer func() { inTxn = false }()
				return f(executor.NewMemTxnExecutor(query, nil))
			}
			err := NewService(sql).cleanupHistory(ctx)
			if phase == "success" {
				require.NoError(t, err)
			} else if phase == "cancelled fence" {
				require.ErrorIs(t, err, context.Canceled)
			} else {
				require.ErrorIs(t, err, failure)
			}
			require.Equal(t, 1, txns)
			require.Equal(t, 1, fences)
			if phase == "success" || phase == "cursor failure" {
				require.Equal(t, 1, cursors)
			} else {
				require.Zero(t, cursors)
			}
		})
	}
}

func (s *scheduledTasks) CreateCronTask(_ context.Context, metadata task.TaskMetadata, expression string) error {
	s.crons = append(s.crons, metadata)
	s.exprs = append(s.exprs, expression)
	return s.cronErr
}

func TestMaterializedViewCronAdmission(t *testing.T) {
	for _, name := range []string{"ready", "missing catalogs", "partial catalogs", "catalog query failure", "old consumer", "protocol query failure", "empty inventory", "task store failure"} {
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZeroNoFixed()
			t.Cleanup(func() {
				defer mpool.DeleteMPool(mp)
				require.Zero(t, mp.CurrNB())
			})
			injected := errors.New("cron admission dependency unavailable")
			protocolQueries := 0
			sql := executor.NewMemExecutor(func(query string) (executor.Result, error) {
				if query == CatalogReadinessSQL {
					if name == "catalog query failure" {
						return executor.Result{}, injected
					}
					count := int64(4)
					if name == "missing catalogs" {
						count = 0
					} else if name == "partial catalogs" {
						count = 2
					}
					r := executor.NewMemResult([]types.Type{types.T_int64.ToType()}, mp)
					r.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(r, 0, []int64{count}))
					return r.GetResult(), nil
				}
				require.Equal(t, "SELECT mo_ctl('cn', 'GetProtocolVersion', '')", query)
				protocolQueries++
				if name == "protocol query failure" {
					return executor.Result{}, injected
				}
				if name == "empty inventory" {
					return executor.Result{}, nil
				}
				value := `{"result":"cn-new:122,tn-new:122"}`
				if name == "old consumer" {
					value = `{"result":"cn-new:122,cn-old:121,tn-new:122"}`
				}
				r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
				r.NewBatchWithRowCount(1)
				require.NoError(t, executor.AppendStringRows(r, 0, []string{value}))
				return r.GetResult(), nil
			})
			tasks := &scheduledTasks{}
			if name == "task store failure" {
				tasks.cronErr = injected
			}
			err := NewService(sql).RegisterCron(t.Context(), tasks)
			if name == "ready" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if name == "ready" || name == "task store failure" {
				require.Equal(t, []task.TaskMetadata{CronMetadata()}, tasks.crons)
				require.Equal(t, []string{CronExpression}, tasks.exprs)
			} else {
				require.Empty(t, tasks.crons, "no new task code may be published before admission")
			}
			if strings.Contains(name, "catalog") {
				require.Zero(t, protocolQueries)
			} else {
				require.Equal(t, 1, protocolQueries)
			}
		})
	}
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
			loads, calls, fences := 0, 0, 0
			sql := executor.NewMemExecutor(func(query string) (executor.Result, error) {
				if strings.HasPrefix(query, "select revision from mo_catalog.mo_mv_account_state ") {
					require.Empty(t, writes)
					require.Contains(t, query, "where account_id=7")
					fences++
					return executor.Result{}, nil
				}
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
			if name == "definer failure" || name == "already published failure" {
				require.Equal(t, 1, fences)
			} else {
				require.Zero(t, fences)
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
				if strings.HasPrefix(query, "select revision from mo_catalog.mo_mv_account_state ") {
					require.Empty(t, writes)
					require.Contains(t, query, "where account_id=7")
					return executor.Result{}, nil
				}
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
	for _, phase := range []string{"overlapping pages", "key scan failure", "count page failure", "age page failure", "delete failure", "fence failure", "cursor failure", "cancelled page"} {
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
				case strings.HasPrefix(query, "select revision from mo_catalog.mo_mv_account_state "):
					require.Contains(t, query, "where account_id=7")
					if phase == "fence failure" {
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

func TestMaterializedViewHistoryBacklogConverges(t *testing.T) {
	for _, tc := range []struct {
		name    string
		pages   [2]int
		batches bool
		cancel  bool
		more    bool
	}{
		{"both short", [2]int{127, 127}, false, false, false},
		{"count full age short", [2]int{128, 1}, false, false, true},
		{"age full count short", [2]int{1, 128}, false, false, true},
		{"full overlapping pages", [2]int{128, 128}, false, false, true},
		{"full across batches", [2]int{128, 0}, true, false, true},
		{"cancel after history page", [2]int{128, 0}, false, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			invocation, pages := 0, 0
			deleted := map[string]bool{}
			var cursors []string
			sql := executor.NewMemExecutor(func(q string) (executor.Result, error) {
				if err := ctx.Err(); err != nil {
					return executor.Result{}, err
				}
				switch {
				case strings.HasPrefix(q, "select account_id,mv_id,run_id,epoch"):
					return executor.Result{}, nil
				case strings.HasPrefix(q, "select account_id,mv_id,epoch,"):
					r := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType(), types.T_uint64.ToType(), types.T_int64.ToType()}, mp)
					r.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(r, 0, []uint32{7}))
					require.NoError(t, executor.AppendFixedRows(r, 1, []uint64{999}))
					require.NoError(t, executor.AppendFixedRows(r, 2, []uint64{2}))
					require.NoError(t, executor.AppendFixedRows(r, 3, []int64{1}))
					return r.GetResult(), nil
				case strings.HasPrefix(q, "select account_id,mv_id from"):
					require.Contains(t, q, "order by next_cleanup,account_id,mv_id limit 16")
					r := executor.NewMemResult([]types.Type{types.T_uint32.ToType(), types.T_uint64.ToType()}, mp)
					r.NewBatchWithRowCount(1)
					require.NoError(t, executor.AppendFixedRows(r, 0, []uint32{7}))
					require.NoError(t, executor.AppendFixedRows(r, 1, []uint64{99}))
					return r.GetResult(), nil
				case strings.HasPrefix(q, "select run_id"):
					require.Contains(t, q, "status<>'RUNNING'")
					require.Contains(t, q, "limit 128")
					page := 0
					if !strings.Contains(q, "offset 100") {
						page = 1
					}
					pages++
					n := tc.pages[page]
					if invocation > 0 {
						n = 0
					} // Independent drained-store response.
					r := executor.NewMemResult([]types.Type{types.T_varchar.ToType()}, mp)
					for start := 0; start < n; {
						end := n
						if tc.batches && end-start > 64 {
							end = start + 64
						}
						values := make([]string, end-start)
						for i := range values {
							values[i] = fmt.Sprintf("terminal-%03d", start+i)
						}
						r.NewBatchWithRowCount(len(values))
						require.NoError(t, executor.AppendStringRows(r, 0, values))
						start = end
					}
					if tc.cancel {
						cancel()
					}
					return r.GetResult(), nil
				case strings.HasPrefix(q, "delete from"):
					require.Contains(t, q, "account_id=7 and mv_id=99")
					require.Contains(t, q, "and status<>'RUNNING'")
					require.False(t, deleted[q], "overlapping pages must delete each terminal ID once")
					deleted[q] = true
				case strings.HasPrefix(q, "select revision from mo_catalog.mo_mv_account_state "):
					require.Contains(t, q, "where account_id=7")
				case strings.HasPrefix(q, "update "):
					cursors = append(cursors, q)
				default:
					t.Fatalf("unexpected maintenance SQL: %s", q)
				}
				return executor.Result{}, nil
			})
			svc := NewService(sql)
			tasks := &scheduledTasks{}
			err := svc.reconcile(ctx, tasks)
			require.Len(t, tasks.batches, 1, "maintenance backlog must not suppress task publication")
			require.Len(t, tasks.batches[0], 1)
			var spec TaskSpec
			require.NoError(t, json.Unmarshal(tasks.batches[0][0].Context, &spec))
			require.Equal(t, Key{7, 999}, spec.Key)
			require.Equal(t, uint64(2), spec.Epoch)
			if tc.cancel {
				require.ErrorIs(t, err, context.Canceled)
				require.Empty(t, cursors)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 2, pages)
			require.Len(t, deleted, max(tc.pages[0], tc.pages[1]))
			require.Len(t, cursors, 1)
			if tc.more {
				require.Contains(t, cursors[0], "next_cleanup=current_timestamp() ")
			} else {
				require.Contains(t, cursors[0], "next_cleanup=date_add(current_timestamp(),interval 1 hour)")
			}
			invocation++
			require.NoError(t, svc.reconcile(ctx, tasks))
			require.Len(t, tasks.batches, 2)
			require.Len(t, cursors, 2)
			require.Contains(t, cursors[1], "next_cleanup=date_add(current_timestamp(),interval 1 hour)")
		})
	}
}
