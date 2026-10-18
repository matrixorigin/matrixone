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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/bootstrap/versions"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
)

const CronExpression = "*/30 * * * * *"

// TaskSpec is an internal, versioned task envelope, never a user SQL body.
type TaskSpec struct {
	Version   int
	Reconcile bool
	Key       Key
	Epoch     uint64
	RunID     string
}

func CronMetadata() task.TaskMetadata {
	data, _ := json.Marshal(TaskSpec{Version: 1, Reconcile: true})
	return task.TaskMetadata{ID: "materialized-view-reconcile-v1", Executor: task.TaskCode_MaterializedView, Context: data, Options: task.TaskOptions{Concurrency: 1}}
}

// RegisterCron publishes the new task code only after its catalogs and every
// inventoried consumer are ready. Registering an executor on this CN alone
// cannot prevent an old CN from claiming a globally scheduled reconcile task.
// The CN's existing registration loop owns the deadline, retry and shutdown.
func (s *Service) RegisterCron(ctx context.Context, ts taskservice.TaskService) error {
	if err := s.SQL.ExecTxn(ctx, func(tx executor.TxnExecutor) error {
		ready, err := CatalogReady(tx)
		if err != nil {
			return err
		}
		if !ready {
			return moerr.NewInvalidStateNoCtx("materialized view catalogs are not installed")
		}
		return versions.CheckCommonProtocolVersion(tx, defines.MORPCVersion120)
	}, executor.Options{}.WithAccountID(catalog.System_Account)); err != nil {
		return err
	}
	return ts.CreateCronTask(ctx, CronMetadata(), CronExpression)
}

func runMetadata(key Key, epoch uint64, due int64) task.TaskMetadata {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%d:%d:%d:%d", key.Account, key.ID, epoch, due)))
	id := hex.EncodeToString(digest[:])
	data, _ := json.Marshal(TaskSpec{Version: 1, Key: key, Epoch: epoch, RunID: id})
	// sys_async_task.task_metadata_id is VARCHAR(50). Keep 160 digest bits in
	// the queue identity while the persisted refresh run uses the full digest.
	return task.TaskMetadata{ID: "mv:" + id[:40], Executor: task.TaskCode_MaterializedView, Context: data, Options: task.TaskOptions{MaxRetryTimes: 2, RetryInterval: int64(5 * time.Second), Concurrency: 1}}
}

func (s *Service) TaskExecutor(ts taskservice.TaskService, factory func() ie.InternalExecutor) taskservice.TaskExecutor {
	return func(ctx context.Context, value task.Task) error {
		ctx, cancel := context.WithTimeout(ctx, Timeout)
		defer cancel()
		async, ok := value.(*task.AsyncTask)
		if !ok {
			return moerr.NewInvalidInputNoCtx("invalid materialized view task")
		}
		var spec TaskSpec
		if err := json.Unmarshal(async.Metadata.Context, &spec); err != nil {
			return err
		}
		if spec.Version != 1 {
			return moerr.NewNotSupportedNoCtx("materialized view task version")
		}
		if spec.Reconcile {
			return s.reconcile(ctx, ts)
		}
		st, err := s.State(ctx, spec.Key)
		if err != nil {
			return err
		}
		if st.Epoch != spec.Epoch || st.Interval == 0 {
			return nil
		}
		ctx = defines.AttachAccount(ctx, st.Account, st.Definition.OwnerUser, st.Definition.OwnerRole)
		ctx = defines.WithMaterializedViewTask(ctx, defines.MaterializedViewTask{Account: st.Account, Table: st.ID, Epoch: spec.Epoch, RunID: spec.RunID})
		// The frontend rechecks owner existence, active role and source ACL. It never
		// uses the privileged catalog executor for source permission admission.
		opts := ie.NewOptsBuilder().AccountId(st.Account).UserId(st.Definition.OwnerUser).DefaultRoleId(st.Definition.OwnerRole).Username(st.Definition.OwnerName).Database(st.Database).Internal(false).Finish()
		err = factory().Exec(ctx, "refresh materialized view "+Quote(st.Database)+"."+Quote(st.Name), opts)
		if err != nil {
			s.recordTaskFailure(ctx, spec)
		}
		return err
	}
}

// Admission failures (including revoked definer permissions) occur before
// Refresh can create a run. Record a sanitized terminal attempt and move the
// due time together so completed task deduplication cannot strand the policy.
func (s *Service) recordTaskFailure(ctx context.Context, spec TaskSpec) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = s.metadataTxn(ctx, spec.Key, func(tx executor.TxnExecutor) error {
		st, err := Load(tx, spec.Key)
		if err != nil {
			return err
		}
		if st.Epoch != spec.Epoch || st.LastRun == spec.RunID {
			return nil
		}
		prior, err := loadRun(tx, spec.Key, spec.RunID)
		if err != nil {
			return err
		}
		if prior == nil {
			sequence := st.Requested + 1
			if err := consume(tx, fmt.Sprintf("update mo_catalog.mo_materialized_views set requested_sequence=%d where account_id=%d and mv_id=%d", sequence, st.Account, st.ID)); err != nil {
				return err
			}
			if err := consume(tx, fmt.Sprintf("insert into mo_catalog.mo_mv_refresh_runs(account_id,mv_id,run_id,sequence,epoch,status,started_at,finished_at,error_message) values(%d,%d,%s,%d,%d,'FAILED',current_timestamp(),current_timestamp(),'refresh admission failed')", st.Account, st.ID, literal(spec.RunID), sequence, st.Epoch)); err != nil {
				return err
			}
		}
		return consume(tx, fmt.Sprintf("update mo_catalog.mo_materialized_views set next_due=date_add(current_timestamp(),interval 60 second) where account_id=%d and mv_id=%d and interval_seconds>0", st.Account, st.ID))
	}, executor.Options{}.WithAccountID(spec.Key.Account).WithWaitCommittedLogApplied())
}
func (s *Service) reconcile(ctx context.Context, ts taskservice.TaskService) error {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()
	opts := executor.Options{}.WithAccountID(catalog.System_Account)
	// Recover a bounded, indexed page of interrupted attempts.
	r, err := s.SQL.Exec(ctx, "select account_id,mv_id,run_id,epoch from mo_catalog.mo_mv_refresh_runs where status='RUNNING' and started_at<date_sub(current_timestamp(),interval 360 second) order by started_at limit 128", opts)
	if err != nil {
		return err
	}
	var expired []TaskSpec
	r.ReadRows(func(n int, c []*vector.Vector) bool {
		for i := 0; i < n; i++ {
			expired = append(expired, TaskSpec{Key: Key{vector.GetFixedAtWithTypeCheck[uint32](c[0], i), vector.GetFixedAtWithTypeCheck[uint64](c[1], i)}, RunID: c[2].GetStringAt(i), Epoch: vector.GetFixedAtWithTypeCheck[uint64](c[3], i)})
		}
		return true
	})
	r.Close()
	for _, spec := range expired {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := s.metadataTxn(ctx, spec.Key, func(tx executor.TxnExecutor) error {
			if err := consume(tx, fmt.Sprintf("update mo_catalog.mo_materialized_views set next_due=date_add(current_timestamp(),interval 60 second) where account_id=%d and mv_id=%d and epoch=%d and interval_seconds>0 and last_run<>%s and exists(select 1 from mo_catalog.mo_mv_refresh_runs where account_id=%d and mv_id=%d and run_id=%s and status='RUNNING' and started_at<date_sub(current_timestamp(),interval 360 second))", spec.Key.Account, spec.Key.ID, spec.Epoch, literal(spec.RunID), spec.Key.Account, spec.Key.ID, literal(spec.RunID))); err != nil {
				return err
			}
			return consume(tx, fmt.Sprintf("update mo_catalog.mo_mv_refresh_runs set status='ABORTED',finished_at=current_timestamp(),error_message='refresh interrupted' where account_id=%d and mv_id=%d and run_id=%s and status='RUNNING' and started_at<date_sub(current_timestamp(),interval 360 second)", spec.Key.Account, spec.Key.ID, literal(spec.RunID)))
		}, opts)
		if err != nil {
			return err
		}
	}
	r, err = s.SQL.Exec(ctx, "select account_id,mv_id,epoch,cast(unix_timestamp(next_due) as bigint) from mo_catalog.mo_materialized_views v where interval_seconds>0 and next_due<=current_timestamp() and not exists (select 1 from mo_catalog.mo_mv_refresh_runs r where r.account_id=v.account_id and r.mv_id=v.mv_id and r.status='RUNNING') order by next_due,account_id,mv_id limit 128", opts)
	if err != nil {
		return err
	}
	var batch []task.TaskMetadata
	r.ReadRows(func(n int, c []*vector.Vector) bool {
		for i := 0; i < n; i++ {
			batch = append(batch, runMetadata(Key{vector.GetFixedAtWithTypeCheck[uint32](c[0], i), vector.GetFixedAtWithTypeCheck[uint64](c[1], i)}, vector.GetFixedAtWithTypeCheck[uint64](c[2], i), vector.GetFixedAtWithTypeCheck[int64](c[3], i)))
		}
		return true
	})
	r.Close()
	if len(batch) > 0 {
		if err := ts.CreateBatch(ctx, batch); err != nil {
			return err
		}
	}
	return s.cleanupHistory(ctx)
}

// Visit 16 indexed maintenance intents. Each visits at most 128 terminal run
// IDs per retention rule and advances its cursor, including manual-only MVs.
func (s *Service) cleanupHistory(ctx context.Context) error {
	opts := executor.Options{}.WithAccountID(catalog.System_Account)
	r, err := s.SQL.Exec(ctx, "select account_id,mv_id from mo_catalog.mo_materialized_views where next_cleanup<=current_timestamp() order by next_cleanup,account_id,mv_id limit 16", opts)
	if err != nil {
		return err
	}
	var keys []Key
	r.ReadRows(func(n int, c []*vector.Vector) bool {
		for i := 0; i < n; i++ {
			keys = append(keys, Key{vector.GetFixedAtWithTypeCheck[uint32](c[0], i), vector.GetFixedAtWithTypeCheck[uint64](c[1], i)})
		}
		return true
	})
	r.Close()
	for _, key := range keys {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Stable primary-key deletes are conditional on terminal status. The two
		// pages may overlap; DELETE is idempotent and never removes an active run.
		ids := map[string]struct{}{}
		more := false
		for _, condition := range []string{"order by sequence desc limit 128 offset 100", "and started_at<date_sub(current_timestamp(),interval 7 day) order by started_at limit 128"} {
			r, err := s.SQL.Exec(ctx, fmt.Sprintf("select run_id from mo_catalog.mo_mv_refresh_runs where account_id=%d and mv_id=%d and status<>'RUNNING' %s", key.Account, key.ID, condition), opts)
			if err != nil {
				return err
			}
			pageRows := 0
			r.ReadRows(func(n int, c []*vector.Vector) bool {
				pageRows += n
				for i := 0; i < n; i++ {
					ids[c[0].GetStringAt(i)] = struct{}{}
				}
				return true
			})
			r.Close()
			more = more || pageRows >= 128
		}
		for id := range ids {
			r, err := s.SQL.Exec(ctx, fmt.Sprintf("delete from mo_catalog.mo_mv_refresh_runs where account_id=%d and mv_id=%d and run_id=%s and status<>'RUNNING'", key.Account, key.ID, literal(id)), opts)
			if err != nil {
				return err
			}
			r.Close()
		}
		next := "date_add(current_timestamp(),interval 1 hour)"
		if more {
			next = "current_timestamp()"
		}
		// A stale maintenance intent must not race definition retirement. Share
		// DROP's lifecycle fence and transaction mode before writing the cursor.
		err := s.metadataTxn(ctx, key, func(tx executor.TxnExecutor) error {
			return consume(tx, fmt.Sprintf("update mo_catalog.mo_materialized_views set next_cleanup=%s where account_id=%d and mv_id=%d", next, key.Account, key.ID))
		}, opts)
		if err != nil {
			return err
		}
	}
	return nil
}
