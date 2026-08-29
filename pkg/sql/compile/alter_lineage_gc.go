// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	// Discovery does not own the lifecycle gate. Each successful transaction
	// validates its discovery by writing the shared gate, then commits at most
	// one bounded batch. Repeated transactions make durable progress without
	// retaining the foreground gate through a full-catalog scan.
	dataBranchLineageGCBatchSize       = 128
	dataBranchLineageGCLockWaitTimeout = time.Second
	dataBranchLineageGCTimeBudget      = time.Minute
)

func DataBranchLineageGCExecutor(
	sqlExecutor executor.SQLExecutor,
) taskservice.TaskExecutor {
	return dataBranchLineageGCExecutor(
		sqlExecutor,
		dataBranchLineageGCBatchSize,
	)
}

func dataBranchLineageGCExecutor(
	sqlExecutor executor.SQLExecutor,
	batchSize int,
) taskservice.TaskExecutor {
	return dataBranchLineageGCExecutorWithBudget(
		sqlExecutor, batchSize, dataBranchLineageGCTimeBudget,
	)
}

func dataBranchLineageGCExecutorWithBudget(
	sqlExecutor executor.SQLExecutor,
	batchSize int,
	timeBudget time.Duration,
) taskservice.TaskExecutor {
	return func(ctx context.Context, _ task.Task) error {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		// One invocation performs one fixed-SI discovery and at most one bounded
		// mutation batch. This removes the former 16x full-catalog rescan while
		// retaining durable progress across scheduled invocations. The local
		// budget bounds discovery CPU/I/O and task-worker occupancy; its expiry
		// rolls back and defers, while a parent cancellation remains visible.
		gcCtx, cancel := context.WithTimeout(ctx, timeBudget)
		defer cancel()
		_, err := compactExpiredAlterDataBranchLineageBatchWithExecutor(
			gcCtx, sqlExecutor, time.Now().UTC(), batchSize,
		)
		if err != nil {
			// Parent control always wins over local contention classification.
			// In particular, a lock error racing cancellation must not turn a
			// canceled task into a successful maintenance attempt.
			if cause := context.Cause(ctx); cause != nil {
				return cause
			}
			if errors.Is(context.Cause(gcCtx), context.DeadlineExceeded) || isDataBranchLineageGCContention(err) {
				// ExecTxn rolled back this batch. Local budget exhaustion or a
				// foreground owner writer defers work to the next invocation.
				return nil
			}
			return moerr.AttachCause(gcCtx, err)
		}
		return nil
	}
}

func isDataBranchLineageGCContention(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrLockConflict) ||
		moerr.IsMoErrCode(err, moerr.ErrLockWaitTimeout) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry) ||
		moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged)
}

func compactExpiredAlterDataBranchLineageWithExecutor(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	now time.Time,
) error {
	_, err := compactExpiredAlterDataBranchLineageBatchWithExecutor(
		ctx, sqlExecutor, now, dataBranchLineageGCBatchSize,
	)
	return err
}

func compactExpiredAlterDataBranchLineageBatchWithExecutor(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	now time.Time,
	batchSize int,
) (bool, error) {
	compacted := false
	err := sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		statementOpts := executor.StatementOption{}.WithAccountID(catalog.System_Account)
		query := func(sql string) (executor.Result, error) {
			return txn.Exec(sql, statementOpts)
		}

		// Read a transactionally consistent candidate plan without owning the
		// foreground lifecycle gate. The gate write below is the validation
		// point: explicit snapshot isolation keeps this discovery snapshot fixed,
		// and every owner writer writes the same row before catalog mutation. A
		// writer that crossed after discovery therefore makes this transaction
		// conflict at the gate or commit instead of applying a stale plan.
		dag, err := loadAlterDataBranchDAGWithQuery(query, false)
		if err != nil || len(dag.Info) == 0 {
			return err
		}
		edges, err := loadAlterDataBranchLineageEdgesWithQuery(query)
		if err != nil {
			return err
		}
		sources, err := loadAlterDataBranchHistoricalSourcesWithQuery(query, now)
		if err != nil {
			return err
		}
		plan := databranchutils.ComputeAlterLineageCompactionPlan(dag, edges, sources)
		if len(plan.TableIDs) == 0 {
			return nil
		}
		if batchSize <= 0 {
			return moerr.NewInternalErrorNoCtx("invalid data branch lineage GC batch size")
		}
		if len(plan.TableIDs) > batchSize {
			plan.TableIDs = plan.TableIDs[:batchSize]
		}
		plan.SnapshotNames = make([]string, len(plan.TableIDs))
		for i, tableID := range plan.TableIDs {
			plan.SnapshotNames[i] = databranchutils.BranchSnapshotName(tableID)
		}

		gateOpts := statementOpts.WithWaitPolicy(lock.WaitPolicy_FastFail)
		gate, err := txn.Exec(databranchutils.LineageOwnerLifecycleLockSQL(), gateOpts)
		if err != nil {
			return err
		}
		gate.Close()
		for _, sql := range []string{
			databranchutils.BuildAlterLineageSnapshotDeleteSQL(plan.SnapshotNames),
			databranchutils.BuildAlterLineageMetadataDeleteSQL(plan.TableIDs),
		} {
			res, execErr := txn.Exec(sql, statementOpts)
			res.Close()
			if execErr != nil {
				return execErr
			}
		}
		compacted = true
		return nil
	}, executor.Options{}.
		WithAccountID(catalog.System_Account).
		WithTxnIsolation(txn.TxnIsolation_SI).
		WithLockWaitTimeout(dataBranchLineageGCLockWaitTimeout))
	return compacted, err
}
