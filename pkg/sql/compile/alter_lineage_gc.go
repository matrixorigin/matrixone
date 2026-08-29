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
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

const (
	// Bound the complete transaction because it owns the foreground lifecycle
	// gate from its first statement through commit or rollback. This is also the
	// maximum time a restore arriving just after GC admission can wait on GC.
	dataBranchLineageGCCriticalSectionTimeout = 5 * time.Second
	dataBranchLineageGCLockWaitTimeout        = time.Second
)

var errDataBranchLineageGCCriticalSectionTimeout = moerr.NewInternalErrorNoCtx(
	"data branch lineage GC critical section timeout",
)

func DataBranchLineageGCExecutor(
	sqlExecutor executor.SQLExecutor,
) taskservice.TaskExecutor {
	return dataBranchLineageGCExecutor(
		sqlExecutor, dataBranchLineageGCCriticalSectionTimeout,
	)
}

func dataBranchLineageGCExecutor(
	sqlExecutor executor.SQLExecutor,
	criticalSectionTimeout time.Duration,
) taskservice.TaskExecutor {
	return func(ctx context.Context, _ task.Task) error {
		ctx, cancel := context.WithTimeoutCause(
			ctx, criticalSectionTimeout,
			errDataBranchLineageGCCriticalSectionTimeout,
		)
		defer cancel()
		err := compactExpiredAlterDataBranchLineageWithExecutor(
			ctx, sqlExecutor, time.Now().UTC(),
		)
		if isDataBranchLineageGCContention(err) ||
			isDataBranchLineageGCCriticalSectionTimeout(ctx, err) {
			// Background maintenance yields to foreground owner publication and
			// restore. ExecTxn has already rolled back the complete GC attempt;
			// the next scheduled run will recompute it from catalog state.
			return nil
		}
		return moerr.AttachCause(ctx, err)
	}
}

func isDataBranchLineageGCContention(err error) bool {
	return moerr.IsMoErrCode(err, moerr.ErrLockConflict) ||
		moerr.IsMoErrCode(err, moerr.ErrLockWaitTimeout)
}

func isDataBranchLineageGCCriticalSectionTimeout(ctx context.Context, err error) bool {
	if !errors.Is(context.Cause(ctx), errDataBranchLineageGCCriticalSectionTimeout) {
		return false
	}
	// Do not hide a substantive execution failure merely because it raced with
	// the maintenance budget. Only cancellation-shaped results caused by this
	// executor's own deadline are a safe, retryable deferral.
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, errDataBranchLineageGCCriticalSectionTimeout) ||
		moerr.IsMoErrCode(err, moerr.ErrQueryInterrupted)
}

func compactExpiredAlterDataBranchLineageWithExecutor(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	now time.Time,
) error {
	return sqlExecutor.ExecTxn(ctx, func(txn executor.TxnExecutor) error {
		statementOpts := executor.StatementOption{}.WithAccountID(catalog.System_Account)
		gateOpts := statementOpts.WithWaitPolicy(lock.WaitPolicy_FastFail)
		gate, err := txn.Exec(databranchutils.LineageOwnerLifecycleLockSQL(), gateOpts)
		if err != nil {
			return err
		}
		gate.Close()
		query := func(sql string) (executor.Result, error) {
			return txn.Exec(sql, statementOpts)
		}

		dag, err := loadAlterDataBranchDAGWithQuery(query, true)
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
		return nil
	}, executor.Options{}.
		WithAccountID(catalog.System_Account).
		WithLockWaitTimeout(dataBranchLineageGCLockWaitTimeout))
}
