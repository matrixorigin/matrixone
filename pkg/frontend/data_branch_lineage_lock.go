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

package frontend

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/frontend/databranchutils"
	lockpb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
)

func lockDataBranchLineageOwnerLifecycle(ctx context.Context, bh BackgroundExec) error {
	return databranchutils.LockLineageOwnerLifecycle(func(sql string) error {
		return execDataBranchLineageOwnerLifecycleSQL(ctx, bh, sql)
	})
}

func execDataBranchLineageOwnerLifecycleSQL(
	ctx context.Context,
	bh BackgroundExec,
	sql string,
) error {
	lockCtx := defines.AttachAccountId(ctx, catalog.System_Account)
	bh.ClearExecResultSet()
	err := bh.Exec(lockCtx, sql)
	bh.ClearExecResultSet()
	return err
}

func lockDataBranchLineageOwnerLifecycleForFeatureAdmission(
	ctx context.Context,
	bh BackgroundExec,
) error {
	txnOp := backgroundExecTxnOperator(bh)
	gateSQL := databranchutils.LineageOwnerLifecycleLockSQLForTxn(txnOp)
	if err := execDataBranchLineageOwnerLifecycleSQL(ctx, bh, gateSQL); err != nil {
		return err
	}
	if gateSQL == databranchutils.LineageOwnerLifecyclePessimisticLockSQL() {
		if backExec, ok := bh.(*backExec); ok && backExec != nil && backExec.backSes != nil {
			backExec.backSes.lineageOwnerLifecycleWritePending = true
		}
	}
	return nil
}

func writePendingDataBranchLineageOwnerLifecycle(
	ctx context.Context,
	bh BackgroundExec,
) error {
	backExec, ok := bh.(*backExec)
	if !ok || backExec == nil || backExec.backSes == nil ||
		!backExec.backSes.lineageOwnerLifecycleWritePending {
		return nil
	}
	backExec.backSes.lineageOwnerLifecycleWritePending = false
	return execDataBranchLineageOwnerLifecycleSQL(
		ctx, bh, databranchutils.LineageOwnerLifecycleLockSQL(),
	)
}

func backgroundExecTxnOperator(bh BackgroundExec) client.TxnOperator {
	backExec, ok := bh.(*backExec)
	if !ok || backExec == nil || backExec.backSes == nil || backExec.backSes.GetTxnHandler() == nil {
		return nil
	}
	return backExec.backSes.GetTxnHandler().GetTxn()
}

func validateDataBranchLineageOwnerLifecycleAtCommit(
	ctx context.Context,
	ses FeSession,
	txnOp client.TxnOperator,
) error {
	rt := moruntime.ServiceRuntime(ses.GetService())
	if rt == nil {
		return moerr.NewInternalErrorNoCtx("missing runtime for lifecycle commit validation")
	}
	value, ok := rt.GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		return moerr.NewInternalErrorNoCtx("missing executor for lifecycle commit validation")
	}
	sqlExecutor, ok := value.(executor.SQLExecutor)
	if !ok {
		return moerr.NewInternalErrorNoCtx("invalid executor for lifecycle commit validation")
	}
	return validateDataBranchLineageOwnerLifecycleWithExecutor(
		ctx, sqlExecutor, txnOp, ses.GetTimeZone(),
	)
}

func validateDataBranchLineageOwnerLifecycleWithExecutor(
	ctx context.Context,
	sqlExecutor executor.SQLExecutor,
	txnOp client.TxnOperator,
	timeZone *time.Location,
) error {
	// Commit validation intentionally retains the write barrier for pessimistic
	// transactions. The write dependency detects an owner writer that completed
	// after this transaction mutated branch catalogs; a row-locking read would
	// only serialize the statement and would let the stale transaction commit.
	opts := executor.Options{}.
		WithDisableIncrStatement().
		WithTxn(txnOp).
		WithKeepTxnAlive().
		WithTimeZone(timeZone).
		WithAccountID(catalog.System_Account).
		WithStatementOption(executor.StatementOption{}.
			WithWaitPolicy(lockpb.WaitPolicy_FastFail).
			WithAccountID(catalog.System_Account))
	result, err := sqlExecutor.Exec(
		ctx,
		databranchutils.LineageOwnerLifecycleLockSQL(),
		opts,
	)
	if err != nil {
		return err
	}
	result.Close()
	return nil
}

// admitFeatureLimitedLineageOwnerMutation installs the TN-ordered catalog
// frontier before crossing the lifecycle write barrier. An explicit-SI data
// branch transaction keeps its fixed snapshot; its quota check uses a separate
// RC transaction for freshness. Advancing an RC snapshot after the gate write
// can expose both workspace versions of the feature-registry row to later
// quota reads.
func admitFeatureLimitedLineageOwnerMutation(
	ctx context.Context,
	ses *Session,
	bh BackgroundExec,
) error {
	if !featureLimitTxnUsesFixedSnapshot(bh) {
		if err := advanceFeatureLimitSnapshot(ctx, ses, bh); err != nil {
			return err
		}
	}
	return lockDataBranchLineageOwnerLifecycleForFeatureAdmission(ctx, bh)
}
