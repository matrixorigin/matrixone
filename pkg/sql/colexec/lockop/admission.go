// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockop

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/util/trace/impl/motrace/statistic"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

type exactMutationRowsKey struct{}

// WithExactMutationRows keeps every lock target of one internal catalog DML
// statement at row granularity, including its hidden index tables. The caller
// must bound the affected row count and retain its admission locks until the
// transaction ends.
func WithExactMutationRows(ctx context.Context) context.Context {
	return context.WithValue(ctx, exactMutationRowsKey{}, true)
}

func exactMutationRows(ctx context.Context) bool {
	return ctx.Value(exactMutationRowsKey{}) == true
}

// LockRowsForAdmissionWithContext locks exactly the supplied non-null catalog
// keys in an existing pessimistic RC transaction. It records the lock binding
// but does not validate a data plan or advance the snapshot. The caller must
// install an applied frontier and revalidate catalog identity before effects.
// The returned timestamp can include lock-table creation time and is not a TN
// read barrier. Capacity exhaustion fails instead of widening to a table lock.
// The optional wait policy applies only to this request, allowing a retained
// shared owner to attempt a nonwaiting exclusive promotion without changing
// the process's policy for other locks. The caller retains batch ownership
// and must finish the owning transaction.
func LockRowsForAdmissionWithContext(
	ctx context.Context, eng engine.Engine, proc *process.Process,
	tableID uint64, bat *batch.Batch, idx int32, pkType types.Type,
	mode lock.LockMode, group uint32,
	waitPolicy ...lock.WaitPolicy,
) (timestamp.Timestamp, error) {
	result, err := LockRowsForAdmissionResult(ctx, eng, proc, tableID, bat, idx, pkType, mode, group, waitPolicy...)
	return result.Timestamp, err
}

// LockRowsForAdmissionResult exposes the checked owner and watermark from the
// same admission grant. The watermark invalidates facts; it is not visibility.
func LockRowsForAdmissionResult(
	ctx context.Context, eng engine.Engine, proc *process.Process,
	tableID uint64, bat *batch.Batch, idx int32, pkType types.Type,
	mode lock.LockMode, group uint32,
	waitPolicy ...lock.WaitPolicy,
) (lock.Result, error) {
	return lockRowsForAdmissionResult(ctx, eng, proc, tableID, bat, idx, pkType, mode, group, true, waitPolicy...)
}

// LockRowsForFixedSnapshotAdmissionResult reserves namespace keys in a
// pessimistic SI transaction without advancing its snapshot. Unlike ordinary
// row locking, opaque namespace keys have no MVCC row to probe. A table-wide
// grant watermark newer than the fixed snapshot therefore conservatively
// invalidates admission, even if acquiring this particular key did not wait.
// The watermark may include cold lock-table creation or unrelated commits;
// callers must retry those conflicts in a new transaction, not refresh SI.
// On success the caller must still validate names at its original snapshot.
// Ownership, binding validation, cancellation and terminal release are the
// same as the RC admission API; its RC-only contract remains unchanged.
func LockRowsForFixedSnapshotAdmissionResult(
	ctx context.Context, eng engine.Engine, proc *process.Process,
	tableID uint64, bat *batch.Batch, idx int32, pkType types.Type,
	mode lock.LockMode, group uint32,
	waitPolicy ...lock.WaitPolicy,
) (lock.Result, error) {
	if proc == nil || proc.GetTxnOperator() == nil || proc.GetTxnOperator().Txn().Isolation != txn.TxnIsolation_SI {
		return lock.Result{}, moerr.NewInternalError(ctx, "fixed snapshot admission requires SI")
	}
	result, err := lockRowsForAdmissionResult(ctx, eng, proc, tableID, bat, idx, pkType, mode, group, false, waitPolicy...)
	if err != nil {
		return lock.Result{}, err
	}
	if result.Timestamp.IsEmpty() {
		return lock.Result{}, moerr.NewLockTableBindChangedNoCtx()
	}
	if proc.GetTxnOperator().Txn().SnapshotTS.Less(result.Timestamp) {
		return lock.Result{}, moerr.NewTxnWWConflict(ctx, tableID, "catalog namespace snapshot is stale")
	}
	return result, nil
}

func lockRowsForAdmissionResult(
	ctx context.Context, eng engine.Engine, proc *process.Process,
	tableID uint64, bat *batch.Batch, idx int32, pkType types.Type,
	mode lock.LockMode, group uint32, requireRC bool,
	waitPolicy ...lock.WaitPolicy,
) (lock.Result, error) {
	if len(waitPolicy) > 1 {
		return lock.Result{}, moerr.NewInternalError(ctx, "invalid lifecycle lock wait policy")
	}
	if bat == nil || idx < 0 || int(idx) >= len(bat.Vecs) || bat.Vecs[idx] == nil ||
		bat.Vecs[idx].Length() == 0 || bat.Vecs[idx].HasNull() ||
		bat.Vecs[idx].GetType().Oid != pkType.Oid {
		return lock.Result{}, moerr.NewInternalError(ctx, "invalid lifecycle admission keys")
	}
	if proc == nil || proc.GetTxnOperator() == nil || !proc.GetTxnOperator().Txn().IsPessimistic() ||
		(requireRC && !proc.GetTxnOperator().Txn().IsRCIsolation()) ||
		tableID == 0 || (mode != lock.LockMode_Shared && mode != lock.LockMode_Exclusive) ||
		getFetchRowsFunc(pkType) == nil {
		return lock.Result{}, moerr.NewInternalError(ctx, "invalid lifecycle lock admission")
	}
	if err := ctx.Err(); err != nil {
		return lock.Result{}, err
	}
	packer := types.NewPacker()
	defer packer.Close()
	analyzer := process.NewTempAnalyzer()
	defer func() {
		statistic.StatsInfoFromContext(ctx).AddPreRunOnceWaitLockDuration(
			analyzer.GetOpStats().GetMetricByKey(process.OpWaitLockTime))
	}()
	opts := DefaultLockOptions(packer).WithLockMode(mode).WithLockGroup(group).
		WithLockTable(false, false)
	var result lock.Result
	opts.admissionOnly = true
	opts.admissionResult = &result
	if len(waitPolicy) == 1 {
		opts.waitPolicy = &waitPolicy[0]
	}
	// Also prevent the fetcher from promoting a large Exclusive key batch.
	opts.maxCountPerLock = bat.Vecs[idx].Length()
	var deadline int64
	if value, ok := ctx.Deadline(); ok {
		deadline = value.UnixNano()
	}
	_, _, _, err := doLock(ctx, eng, analyzer, nil, tableID, proc, bat, idx, pkType, -1, opts, deadline)
	return result, err
}
