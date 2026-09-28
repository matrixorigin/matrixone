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
// The caller retains batch ownership and must finish the owning transaction.
func LockRowsForAdmissionWithContext(
	ctx context.Context, eng engine.Engine, proc *process.Process,
	tableID uint64, bat *batch.Batch, idx int32, pkType types.Type,
	mode lock.LockMode, group uint32,
) (timestamp.Timestamp, error) {
	if bat == nil || idx < 0 || int(idx) >= len(bat.Vecs) || bat.Vecs[idx] == nil ||
		bat.Vecs[idx].Length() == 0 || bat.Vecs[idx].HasNull() ||
		bat.Vecs[idx].GetType().Oid != pkType.Oid {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "invalid lifecycle admission keys")
	}
	if proc == nil || proc.GetTxnOperator() == nil || !proc.GetTxnOperator().Txn().IsPessimistic() ||
		!proc.GetTxnOperator().Txn().IsRCIsolation() ||
		tableID == 0 || (mode != lock.LockMode_Shared && mode != lock.LockMode_Exclusive) ||
		getFetchRowsFunc(pkType) == nil {
		return timestamp.Timestamp{}, moerr.NewInternalError(ctx, "invalid lifecycle lock admission")
	}
	if err := ctx.Err(); err != nil {
		return timestamp.Timestamp{}, err
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
	opts.admissionOnly = true
	// Also prevent the fetcher from promoting a large Exclusive key batch.
	opts.maxCountPerLock = bat.Vecs[idx].Length()
	var deadline int64
	if value, ok := ctx.Deadline(); ok {
		deadline = value.UnixNano()
	}
	_, _, grantedAt, err := doLock(ctx, eng, analyzer, nil, tableID, proc, bat, idx, pkType, -1, opts, deadline)
	return grantedAt, err
}
