// Copyright 2026 MatrixOne
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

package lockop

// rev-17 members (pre-PR review round 3, P2): the production lock/retry
// consumers of the marked SI locking current read. The members drive the
// REAL doLock/handleLockRes/performLock path through the real lock service
// under an explicit SI transaction, and pin the D-5 contract's observable
// behavior: the revalidation raises ONE ErrTxnNeedRetryLockingRead token
// whose embedded frontier strictly progresses past the stamped attempt
// frontier, the surfaced floor is the aggregate of every marked branch's
// floor, the retry is never re-raised as a second conflicting driver, and
// the SI transaction snapshot never moves (lockop only advances the session
// operator under RC).

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func startMarkedTimestampWaiter() (tw client.TimestampWaiter, stop func()) {
	tw = client.NewTimestampWaiter(runtime.GetLogger(""))
	sp := stopper.NewStopper("marked-ts-waiter")
	sp.RunTask(func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond * 100):
				tw.NotifyLatestCommitTS(timestamp.Timestamp{PhysicalTime: time.Now().UTC().UnixNano()})
			}
		}
	})
	return tw, func() { sp.Stop() }
}

// changedInRange replaces the version probe: the target predicate rows DID
// change inside the revalidated window, so the marked SI path must record
// the refresh outcome as this branch's retry floor.
func changedInRange(
	proc *process.Process,
	rel engine.Relation,
	analyzer process.Analyzer,
	tableID uint64,
	eng engine.Engine,
	bat *batch.Batch,
	idx int32,
	partitionIdx int32,
	from, to timestamp.Timestamp) (bool, error) {
	return true, nil
}

// buildMarkedArgument mirrors the non-blocking driver's construction for a
// marked statement: one lock target per table, the batch carries pk/ts pairs,
// and the operator is armed with THIS attempt's frontier and marks.
func buildMarkedArgument(
	proc *process.Process,
	tables []uint64,
	values [][]int32,
) *LockOp {
	bat := batch.NewWithSize(len(tables) * 2)
	bat.SetRowCount(len(tables) * 2)

	offset := int32(0)
	pkType := types.New(types.T_int32, 0, 0)
	tsType := types.New(types.T_TS, 0, 0)
	arg := NewArgumentByEngine(nil)
	arg.OperatorBase.OperatorInfo = vm.OperatorInfo{
		Idx:     0,
		IsFirst: false,
		IsLast:  false,
	}
	for idx, table := range tables {
		arg.AddLockTarget(table, nil, offset, pkType, -1, offset+1, nil, true)

		vec := vector.NewVec(pkType)
		vector.AppendFixedList(vec, values[idx], nil, proc.Mp())
		bat.Vecs[offset] = vec

		vec = vector.NewVec(tsType)
		bat.Vecs[offset+1] = vec
		offset += 2
	}
	resetChildren(arg, bat)
	return arg
}

// The production consumer chain (doLock -> handleLockRes's marked SI
// revalidation -> performLock's floor aggregation) raises exactly one
// ErrTxnNeedRetryLockingRead whose frontier STRICTLY progresses past the
// stamped attempt frontier, and the SI transaction snapshot is preserved
// through the whole revalidated sequence (design D-5: the snapshot is never
// advanced on any marked exit path).
func TestMarkedLockOpSurfacesProgressiveRetryTokenUnderSI(t *testing.T) {
	tw, stop := startMarkedTimestampWaiter()
	defer stop()

	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			arg := buildMarkedArgument(proc, []uint64{1}, [][]int32{{0, 1, 2}})
			defer arg.Free(proc, false, nil)
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = changedInRange

			arg.MarkLockingReadTarget(0, true)
			attemptFrontier := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
			arg.SetLockingReadCurrentFrontier(attemptFrontier, proc.GetTxnOperator().Txn().SnapshotTS)

			sessionTS := proc.GetTxnOperator().Txn().SnapshotTS
			require.Equal(t, txn.TxnIsolation_SI, proc.GetTxnOperator().Txn().Isolation,
				"the member runs the marked path under explicit SI")

			_, err := vm.Exec(arg, proc)
			require.Error(t, err, "the marked SI path surfaces its refresh outcome as a retry")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryLockingRead), "%v", err)

			phys, logical, ok := moerr.GetTxnNeedRetryLockingReadFrontier(err)
			require.True(t, ok, "the surfaced retry carries the frontier token")
			token := timestamp.Timestamp{PhysicalTime: phys, LogicalTime: logical}
			require.True(t, token.Greater(attemptFrontier),
				"the token frontier STRICTLY progresses past the stamped attempt frontier (D-5)")
			require.True(t, arg.ctr.lockingReadFloor.Equal(token),
				"the operator's diagnostic floor is exactly the surfaced token")

			require.Equal(t, sessionTS, proc.GetTxnOperator().Txn().SnapshotTS,
				"the revalidated lock pass, logtail wait and floor record never move the SI snapshot")
		},
		[]client.TxnOption{client.WithTxnIsolation(txn.TxnIsolation_SI)},
		client.WithTimestampWaiter(tw),
	)
}

// The D-5 single-surface aggregate: ONE performLock covering TWO marked
// targets raises at most one retry token, whose frontier carries the
// aggregate of the floors both marked branches recorded, and the transaction
// snapshot stays SI-immutable through both.
func TestMarkedLockOpSingleRetryTokenAcrossMarkedTargets(t *testing.T) {
	tw, stop := startMarkedTimestampWaiter()
	defer stop()

	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			arg := buildMarkedArgument(proc, []uint64{1, 2}, [][]int32{{0, 1, 2}, {0, 1, 2}})
			defer arg.Free(proc, false, nil)
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = changedInRange

			arg.MarkLockingReadTarget(0, true)
			arg.MarkLockingReadTarget(1, true)
			attemptFrontier := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
			arg.SetLockingReadCurrentFrontier(attemptFrontier, proc.GetTxnOperator().Txn().SnapshotTS)

			sessionTS := proc.GetTxnOperator().Txn().SnapshotTS

			_, err := vm.Exec(arg, proc)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryLockingRead), "%v", err)

			phys, logical, ok := moerr.GetTxnNeedRetryLockingReadFrontier(err)
			require.True(t, ok)
			token := timestamp.Timestamp{PhysicalTime: phys, LogicalTime: logical}
			require.True(t, token.Greater(attemptFrontier),
				"the aggregated token strictly progresses past the attempt frontier")
			require.True(t, arg.ctr.lockingReadFloor.Equal(token),
				"the operator floor is the aggregate floor both marked branches raised it to")

			require.Equal(t, sessionTS, proc.GetTxnOperator().Txn().SnapshotTS,
				"two marked targets in one lock pass still never move the SI snapshot")

			// A second pass of the same operator state surfaces the SAME
			// single-family driver again — never a second, conflicting token.
			_, err2 := vm.Exec(arg, proc)
			require.Error(t, err2)
			require.True(t, moerr.IsMoErrCode(err2, moerr.ErrTxnNeedRetryLockingRead), "%v", err2)
			phys2, logical2, ok2 := moerr.GetTxnNeedRetryLockingReadFrontier(err2)
			require.True(t, ok2)
			token2 := timestamp.Timestamp{PhysicalTime: phys2, LogicalTime: logical2}
			require.True(t, token2.GreaterEq(arg.ctr.lockingReadFloor),
				"a later pass never regresses the floor it inherits")
			require.Equal(t, sessionTS, proc.GetTxnOperator().Txn().SnapshotTS)
		},
		[]client.TxnOption{client.WithTxnIsolation(txn.TxnIsolation_SI)},
		client.WithTimestampWaiter(tw),
	)
}
