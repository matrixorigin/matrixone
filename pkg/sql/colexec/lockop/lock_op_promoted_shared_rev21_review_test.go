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

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Pre-PR review round 8, finding 2: a promoted marked Shared table target
// (applyLockTableFallback promoted it past the MaxLockRowCount budget) is
// staged into Compile.lockTables and acquired by the PRE-PIPELINE lockTable
// sweep before source admission, through options that never set the marking
// fields. Under SI a wait-resolved conflict — the writer committed while
// this reader waited on its lock: lock_table_local.go's waiter wake records
// HasConflict=true and HasPrevCommit=true, and the granted result's
// timestamp sits above this transaction's fixed snapshot — then reached
// doLock's unmarked SI terminal "SI not support retry" WW-conflict, so a
// promoted FOR SHARE / LOCK IN SHARE MODE statement failed exactly where its
// admitted current-read contract says the committed value must serve.
// LockTableWithModeMarked carries that contract through the acquisition.
// These members drive the real lock service with the observable waiter
// shapes and a retained definition fence, and pin BOTH sides of every
// branch: the marked table lock's outcome and the legacy call's outcome on
// the same shapes — the marked call must cost the legacy path nothing.

// shapeTableLockResult installs an adjust hook that rewrites the granted
// result of exactly the given table with the raw waiter shape under test.
// The shapes are the exact observable outcomes a promoted shared table-lock
// request can see on grant (raw fields, no fabrication of internal locks):
func shapeTableLockResult(
	proc *process.Process,
	tableID uint64,
	shape func(result *lock.Result),
	fired *int,
) func() {
	runtime.SetupServiceRuntimeTestingContext(proc.GetService())
	testingContext := runtime.MustGetTestingContext(proc.GetService())
	testingContext.SetBeforeLockFunc(func(_ []byte, _ uint64) {})
	testingContext.SetAdjustLockResultFunc(func(_ []byte, table uint64, result *lock.Result) {
		if table == tableID {
			if fired != nil {
				*fired = *fired + 1
			}
			shape(result)
		}
	})
	return func() {
		testingContext.SetAdjustLockResultFunc(nil)
		testingContext.SetBeforeLockFunc(nil)
	}
}

// The promoted shared acquire under SI, one member per observable waiter
// shape. The mid-wait committed writer is the reviewer's exact reproduction
// (marked: SUCCESS with the grant-relevant apply waited inside doLock and
// the fixed snapshot untouched; legacy: today's terminal
// "SI not support retry"). The defensive shapes (a holder that ABORTED, a
// reentrant lock observed at a newer table timestamp) succeed for BOTH
// calls. A retained definition fence stays terminal for the marked call —
// non-goals N3/G3 keep table/DDL fences byte-identical for every SI
// statement.
func TestPromotedSharedTableLockMarkedContractPerWaitShape(t *testing.T) {
	// shapes := [...]
	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			statementSnapshot := proc.GetTxnOperator().Txn().SnapshotTS
			require.False(t, statementSnapshot.IsEmpty())
			require.Equal(t, txn.TxnIsolation_SI, proc.GetTxnOperator().Txn().Isolation,
				"the shape members run the promoted table path under explicit SI")
			pkType := types.T_int32.ToType()
			higherCommit := statementSnapshot.Next()
			require.True(t, statementSnapshot.Less(higherCommit))

			// (1) The mid-wait committed writer (reviewer's shape), marked
			// call: SUCCESS, and the fixed SI snapshot never moves.
			const waitCommitTable = uint64(21)
			hookFired := 0
			restore := shapeTableLockResult(proc, waitCommitTable,
				func(result *lock.Result) {
					result.NewLockAdd = true
					result.HasConflict = true
					result.HasPrevCommit = true
					result.Timestamp = higherCommit
				}, &hookFired)
			require.NoError(t, LockTableWithModeMarked(nil, proc, waitCommitTable,
				pkType, lock.LockMode_Shared, false, statementSnapshot),
				"a marked promoted shared table lock survives the mid-wait committed writer")
			restore()
			require.Equal(t, 1, hookFired, "the shape was observed at the granted result")
			require.Equal(t, statementSnapshot, proc.GetTxnOperator().Txn().SnapshotTS,
				"the marked wait-resolved success never advances the fixed SI snapshot")

			// (2) The same shape through the legacy entry: today's terminal
			// WW-conflict — the pre-fix behavior of the promoted shared
			// statement, byte-identical for every unmarked caller.
			const legacyWaitCommitTable = uint64(22)
			restore = shapeTableLockResult(proc, legacyWaitCommitTable,
				func(result *lock.Result) {
					result.NewLockAdd = true
					result.HasConflict = true
					result.HasPrevCommit = true
					result.Timestamp = higherCommit
				}, &hookFired)
			err := LockTableWithMode(nil, proc, legacyWaitCommitTable,
				pkType, lock.LockMode_Shared, false)
			require.Error(t, err,
				"the legacy call keeps today's terminal SI not-support-retry conflict on the same shape")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "got %v", err)
			restore()
			require.Equal(t, statementSnapshot, proc.GetTxnOperator().Txn().SnapshotTS)

			// (3) A holder that ABORTED: both calls succeed — the defensive
			// stale-flag branch probes and falls through to success.
			const abortedTable = uint64(23)
			restore = shapeTableLockResult(proc, abortedTable,
				func(result *lock.Result) {
					result.NewLockAdd = true
					result.HasConflict = true
					result.HasPrevCommit = false
					result.Timestamp = higherCommit
				}, &hookFired)
			require.NoError(t, LockTableWithModeMarked(nil, proc, abortedTable,
				pkType, lock.LockMode_Shared, false, statementSnapshot),
				"a conflict whose holder aborted succeeds for the marked call")
			require.NoError(t, LockTableWithMode(nil, proc, abortedTable,
				pkType, lock.LockMode_Shared, false),
				"and for the legacy call: byte-identical defensive outcome")
			restore()
			require.Equal(t, statementSnapshot, proc.GetTxnOperator().Txn().SnapshotTS)

			// (4) A reentrant re-acquire observed at a newer table
			// timestamp: both calls succeed through the marked/defensive
			// no-conflict fall-through.
			const reentrantTable = uint64(24)
			restore = shapeTableLockResult(proc, reentrantTable,
				func(result *lock.Result) {
					result.NewLockAdd = false
					result.HasConflict = false
					result.HasPrevCommit = false
					result.Timestamp = higherCommit
				}, &hookFired)
			require.NoError(t, LockTableWithModeMarked(nil, proc, reentrantTable,
				pkType, lock.LockMode_Shared, false, statementSnapshot),
				"a reentrant acquire succeeds for the marked call")
			require.NoError(t, LockTableWithMode(nil, proc, reentrantTable,
				pkType, lock.LockMode_Shared, false),
				"and for the legacy call: byte-identical reentrant outcome")
			restore()
			require.Equal(t, statementSnapshot, proc.GetTxnOperator().Txn().SnapshotTS)

			// (5) A retained definition fence stays TERMINAL for the marked
			// table call exactly as for the legacy one (N3/G3, rev-20's
			// contract): the fence is not a wait outcome and never becomes
			// one through the marked contract.
			fence := higherCommit.Next()
			const fenceTable = uint64(25)
			restore = shapeTableLockResult(proc, fenceTable,
				func(result *lock.Result) {
					result.NewLockAdd = true
					result.HasConflict = false
					result.HasPrevCommit = false
					result.Timestamp = fence
					result.TableDefChangedAt = &fence
				}, &hookFired)
			err = LockTableWithModeMarked(nil, proc, fenceTable,
				pkType, lock.LockMode_Shared, false, statementSnapshot)
			require.Error(t, err,
				"a retained definition fence keeps its terminal outcome for the marked table call")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "got %v", err)
			require.Contains(t, err.Error(), "table definition changed")
			err = LockTableWithMode(nil, proc, fenceTable,
				pkType, lock.LockMode_Shared, false)
			require.Error(t, err,
				"the legacy call keeps the same terminal fence")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict), "got %v", err)
			restore()
			require.Equal(t, statementSnapshot, proc.GetTxnOperator().Txn().SnapshotTS,
				"no table-lock outcome of the round moved the fixed SI snapshot")
		},
		[]client.TxnOption{client.WithTxnIsolation(txn.TxnIsolation_SI)},
		client.WithTimestampWaiter(immediateLockTimestampWaiter{}),
	)
}

// Under RC the marked entry costs nothing: both entries carry the base
// retryOnRefresh=true contract, so a mid-wait committed writer advances the
// mutable RC snapshot past the granted commit inside doLock and surfaces
// the plain RC retry signal - the caller's RC retry loop owns replaying the
// statement. The marking fields only alter the fixed-SI branches, so the
// marked entry must be byte-identical to the legacy one on the same shape.
func TestPromotedSharedTableLockMarkedKeepsRCBaseBranches(t *testing.T) {
	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			require.Equal(t, txn.TxnIsolation_RC, proc.GetTxnOperator().Txn().Isolation)
			snapshotBefore := proc.GetTxnOperator().Txn().SnapshotTS
			require.False(t, snapshotBefore.IsEmpty())
			proc.SetPlanSnapshotTS(snapshotBefore)
			higherCommit := snapshotBefore.Next()
			pkType := types.T_int32.ToType()

			const markedTable = uint64(31)
			seen := 0
			restore := shapeTableLockResult(proc, markedTable,
				func(result *lock.Result) {
					result.NewLockAdd = true
					result.HasConflict = true
					result.HasPrevCommit = true
					result.Timestamp = higherCommit
				}, &seen)
			err := LockTableWithModeMarked(nil, proc, markedTable,
				pkType, lock.LockMode_Shared, false, snapshotBefore)
			restore()
			require.Equal(t, 1, seen)
			require.Error(t, err,
				"under RC the marked entry keeps the base retry-on-refresh signal for the mid-wait commit")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetry), "got %v", err)
			require.True(t, proc.GetTxnOperator().Txn().SnapshotTS.Greater(higherCommit),
				"the RC refresh advanced the mutable snapshot past the granted commit before returning")

			// The legacy entry lands on the identical outcome.
			const legacyTable = uint64(32)
			snapshotSecond := proc.GetTxnOperator().Txn().SnapshotTS
			// The shaped commit must sit ABOVE the RC transaction's LIVE
			// snapshot: the mutable snapshot already moved past the first
			// call's commit, so a stale high-water would fall below it and
			// resolve as an ordinary no-conflict acquire.
			restore = shapeTableLockResult(proc, legacyTable,
				func(result *lock.Result) {
					result.NewLockAdd = true
					result.HasConflict = true
					result.HasPrevCommit = true
					result.Timestamp = proc.GetTxnOperator().Txn().SnapshotTS.Next()
				}, &seen)
			legacyErr := LockTableWithMode(nil, proc, legacyTable,
				pkType, lock.LockMode_Shared, false)
			restore()
			require.Error(t, legacyErr, "the legacy call keeps the identical RC retry signal")
			require.True(t, moerr.IsMoErrCode(legacyErr, moerr.ErrTxnNeedRetry), "got %v", legacyErr)
			require.True(t, proc.GetTxnOperator().Txn().SnapshotTS.Greater(snapshotSecond),
				"the legacy call advances the RC snapshot through the same base branch")
		},
		[]client.TxnOption{client.WithTxnIsolation(txn.TxnIsolation_RC)},
		client.WithTimestampWaiter(immediateLockTimestampWaiter{}),
	)
}

// The marked frontier's meaning: the wait-resolved branch compares the lock
// result against the view the call is already guaranteed to cover; a result
// timestamp that PREDATES that view needs no wait at all (the commit is
// already visible), while a timestamp above it is exactly the window the
// grant-time wait resolves.
func TestPromotedSharedTableLockFrontierBelowGrantTimestampNeedsNoWait(t *testing.T) {
	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			statementSnapshot := proc.GetTxnOperator().Txn().SnapshotTS
			require.Equal(t, txn.TxnIsolation_SI, proc.GetTxnOperator().Txn().Isolation)
			pkType := types.T_int32.ToType()

			// A below-snapshot grant timestamp: the committed change is
			// already inside the fixed view — the marked call succeeds with
			// nothing to wait for.
			const belowTable = uint64(41)
			seen := 0
			restore := shapeTableLockResult(proc, belowTable,
				func(result *lock.Result) {
					result.NewLockAdd = true
					result.HasConflict = true
					result.HasPrevCommit = true
					result.Timestamp = statementSnapshot.Prev()
				}, &seen)
			require.NoError(t, LockTableWithModeMarked(nil, proc, belowTable,
				pkType, lock.LockMode_Shared, false, statementSnapshot),
				"a predating grant timestamp is already covered by the fixed view")
			restore()
			require.Equal(t, 1, seen)
			require.Equal(t, statementSnapshot, proc.GetTxnOperator().Txn().SnapshotTS,
				"the fixed SI snapshot stays untouched end to end")
		},
		[]client.TxnOption{client.WithTxnIsolation(txn.TxnIsolation_SI)},
		client.WithTimestampWaiter(immediateLockTimestampWaiter{}),
	)
}
