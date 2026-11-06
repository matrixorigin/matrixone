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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// Pre-PR review round 7, finding 2: a RETAINED definition fence (the last
// committed DDL timestamp kept in the lock table — the same-ID ALTER class
// whose change lands after the transaction snapshot) cannot promise progress
// for a marked SI statement. The compile driver's rebuild recaptures the
// SAME fixed SI snapshot (capturePlanSnapshot reads Txn().SnapshotTS),
// txn-op IncrStatementID never advances SI, and the progress validator
// ignores definition retries, so pre-rev-20 each rebuilt attempt still
// predates the fence and the statement rebuilt until cancelled. Non-goals
// N3/G3 keep table/DDL fences byte-identical (terminal conflict under SI),
// and R13-F4 keeps the tokenized locking-read driver the ONLY marked-SI
// retry class: the fence therefore stays terminal for EVERY SI statement —
// marked or not — while RC keeps its in-place snapshot-advance retry.
// These members pin all three sides through the real doLock/handleLockRes/
// performLock path against the real lock service.

// A marked SI statement whose plan generation predates a retained
// definition fence terminates with today's WW-conflict at the FIRST
// attempt and at every rebuilt attempt (the fixed SI snapshot cannot
// advance past the fence, so a rebuild re-feeds the same input rows
// against the same retained fence), and the terminal outcome never moves
// the transaction snapshot — ordinary RR reads inside the same transaction
// keep reading their snapshot.
func execMarkedFenceAttempt(t *testing.T, proc *process.Process) error {
	t.Helper()
	arg := buildMarkedArgument(proc, []uint64{1}, [][]int32{{0, 1, 2}})
	defer arg.Free(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	arg.ctr.hasNewVersionInRange = func(
		_ *process.Process,
		_ engine.Relation,
		_ process.Analyzer,
		_ uint64,
		_ engine.Engine,
		_ *batch.Batch,
		_ int32,
		_ int32,
		_, _ timestamp.Timestamp,
	) (bool, error) {
		t.Fatal("a definition-change fence must bypass row-version scanning")
		return false, nil
	}
	// Presence is bind provenance; the stamp resolves the occurrence's
	// EFFECTIVE activation for this attempt against the executing
	// transaction's SnapshotTS (rev 24), so mark before stamping.
	arg.MarkLockingReadTarget(0, true)
	attemptFrontier := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	arg.SetLockingReadCurrentFrontier(attemptFrontier, proc.GetTxnOperator().Txn().SnapshotTS)
	_, err := vm.Exec(arg, proc)
	return err
}

func TestMarkedSIDefinitionFenceIsTerminalAndBounded(t *testing.T) {
	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			statementSnapshot := proc.GetTxnOperator().Txn().SnapshotTS
			proc.SetPlanSnapshotTS(statementSnapshot)
			fence := statementSnapshot.Next()
			runtime.SetupServiceRuntimeTestingContext(proc.GetService())
			testingContext := runtime.MustGetTestingContext(proc.GetService())
			testingContext.SetBeforeLockFunc(func(_ []byte, _ uint64) {})
			defer testingContext.SetBeforeLockFunc(nil)
			testingContext.SetAdjustLockResultFunc(func(_ []byte, _ uint64, result *lock.Result) {
				// The lock grants cleanly; the table only reports the newer
				// DEFINITION generation the plan was not built against.
				result.NewLockAdd = true
				result.HasConflict = false
				result.Timestamp = fence
				result.TableDefChangedAt = &fence
			})
			defer testingContext.SetAdjustLockResultFunc(nil)

			require.Equal(t, txn.TxnIsolation_SI, proc.GetTxnOperator().Txn().Isolation,
				"the member runs the marked path under explicit SI")

			// First attempt: the retained fence is terminal for a marked SI
			// statement, exactly the unmarked statement's outcome.
			err := execMarkedFenceAttempt(t, proc)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict),
				"the retained definition fence stays terminal for a marked SI statement, got %v", err)

			// Bounded termination at the operator contract: every rebuilt
			// attempt re-feeds the input rows against the same retained fence
			// under the same fixed plan snapshot and surfaces the SAME
			// terminal conflict, never a retry outcome, so the compile
			// driver's rebuild loop cannot run away.
			for attempt := 0; attempt < 3; attempt++ {
				err = execMarkedFenceAttempt(t, proc)
				require.Error(t, err, "rebuilt attempt %d must still reach the fence", attempt)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict),
					"rebuilt attempt %d keeps the terminal definition-fence conflict", attempt)
			}

			require.Equal(t, statementSnapshot, proc.GetTxnOperator().Txn().SnapshotTS,
				"the terminal fence never moves the fixed SI snapshot (ordinary RR reads stay untouched)")
		},
		[]client.TxnOption{client.WithTxnIsolation(txn.TxnIsolation_SI)},
		client.WithTimestampWaiter(immediateLockTimestampWaiter{}),
	)
}

// The unmarked SI statement already kept the terminal fence outcome at base
// and every revision; a marked statement must now stay byte-identical to
// it — non-goal N3/G3, R13-F4's single marked-SI retry class.
func TestUnmarkedSIDefinitionFenceStaysTerminal(t *testing.T) {
	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			arg := buildMarkedArgument(proc, []uint64{1}, [][]int32{{0, 1, 2}})
			defer arg.Free(proc, false, nil)
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = func(
				_ *process.Process,
				_ engine.Relation,
				_ process.Analyzer,
				_ uint64,
				_ engine.Engine,
				_ *batch.Batch,
				_ int32,
				_ int32,
				_, _ timestamp.Timestamp,
			) (bool, error) {
				t.Fatal("a definition-change fence must bypass row-version scanning")
				return false, nil
			}

			statementSnapshot := proc.GetTxnOperator().Txn().SnapshotTS
			proc.SetPlanSnapshotTS(statementSnapshot)
			fence := statementSnapshot.Next()
			runtime.SetupServiceRuntimeTestingContext(proc.GetService())
			testingContext := runtime.MustGetTestingContext(proc.GetService())
			testingContext.SetBeforeLockFunc(func(_ []byte, _ uint64) {})
			defer testingContext.SetBeforeLockFunc(nil)
			testingContext.SetAdjustLockResultFunc(func(_ []byte, _ uint64, result *lock.Result) {
				result.NewLockAdd = true
				result.HasConflict = false
				result.Timestamp = fence
				result.TableDefChangedAt = &fence
			})
			defer testingContext.SetAdjustLockResultFunc(nil)

			// No marks: an unmarked statement, exactly today's shape.
			require.Equal(t, txn.TxnIsolation_SI, proc.GetTxnOperator().Txn().Isolation)

			_, err := vm.Exec(arg, proc)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnWWConflict),
				"the unmarked SI definition fence keeps its terminal conflict, got %v", err)
			require.Equal(t, statementSnapshot, proc.GetTxnOperator().Txn().SnapshotTS)
		},
		[]client.TxnOption{client.WithTxnIsolation(txn.TxnIsolation_SI)},
		client.WithTimestampWaiter(immediateLockTimestampWaiter{}),
	)
}

// Under RC the same fenced lock outcome keeps today's in-place refresh: the
// transaction's mutable snapshot advances to the wait-authority timestamp
// and the statement retries with a rebuilt plan that can observe the newer
// definition generation. The fix must cost RC nothing.
func TestRCSIDefinitionFenceKeepsInPlaceRefreshRetry(t *testing.T) {
	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			arg := buildMarkedArgument(proc, []uint64{1}, [][]int32{{0, 1, 2}})
			defer arg.Free(proc, false, nil)
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = func(
				_ *process.Process,
				_ engine.Relation,
				_ process.Analyzer,
				_ uint64,
				_ engine.Engine,
				_ *batch.Batch,
				_ int32,
				_ int32,
				_, _ timestamp.Timestamp,
			) (bool, error) {
				t.Fatal("a definition-change fence must bypass row-version scanning")
				return false, nil
			}

			statementSnapshot := proc.GetTxnOperator().Txn().SnapshotTS
			proc.SetPlanSnapshotTS(statementSnapshot)
			fence := statementSnapshot.Next()
			runtime.SetupServiceRuntimeTestingContext(proc.GetService())
			testingContext := runtime.MustGetTestingContext(proc.GetService())
			testingContext.SetBeforeLockFunc(func(_ []byte, _ uint64) {})
			defer testingContext.SetBeforeLockFunc(nil)
			testingContext.SetAdjustLockResultFunc(func(_ []byte, _ uint64, result *lock.Result) {
				result.NewLockAdd = true
				result.HasConflict = false
				result.Timestamp = fence
				result.TableDefChangedAt = &fence
			})
			defer testingContext.SetAdjustLockResultFunc(nil)

			require.Equal(t, txn.TxnIsolation_RC, proc.GetTxnOperator().Txn().Isolation)

			_, err := vm.Exec(arg, proc)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged),
				"RC keeps the definition-retry outcome with its in-place snapshot advance, got %v", err)
			require.False(t, proc.GetTxnOperator().Txn().SnapshotTS.Less(fence),
				"the RC retry advanced the mutable transaction snapshot to the fence's authority timestamp")
		},
		[]client.TxnOption{client.WithTxnIsolation(txn.TxnIsolation_RC)},
		client.WithTimestampWaiter(immediateLockTimestampWaiter{}),
	)
}
