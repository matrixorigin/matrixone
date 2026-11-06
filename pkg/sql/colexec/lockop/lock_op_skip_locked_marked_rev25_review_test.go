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

// rev-25 members (design revision 15, R15-F2; iamlinjunhong P1): the marked
// SI counterpart of TestLockOpSkipLockedPreservesRefreshBeforeLaterTarget-
// Conflict. An earlier marked target can record its revalidation floor while
// returning an EMPTY refreshTS, and a later target's direct SKIP LOCKED
// conflict used to end performLock before the post-loop conversion, leaving
// ctr.retryError nil so callSkipLocked silently skipped the candidate. This
// family drives the real doLock -> performLock -> callSkipLocked path under
// an explicit SI transaction and pins the D-5 rev-15 invariant: once any
// target recorded a floor, the tokenized retry surfaces on every terminal
// path of the lock batch — the recorded floor MAX-aggregates, the token
// regenerates from that max, a definition rebuild keeps precedence in both
// orders, and no accepted prefix is published through the retry.

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/lockservice"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	limitop "github.com/matrixorigin/matrixone/pkg/sql/colexec/limit"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// buildSkipLockedMarkedArgument mirrors the RC counterpart's two-target
// construction with the targeted-mark revision of the batch: every target is
// Shared + SKIP LOCKED, and the caller marks/stamps the exact targets that
// revalidate the locking current read in THIS execution.
func buildSkipLockedMarkedArgument(
	firstTableID, secondTableID uint64,
	input *batch.Batch) *LockOp {
	arg := NewArgumentByEngine(nil)
	arg.AddLockTargetWithPolicy(
		firstTableID, nil, lock.LockMode_Shared, 0, types.T_int32.ToType(),
		-1, -1, nil, false, false, true,
	)
	arg.AddLockTargetWithPolicy(
		secondTableID, nil, lock.LockMode_Shared, 1, types.T_int32.ToType(),
		-1, -1, nil, false, false, true,
	)
	arg.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	return arg
}

// The P1 counterexample shape: the FIRST target admits as a marked locking
// read and records its revalidation floor with an EMPTY refreshTS, while
// EVERY candidate row of the LATER target is directly locked by another
// transaction. The recorded retry must survive that later conflict instead
// of being discarded as an ordinary skipped candidate — otherwise the
// statement would end with an empty result, even though a downstream LIMIT
// would happily publish that empty prefix.
func TestLockOpSkipLockedMarkedPreservesRecordedFloorBeforeLaterTargetConflict(t *testing.T) {
	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			const firstTableID = uint64(884)
			const secondTableID = uint64(885)
			service := proc.GetLockService()

			holderTxnID := []byte("rev25-marked-skip-locked-holder")
			for _, value := range []int32{1, 2} {
				rowKey := int32LockTestKey(value)
				_, err := lockTestRows(proc, secondTableID, [][]byte{rowKey},
					holderTxnID, lock.LockMode_Exclusive, lock.WaitPolicy_Wait)
				require.NoError(t, err)
			}
			defer func() {
				require.NoError(t, service.Unlock(
					proc.Ctx, holderTxnID, timestamp.Timestamp{}))
			}()

			runtime.SetupServiceRuntimeTestingContext(proc.GetService())
			testingContext := runtime.MustGetTestingContext(proc.GetService())
			testingContext.SetBeforeLockFunc(func(_ []byte, _ uint64) {})
			defer testingContext.SetBeforeLockFunc(nil)

			sessionTS := proc.GetTxnOperator().Txn().SnapshotTS
			fence := sessionTS.Next()
			adjusted := false
			testingContext.SetAdjustLockResultFunc(func(_ []byte, tableID uint64, result *lock.Result) {
				if tableID == firstTableID {
					adjusted = true
					// Preserve the real Shared grant while reporting the
					// committed version newer than the attempt frontier.
					result.NewLockAdd = true
					result.HasConflict = false
					result.Timestamp = fence
				}
			})
			defer testingContext.SetAdjustLockResultFunc(nil)

			input := makeLockTestInt32Batch(proc, []int32{1, 2}, []int32{1, 2})
			defer input.Clean(proc.Mp())

			arg := buildSkipLockedMarkedArgument(firstTableID, secondTableID, input)
			defer arg.Free(proc, false, nil)
			require.NoError(t, arg.Prepare(proc))
			// Every admitting grant of the marked target reports a commit
			// newer than the attempt frontier inside the revalidated window.
			arg.ctr.hasNewVersionInRange = changedInRange

			arg.MarkLockingReadTarget(0, true)
			attemptFrontier := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
			arg.SetLockingReadCurrentFrontier(attemptFrontier, proc.GetTxnOperator().Txn().SnapshotTS)
			require.Equal(t, txn.TxnIsolation_SI, proc.GetTxnOperator().Txn().Isolation,
				"the member runs the marked path under explicit SI")

			limit := limitop.NewArgument().WithLimit(plan2.MakePlan2Uint64ConstExprWithType(1))
			defer limit.Free(proc, false, nil)
			limit.AppendChild(arg)
			require.NoError(t, limit.Prepare(proc))

			result, err := vm.Exec(limit, proc)
			require.True(t, adjusted, "the marked first-target revalidation must actually run")
			require.Error(t, err,
				"a marked record of a newer commit must not silently become an all-skipped batch")
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryLockingRead), "%v", err)

			phys, logical, ok := moerr.GetTxnNeedRetryLockingReadFrontier(err)
			require.True(t, ok, "the surfaced retry carries the recorded floor token")
			token := timestamp.Timestamp{PhysicalTime: phys, LogicalTime: logical}
			require.True(t, token.Greater(attemptFrontier),
				"the token strictly progresses past the stamped attempt frontier (D-5)")
			require.True(t, token.Equal(fence.Next()),
				"the recorded floor is the wait-resolved commit pointer of the admitted grant")
			require.True(t, arg.ctr.lockingReadFloor.Equal(token),
				"the operator's diagnostic floor is exactly the surfaced token")
			require.Nil(t, result.Batch,
				"the later conflict must not publish any candidate prefix, not even to a LIMIT")

			require.Equal(t, sessionTS, proc.GetTxnOperator().Txn().SnapshotTS,
				"the recorded floor and surfaced retry never move the SI snapshot")

			// The marked first target's Shared grant is retained until
			// transaction cleanup; the retry must not silently release a
			// partial target prefix.
			_, err = lockTestRows(proc, firstTableID, [][]byte{int32LockTestKey(1)},
				[]byte("rev25-first-target-prefix-probe"), lock.LockMode_Exclusive,
				lock.WaitPolicy_FastFail)
			require.ErrorIs(t, err, lockservice.ErrLockConflict,
				"the first target's actual grant remains owned while the retry propagates")
			require.NoError(t, service.Unlock(
				proc.Ctx, []byte("rev25-first-target-prefix-probe"), timestamp.Timestamp{}))
		},
		[]client.TxnOption{client.WithTxnIsolation(txn.TxnIsolation_SI)},
		client.WithTimestampWaiter(immediateLockTimestampWaiter{}),
	)
}

// Two marked targets record two floors in ONE lock batch: the surfaced token
// must be the MAXIMUM of both recorded floors. This guards the rev-15
// regeneration rule — persisting the first floor and never refreshing the
// token when a later target records a higher floor would surface a stale,
// non-maximum frontier as the revalidation point of the retry.
func TestLockOpSkipLockedMarkedAggregateIsMaximumAcrossMarkedTargets(t *testing.T) {
	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			const firstTableID = uint64(886)
			const secondTableID = uint64(887)

			runtime.SetupServiceRuntimeTestingContext(proc.GetService())
			testingContext := runtime.MustGetTestingContext(proc.GetService())
			testingContext.SetBeforeLockFunc(func(_ []byte, _ uint64) {})
			defer testingContext.SetBeforeLockFunc(nil)

			sessionTS := proc.GetTxnOperator().Txn().SnapshotTS
			firstFence := sessionTS.Next()
			secondFence := firstFence.Next().Next()
			testingContext.SetAdjustLockResultFunc(func(_ []byte, tableID uint64, result *lock.Result) {
				result.NewLockAdd = true
				result.HasConflict = false
				if tableID == firstTableID {
					result.Timestamp = firstFence
				} else if tableID == secondTableID {
					result.Timestamp = secondFence
				}
			})
			defer testingContext.SetAdjustLockResultFunc(nil)

			input := makeLockTestInt32Batch(proc, []int32{1}, []int32{1})
			defer input.Clean(proc.Mp())

			arg := buildSkipLockedMarkedArgument(firstTableID, secondTableID, input)
			defer arg.Free(proc, false, nil)
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = changedInRange

			arg.MarkLockingReadTarget(0, true)
			arg.MarkLockingReadTarget(1, true)
			attemptFrontier := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
			arg.SetLockingReadCurrentFrontier(attemptFrontier, proc.GetTxnOperator().Txn().SnapshotTS)

			_, err := vm.Exec(arg, proc)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryLockingRead), "%v", err)

			phys, logical, ok := moerr.GetTxnNeedRetryLockingReadFrontier(err)
			require.True(t, ok)
			token := timestamp.Timestamp{PhysicalTime: phys, LogicalTime: logical}
			require.True(t, token.Greater(attemptFrontier))
			require.True(t, token.Equal(secondFence.Next()),
				"the surfaced token is the MAXIMUM of the two recorded floors, not the first one")
			require.True(t, arg.ctr.lockingReadFloor.Equal(token),
				"the operator floor carries the same maximum")

			require.Equal(t, sessionTS, proc.GetTxnOperator().Txn().SnapshotTS,
				"aggregating two marked floors never moves the SI snapshot")
		},
		[]client.TxnOption{client.WithTxnIsolation(txn.TxnIsolation_SI)},
		client.WithTimestampWaiter(immediateLockTimestampWaiter{}),
	)
}

// A definition-change retry detected AFTER a marked floor records keeps its
// higher precedence: the retried statement must rebuild the plan, never
// merely re-read the data at the recorded floor. Under SI a DDL fence stays a
// terminal WW-conflict by design (non-goals N3/G3), so the definition retry
// travels the SI-reachable stale-signal path: a marked reentrant grant
// (case A) whose revalidation returns a definition change alongside the
// recorded floor.
func TestLockOpSkipLockedMarkedDefinitionChangeAfterRecordedFloorWins(t *testing.T) {
	defChangeSkipLockedMarkedOrder(t, false)
}

// The same precedence holds when the definition change is recorded on the
// FIRST target, before a later marked floor records (a fix that regenerated
// the tokenized retry over any non-nil retry error would break this order).
func TestLockOpSkipLockedMarkedDefinitionChangeBeforeRecordedFloorWins(t *testing.T) {
	defChangeSkipLockedMarkedOrder(t, true)
}

func defChangeSkipLockedMarkedOrder(t *testing.T, definitionChangeFirst bool) {
	t.Helper()
	runLockOpTestWithTxnOptions(
		t,
		func(proc *process.Process) {
			const firstTableID = uint64(888)
			const secondTableID = uint64(889)

			runtime.SetupServiceRuntimeTestingContext(proc.GetService())
			testingContext := runtime.MustGetTestingContext(proc.GetService())
			testingContext.SetBeforeLockFunc(func(_ []byte, _ uint64) {})
			defer testingContext.SetBeforeLockFunc(nil)

			sessionTS := proc.GetTxnOperator().Txn().SnapshotTS
			fence := sessionTS.Next()
			defTable := secondTableID
			if definitionChangeFirst {
				defTable = firstTableID
			}
			testingContext.SetAdjustLockResultFunc(func(_ []byte, tableID uint64, result *lock.Result) {
				if tableID == defTable {
					// Stale-signal case A: the reentrant grant reports the
					// definition change without a prev commit, so the marked
					// revalidation records its floor and returns the change.
					result.NewLockAdd = false
					result.HasConflict = false
					result.TableDefChanged = true
					result.Timestamp = fence
				} else {
					result.NewLockAdd = true
					result.HasConflict = false
					result.Timestamp = fence
				}
			})
			defer testingContext.SetAdjustLockResultFunc(nil)

			input := makeLockTestInt32Batch(proc, []int32{1}, []int32{1})
			defer input.Clean(proc.Mp())

			arg := buildSkipLockedMarkedArgument(firstTableID, secondTableID, input)
			defer arg.Free(proc, false, nil)
			require.NoError(t, arg.Prepare(proc))
			arg.ctr.hasNewVersionInRange = changedInRange

			// Both targets are marked: the floor target records the newer
			// commit inside the revalidated window, and the definition target
			// records the definition change the same window's revalidation
			// observes, so the two signals race in the requested order.
			arg.MarkLockingReadTarget(0, true)
			arg.MarkLockingReadTarget(1, true)
			attemptFrontier := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
			arg.SetLockingReadCurrentFrontier(attemptFrontier, proc.GetTxnOperator().Txn().SnapshotTS)

			result, err := vm.Exec(arg, proc)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrTxnNeedRetryWithDefChanged),
				"the definition rebuild keeps precedence over a recorded floor, in both orders: %v", err)
			require.Nil(t, result.Batch, "the definition retry publishes no candidate prefix")
			require.Equal(t, sessionTS, proc.GetTxnOperator().Txn().SnapshotTS)
		},
		[]client.TxnOption{client.WithTxnIsolation(txn.TxnIsolation_SI)},
		client.WithTimestampWaiter(immediateLockTimestampWaiter{}),
	)
}
