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

// Round-11 member (pre-PR review round 11, single finding): an occurrence's
// EFFECTIVE marked activation resolves per EXECUTION from its bind-time
// presence together with its raw bind-time explicit-snapshot provenance,
// exactly where the marking frontier is stamped and against the executing
// transaction's SnapshotTS (the same engagement predicate compile's scan
// admission uses). A locked occurrence whose stored H is nonzero and
// strictly older than the executing SnapshotTS ENGAGES the historical cohort
// for that execution and is EXEMPT from marked revalidation: it keeps
// today's lock-only behavior (a mid-wait commit surfaces the legacy
// terminal conflict), it never receives the marked wire fields in that
// execution (G2 withholds the current-read machinery), and a LATER
// execution whose SnapshotTS no longer engages the stored H re-attaches
// the marked contract (R8-F3 with R9-F3's per-execution engagement). The
// per-attempt activation retires with the frontier at Reset; presence and
// the raw H provenance survive, exactly like the frontier-bearing attempt
// state.

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestHistoricalOccurrenceExemptFromMarkedRevalidationUnderStampedAttempt(t *testing.T) {
	pkType := types.T_int32.ToType()
	arg := NewArgument()
	defer arg.Release()

	// The executing transaction's SnapshotTS for the first attempt.
	executingSnapshot := timestamp.Timestamp{PhysicalTime: 100, LogicalTime: 1}
	// An engaging explicit snapshot: nonzero and strictly older.
	engagingH := timestamp.Timestamp{PhysicalTime: 50, LogicalTime: 1}
	// A non-engaging explicit snapshot: at or after the executing snapshot.
	laterH := timestamp.Timestamp{PhysicalTime: 150, LogicalTime: 1}

	add := func(tableID uint64, lockingRead bool, h timestamp.Timestamp) {
		arg.AddLockTargetWithOccurrence(tableID, nil, lock.LockMode_Shared, 0,
			pkType, -1, -1, nil, false, false, false, lockingRead, h)
	}
	// Occurrence 0: presence WITH an engaging H — the historical current
	// shape the review demanded exempting. Occurrence 1: ordinary presence
	// (no named snapshot). Occurrence 2: presence with a NON-engaging H
	// (comparison stays admitted). Occurrence 3: engaging H with NO
	// presence — partial provenance must never activate on its own.
	add(42, true, engagingH)
	add(43, true, timestamp.Timestamp{})
	add(44, true, laterH)
	add(45, false, engagingH)

	// Unstamped: no occurrence carries the marked contract.
	for i := range arg.targets {
		require.False(t, arg.markingTarget(arg.targets[i]),
			"unstamped occurrence %d keeps today's lock-only behavior", i)
	}

	// The attempt stamp resolves EFFECTIVE activation per occurrence
	// against the executing SnapshotTS.
	frontier := timestamp.Timestamp{PhysicalTime: 900, LogicalTime: 9}
	arg.SetLockingReadCurrentFrontier(frontier, executingSnapshot)
	require.False(t, arg.markingTarget(arg.targets[0]),
		"the engaging historical occurrence stays exempt from marked revalidation in this execution")
	require.True(t, arg.markingTarget(arg.targets[1]),
		"the ordinary occurrence keeps the marked revalidation contract")
	require.True(t, arg.markingTarget(arg.targets[2]),
		"the non-engaging snapshot occurrence (H >= SnapshotTS) keeps the marked contract")
	require.False(t, arg.markingTarget(arg.targets[3]),
		"an occurrence without presence never activates, whatever its provenance")

	// The wire: the exempt occurrence never receives the marked fields in
	// this execution, so the remote rebuild serves its lock on today's
	// lock-only behavior.
	targets := arg.CopyToPipelineTarget()
	require.False(t, targets[0].GetLockingRead(),
		"the engaging historical occurrence never receives the marked wire fields in that execution")
	require.True(t, targets[1].GetLockingRead())
	require.Equal(t, int64(900), targets[1].GetCurrentReadFrontierPhysical())
	require.Equal(t, uint32(9), targets[1].GetCurrentReadFrontierLogical())

	// Reset retires the per-attempt activation with the frontier; presence
	// and the raw H provenance survive for the next attempt.
	proc := testutil.NewProcess(t)
	arg.Reset(proc, false, nil)
	for i := range arg.targets {
		require.False(t, arg.markingTarget(arg.targets[i]),
			"after Reset every occurrence is inert until the next stamp")
	}
	require.True(t, arg.targets[0].lockingRead,
		"Reset retains the exempt occurrence's presence provenance")
	require.Equal(t, engagingH, arg.targets[0].explicitSnapshot,
		"Reset retains the exempt occurrence's raw bind-time H provenance")

	// A LATER execution whose SnapshotTS no longer engages the stored H
	// re-attaches the marked contract for exactly that occurrence
	// (R9-F3's per-execution engagement, re-resolved at the stamp).
	nextSnapshot := timestamp.Timestamp{PhysicalTime: 40, LogicalTime: 1}
	arg.SetLockingReadCurrentFrontier(timestamp.Timestamp{PhysicalTime: 901, LogicalTime: 1}, nextSnapshot)
	require.True(t, arg.markingTarget(arg.targets[0]),
		"a later execution whose SnapshotTS no longer engages the stored H re-attaches the marked contract")
	require.True(t, arg.markingTarget(arg.targets[2]),
		"the non-engaging occurrence keeps the marked contract across executions")
	require.False(t, arg.markingTarget(arg.targets[3]),
		"absence of presence still never activates after the re-stamp")

	// CopyTargetsFrom (the parallel dup path) carries presence AND the raw
	// H provenance AND the resolved activation of this attempt: the
	// duplicate resolves the same profile when it is stamped for its own
	// pipeline.
	dup := NewArgument()
	defer dup.Release()
	dup.CopyTargetsFrom(arg)
	require.False(t, dup.markingTarget(dup.targets[0]), "an unstamped dup is inert")
	dup.SetLockingReadCurrentFrontier(timestamp.Timestamp{PhysicalTime: 902, LogicalTime: 2}, nextSnapshot)
	require.True(t, dup.markingTarget(dup.targets[0]),
		"the stamped dup resolves the same per-execution profile from the carried provenance")
	require.Equal(t, engagingH, dup.targets[0].explicitSnapshot,
		"the dup carries the raw H provenance")

	// The same execution re-resolved with the stored H engaging again keeps
	// the exemption: resolution is per execution, never sticky.
	arg.SetLockingReadCurrentFrontier(timestamp.Timestamp{PhysicalTime: 903, LogicalTime: 3}, executingSnapshot)
	require.False(t, arg.markingTarget(arg.targets[0]),
		"an execution whose SnapshotTS re-engages the stored H keeps the occurrence exempt again")
}
