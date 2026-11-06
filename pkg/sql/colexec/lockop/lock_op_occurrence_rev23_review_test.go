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

// Round-10 members (pre-PR review round 10, finding 2): lock marks stay per
// OCCURRENCE. The supported LEFT-lowered same-table scalar-subquery shape
// puts TWO occurrences of ONE table into a single LockOp — the outer locked
// occurrence marked current-read and the nested occurrence of the same
// table snapshot-bound — so presence must act on the occurrence that
// carries it through the acquisition predicate (markingTarget), the wire
// serialization and the parallel dup path, and must survive the retained
// pipeline's Reset with the physical generation. A table-ID expansion
// would hand the nested occurrence's lock the marked suppression/retry
// contract (design D-1's per-target representation, R4-F2/R5-F1's
// same-table identity).

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestMarkingTargetStaysPerOccurrence(t *testing.T) {
	pkType := types.T_int32.ToType()
	arg := NewArgument()
	defer arg.Release()
	// One LockOp, TWO occurrences of table 42: the marked outer occurrence
	// and the same-table snapshot-bound nested occurrence.
	arg.AddLockTargetWithOccurrence(42, nil, lock.LockMode_Shared, 0, pkType, -1, -1, nil, false, false, false, true, timestamp.Timestamp{})
	arg.AddLockTargetWithOccurrence(42, nil, lock.LockMode_Shared, 0, pkType, -1, -1, nil, false, false, false, false, timestamp.Timestamp{})

	// Unstamped: neither occurrence carries the marked contract.
	require.False(t, arg.markingTarget(arg.targets[0]))
	require.False(t, arg.markingTarget(arg.targets[1]))

	frontier := timestamp.Timestamp{PhysicalTime: 111, LogicalTime: 1}
	// An ordinary marked occurrence (zero explicit-snapshot provenance)
	// resolves effective against any executing SnapshotTS.
	arg.SetLockingReadCurrentFrontier(frontier, timestamp.Timestamp{PhysicalTime: 900, LogicalTime: 9})
	require.True(t, arg.markingTarget(arg.targets[0]),
		"the marked occurrence of table 42 carries the marked contract")
	require.False(t, arg.markingTarget(arg.targets[1]),
		"the nested same-table occurrence never inherits the marked contract — presence is per occurrence")

	// The wire side: only the marked occurrence carries presence and the
	// attempt's frontier.
	targets := arg.CopyToPipelineTarget()
	require.True(t, targets[0].GetLockingRead())
	require.Equal(t, int64(111), targets[0].GetCurrentReadFrontierPhysical())
	require.Equal(t, uint32(1), targets[0].GetCurrentReadFrontierLogical())
	require.False(t, targets[1].GetLockingRead())
	require.Equal(t, int64(0), targets[1].GetCurrentReadFrontierPhysical())

	// CopyTargetsFrom (the parallel dup path, operator.go's dupOperator)
	// preserves the per-occurrence profile exactly; the duplicate still
	// needs its own activation stamp (dupOperator never copies per-attempt
	// state — a parallel duplicate is inert until stamped for its pipeline,
	// exactly like the original before its stamp).
	dup := NewArgument()
	defer dup.Release()
	dup.CopyTargetsFrom(arg)
	require.False(t, dup.markingTarget(dup.targets[0]),
		"an unstamped parallel copy is inert like any unstamped operator")
	require.False(t, dup.markingTarget(dup.targets[1]))
	require.True(t, dup.targets[0].lockingRead,
		"a parallel copy keeps the marked occurrence's presence provenance")
	require.False(t, dup.targets[1].lockingRead,
		"a parallel copy keeps the nested same-table occurrence unmarked")
	dup.SetLockingReadCurrentFrontier(timestamp.Timestamp{PhysicalTime: 113, LogicalTime: 7}, timestamp.Timestamp{PhysicalTime: 900, LogicalTime: 9})
	require.True(t, dup.markingTarget(dup.targets[0]),
		"the stamped parallel copy serves exactly the marked occurrence")
	require.False(t, dup.markingTarget(dup.targets[1]),
		"the anchored parallel copy never expands the mark to the same-table occurrence")

	// Reset retires the attempt state but keeps the per-occurrence
	// provenance with the retained physical generation: the marked
	// occurrence re-activates on the next stamp while the nested occurrence
	// stays legacy forever.
	proc := testutil.NewProcess(t)
	arg.Reset(proc, false, nil)
	require.False(t, arg.markingTarget(arg.targets[0]),
		"unstamped-after-reset: the marked occurrence is inert without this attempt's frontier")
	require.False(t, arg.markingTarget(arg.targets[1]))
	require.True(t, arg.targets[0].lockingRead,
		"Reset retains the marked occurrence's bind-time provenance")
	require.False(t, arg.targets[1].lockingRead,
		"Reset retains the nested occurrence's unmarked provenance")
	arg.SetLockingReadCurrentFrontier(timestamp.Timestamp{PhysicalTime: 222, LogicalTime: 2}, timestamp.Timestamp{PhysicalTime: 900, LogicalTime: 9})
	require.True(t, arg.markingTarget(arg.targets[0]),
		"the next attempt's stamp re-activates exactly the marked occurrence")
	require.False(t, arg.markingTarget(arg.targets[1]))
}
