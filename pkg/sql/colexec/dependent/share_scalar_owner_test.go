//go:build matrixone_test

// Review #6031303617, glm91 (root's GLM90 finding 3): the reservation-
// ownership unit on the REAL typed tuple. The reducer is genuinely FED one
// row and Finished; the invocation's result IS the exact reducer tuple the
// share detaches, so the donor/consumer ownership is the production path.
// The denial proves the sealed generation (zero-headroom drain) leaves NO
// memo, NO charge transfer, the first winner's finished tuple intact and
// still readable exactly once, with the donor's own Close the single
// measured release. The success proves exactly ONE memo entry on the real
// tuple, the measured capacity transfer (the generation's Used/RejectCount
// deltas), the donor's Close not touching the donated tuple, the
// Execution's close releasing it exactly once (the Used delta proves no
// double release), and the idempotent second close.
package dependent

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	colexec "github.com/matrixorigin/matrixone/pkg/sql/colexec"
	testutil "github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// newFinishedShareOwnerInvocation feeds one DECIMAL(10,2) row through the
// real reducer and uses exactly its Finshed tuple as the invocation result,
// with the generation measured before any sharing decision.
func newFinishedShareOwnerInvocation(t *testing.T) (*invocation, *Execution, *colexec.DependentReducer, *process.ExecutionResourceGeneration) {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	call := &plan.DependentExpr{
		ProgramId:    1,
		OccurrenceId: 7,
		StageId:      1,
		Reducer:      plan.SubqueryRef_SCALAR,
	}
	// Align the declared output to the fed vector's own physical type so
	// the reducer's feed type-check sees one consistent type (run4's
	// "dependent output type changed" came from declaring (10,2) while the
	// fed vector carried types.T_decimal64.ToType()).
	dt := types.T_decimal64.ToType()
	output := []plan.Type{{Id: int32(types.T_decimal64), Width: dt.Width, Scale: dt.Scale}}
	reducer, err := colexec.NewDependentReducer(proc, call, output, nil, proc.Mp())
	require.NoError(t, err)
	in := batch.NewWithSize(1)
	in.Vecs[0] = vector.NewVec(dt, proc.Mp())
	require.NoError(t, vector.AppendFixed[types.Decimal64](in.Vecs[0], types.Decimal64(40960), false, proc.Mp()))
	in.SetRowCount(1)
	stop, err := reducer.Feed(in)
	require.NoError(t, err)
	require.False(t, stop)
	tuple, err := reducer.Finish()
	require.NoError(t, err)
	require.NotNil(t, tuple)
	require.Equal(t, 1, tuple.RowCount(), "the finished tuple carries exactly the fed row")
	// The exact value of the first (and only) row the winner will read
	// (this package's own tests read fixed cells through
	// GetFixedAtNoTypeCheck - join_test.go/program_test.go).
	require.Equal(t, types.Decimal64(40960),
		vector.GetFixedAtNoTypeCheck[types.Decimal64](tuple.Vecs[0], 0),
		"the finished tuple's exact value")

	gen, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	t.Cleanup(func() { in.Clean(proc.Mp()) })
	inv := &invocation{proc: proc, reducer: reducer, result: tuple}
	e := &Execution{proc: proc, allocation: proc.Mp(), scalarShares: make(map[uint32]*batch.Batch), main: &region{}}
	return inv, e, reducer, gen
}

func TestShareScalarResultDeniedReserveKeepsDonorOwnership(t *testing.T) {
	inv, e, reducer, gen := newFinishedShareOwnerInvocation(t)
	tuple := inv.result
	usedBefore := gen.Used()
	require.NotZero(t, usedBefore, "the finished tuple's charge is live on the generation")

	// Deterministic sealed generation: drain the per-query capacity to real
	// zero headroom at one-byte granularity so the next byte reservation
	// fails - this is the physical rejection, not merely a big request. The
	// drained amount is counted and released by this test's own guaranteed
	// cleanup so the synthetic reservation never leaks.
	drained := uint64(0)
	for i := 0; i < 1<<24; i++ {
		if gen.AcquireAllocationCapacity(1) != nil {
			break
		}
		drained++
	}
	require.NotZero(t, drained, "the drain genuinely consumed capacity")
	require.Error(t, gen.AcquireAllocationCapacity(1), "zero headroom: the generation is sealed")
	t.Cleanup(func() { gen.ReleaseAllocationCapacity(drained) })
	rejectsBefore := gen.Snapshot().RejectCount

	deniedBefore := gen.Snapshot()
	e.shareScalarResult(7, inv)
	deniedSnapshot := gen.Snapshot()
	require.Equal(t, rejectsBefore+1, deniedSnapshot.RejectCount, "one measured rejection for the denied reservation")
	require.Equal(t, deniedBefore.Used, deniedSnapshot.Used, "the denial moved NO charge onto the generation")
	require.Empty(t, e.scalarShares, "the denied reservation creates NO memo entry")
	require.Zero(t, e.scalarShareCharged, "no charge moved on denial")
	require.Same(t, tuple, inv.result, "the first winner still owns its finished tuple")
	// The finished tuple is still readable exactly as the first row.
	require.Equal(t, types.Decimal64(40960),
		vector.GetFixedAtNoTypeCheck[types.Decimal64](tuple.Vecs[0], 0),
		"the exact first-row value survives the denial")

	// run5's fact: the drain holds ~1MiB of this test generation's own
	// reservation, so the donor's release must be measured from the
	// post-drain sealed level, not the pre-drain baseline.
	usedSealed := gen.Snapshot().Used
	require.Greater(t, usedSealed, usedBefore, "the drain reservation is held on the generation")
	donorReleases := gen.Snapshot().ReleaseCount
	reducer.Close()
	afterDonor := gen.Snapshot()
	require.Less(t, afterDonor.Used, usedSealed, "the donor single release returns the tuple charge from the sealed level")
	require.Equal(t, donorReleases+1, afterDonor.ReleaseCount,
		"the donor's release is counted EXACTLY once at the known single-release granularity (root glm93 F3)")
	secondBefore := gen.Snapshot()
	reducer.Close()
	require.Equal(t, secondBefore, gen.Snapshot(), "the second Close releases nothing more")
	// The receive-side finalize on this denied arrangement (no memo was
	// created) still runs once and is idempotent.
	e.close(nil)
	e.close(nil)
}

func TestShareScalarResultSuccessTransfersOneMeasuredRelease(t *testing.T) {
	inv, e, reducer, gen := newFinishedShareOwnerInvocation(t)
	tuple := inv.result
	cost := reducer.TupleCost()
	require.NotZero(t, cost, "the finished tuple carries a real charged cost")
	usedBefore := gen.Used()
	usedSealed := usedBefore

	e.shareScalarResult(7, inv)
	require.Len(t, e.scalarShares, 1, "exactly one memo entry for the occurrence")
	require.Same(t, tuple, e.scalarShares[uint32(7)], "the memo shares the exact finished tuple")
	require.Equal(t, cost, e.scalarShareCharged, "the tuple's cost transferred to the Execution")
	sharedSnapshot := gen.Snapshot()
	require.Equal(t, usedSealed, sharedSnapshot.Used,
		"the reservation SWAPS, not stacks: the donor tuple's original charge is released as the new one is acquired, so the generation is net-unchanged")

	// The donor's Close no longer owns the detached tuple: its release is
	// only its own non-tuple charge, and the memo survives intact.
	reducer.Close()
	require.Len(t, e.scalarShares, 1, "the donor's Close does not touch the memo")
	require.Same(t, tuple, e.scalarShares[uint32(7)], "the donated pointer survives the donor close")

	// The Execution's close is THE single release of the transferred charge,
	// measured on the generation's own ledger (root glm91 F3).
	closeBefore := gen.Snapshot()
	require.NoError(t, e.close(nil))
	closeAfter := gen.Snapshot()
	require.Empty(t, e.scalarShares, "the Execution's close releases the memo exactly once")
	require.Zero(t, e.scalarShareCharged, "the transferred charge is released by the Execution")
	require.Equal(t, cost, closeBefore.Used,
		"after the donor's own release, the generation holds ONLY the recipient's transferred charge (other owners isolated)")
	require.Zero(t, closeAfter.Used,
		"the Execution close releases the memo charge and closes the dependent generation exactly once")
	require.Equal(t, closeBefore.ReleaseCount+1, closeAfter.ReleaseCount,
		"the recipient's close is EXACTLY ONE new release: the exact measured byte delta is closeBefore.Used(cost) -> 0 and no other owner released in between")
	// A second close is a no-op (idempotent cleanup path): the full ledger
	// is byte-identical after it.
	require.NoError(t, e.close(nil))
	require.Equal(t, closeAfter, gen.Snapshot(), "the second close changes nothing: the ledger is identical")
}

// Root glm91 F3: the applicable cancel/finalize leg - a memo-bearing
// Execution finalized WITH a real cause still releases the memo and the
// transferred charge exactly once, and the generation's release ledger
// counts that finalize.
func TestShareScalarResultFinalizeWithCauseReleasesMemoOnce(t *testing.T) {
	inv, e, reducer, gen := newFinishedShareOwnerInvocation(t)
	tuple := inv.result
	cost := reducer.TupleCost()
	e.shareScalarResult(7, inv)
	require.Len(t, e.scalarShares, 1)
	require.Same(t, tuple, e.scalarShares[uint32(7)])
	require.Equal(t, cost, e.scalarShareCharged)
	before := gen.Snapshot()
	cause := moerr.NewInternalErrorNoCtx("cancelled in flight")
	e.close(cause)
	after := gen.Snapshot()
	require.Empty(t, e.scalarShares, "finalize with a cause still releases the memo exactly once")
	require.Zero(t, e.scalarShareCharged, "the transferred charge is released by the finalize")
	require.Equal(t, before.Used-cost, after.Used,
		"the finalize releases EXACTLY the recipient's transferred memo charge; the donor's own charge stays with the donor")
	require.Equal(t, before.ReleaseCount+1, after.ReleaseCount,
		"the finalize is EXACTLY ONE new release: the measured byte delta is the already-asserted before.Used-cost -> after.Used with a real cause through the finished tuple")
	// The DONOR's own single release then settles its remaining charge and
	// closes the generation fully - the exact donor-then-recipient owner
	// chain root glm91 F3 requires.
	donorAfter := gen.Snapshot()
	reducer.Close()
	require.Less(t, gen.Snapshot().Used, donorAfter.Used,
		"the donor's own release returns its remaining charge")
	require.Equal(t, donorAfter.Used-gen.Snapshot().Used, donorAfter.Used,
		"the donor's release returns its EXACT remaining charge (isolation: only the donor's own bytes)")
	require.Zero(t, gen.Snapshot().Used, "the donor then closes the generation fully")
	require.Equal(t, donorAfter.ReleaseCount+1, gen.Snapshot().ReleaseCount,
		"the donor's own Close is EXACTLY ONE new release on the generation ledger")
	// root glm93 F3: capture the ledger IMMEDIATELY BEFORE the repeated
	// finalize - the earlier version compared two readings taken after the
	// same close, which could never observe a second release.
	preSecondFinalize := gen.Snapshot()
	e.close(cause)
	final := gen.Snapshot()
	require.Equal(t, preSecondFinalize, final,
		"the second finalize releases NOTHING more: the full captured ledger - Used, ReleaseCount and every counter - is identical before and after")
	require.Equal(t, after.ID, final.ID, "the generation identity is stable across the finalize")
	require.Empty(t, e.scalarShares, "the memo stays released")
}
