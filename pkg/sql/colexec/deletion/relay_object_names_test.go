package deletion

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// issue25590TombstoneStatsBatch builds the serialized tombstone stats
// batch a flushed-tombstone row's DeltaLoc column carries: the
// [ObjectMeta_ObjectStats] batch with one serialized ObjectStats row
// per tombstone object.
func issue25590TombstoneStatsBatch(
	t *testing.T,
	proc *process.Process,
	n int,
) (*batch.Batch, []string) {
	t.Helper()
	inner := colexec.AllocCNS3ResultBat(true)
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		rowid := types.RandomRowid()
		oid := rowid.BorrowObjectID()
		stats := objectio.NewObjectStatsWithObjectID(oid, false, false, true)
		require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
		require.NoError(t, colexec.ExpandObjectStatsToBatch(
			proc.Mp(), true, inner, true, *stats))
		names = append(names, stats.ObjectName().String())
	}
	inner.SetRowCount(inner.Vecs[0].Length())
	return inner, names
}

// TestIssue25590DeletionRelayObjectNames pins the r23 standalone-delete
// intake classifier (issue #25590 round-23 review, finding 3): the
// remote deletion operator's own batch layout adopts exactly the
// flushed tombstone objects, the retained-block class owns nothing,
// foreign attributes are ordinary data, and a DeltaLoc that cannot
// prove its tombstone rejects the ack.
func TestIssue25590DeletionRelayObjectNames(t *testing.T) {
	proc := testutil.NewProc(t)
	mp := proc.Mp()

	statsBat, want := issue25590TombstoneStatsBatch(t, proc, 2)
	defer statsBat.Clean(mp)
	rowOnly := batch.New([]string{"__mo_rowid", "pk"})
	rowOnly.Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
	rowOnly.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	defer rowOnly.Clean(mp)
	require.NoError(t, vector.AppendFixed(rowOnly.Vecs[0], types.RandomRowid(), false, mp))
	require.NoError(t, vector.AppendFixed(rowOnly.Vecs[1], int64(7), false, mp))
	rowOnly.SetRowCount(1)

	res := makeDelRemoteBatch()
	res.Vecs[4] = vector.NewVec(types.T_uint64.ToType())
	defer func() {
		res.Vecs[4].Free(mp)
		res.Vecs[4] = nil
		res.Clean(mp)
	}()
	statsBytes, err := statsBat.MarshalBinary()
	require.NoError(t, err)
	rowOnlyBytes, err := rowOnly.MarshalBinary()
	require.NoError(t, err)
	// A retained block's class row: not a flushed tombstone.
	require.NoError(t, vector.AppendBytes(res.Vecs[0], []byte("blk-a"), false, mp))
	require.NoError(t, vector.AppendBytes(res.Vecs[1], rowOnlyBytes, false, mp))
	require.NoError(t, vector.AppendFixed(res.Vecs[2], int8(1), false, mp))
	require.NoError(t, vector.AppendFixed(res.Vecs[3], int32(0), false, mp))
	// FlushDeltaLoc rows: the tombstone objects.
	require.NoError(t, vector.AppendBytes(res.Vecs[0], []byte("obj-id"), false, mp))
	require.NoError(t, vector.AppendBytes(res.Vecs[1], statsBytes, false, mp))
	require.NoError(t, vector.AppendFixed(res.Vecs[2], int8(FlushDeltaLoc), false, mp))
	require.NoError(t, vector.AppendFixed(res.Vecs[3], int32(0), false, mp))
	res.SetRowCount(2)

	names, ok, err := TryExtractRemoteDeleteObjectNames(res, mp)
	require.NoError(t, err)
	require.True(t, ok, "the standalone deletion remote layout adopts")
	require.Equal(t, want, names,
		"adoption enumerates every flushed tombstone object and excludes the retained-block class")

	// A FlushDeltaLoc row whose DeltaLoc cannot decode rejects the ack.
	corrupt := makeDelRemoteBatch()
	corrupt.Vecs[4] = vector.NewVec(types.T_uint64.ToType())
	defer func() {
		corrupt.Vecs[4].Free(mp)
		corrupt.Vecs[4] = nil
		corrupt.Clean(mp)
	}()
	require.NoError(t, vector.AppendBytes(corrupt.Vecs[0], []byte("obj-id"), false, mp))
	require.NoError(t, vector.AppendBytes(corrupt.Vecs[1], []byte("not-a-batch"), false, mp))
	require.NoError(t, vector.AppendFixed(corrupt.Vecs[2], int8(FlushDeltaLoc), false, mp))
	require.NoError(t, vector.AppendFixed(corrupt.Vecs[3], int32(0), false, mp))
	corrupt.SetRowCount(1)
	_, _, err = TryExtractRemoteDeleteObjectNames(corrupt, mp)
	require.Error(t, err,
		"a flushed tombstone row whose stats batch cannot decode rejects the ack fail-closed")

	// Foreign attributes are ordinary data rows.
	foreign := batch.NewWithSize(4)
	foreign.SetAttributes([]string{"a", "b", "c", "d"})
	foreign.Vecs[0] = vector.NewVec(types.T_text.ToType())
	foreign.Vecs[1] = vector.NewVec(types.T_text.ToType())
	foreign.Vecs[2] = vector.NewVec(types.T_int8.ToType())
	foreign.Vecs[3] = vector.NewVec(types.T_int32.ToType())
	defer foreign.Clean(mp)
	require.NoError(t, vector.AppendBytes(foreign.Vecs[0], []byte("x"), false, mp))
	require.NoError(t, vector.AppendBytes(foreign.Vecs[1], []byte("y"), false, mp))
	require.NoError(t, vector.AppendFixed(foreign.Vecs[2], int8(FlushDeltaLoc), false, mp))
	require.NoError(t, vector.AppendFixed(foreign.Vecs[3], int32(0), false, mp))
	foreign.SetRowCount(1)
	_, ok, err = TryExtractRemoteDeleteObjectNames(foreign, mp)
	require.NoError(t, err)
	require.False(t, ok,
		"attributes the producer never emits never look like ownership metadata")
}
