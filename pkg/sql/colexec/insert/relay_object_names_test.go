package insert

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

// TestIssue25590InsertRelayObjectNames pins the r23 standalone-insert
// intake classifier (issue #25590 round-23 review, finding 3): every
// persisted insert object of the writer's own result batch is
// adoptable, non-matching attr sets are ordinary data, and a stats
// value that cannot carry an ObjectStats rejects the ack before the
// fragment can release its local owner.
func TestIssue25590InsertRelayObjectNames(t *testing.T) {
	proc := testutil.NewProc(t)

	bat := colexec.AllocCNS3ResultBat(false)
	defer bat.Clean(proc.Mp())
	want := make([]string, 0, 3)
	for i := 0; i < 3; i++ {
		rowid := types.RandomRowid()
		oid := rowid.BorrowObjectID()
		stats := objectio.NewObjectStatsWithObjectID(oid, false, false, true)
		require.NoError(t, objectio.SetObjectStatsBlkCnt(stats, 1))
		require.NoError(t, objectio.SetObjectStatsRowCnt(stats, 1))
		require.NoError(t, colexec.ExpandObjectStatsToBatch(
			proc.Mp(), false, bat, true, *stats))
		want = append(want, stats.ObjectName().String())
	}
	bat.SetRowCount(bat.Vecs[1].Length())
	require.NotEmpty(t, bat.Attrs[0])
	require.NotEmpty(t, bat.Attrs[1])

	names, ok, err := TryExtractS3InsertObjectNames(bat)
	require.NoError(t, err)
	require.True(t, ok, "the standalone insert result layout adopts")
	require.Equal(t, want, names,
		"adoption enumerates every persisted insert object, one name per stats row")

	// An empty result batch still reports its class with no names.
	empty := colexec.AllocCNS3ResultBat(false)
	defer empty.Clean(proc.Mp())
	empty.SetRowCount(0)
	_, ok, err = TryExtractS3InsertObjectNames(empty) // zero rows: not the layout (no rows to name)
	require.NoError(t, err)
	require.False(t, ok, "a zero-row batch is ordinary for the intake: nothing to adopt")

	// Foreign attributes are ordinary data rows, not ownership metadata.
	foreign := batch.NewWithSize(2)
	foreign.SetAttributes([]string{"user_col_a", "user_col_b"})
	foreign.Vecs[0] = vector.NewVec(types.T_text.ToType())
	foreign.Vecs[1] = vector.NewVec(types.T_binary.ToType())
	defer foreign.Clean(proc.Mp())
	require.NoError(t, vector.AppendBytes(
		foreign.Vecs[0], []byte("committed_object_name"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(
		foreign.Vecs[1], []byte("short"), false, proc.Mp()))
	foreign.SetRowCount(1)
	_, ok, err = TryExtractS3InsertObjectNames(foreign)
	require.NoError(t, err)
	require.False(t, ok,
		"attributes the producer never emits never look like ownership metadata")

	// A stats value too short to carry an ObjectStats rejects the ack.
	broken := colexec.AllocCNS3ResultBat(false)
	defer broken.Clean(proc.Mp())
	require.NoError(t, vector.AppendBytes(
		broken.Vecs[0], []byte("block-info-bytes"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(
		broken.Vecs[1], make([]byte, 4), false, proc.Mp()))
	broken.SetRowCount(1)
	_, _, err = TryExtractS3InsertObjectNames(broken)
	require.Error(t, err,
		"a result row claiming a persisted object it cannot prove rejects the ack fail-closed")
}
