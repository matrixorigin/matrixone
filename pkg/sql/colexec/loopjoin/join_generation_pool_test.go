package loopjoin

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestLoopJoinResetReleasesGenerationJoinBatch(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	source := batch.NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(source.Vecs[0], bytes.Repeat([]byte("x"), 1024), false, proc.Mp()))
	source.SetRowCount(1)
	defer source.Clean(proc.Mp())
	baseline := proc.Mp().CurrNB()

	join := &LoopJoin{}
	defer join.Free(proc, false, nil)
	join.ctr.joinBat, join.ctr.cfs = colexec.NewJoinBatch(source, proc.Mp())
	require.NoError(t, colexec.SetJoinBatchValues(join.ctr.joinBat, source, 0, 1, join.ctr.cfs))
	require.Greater(t, proc.Mp().CurrNB(), baseline)

	join.Reset(proc, false, nil)
	require.Equal(t, baseline, proc.Mp().CurrNB(), "Reset must release allocations owned by the previous process")
	require.Nil(t, join.ctr.joinBat)
	require.Nil(t, join.ctr.cfs)
}
