package indexjoin

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIndexJoinResetReleasesGenerationResultBuffer(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	join := &IndexJoin{}
	defer join.Free(proc, false, nil)
	join.ctr.buf = batch.NewOffHeapWithSize(1)
	join.ctr.buf.Vecs[0] = vector.NewOffHeapVecWithType(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(join.ctr.buf.Vecs[0], bytes.Repeat([]byte("x"), 1024), false, proc.Mp()))
	require.Greater(t, proc.Mp().CurrNB(), int64(0))

	join.Reset(proc, false, nil)
	require.Zero(t, proc.Mp().CurrNB(), "Reset must release the previous generation's result buffer")
	require.Nil(t, join.ctr.buf)
}
