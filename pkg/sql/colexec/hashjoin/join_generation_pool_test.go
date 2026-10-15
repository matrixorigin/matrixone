// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashjoin

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

func TestHashJoinResetReleasesGenerationJoinBatches(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	source := batch.NewWithSize(1)
	source.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(source.Vecs[0], bytes.Repeat([]byte("x"), 1024), false, proc.Mp()))
	source.SetRowCount(1)
	defer source.Clean(proc.Mp())
	baseline := proc.Mp().CurrNB()

	join := &HashJoin{}
	defer join.Free(proc, false, nil)
	join.ctr.joinBats = make([]*batch.Batch, 2)
	join.ctr.joinBats[0], join.ctr.cfs1 = colexec.NewJoinBatch(source, proc.Mp())
	join.ctr.joinBats[1], join.ctr.cfs2 = colexec.NewJoinBatch(source, proc.Mp())
	require.NoError(t, colexec.SetJoinBatchValues(join.ctr.joinBats[0], source, 0, 1, join.ctr.cfs1))
	require.NoError(t, colexec.SetJoinBatchValues(join.ctr.joinBats[1], source, 0, 1, join.ctr.cfs2))
	require.Greater(t, proc.Mp().CurrNB(), baseline)

	join.Reset(proc, false, nil)
	require.Equal(t, baseline, proc.Mp().CurrNB(), "Reset must release allocations owned by the previous process")
	require.Nil(t, join.ctr.joinBats[0])
	require.Nil(t, join.ctr.joinBats[1])
	require.Nil(t, join.ctr.cfs1)
	require.Nil(t, join.ctr.cfs2)
}
