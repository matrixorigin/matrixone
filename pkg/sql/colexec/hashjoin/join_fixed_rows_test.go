// Copyright 2026 Matrix Origin
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

package hashjoin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestHashJoinFixedBuildRows(t *testing.T) {
	typ := types.T_int32.ToType()
	tc := newTestCase(t, []types.Type{typ, typ}, []colexec.ResultPos{
		colexec.NewResultPos(0, 0), colexec.NewResultPos(1, 1),
	}, [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
	t.Cleanup(func() {
		tc.arg.Free(tc.proc, false, nil)
		tc.barg.Free(tc.proc, false, nil)
		tc.proc.Free()
		tc.cancel()
		require.Zero(t, tc.proc.Mp().CurrNB())
	})
	tc.arg.JoinType = plan.Node_INNER
	tc.arg.NonEqCond = nil
	// The first chunk exercises the gather; its singleton tail uses UnionOne.
	const count = hashmap.UnitLimit + 1
	build := batch.NewWithSize(2)
	t.Cleanup(func() { build.Clean(tc.proc.Mp()) })
	for i := range build.Vecs {
		build.Vecs[i] = vector.NewVec(typ)
	}
	for i := range count {
		require.NoError(t, vector.AppendFixed(build.Vecs[0], int32(1), false, tc.proc.Mp()))
		require.NoError(t, vector.AppendFixed(build.Vecs[1], int32(i), i%7 == 0, tc.proc.Mp()))
	}
	build.SetRowCount(count)
	probe := makeInt32Batch(tc.proc, []int32{1, 1, 9})
	t.Cleanup(func() { probe.Clean(tc.proc.Mp()) })
	resetHashBuildChildrenWithBatch(tc.barg, build)
	resetChildrenWithBatch(tc.arg, probe)
	require.NoError(t, tc.arg.Prepare(tc.proc))
	require.NoError(t, tc.barg.Prepare(tc.proc))
	res, err := vm.Exec(tc.barg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
	res, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.NotNil(t, res.Batch)
	require.Equal(t, count*2, res.Batch.RowCount())
	left := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[0])
	right := vector.MustFixedColNoTypeCheck[int32](res.Batch.Vecs[1])
	for i := range count * 2 {
		require.Equal(t, int32(1), left[i])
		ordinal := i % count
		require.Equal(t, ordinal%7 == 0, res.Batch.Vecs[1].IsNull(uint64(i)))
		if ordinal%7 != 0 {
			require.Equal(t, int32(ordinal), right[i])
		}
	}
	res, err = vm.Exec(tc.arg, tc.proc)
	require.NoError(t, err)
	require.Nil(t, res.Batch)
}
