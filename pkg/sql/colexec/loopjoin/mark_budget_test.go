// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loopjoin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/stretchr/testify/require"
)

func TestMarkIndexBudgetFallbackResults(t *testing.T) {
	for _, cap := range []int64{2 << 20, 32 << 20} {
		mp, err := mpool.NewMPool("mark budget", cap, mpool.NoFixed)
		require.NoError(t, err)
		proc := testutil.NewProcessWithMPool(t, "", mp)
		proc.SetMessageBoard(message.NewMessageBoard())
		srcMP := mpool.MustNewZero()
		source := batch.NewWithSize(1)
		vals := make([]int32, 100000)
		for i := range vals {
			vals[i] = int32(i)
		}
		source.Vecs[0] = testutil.MakeInt32Vector(vals, []uint64{99999}, srcMP)
		source.SetRowCount(len(vals))
		probe := makeInt32LoopJoinBatch(mp, []int32{1, 200000, 0})
		nulls.Add(probe.Vecs[0].GetNulls(), 2)
		b := &hashbuild.HashBuild{NeedHashMap: true, NeedBatches: true, OptionalIndex: true, Conditions: []*plan.Expr{{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}}}, JoinMapTag: 255, JoinMapRefCnt: 1}
		b.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}))
		join := &LoopJoin{JoinType: plan.Node_MARK, NonEqCond: markEquality(t, types.T_int32, "="), JoinMapTag: 255, LeftTypes: []types.Type{types.T_int32.ToType()}, ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(-1, 0)}}
		join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probe}))
		func() {
			defer func() {
				join.Free(proc, false, nil)
				b.Free(proc, false, nil)
				probe.Clean(mp)
				source.Clean(srcMP)
				proc.GetMessageBoard().Reset()
				proc.Free()
				require.Zero(t, mp.CurrNB())
				require.Zero(t, srcMP.CurrNB())
			}()
			require.NoError(t, b.Prepare(proc))
			require.NoError(t, join.Prepare(proc))
			_, err := vm.Exec(b, proc)
			require.NoError(t, err)
			res, err := vm.Exec(join, proc)
			require.NoError(t, err)
			require.Equal(t, cap > 2<<20, join.ctr.markHash.iter != nil)
			require.Equal(t, 3, res.Batch.RowCount())
			require.Equal(t, []bool{true, false, false}, vector.MustFixedColNoTypeCheck[bool](res.Batch.Vecs[1]))
			require.False(t, res.Batch.Vecs[1].GetNulls().Contains(0))
			require.True(t, res.Batch.Vecs[1].GetNulls().Contains(1))
			require.True(t, res.Batch.Vecs[1].GetNulls().Contains(2))
		}()
	}
}
