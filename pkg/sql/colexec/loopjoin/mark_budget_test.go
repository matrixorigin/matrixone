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
	"context"
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
	for _, tc := range []struct {
		name                      string
		cap                       int64
		repeated, cancel, strings bool
	}{
		{"distinct fallback", 2 << 20, false, false, false},
		{"distinct indexed", 32 << 20, false, false, false},
		{"repeated indexed", 2 << 20, true, false, false},
		{"cancel fallback", 2 << 20, false, true, false},
		{"string repeated indexed", 32 << 20, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cap := tc.cap
			mp, err := mpool.NewMPool("mark budget", cap, mpool.NoFixed)
			require.NoError(t, err)
			proc := testutil.NewProcessWithMPool(t, "", mp)
			proc.SetMessageBoard(message.NewMessageBoard())
			srcMP := mpool.MustNewZero()
			source := batch.NewWithSize(1)
			var probe *batch.Batch
			var b *hashbuild.HashBuild
			var join *LoopJoin
			t.Cleanup(func() {
				if join != nil {
					join.Free(proc, false, nil)
				}
				if b != nil {
					b.Free(proc, false, nil)
				}
				if probe != nil {
					probe.Clean(mp)
				}
				source.Clean(srcMP)
				proc.GetMessageBoard().Reset()
				proc.Free()
				require.Zero(t, mp.CurrNB())
				require.Zero(t, srcMP.CurrNB())
			})
			vals := make([]int32, 100000)
			for i := range vals {
				vals[i] = int32(i)
				if tc.repeated {
					vals[i] = 1
				}
			}
			typ := types.T_int32.ToType()
			if tc.strings {
				typ = types.T_varchar.ToType()
				values := make([]string, len(vals))
				for i := range values {
					values[i] = "present"
				}
				source.Vecs[0] = testutil.MakeVarcharVector(values, []uint64{99999}, srcMP)
				probe = batch.NewWithSize(1)
				probe.Vecs[0] = testutil.MakeVarcharVector([]string{"present", "missing", ""}, nil, mp)
				probe.SetRowCount(3)
			} else {
				source.Vecs[0] = testutil.MakeInt32Vector(vals, []uint64{99999}, srcMP)
				probe = makeInt32LoopJoinBatch(mp, []int32{1, 200000, 0})
			}
			source.SetRowCount(len(vals))
			nulls.Add(probe.Vecs[0].GetNulls(), 2)
			b = &hashbuild.HashBuild{NeedHashMap: true, NeedBatches: true, OptionalIndex: true, Conditions: []*plan.Expr{{Typ: plan.Type{Id: int32(typ.Oid)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}}}, JoinMapTag: 255, JoinMapRefCnt: 1}
			b.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}))
			join = &LoopJoin{JoinType: plan.Node_MARK, NonEqCond: markEquality(t, typ.Oid, "="), JoinMapTag: 255, LeftTypes: []types.Type{typ}, ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0), colexec.NewResultPos(-1, 0)}}
			join.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probe}))
			require.NoError(t, b.Prepare(proc))
			require.NoError(t, join.Prepare(proc))
			_, err = vm.Exec(b, proc)
			require.NoError(t, err)
			if tc.cancel {
				original := proc.Ctx
				ctx, cancel := context.WithCancel(original)
				defer cancel()
				proc.Ctx = ctx
				// The first probe misses every RHS batch: cancellation must not
				// wait for the full nested scan to finish.
				vector.MustFixedColNoTypeCheck[int32](probe.Vecs[0])[0] = 200000
				calls := 0
				join.ctr.expr = cancelMarkKey{join.ctr.expr, func() { calls++; cancel() }}
				res, err := vm.Exec(join, proc)
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 1, calls)
				require.Nil(t, res.Batch)
				require.Nil(t, join.ctr.markHash.iter)
				join.Reset(proc, true, err)
				proc.Ctx = original
				vector.MustFixedColNoTypeCheck[int32](probe.Vecs[0])[0] = 1
				join.ctr.expr = join.ctr.expr.(cancelMarkKey).ExpressionExecutor
				b.Reset(proc, false, nil)
				proc.GetMessageBoard().Reset()
				b.SetChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}), 0)
				join.SetChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{probe}), 0)
				require.NoError(t, b.Prepare(proc))
				require.NoError(t, join.Prepare(proc))
				_, err = vm.Exec(b, proc)
				require.NoError(t, err)
			}
			res, err := vm.Exec(join, proc)
			require.NoError(t, err)
			require.Equal(t, cap > 2<<20 || tc.repeated, join.ctr.markHash.iter != nil)
			require.Equal(t, 3, res.Batch.RowCount())
			require.Equal(t, []bool{true, false, false}, vector.MustFixedColNoTypeCheck[bool](res.Batch.Vecs[1]))
			require.False(t, res.Batch.Vecs[1].GetNulls().Contains(0))
			require.True(t, res.Batch.Vecs[1].GetNulls().Contains(1))
			require.True(t, res.Batch.Vecs[1].GetNulls().Contains(2))
		})
	}
}
