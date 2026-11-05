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

package window

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestStreamingRowNumberOwnershipAndBoundaries(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	child := colexec.NewMockOperator()
	spec := makeWindowSpec()
	spec.GetW().Name = "row_number"
	spec.GetW().WindowFunc = newFunExpr("row_number")
	spec.GetW().PartitionBy = nil
	spec.GetW().OrderBy = nil
	arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)}}
	arg.AppendChild(child)
	registry, e := mpool.NewAllocationAccountRegistry(1, 128)
	require.NoError(t, e)
	account, e := registry.Open(1 << 20)
	require.NoError(t, e)
	require.NoError(t, arg.SetAllocationAccount(account))
	t.Cleanup(func() {
		arg.Reset(proc, false, nil)
		arg.Free(proc, false, nil)
		child.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, mp.CurrNB())
		require.Zero(t, account.Snapshot().Used)
		require.NoError(t, arg.ClearAllocationAccount(account))
	})
	makeBatch := func(n int) *batch.Batch {
		b := batch.NewWithSize(1)
		b.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		for range n {
			require.NoError(t, vector.AppendBytes(b.Vecs[0], []byte("borrowed"), false, mp))
		}
		b.SetRowCount(n)
		return b
	}
	for range 2 {
		a, b := makeBatch(8192), makeBatch(1)
		child.WithBatchs([]*batch.Batch{a, b})
		require.NoError(t, arg.Prepare(proc))
		out, e := arg.Call(proc)
		require.NoError(t, e)
		require.Same(t, a.Vecs[0], out.Batch.Vecs[0])
		require.Nil(t, arg.ctr.bat)
		require.Equal(t, int64(8192), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[1], 8191))
		out, e = arg.Call(proc)
		require.NoError(t, e)
		require.Same(t, b.Vecs[0], out.Batch.Vecs[0])
		require.Equal(t, int64(8193), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[1], 0))
		require.Equal(t, []byte("borrowed"), a.Vecs[0].GetBytesAt(0))
		arg.Reset(proc, false, nil)
		child.Free(proc, false, nil)
		require.Zero(t, account.Snapshot().Used)
	}
	child.WithBatchs([]*batch.Batch{makeBatch(1)})
	require.NoError(t, arg.Prepare(proc))
	arg.ctr.ordinalCount = math.MaxInt64
	_, e = arg.Call(proc)
	require.ErrorContains(t, e, "INT64")
	arg.Reset(proc, true, e)
	child.Free(proc, false, nil)
	ctx, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = ctx
	_, e = arg.Call(proc)
	require.ErrorIs(t, e, context.Canceled)
}

func TestStreamingRowNumberMultipleWideBorrowAndBudget(t *testing.T) {
	for _, limit := range []uint64{1, 64 << 10} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			child := colexec.NewMockOperator()
			specs := make([]*plan.Expr, 2)
			for i := range specs {
				specs[i] = makeWindowSpec()
				w := specs[i].GetW()
				w.Name = "row_number"
				w.WindowFunc = newFunExpr("row_number")
				w.PartitionBy = nil
				w.OrderBy = nil
			}
			arg := &Window{WinSpecList: specs, Aggs: []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t), newRowNumberAggExpr(t)}}
			arg.AppendChild(child)
			registry, e := mpool.NewAllocationAccountRegistry(1, 128)
			require.NoError(t, e)
			account, e := registry.Open(limit)
			require.NoError(t, e)
			require.NoError(t, arg.SetAllocationAccount(account))
			t.Cleanup(func() {
				arg.Reset(proc, false, nil)
				arg.Free(proc, false, nil)
				child.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, mp.CurrNB())
				require.Zero(t, account.Snapshot().Used)
				require.NoError(t, arg.ClearAllocationAccount(account))
			})
			b := batch.NewWithSize(1)
			b.Vecs[0] = vector.NewVec(types.T_varchar.ToType())
			require.NoError(t, vector.AppendBytes(b.Vecs[0], []byte(strings.Repeat("wide", 1<<18)), false, mp))
			b.SetRowCount(1)
			child.WithBatchs([]*batch.Batch{b})
			require.NoError(t, arg.Prepare(proc))
			out, e := arg.Call(proc)
			if limit == 1 {
				require.True(t, moerr.IsMoErrCode(e, moerr.ErrOOM), "%v", e)
				return
			}
			require.NoError(t, e)
			require.Same(t, b.Vecs[0], out.Batch.Vecs[0])
			require.NotSame(t, out.Batch.Vecs[1], out.Batch.Vecs[2])
			require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[1], 0))
			require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](out.Batch.Vecs[2], 0))
			require.Less(t, account.Snapshot().Used, uint64(64<<10), "wide child payload must not be copied by ROW_NUMBER")
		})
	}
}
