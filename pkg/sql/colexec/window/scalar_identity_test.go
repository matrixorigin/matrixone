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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func newScalarIdentityForTest(t *testing.T, limit uint64) (*Window, *mpool.AllocationAccount, *mpool.AllocationAccountRegistry) {
	t.Helper()
	r, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	a, err := r.Open(limit)
	require.NoError(t, err)
	f, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "row_number", nil)
	require.NoError(t, err)
	w := &Window{ScalarSetReaggregation: true, WinSpecList: []*plan.Expr{{Typ: f.Typ, Expr: &plan.Expr_W{W: &plan.WindowSpec{Name: "row_number", WindowFunc: f}}}}}
	require.NoError(t, w.SetAllocationAccount(a))
	return w, a, r
}

func TestScalarIdentityStreamingAndReset(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	w, a, r := newScalarIdentityForTest(t, 1<<20)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt64Vector(make([]int64, colexec.DefaultBatchSize+7), nil, proc.Mp())
	bat.SetRowCount(colexec.DefaultBatchSize + 7)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	w.AppendChild(child)
	defer child.Free(proc, false, nil)
	defer w.Free(proc, false, nil)
	require.NoError(t, w.Prepare(proc))
	var seen uint64
	for {
		out, err := w.Call(proc)
		require.NoError(t, err)
		if out.Batch == nil {
			break
		}
		require.LessOrEqual(t, out.Batch.RowCount(), colexec.DefaultBatchSize)
		require.Equal(t, types.T_uint64, out.Batch.Vecs[1].GetType().Oid)
		for _, id := range vector.MustFixedColWithTypeCheck[uint64](out.Batch.Vecs[1]) {
			seen++
			require.Equal(t, seen, id)
		}
	}
	require.Equal(t, uint64(colexec.DefaultBatchSize+7), seen)
	require.Zero(t, a.Snapshot().Used)
	w.Reset(proc, false, nil)
	require.Zero(t, w.scalarIdentity.counter)
	require.NoError(t, w.ClearAllocationAccount(a))
	_, _, err := r.CompleteTerminal(a)
	require.NoError(t, err)
}

func TestScalarIdentityRetainsFullUnaccountedSource(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	w, a, r := newScalarIdentityForTest(t, 4096)
	source := testutil.MakeInt64Vector(make([]int64, 4096), nil, proc.Mp())
	defer source.Free(proc.Mp())
	selection, err := vector.NewAllocationAccountSelection(a, mpool.AllocationOwnerWindow, 1, 2, 3, 4)
	require.NoError(t, err)
	view, err := source.WindowWithAllocation(4000, 4096, proc.Mp(), selection)
	require.NoError(t, err)
	defer view.Free(proc.Mp())
	offset, err := view.WindowWithAllocation(95, 96, proc.Mp(), selection)
	require.NoError(t, err)
	b := batch.NewWithSize(1)
	b.Vecs[0] = offset
	b.SetRowCount(1)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{b})
	w.AppendChild(child)
	require.NoError(t, w.Prepare(proc))
	_, err = w.Call(proc)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	w.Free(proc, true, err)
	child.Free(proc, true, err)
	require.Zero(t, a.Snapshot().Used)
	require.NoError(t, w.ClearAllocationAccount(a))
	_, _, err = r.CompleteTerminal(a)
	require.NoError(t, err)
}

func TestScalarIdentityFailClosed(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	w, a, r := newScalarIdentityForTest(t, 1<<20)
	b := batch.NewWithSize(1)
	b.Vecs[0] = testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp())
	b.SetRowCount(1)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{b})
	w.AppendChild(child)
	require.NoError(t, w.Prepare(proc))
	w.scalarIdentity.counter = math.MaxUint64
	_, err := w.Call(proc)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrScalarSetResourceBudgetExceeded))
	w.Reset(proc, true, err)
	require.Zero(t, a.Snapshot().Used)
	require.Zero(t, w.scalarIdentity.counter)
	w.WinSpecList[0].GetW().OrderBy = []*plan.OrderBySpec{{}}
	require.Error(t, w.Prepare(proc))
	w.Free(proc, false, nil)
	child.Free(proc, false, nil)
	require.NoError(t, w.ClearAllocationAccount(a))
	_, _, err = r.CompleteTerminal(a)
	require.NoError(t, err)
}

func TestScalarIdentityRetainedValidityAdmission(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	w, a, r := newScalarIdentityForTest(t, 4096)
	source := testutil.MakeInt64Vector([]int64{1}, nil, proc.Mp())
	defer source.Free(proc.Mp())
	released := 0
	raw := make([]byte, 8192)
	lease, err := vector.NewRefCountedBufferLease(raw, 0, func() { released++ })
	require.NoError(t, err)
	require.NoError(t, source.GetNulls().InstallBorrowedValidity(raw, 0, 1, 1, lease))
	lease.Release()
	view, err := source.Window(0, 1)
	require.NoError(t, err)
	b := batch.NewWithSize(1)
	b.Vecs[0] = view
	b.SetRowCount(1)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{b})
	w.AppendChild(child)
	defer child.Free(proc, true, nil)
	defer w.Free(proc, true, nil)
	require.GreaterOrEqual(t, view.RetainedBytes(), uint64(8192))
	require.True(t, view.GetNulls().HasBorrowedValidity(), "measuring retention must not materialize validity")
	require.NoError(t, w.Prepare(proc))
	_, err = w.Call(proc)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	w.Free(proc, true, err)
	child.Free(proc, true, err)
	source.Free(proc.Mp())
	require.Equal(t, 1, released)
	require.Zero(t, a.Snapshot().Used)
	require.NoError(t, w.ClearAllocationAccount(a))
	_, _, err = r.CompleteTerminal(a)
	require.NoError(t, err)
}
