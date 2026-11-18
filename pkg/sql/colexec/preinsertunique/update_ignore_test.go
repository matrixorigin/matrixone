// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package preinsertunique

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

type updateIgnoreTestRow struct {
	id                     uint32
	oldU, newU, oldV, newV int32
	uNull                  bool
	conflictU, conflictV   bool
}

func TestUpdateIgnoreAtomicKeysAcrossBatchesAndReset(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &PreInsertUnique{PreInsertCtx: &plan.PreInsertUkCtx{
		UpdateIgnoreArbitration: true,
		UpdateRowId:             0, UpdateOldKeys: []int32{1, 4}, UpdateNewKeys: []int32{2, 5},
		UpdateInitialConflicts: []int32{3, 6}, UpdateOutputs: []int32{7},
	}}
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	account, err := registry.Open(1 << 30)
	require.NoError(t, err)
	require.NoError(t, arg.SetAllocationAccount(account))
	var batches []*batch.Batch
	defer func() {
		arg.Free(proc, false, nil)
		require.NoError(t, arg.ClearAllocationAccount(account))
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
		for _, b := range batches {
			b.Clean(proc.Mp())
		}
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}()
	require.NoError(t, arg.Prepare(proc))
	run := func(rows []updateIgnoreTestRow) []int32 {
		t.Helper()
		b := batch.NewWithSize(8)
		batches = append(batches, b)
		for i, typ := range []types.Type{types.T_Rowid.ToType(), types.T_int32.ToType(), types.T_int32.ToType(), types.T_bool.ToType(), types.T_int32.ToType(), types.T_int32.ToType(), types.T_bool.ToType(), types.T_int32.ToType()} {
			b.Vecs[i] = vector.NewVec(typ)
		}
		var block types.Blockid
		for _, r := range rows {
			require.NoError(t, vector.AppendFixed(b.Vecs[0], types.NewRowid(&block, r.id), false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(b.Vecs[1], r.oldU, r.uNull, proc.Mp()))
			require.NoError(t, vector.AppendFixed(b.Vecs[2], r.newU, r.uNull, proc.Mp()))
			require.NoError(t, vector.AppendFixed(b.Vecs[3], r.conflictU, false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(b.Vecs[4], r.oldV, false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(b.Vecs[5], r.newV, false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(b.Vecs[6], r.conflictV, false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(b.Vecs[7], int32(r.id), false, proc.Mp()))
		}
		b.SetRowCount(len(rows))
		result, err := arg.callUpdateIgnore(proc, vm.CallResult{Batch: b})
		require.NoError(t, err)
		var ids []int32
		if result.Batch != nil {
			for i := 0; i < result.Batch.RowCount(); i++ {
				ids = append(ids, vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[0], i))
			}
		}
		return ids
	}
	require.Equal(t, []int32{2}, run([]updateIgnoreTestRow{
		{id: 1, oldU: 10, newU: 500, oldV: 100, newV: 300, conflictV: true},
		{id: 2, oldU: 20, newU: 500, oldV: 200, newV: 400},
	}))
	require.Equal(t, []int32{3, 5}, run([]updateIgnoreTestRow{
		{id: 3, oldU: 30, newU: 20, oldV: 300, newV: 501, conflictU: true},
		{id: 4, oldU: 40, newU: 500, oldV: 400, newV: 502},
		{id: 5, uNull: true, oldV: 500, newV: 503, conflictU: true},
	}))
	arg.Reset(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	require.Equal(t, []int32{4}, run([]updateIgnoreTestRow{{id: 4, oldU: 40, newU: 500, oldV: 400, newV: 502}}))
	require.Equal(t, []int32{6, 8}, run([]updateIgnoreTestRow{
		{id: 6, oldU: 60, newU: 1000, oldV: 600, newV: 1000},
		{id: 7, oldU: 70, newU: 1001, oldV: 700, newV: 1000},
		{id: 8, oldU: 80, newU: 1001, oldV: 800, newV: 1002},
	}))
}

func TestUpdateIgnoreArbiterRejectsMalformedContextAndCancellation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &PreInsertUnique{PreInsertCtx: &plan.PreInsertUkCtx{UpdateIgnoreArbitration: true, UpdateNewKeys: []int32{1}}}
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	account, err := registry.Open(1 << 30)
	require.NoError(t, err)
	require.NoError(t, arg.SetAllocationAccount(account))
	defer func() {
		arg.Free(proc, false, nil)
		require.NoError(t, arg.ClearAllocationAccount(account))
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}()
	require.Error(t, arg.Prepare(proc))
	arg.PreInsertCtx = &plan.PreInsertUkCtx{UpdateIgnoreArbitration: true, UpdateRowId: 0,
		UpdateOldKeys: []int32{1}, UpdateNewKeys: []int32{2}, UpdateInitialConflicts: []int32{3}, UpdateOutputs: []int32{0}}
	require.NoError(t, arg.Prepare(proc))
	b := batch.NewWithSize(4)
	for i, typ := range []types.Type{types.T_Rowid.ToType(), types.T_int32.ToType(), types.T_int32.ToType(), types.T_bool.ToType()} {
		b.Vecs[i] = vector.NewVec(typ)
	}
	defer b.Clean(proc.Mp())
	var block types.Blockid
	require.NoError(t, vector.AppendFixed(b.Vecs[0], types.NewRowid(&block, 1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(b.Vecs[1], int32(10), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(b.Vecs[2], int32(20), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(b.Vecs[3], false, false, proc.Mp()))
	b.SetRowCount(1)
	cancelled, cancel := context.WithCancel(proc.Ctx)
	cancel()
	proc.Ctx = cancelled
	_, err = arg.callUpdateIgnore(proc, vm.CallResult{Batch: b})
	require.ErrorIs(t, err, context.Canceled)
}
func TestUpdateIgnoreArbiterAllocationFailureCleansUp(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	arg := &PreInsertUnique{PreInsertCtx: &plan.PreInsertUkCtx{UpdateIgnoreArbitration: true, UpdateRowId: 0,
		UpdateOldKeys: []int32{1}, UpdateNewKeys: []int32{2}, UpdateInitialConflicts: []int32{3}, UpdateOutputs: []int32{0}}}
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	require.NoError(t, arg.SetAllocationAccount(account))
	var filler []byte
	defer func() {
		if filler != nil {
			proc.Mp().Free(filler)
		}
		arg.Free(proc, false, nil)
		require.NoError(t, arg.ClearAllocationAccount(account))
		require.Zero(t, account.Snapshot().Used)
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}()
	require.NoError(t, arg.Prepare(proc))
	b := batch.NewWithSize(4)
	for i, typ := range []types.Type{types.T_Rowid.ToType(), types.T_int32.ToType(), types.T_int32.ToType(), types.T_bool.ToType()} {
		b.Vecs[i] = vector.NewVec(typ)
	}
	defer b.Clean(proc.Mp())
	var block types.Blockid
	require.NoError(t, vector.AppendFixed(b.Vecs[0], types.NewRowid(&block, 1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(b.Vecs[1], int32(10), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(b.Vecs[2], int32(20), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(b.Vecs[3], false, false, proc.Mp()))
	b.SetRowCount(1)
	available := account.Snapshot().Limit - account.Snapshot().Used
	require.Greater(t, available, uint64(1024))
	filler, err = proc.Mp().AllocAccounted(int(available-32), account, updateIgnoreAllocationOwner, updateIgnoreVectorData)
	require.NoError(t, err)
	result, err := arg.callUpdateIgnore(proc, vm.CallResult{Batch: b})
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Nil(t, result.Batch, "no partial result may be published on allocation failure")
	proc.Mp().Free(filler)
	filler = nil
	arg.Reset(proc, true, err)
	require.Zero(t, account.Snapshot().Used, "the failed attempt must release every retained key and scratch buffer")
}
