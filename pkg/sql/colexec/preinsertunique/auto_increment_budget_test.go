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

package preinsertunique

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestInsertIgnoreFinalKeysRespectMemoryBudgetAndReuse(t *testing.T) {
	mp, err := mpool.NewMPool("ignore-budget", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, "", mp)
	const rows = 1024
	ids, keys := make([]int32, rows), make([]int32, rows)
	flags, generated := make([]bool, rows), make([]bool, rows)
	for i := range generated {
		generated[i] = true
	}
	input := makeInsertIgnoreAutoIncrementBatch(t, proc, ids, keys, flags, flags, generated)
	defer input.Clean(mp)
	// NULL UKs are distinct. Only the final-PK set grows between batches.
	input.Vecs[1].Free(mp)
	input.Vecs[1] = vector.NewVec(types.T_int32.ToType())
	for range rows {
		require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(0), true, mp))
	}
	arg := newInsertIgnoreAutoIncrementArgument()
	defer arg.Free(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	var budgetErr error
	for part := 0; part < 128; part++ {
		values := vector.MustFixedColNoTypeCheck[int32](input.Vecs[0])
		for i := range values {
			values[i] = int32(part*rows + i + 1)
		}
		result, err := arg.callInsertIgnoreMultiDedup(proc, vm.CallResult{Batch: input})
		if err != nil {
			budgetErr = err
			break
		}
		require.Equal(t, rows, result.Batch.RowCount())
	}
	require.Error(t, budgetErr, "input-sized final-key storage must fail at the query budget")
	require.True(t, moerr.IsMoErrCode(budgetErr, moerr.ErrMPoolCapacity), "%v", budgetErr)
	arg.Reset(proc, true, budgetErr)
	require.NoError(t, arg.Prepare(proc))
	// These keys belonged to the failed statement; reuse must not retain them.
	values := vector.MustFixedColNoTypeCheck[int32](input.Vecs[0])
	for i := range values {
		values[i] = int32(i + 1)
	}
	result, err := arg.callInsertIgnoreMultiDedup(proc, vm.CallResult{Batch: input})
	require.NoError(t, err)
	require.Equal(t, rows, result.Batch.RowCount())
	arg.Free(proc, false, nil)
	input.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestInsertIgnoreAllocationFailureDoesNotPublishAndCanReset(t *testing.T) {
	for _, phase := range []string{"prepare", "output"} {
		t.Run(phase, func(t *testing.T) {
			mp, err := mpool.NewMPool("ignore-failure-"+phase, 1<<20, mpool.NoFixed)
			require.NoError(t, err)
			proc := testutil.NewProcessWithMPool(t, "", mp)
			proc.SetStatementLastInsertID(700)
			input := makeInsertIgnoreAutoIncrementBatch(t, proc,
				[]int32{1}, []int32{10}, []bool{false}, []bool{false}, []bool{true})
			payload := vector.NewVec(types.T_varchar.ToType())
			require.NoError(t, vector.AppendBytes(payload, []byte(strings.Repeat("x", 16384)), false, mp))
			input.Vecs = append(input.Vecs[:2:2], payload, input.Vecs[2], input.Vecs[3], input.Vecs[4])
			defer input.Clean(mp)
			arg := newInsertIgnoreAutoIncrementArgument()
			arg.PreInsertCtx.OutputColumns = 3
			arg.PreInsertCtx.ConflictColumns = []int32{3, 4}
			arg.PreInsertCtx.AutoIncrementGeneratedColumn = 5
			defer arg.Free(proc, false, nil)
			available := int64(0)
			if phase == "output" {
				require.NoError(t, arg.Prepare(proc))
				available = 4096 // small PK/key buffers fit; the payload cannot.
			}
			pressure, err := mp.Alloc(int(mp.Cap()-mp.CurrNB()-available), true)
			require.NoError(t, err)
			defer func() {
				if pressure != nil {
					mp.Free(pressure)
				}
			}()
			if phase == "prepare" {
				err = arg.Prepare(proc)
			} else {
				result, callErr := arg.callInsertIgnoreMultiDedup(proc, vm.CallResult{Batch: input})
				err = callErr
				require.Nil(t, result.Batch, "an incompletely built batch must not escape")
				require.Equal(t, 1, arg.ctr.buf.Vecs[0].Length(), "failure follows final-PK materialization")
			}
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrMPoolCapacity), "%v", err)
			require.Equal(t, uint64(700), proc.GetStatementLastInsertID())
			require.Equal(t, []int32{1}, vector.MustFixedColNoTypeCheck[int32](input.Vecs[0]))
			mp.Free(pressure)
			pressure = nil
			arg.Reset(proc, true, err)
			require.NoError(t, arg.Prepare(proc))
			result, err := arg.callInsertIgnoreMultiDedup(proc, vm.CallResult{Batch: input})
			require.NoError(t, err)
			require.Equal(t, 1, result.Batch.RowCount())
			require.Equal(t, uint64(1), proc.GetStatementLastInsertID())
			require.Equal(t, strings.Repeat("x", 16384), result.Batch.Vecs[2].GetStringAt(0))
			arg.Free(proc, false, nil)
			input.Clean(mp)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestInsertIgnoreResetAfterCancellationClearsFinalKeysAndFence(t *testing.T) {
	proc := testutil.NewProc(t)
	input := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{100}, []int32{10}, []bool{false}, []bool{false}, []bool{false})
	defer input.Clean(proc.Mp())
	arg := newInsertIgnoreAutoIncrementArgument()
	defer arg.Free(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	_, err := arg.callInsertIgnoreMultiDedup(proc, vm.CallResult{Batch: input})
	require.NoError(t, err)
	arg.Reset(proc, true, context.Canceled)
	require.NoError(t, arg.Prepare(proc))
	vector.MustFixedColNoTypeCheck[int32](input.Vecs[0])[0] = 1
	vector.MustFixedColNoTypeCheck[bool](input.Vecs[4])[0] = true
	result, err := arg.callInsertIgnoreMultiDedup(proc, vm.CallResult{Batch: input})
	require.NoError(t, err)
	require.Equal(t, []int32{1}, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0]))
	arg.Free(proc, false, nil)
	input.Clean(proc.Mp())
	require.Zero(t, proc.Mp().CurrNB())
}

func TestInsertIgnoreRejectedCandidateRunsRespectMemoryBudget(t *testing.T) {
	mp, err := mpool.NewMPool("ignore-rejected-budget", 1<<20, mpool.NoFixed)
	require.NoError(t, err)
	proc := testutil.NewProcessWithMPool(t, "", mp)
	input := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{1}, []int32{10}, []bool{false}, []bool{true}, []bool{true})
	defer input.Clean(mp)
	arg := newInsertIgnoreAutoIncrementArgument()
	defer arg.Free(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	pressure, err := mp.Alloc(int(mp.Cap()-mp.CurrNB()-4096), true)
	require.NoError(t, err)
	defer func() {
		if pressure != nil {
			mp.Free(pressure)
		}
	}()
	var budgetErr error
	var id int32
	// Alternating gaps model disjoint allocator ranges. Every row loses on UK;
	// neither accepted-key hash grows, and the retained candidates do not form
	// one arithmetic run. Leave 4 KiB available to cross the budget boundary
	// with small input rather than making this a volume test.
	for i := 0; i < 1024; i++ {
		id += int32(1 + i%2)
		vector.MustFixedColNoTypeCheck[int32](input.Vecs[0])[0] = id
		result, err := arg.callInsertIgnoreMultiDedup(proc, vm.CallResult{Batch: input})
		if err != nil {
			budgetErr = err
			break
		}
		require.True(t, result.Batch.IsEmpty())
	}
	require.True(t, moerr.IsMoErrCode(budgetErr, moerr.ErrMPoolCapacity), "%v", budgetErr)
	mp.Free(pressure)
	pressure = nil
	arg.Reset(proc, true, budgetErr)
	require.NoError(t, arg.Prepare(proc))
	vector.MustFixedColNoTypeCheck[int32](input.Vecs[0])[0] = 1
	vector.MustFixedColNoTypeCheck[bool](input.Vecs[3])[0] = false
	result, err := arg.callInsertIgnoreMultiDedup(proc, vm.CallResult{Batch: input})
	require.NoError(t, err)
	require.Equal(t, []int32{1}, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0]))
	arg.Free(proc, false, nil)
	input.Clean(mp)
	require.Zero(t, mp.CurrNB())
}

func TestInsertIgnoreFinalKeysAcceptConstNullUniqueKey(t *testing.T) {
	proc := testutil.NewProc(t)
	input := makeInsertIgnoreAutoIncrementBatch(t, proc,
		[]int32{1, 2}, []int32{0, 0}, []bool{false, false}, []bool{false, false}, []bool{true, true})
	defer input.Clean(proc.Mp())
	input.Vecs[1].Free(proc.Mp())
	input.Vecs[1] = vector.NewConstNull(types.T_int32.ToType(), 2, proc.Mp())
	arg := newInsertIgnoreAutoIncrementArgument(input)
	defer arg.Free(proc, false, nil)
	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.Equal(t, 2, result.Batch.RowCount())
	require.Equal(t, []int32{1, 2}, vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0]))
}
