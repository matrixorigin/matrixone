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

package dedupjoin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

type testAllocationOwner interface {
	SetAllocationAccount(*mpool.AllocationAccount) error
}

func installTestAllocation(t testing.TB, owners ...testAllocationOwner) *mpool.AllocationAccount {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 4_096)
	require.NoError(t, err)
	account, err := registry.Open(1 << 60)
	require.NoError(t, err)
	for _, owner := range owners {
		require.NoError(t, owner.SetAllocationAccount(account))
	}
	return account
}

func TestDedupJoinResultAndFinalizeBatchesUseAllocationAccount(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	arg := &DedupJoin{
		Result:     []colexec.ResultPos{{Rel: 0, Pos: 0}},
		LeftTypes:  []types.Type{types.T_int64.ToType()},
		RightTypes: []types.Type{types.T_int64.ToType()},
	}
	account := installTestAllocation(t, arg)
	require.NoError(t, arg.resetRBat())
	require.Same(t, arg.resultAllocation, arg.ctr.rbat.Vecs[0].AllocationAccountSelection())

	arg.ctr.matched = &bitmap.Bitmap{}
	first := batch.NewWithSize(0)
	first.SetRowCount(colexec.DefaultBatchSize)
	second := batch.NewWithSize(0)
	second.SetRowCount(1)
	arg.ctr.batches = []*batch.Batch{first, second}
	require.NoError(t, arg.ctr.finalize(arg, proc))
	require.Len(t, arg.ctr.buf, 2)
	for _, result := range arg.ctr.buf {
		require.Same(t, arg.resultAllocation, result.Vecs[0].AllocationAccountSelection())
	}
	require.Positive(t, account.Snapshot().Used)

	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.rbat)
	require.Empty(t, arg.ctr.buf)
	require.Zero(t, account.Snapshot().Used)
	require.NoError(t, arg.ClearAllocationAccount(account))
}

func TestDedupJoinResultAndFinalizeBatchesHonorAllocationCapacity(t *testing.T) {
	newAccount := func(t *testing.T) *mpool.AllocationAccount {
		registry, err := mpool.NewAllocationAccountRegistry(1, 16)
		require.NoError(t, err)
		account, err := registry.Open(1)
		require.NoError(t, err)
		return account
	}

	t.Run("probe result", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		defer proc.Free()
		account := newAccount(t)
		arg := &DedupJoin{
			Result:    []colexec.ResultPos{{Rel: 0, Pos: 0}},
			LeftTypes: []types.Type{types.T_int64.ToType()},
		}
		require.NoError(t, arg.SetAllocationAccount(account))
		require.NoError(t, arg.resetRBat())
		err := vector.AppendFixed(arg.ctr.rbat.Vecs[0], int64(1), false, proc.Mp())
		require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
		require.Zero(t, account.Snapshot().Used)
		arg.Reset(proc, false, nil)
		require.NoError(t, arg.ClearAllocationAccount(account))
	})

	t.Run("multi batch finalize", func(t *testing.T) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		defer proc.Free()
		account := newAccount(t)
		arg := &DedupJoin{
			Result:    []colexec.ResultPos{{Rel: 0, Pos: 0}},
			LeftTypes: []types.Type{types.T_int64.ToType()},
		}
		require.NoError(t, arg.SetAllocationAccount(account))
		arg.ctr.matched = &bitmap.Bitmap{}
		first := batch.NewWithSize(0)
		first.SetRowCount(colexec.DefaultBatchSize)
		second := batch.NewWithSize(0)
		second.SetRowCount(1)
		arg.ctr.batches = []*batch.Batch{first, second}
		err := arg.ctr.finalize(arg, proc)
		require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
		require.Zero(t, account.Snapshot().Used)
		arg.Reset(proc, false, nil)
		require.NoError(t, arg.ClearAllocationAccount(account))
	})
}
