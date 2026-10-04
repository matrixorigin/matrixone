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

package rightdedupjoin

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

func TestRightDedupJoinResultBatchUsesAllocationAccount(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	arg := &RightDedupJoin{
		Result:    []colexec.ResultPos{{Rel: 0, Pos: 0}},
		LeftTypes: []types.Type{types.T_int64.ToType()},
	}
	account := installTestAllocation(t, arg)
	require.NoError(t, arg.resetResultBatch())
	require.Same(t, arg.resultAllocation, arg.ctr.resultBatch.Vecs[0].AllocationAccountSelection())
	require.NoError(t, vector.AppendFixed(arg.ctr.resultBatch.Vecs[0], int64(1), false, proc.Mp()))
	used := account.Snapshot().Used
	require.Positive(t, used)
	require.NoError(t, arg.resetResultBatch())
	require.Equal(t, used, account.Snapshot().Used)

	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.resultBatch)
	require.Zero(t, account.Snapshot().Used)
	require.NoError(t, arg.ClearAllocationAccount(account))
}

func TestRightDedupJoinResultBatchHonorsAllocationCapacity(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.Open(1)
	require.NoError(t, err)
	arg := &RightDedupJoin{
		Result:    []colexec.ResultPos{{Rel: 0, Pos: 0}},
		LeftTypes: []types.Type{types.T_int64.ToType()},
	}
	require.NoError(t, arg.SetAllocationAccount(account))
	require.NoError(t, arg.resetResultBatch())
	err = vector.AppendFixed(arg.ctr.resultBatch.Vecs[0], int64(1), false, proc.Mp())
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, account.Snapshot().Used)
	arg.Reset(proc, false, nil)
	require.NoError(t, arg.ClearAllocationAccount(account))
}
