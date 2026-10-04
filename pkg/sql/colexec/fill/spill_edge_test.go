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

package fill

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestFillSpillFileAndPartitionSnapshotBoundaries(t *testing.T) {
	_, err := newFillSpillFile(nil, nil, "invalid")
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	require.NoError(t, (*fillSpillFile)(nil).finishWriting())
	require.NoError(t, (*fillSpillFile)(nil).flush())
	require.NoError(t, (*fillSpillFile)(nil).close())
	empty := &fillSpillFile{}
	require.NoError(t, empty.finishWriting())
	require.NoError(t, empty.flush())
	require.NoError(t, empty.close())

	var nilSnapshot *spillPartitionSnapshot
	nilSnapshot.configure(nil, nil, 0)
	require.False(t, nilSnapshot.hasCapacity())
	nilSnapshot.free()
	require.ErrorIs(t, nilSnapshot.cloneFrom(nil), mpool.ErrAllocationAccountInvalid)

	var source spillPartitionSnapshot
	source.ensureShape(3)
	require.NoError(t, source.setKey(0, []byte("first")))
	require.NoError(t, source.setKey(1, []byte("second")))
	source.nulls[2] = true
	source.set = true
	require.True(t, source.hasCapacity())
	var target spillPartitionSnapshot
	require.NoError(t, target.cloneFrom(&source))
	require.Equal(t, source.keys, target.keys)
	require.Equal(t, source.nulls, target.nulls)
	require.True(t, target.set)
	target.ensureShape(1)
	require.Len(t, target.keys, 1)
	require.ErrorIs(t, target.cloneFrom(&target), mpool.ErrAllocationAccountInvalid)
	target.free()
	source.free()
}

func TestFillAllocationAccountBindingBoundaries(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	registry, err := mpool.NewAllocationAccountRegistry(2, 64)
	require.NoError(t, err)
	first, err := registry.Open(1 << 20)
	require.NoError(t, err)
	second, err := registry.Open(1 << 20)
	require.NoError(t, err)

	require.ErrorIs(t, (*Fill)(nil).SetAllocationAccount(first),
		mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, (&Fill{}).SetAllocationAccount(nil),
		mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, (*Fill)(nil).ClearAllocationAccount(first),
		mpool.ErrAllocationAccountInvalid)

	var nilContainer *container
	require.ErrorIs(t, nilContainer.setAllocationAccount(first),
		mpool.ErrAllocationAccountInvalid)
	require.NoError(t, nilContainer.clearAllocationAccount(first))

	prepared := &Fill{}
	prepared.ProjectExecutors = make([]colexec.ExpressionExecutor, 1)
	require.ErrorIs(t, prepared.SetAllocationAccount(first),
		mpool.ErrAllocationAccountInvariant)
	prepared.ProjectExecutors = nil

	fill := &Fill{}
	require.NoError(t, fill.SetAllocationAccount(first))
	require.NoError(t, fill.SetAllocationAccount(first))
	require.ErrorIs(t, fill.SetAllocationAccount(second),
		mpool.ErrAllocationAccountMismatch)
	require.ErrorIs(t, fill.ClearAllocationAccount(second),
		mpool.ErrAllocationAccountMismatch)
	fill.ProjectExecutors = make([]colexec.ExpressionExecutor, 1)
	require.ErrorIs(t, fill.ClearAllocationAccount(first),
		mpool.ErrAllocationAccountInvariant)
	fill.ProjectExecutors = nil

	fill.ctr.nextRun = [][]fillCoord{make([]fillCoord, 0, 1)}
	require.True(t, fill.ctr.hasCoordinateCapacity())
	require.ErrorIs(t, fill.ClearAllocationAccount(first),
		mpool.ErrAllocationAccountInvariant)
	fill.ctr.nextRun = nil
	fill.ctr.linRun = [][]fillCoord{make([]fillCoord, 0, 1)}
	require.True(t, fill.ctr.hasCoordinateCapacity())
	fill.ctr.linRun = nil
	require.False(t, fill.ctr.hasCoordinateCapacity())
	fill.ProjectList = []*plan.Expr{{}}
	fill.ProjectExecutors = make([]colexec.ExpressionExecutor, 1)
	fill.Reset(proc, false, nil)
	require.Empty(t, fill.ProjectExecutors)
	require.NoError(t, fill.ClearAllocationAccount(first))
	require.NoError(t, fill.ClearAllocationAccount(first))

	dirty := &container{buf: batch.NewWithSize(0)}
	require.ErrorIs(t, dirty.setAllocationAccount(first),
		mpool.ErrAllocationAccountInvariant)
	dirty.buf.Clean(nil)
	dirty.buf = nil
	require.NoError(t, dirty.setAllocationAccount(second))
	require.ErrorIs(t, dirty.setAllocationAccount(first),
		mpool.ErrAllocationAccountMismatch)
	require.ErrorIs(t, dirty.clearAllocationAccount(first),
		mpool.ErrAllocationAccountMismatch)
	require.NoError(t, dirty.clearAllocationAccount(second))

	legacy := &Fill{}
	legacy.ProjectList = []*plan.Expr{{}}
	legacy.ProjectExecutors = make([]colexec.ExpressionExecutor, 1)
	legacy.ctr.nextRun = [][]fillCoord{{{seq: 1, row: 2}}}
	legacy.ctr.linRun = [][]fillCoord{{{seq: 3, row: 4}}}
	legacy.Reset(proc, false, nil)
	require.Len(t, legacy.ProjectExecutors, 1)
	require.Empty(t, legacy.ctr.nextRun[0])
	require.Empty(t, legacy.ctr.linRun[0])
	legacy.ProjectExecutors = nil

	for _, account := range []*mpool.AllocationAccount{first, second} {
		account.Seal()
		_, err = registry.Finalize(account)
		require.NoError(t, err)
	}
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}
