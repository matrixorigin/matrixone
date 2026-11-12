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

package process

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/common/rscthrottler"
	"github.com/stretchr/testify/require"
)

func importHashWorkingSet(t *testing.T) (rscthrottler.WorkingSetThrottler, *rscthrottler.WorkingSet) {
	t.Helper()
	m := rscthrottler.NewMemThrottler(t.Name(), 1,
		rscthrottler.WithConstLimit(1<<20), rscthrottler.WithMemorySampleForTesting(1<<20, 0)).(rscthrottler.WorkingSetThrottler)
	w, ok, err := m.TryReserveWorkingSet(context.Background(), rscthrottler.WorkingSetSpec{
		UnitBytes: 1, Inputs: []rscthrottler.InputSlotSpec{{Name: "input", Count: 1, Bytes: 128 << 10}}, IOBytes: 256 << 10,
	})
	require.NoError(t, err)
	require.True(t, ok)
	t.Cleanup(w.Close)
	return m, w
}

func TestImportHashPhysicalAndRecoveryOwners(t *testing.T) {
	m, w := importHashWorkingSet(t)
	g, err := MustNewHashBuildBudget(256<<10, 256<<10).OpenGeneration(1)
	require.NoError(t, err)
	g.importWorkingSet = w
	t.Cleanup(g.Close)
	registry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1<<20, g)
	require.NoError(t, err)
	t.Cleanup(func() { _, _, err := registry.CompleteTerminal(account); require.NoError(t, err) })
	owner, consumer := mpool.MustNewZero(), mpool.MustNewZero()
	t.Cleanup(func() { mpool.DeleteMPool(owner); mpool.DeleteMPool(consumer) })
	headroom := m.WorkingSetHeadroom().Available
	_, err = owner.AllocAccounted(512<<10, account, 1, 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, g.Used())
	require.Equal(t, headroom, m.WorkingSetHeadroom().Available, "hash rejection rolls back the physical D claim")
	data, err := owner.AllocAccounted(128<<10, account, 1, 1)
	require.NoError(t, err)
	t.Cleanup(func() {
		if data != nil {
			consumer.Free(data)
		}
	})
	require.Equal(t, int64(128<<10), owner.AllocatedBytes())
	require.Equal(t, headroom-(128<<10), m.WorkingSetHeadroom().Available)
	recovery, err := NewHashBuildRecoveryCapacity(g)
	require.NoError(t, err)

	var buffer []byte
	t.Cleanup(func() {
		if buffer != nil {
			owner.Free(buffer)
		}
		require.NoError(t, recovery.Close())
	})
	class, err := account.RegisterCapacityController(recovery)
	require.NoError(t, err)
	t.Cleanup(func() {
		if buffer != nil {
			owner.Free(buffer)
			buffer = nil
		}
		require.NoError(t, recovery.Close())
		require.NoError(t, account.UnregisterCapacityController(class, recovery))
	})
	require.NoError(t, recovery.EnsureCapacity(64<<10))
	buffer, err = owner.AllocAccountedWithCapacityClass(32<<10, account, 1, 2, class)
	require.NoError(t, err)
	require.Equal(t, uint64(192<<10), g.Used())
	require.Equal(t, headroom-(192<<10), m.WorkingSetHeadroom().Available, "recovery allocation borrows its floor exactly once")
	w.Close()
	require.ErrorIs(t, recovery.EnsureCapacity(128<<10), mpool.ErrAllocationAccountSealed)
	_, err = owner.AllocAccounted(1, account, 1, 3)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountSealed)
	consumer.Free(data)
	data = nil
	owner.Free(buffer)
	buffer = nil
	require.NoError(t, recovery.Close())
	require.Zero(t, g.Used())
	require.Zero(t, owner.AllocatedBytes())
	require.Equal(t, int64(1<<20), m.WorkingSetHeadroom().Available, "late destructors retire D after I/W root closure")
}

func TestImportHashRetryChangesImmutableWorkingSet(t *testing.T) {
	_, first := importHashWorkingSet(t)
	_, second := importHashWorkingSet(t)
	proc := &Process{Base: &BaseProcess{Lim: Limitation{Size: 2 << 20}, ImportMemory: &ImportMemory{WorkingSet: first}}}
	initial, err := proc.GetHashBuildBudget()
	require.NoError(t, err)
	t.Cleanup(initial.Close)
	require.Same(t, first, initial.importWorkingSet)
	require.NoError(t, initial.AcquireAllocationCapacity(32))
	proc.Base.ImportMemory = &ImportMemory{WorkingSet: second}
	_, err = proc.GetHashBuildBudget()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountLive)
	initial.ReleaseAllocationCapacity(32)
	next, err := proc.GetHashBuildBudget()
	require.NoError(t, err)
	t.Cleanup(next.Close)
	require.NotSame(t, initial, next)
	require.True(t, initial.Closed())
	require.Same(t, second, next.importWorkingSet)
	require.Same(t, initial.budget, next.budget)
	cached, err := proc.GetHashBuildBudget()
	require.NoError(t, err)
	require.Same(t, next, cached)
	proc.Base.ImportMemory = nil
	ordinary, err := proc.GetHashBuildBudget()
	require.NoError(t, err)
	t.Cleanup(ordinary.Close)
	require.Nil(t, ordinary.importWorkingSet, "ordinary execution retains 4.2 admission")
}
