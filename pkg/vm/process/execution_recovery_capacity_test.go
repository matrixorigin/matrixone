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

package process

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/stretchr/testify/require"
)

func TestExecutionRecoveryCapacitySupports256Workers(t *testing.T) {
	const workers = 256

	budget := MustNewExecutionResourceBudget(1, 1)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1, generation)
	require.NoError(t, err)

	recoveries := make([]*ExecutionRecoveryCapacity, workers)
	classes := make([]mpool.AllocationCapacityClass, workers)
	for i := range workers {
		recoveries[i], err = NewExecutionRecoveryCapacity(generation)
		require.NoError(t, err)
		classes[i], err = account.RegisterCapacityController(recoveries[i])
		require.NoError(t, err)
	}
	require.Greater(t, uint32(classes[workers-1]), uint32(math.MaxUint8))

	for i := range workers {
		require.NoError(t, recoveries[i].Close())
		require.NoError(t, account.UnregisterCapacityController(classes[i], recoveries[i]))
	}
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestExecutionRecoveryCapacityTransfersPhysicalCharge(t *testing.T) {
	budget := MustNewExecutionResourceBudget(1024, 1024)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1024, generation)
	require.NoError(t, err)
	recovery, err := NewExecutionRecoveryCapacity(generation)
	require.NoError(t, err)
	class, err := account.RegisterCapacityController(recovery)
	require.NoError(t, err)

	require.NoError(t, recovery.EnsureCapacity(256))
	require.Equal(t, uint64(256), generation.Used())
	mp := mpool.MustNewZero()
	recoveryBuffer, err := mp.AllocAccountedWithCapacityClass(
		128, account, 1, 1, class)
	require.NoError(t, err)
	require.Equal(t, uint64(128), account.Snapshot().Used)
	// The physical allocation borrows the pre-admitted floor; it does not add a
	// second generation charge for the same bytes.
	require.Equal(t, uint64(256), generation.Used())

	defaultBuffer, err := mp.AllocAccounted(100, account, 1, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(228), account.Snapshot().Used)
	require.Equal(t, uint64(356), generation.Used())
	require.NoError(t, recovery.EnsureCapacity(384))
	require.Equal(t, uint64(484), generation.Used())
	require.ErrorIs(t, recovery.Close(), mpool.ErrAllocationAccountLive)
	require.ErrorIs(
		t, account.UnregisterCapacityController(class, recovery),
		mpool.ErrAllocationAccountLive,
	)

	mp.Free(recoveryBuffer)
	require.Equal(t, uint64(100), account.Snapshot().Used)
	require.Equal(t, uint64(484), generation.Used())
	require.NoError(t, recovery.Close())
	require.Equal(t, uint64(100), generation.Used())
	require.NoError(t, account.UnregisterCapacityController(class, recovery))
	mp.Free(defaultBuffer)
	require.Zero(t, account.Snapshot().Used)
	require.Zero(t, generation.Used())
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestExecutionRecoveryCapacityRejectsUncoveredGrowth(t *testing.T) {
	budget := MustNewExecutionResourceBudget(300, 300)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 4)
	require.NoError(t, err)
	account, err := registry.OpenWithController(300, generation)
	require.NoError(t, err)
	recovery, err := NewExecutionRecoveryCapacity(generation)
	require.NoError(t, err)
	class, err := account.RegisterCapacityController(recovery)
	require.NoError(t, err)
	mp := mpool.MustNewZero()
	defaultBuffer, err := mp.AllocAccounted(100, account, 1, 1)
	require.NoError(t, err)
	require.NoError(t, recovery.EnsureCapacity(200))

	_, err = mp.AllocAccountedWithCapacityClass(201, account, 1, 2, class)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, uint64(100), account.Snapshot().Used)
	require.Equal(t, uint64(300), generation.Used())
	capacity, borrowed := recovery.Snapshot()
	require.Equal(t, uint64(200), capacity)
	require.Zero(t, borrowed)

	require.NoError(t, recovery.Close())
	require.NoError(t, account.UnregisterCapacityController(class, recovery))
	mp.Free(defaultBuffer)
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}

func TestExecutionRecoveryCapacityIsolatedAcrossWorkers(t *testing.T) {
	budget := MustNewExecutionResourceBudget(512, 512)
	generation, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.OpenWithController(512, generation)
	require.NoError(t, err)
	first, err := NewExecutionRecoveryCapacity(generation)
	require.NoError(t, err)
	second, err := NewExecutionRecoveryCapacity(generation)
	require.NoError(t, err)
	firstClass, err := account.RegisterCapacityController(first)
	require.NoError(t, err)
	secondClass, err := account.RegisterCapacityController(second)
	require.NoError(t, err)
	require.NotEqual(t, firstClass, secondClass)
	require.NoError(t, first.EnsureCapacity(200))
	require.NoError(t, second.EnsureCapacity(200))

	mp := mpool.MustNewZero()
	ordinary, err := mp.AllocAccounted(112, account, 1, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(512), generation.Used())
	firstBuffer, err := mp.AllocAccountedWithCapacityClass(
		200, account, 1, 2, firstClass)
	require.NoError(t, err)
	secondBuffer, err := mp.AllocAccountedWithCapacityClass(
		200, account, 1, 3, secondClass)
	require.NoError(t, err)
	require.Equal(t, uint64(512), account.Snapshot().Used)
	require.Equal(t, uint64(512), generation.Used())

	mp.Free(firstBuffer)
	mp.Free(secondBuffer)
	require.NoError(t, first.Close())
	require.NoError(t, second.Close())
	require.NoError(t, account.UnregisterCapacityController(firstClass, first))
	require.NoError(t, account.UnregisterCapacityController(secondClass, second))
	mp.Free(ordinary)
	_, _, err = registry.CompleteTerminal(account)
	require.NoError(t, err)
}
