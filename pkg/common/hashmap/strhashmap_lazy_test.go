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

package hashmap

import (
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

func TestStrHashIteratorScratchGrowsLazily(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer hashMap.Free()
	vec := newVector(UnitLimit, types.T_varchar.ToType(), mp, false, nil)
	defer vec.Free(mp)

	itr := hashMap.NewIterator().(*strHashmapIterator)
	assertStrIteratorScratchCapacity(t, itr, 0)
	require.Zero(t, StrIteratorCapacity(itr))

	for _, testCase := range []struct {
		count   int
		wantCap int
	}{
		{count: 0, wantCap: 0},
		{count: 1, wantCap: 1},
		{count: 8, wantCap: 8},
		{count: 2, wantCap: 8},
		{count: 256, wantCap: 256},
		{count: 0, wantCap: 256},
	} {
		values, zValues, err := itr.Find(0, testCase.count, []*vector.Vector{vec})
		require.NoError(t, err)
		require.Len(t, values, testCase.count)
		require.Len(t, zValues, testCase.count)
		assertStrIteratorScratchCapacity(t, itr, testCase.wantCap)
	}

	coreBytes := cap(itr.keys) * int(
		unsafe.Sizeof([]byte(nil))+
			unsafe.Sizeof(uint64(0))+
			unsafe.Sizeof(int64(0))+
			unsafe.Sizeof([3]uint64{}),
	)
	require.Equal(t, cap(itr.keyBuffer)+coreBytes, StrIteratorCapacity(itr))
}

func TestStrHashIteratorDetectDupAllocatesBeforeUsingKeys(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer hashMap.Free()
	vec := newVector(1, types.T_varchar.ToType(), mp, false, nil)
	defer vec.Free(mp)

	itr := hashMap.NewIterator().(*strHashmapIterator)
	newKey, err := itr.DetectDup([]*vector.Vector{vec}, 0)
	require.NoError(t, err)
	require.True(t, newKey)
	newKey, err = itr.DetectDup([]*vector.Vector{vec}, 0)
	require.NoError(t, err)
	require.False(t, newKey)
	assertStrIteratorScratchCapacity(t, itr, 1)
}

func TestStrHashIteratorOwnerTransferPreservesAllocationProvenance(t *testing.T) {
	mp := mpool.MustNewZero()
	first, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer first.Free()
	second, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer second.Free()
	vec := newVector(1, types.T_varchar.ToType(), mp, false, nil)
	defer vec.Free(mp)

	itr := first.NewIterator().(*strHashmapIterator)
	_, _, err = itr.Find(0, 1, []*vector.Vector{vec})
	require.NoError(t, err)
	unaccounted := unsafe.SliceData(itr.keyBuffer)
	require.Nil(t, itr.keyBufferMP)

	IteratorClearOwner(itr)
	require.Nil(t, itr.mp)
	require.Equal(t, unaccounted, unsafe.SliceData(itr.keyBuffer))
	IteratorChangeOwner(itr, second)
	_, _, err = itr.Find(0, 1, []*vector.Vector{vec})
	require.NoError(t, err)
	require.Equal(t, unaccounted, unsafe.SliceData(itr.keyBuffer))

	registry, err := mpool.NewAllocationAccountRegistry(1, 2)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	allocation, err := NewIteratorAllocation(
		account,
		mpool.AllocationOwnerMin,
		mpool.AllocationSiteMin,
	)
	require.NoError(t, err)
	accounted, err := NewStrHashMapWithAllocations(false, mp, nil, allocation)
	require.NoError(t, err)

	IteratorChangeOwner(itr, accounted)
	require.Nil(t, itr.keyBuffer, "unaccounted scratch must not cross into an accounted owner")
	_, _, err = itr.Find(0, 1, []*vector.Vector{vec})
	require.NoError(t, err)
	require.Same(t, mp, itr.keyBufferMP)
	require.Same(t, allocation, itr.keyBufferAllocation)
	require.Positive(t, account.Snapshot().Used)

	IteratorClearOwner(itr)
	require.Nil(t, itr.keyBuffer)
	require.Zero(t, account.Snapshot().Used)
	accounted.Free()
	require.Zero(t, account.Seal().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	_, err = registry.Finalize(account)
	require.NoError(t, err)
}

func TestStrHashIteratorChangesBetweenAccountedOwners(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(2, 2)
	require.NoError(t, err)
	firstAccount, err := registry.Open(1 << 20)
	require.NoError(t, err)
	secondAccount, err := registry.Open(1 << 20)
	require.NoError(t, err)
	firstAllocation, err := NewIteratorAllocation(
		firstAccount, mpool.AllocationOwnerMin, mpool.AllocationSiteMin,
	)
	require.NoError(t, err)
	secondAllocation, err := NewIteratorAllocation(
		secondAccount, mpool.AllocationOwnerMin+1, mpool.AllocationSiteMin,
	)
	require.NoError(t, err)
	first, err := NewStrHashMapWithAllocations(false, mp, nil, firstAllocation)
	require.NoError(t, err)
	second, err := NewStrHashMapWithAllocations(false, mp, nil, secondAllocation)
	require.NoError(t, err)
	vec := newVector(1, types.T_varchar.ToType(), mp, false, nil)
	defer vec.Free(mp)

	itr := first.NewIterator().(*strHashmapIterator)
	_, _, err = itr.Find(0, 1, []*vector.Vector{vec})
	require.NoError(t, err)
	require.Positive(t, firstAccount.Snapshot().Used)
	require.Zero(t, secondAccount.Snapshot().Used)

	IteratorChangeOwner(itr, second)
	require.Zero(t, firstAccount.Snapshot().Used)
	require.Nil(t, itr.keyBuffer)
	_, _, err = itr.Find(0, 1, []*vector.Vector{vec})
	require.NoError(t, err)
	require.Positive(t, secondAccount.Snapshot().Used)
	require.Same(t, secondAllocation, itr.keyBufferAllocation)

	IteratorClearOwner(itr)
	require.Zero(t, secondAccount.Snapshot().Used)
	IteratorChangeOwner(itr, first)
	_, _, err = itr.Find(0, 1, []*vector.Vector{vec})
	require.NoError(t, err)
	require.Positive(t, firstAccount.Snapshot().Used)
	require.Same(t, firstAllocation, itr.keyBufferAllocation)
	IteratorClearOwner(itr)

	first.Free()
	second.Free()
	require.Zero(t, firstAccount.Seal().Used)
	require.Zero(t, secondAccount.Seal().Used)
	require.Zero(t, registry.LiveAllocationMetadata())
	_, err = registry.Finalize(firstAccount)
	require.NoError(t, err)
	_, err = registry.Finalize(secondAccount)
	require.NoError(t, err)
}

func TestTransactionalStrIteratorGrowthInvalidatesPreview(t *testing.T) {
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer hashMap.Free()
	vec := newVector(8, types.T_varchar.ToType(), mp, false, nil)
	defer vec.Free(mp)

	itr := hashMap.NewTransactionalIterator().(*transactionalStrIterator)
	var plan InsertPlan
	require.NoError(t, itr.PreviewInsert(0, 1, []*vector.Vector{vec}, 0, &plan))
	require.NoError(t, itr.Preflight(0, 8, []*vector.Vector{vec}))
	_, _, err = itr.CommitPreview(&plan)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)

	require.NoError(t, itr.PreviewInsert(0, 8, []*vector.Vector{vec}, 0, &plan))
	values, zValues, err := itr.CommitPreview(&plan)
	require.NoError(t, err)
	require.Len(t, values, 8)
	require.Len(t, zValues, 8)
}

func TestTransactionalStrIteratorOwnerLifecycleInvalidatesPreview(t *testing.T) {
	mp := mpool.MustNewZero()
	first, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer first.Free()
	second, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer second.Free()
	vec := newVector(1, types.T_varchar.ToType(), mp, false, nil)
	defer vec.Free(mp)

	itr := first.NewTransactionalIterator().(*transactionalStrIterator)
	var plan InsertPlan
	require.NoError(t, itr.PreviewInsert(0, 1, []*vector.Vector{vec}, 0, &plan))
	IteratorChangeOwner(itr, second)
	_, _, err = itr.CommitPreview(&plan)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)

	require.NoError(t, itr.PreviewInsert(0, 1, []*vector.Vector{vec}, 0, &plan))
	IteratorClearOwner(itr)
	_, _, err = itr.CommitPreview(&plan)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
}

func TestStrHashIteratorIndependentConcurrentFind(t *testing.T) {
	const workers = 8
	mp := mpool.MustNewZero()
	hashMap, err := NewStrHashMap(false, mp)
	require.NoError(t, err)
	defer hashMap.Free()
	vec := newVector(8, types.T_varchar.ToType(), mp, false, nil)
	defer vec.Free(mp)
	_, _, err = hashMap.NewIterator().Insert(0, 8, []*vector.Vector{vec})
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			itr := hashMap.NewIterator()
			for range 50 {
				values, _, err := itr.Find(0, 8, []*vector.Vector{vec})
				if err != nil {
					errs <- err
					return
				}
				if len(values) != 8 {
					errs <- fmt.Errorf("found %d rows, want 8", len(values))
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

func assertStrIteratorScratchCapacity(t *testing.T, itr *strHashmapIterator, want int) {
	t.Helper()
	require.Equal(t, want, cap(itr.keys))
	require.Equal(t, want, cap(itr.values))
	require.Equal(t, want, cap(itr.zValues))
	require.Equal(t, want, cap(itr.strHashStates))
}
