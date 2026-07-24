// Copyright 2022 Matrix Origin
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

package fulltext

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPool(t *testing.T) {
	addrs := make([]uint64, 10)

	fmt.Printf("%v\n", t.TempDir())
	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	m := mpool.MustNewZeroNoFixed()

	proc := &process.Process{
		Base: &process.BaseProcess{
			FileService: localFS,
		},
		Ctx: context.Background(),
	}
	proc.SetMPool(m)

	mp := NewFixedBytePool(proc, 2, 6, 0)

	// total 3 partitions
	for i := 0; i < 10; i++ {
		addr, data, err := mp.NewItem()
		require.Nil(t, err)

		addrs[i] = addr

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	for i, addr := range addrs {
		data, err := mp.GetItem(addr)
		require.Nil(t, err)
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)

		for j := range data {
			require.Equal(t, byte(i), data[j])
			data[j] += 8
		}
	}

	for i, addr := range addrs {
		data, err := mp.GetItem(addr)
		require.Nil(t, err)
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
		for j := range data {
			require.Equal(t, byte(i+8), data[j])
		}
	}

	for _, addr := range addrs {
		err := mp.FreeItem(addr)
		require.Nil(t, err)
	}

	err = mp.FreeItem(addrs[0])
	//fmt.Println(err)
	require.NotNil(t, err)
}

// TestPoolMemAvailGate covers #25638: growing the pool by another partition also grows the
// fulltext TVF's non-spillable per-doc maps. Before creating a new block the pool estimates
// that block's footprint (docvec + map entries) and, if projected live Go heap would exceed
// the budget (a fraction of total memory), fails cleanly instead of OOMing the CN.
func TestPoolMemAvailGate(t *testing.T) {
	origHeap := memGolang
	origTotal := memTotal
	defer func() { memGolang = origHeap; memTotal = origTotal }()

	m := mpool.MustNewZeroNoFixed()
	proc := &process.Process{Base: &process.BaseProcess{}, Ctx: context.Background()}
	proc.SetMPool(m)

	// dsize=2, partition_cap=6 -> 3 items per partition; est per new block = 6 + (6/2)*MapMemPerItem.
	mp := NewFixedBytePool(proc, 2, 6, 0)

	// ~1 TiB total, heap ~empty: the first block (3 items) is created and filled fine.
	// (partition 0 is never gated; the gate only fires when growing PAST it.)
	memTotal = func() uint64 { return 1 << 40 }
	memGolang = func() int { return 0 }
	for i := 0; i < 3; i++ {
		_, _, err := mp.NewItem()
		require.NoError(t, err)
	}

	// heap now at total (well over the 80% budget): creating the next block fails cleanly.
	memGolang = func() int { return 1 << 40 }
	_, _, err := mp.NewItem()
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many documents")

	mp.Close()
}

// TestPoolFirstPartitionOverBudget covers the review hole where the very first partition was
// ungated: a CN that is ALREADY over its heap budget must refuse even partition 0 (its docvec
// plus the per-doc side maps) rather than fail open into an OOM.
func TestPoolFirstPartitionOverBudget(t *testing.T) {
	origHeap := memGolang
	origTotal := memTotal
	defer func() { memGolang = origHeap; memTotal = origTotal }()

	m := mpool.MustNewZeroNoFixed()
	proc := &process.Process{Base: &process.BaseProcess{}, Ctx: context.Background()}
	proc.SetMPool(m)

	mp := NewFixedBytePool(proc, 2, 6, 0)

	// CN heap already past the 80% budget before any partition exists.
	memTotal = func() uint64 { return 1000 }
	memGolang = func() int { return 900 } // > 800 budget
	_, _, err := mp.NewItem()
	require.Error(t, err)
	require.Contains(t, err.Error(), "too many documents")

	mp.Close()
}

// TestPoolPartitionZeroFillCrossesBudget covers the review hole that the budget was only
// re-checked at partition boundaries: the first partition holds ~1M items, so a query whose
// non-spillable side maps push the heap over budget WHILE filling partition 0 (before it is
// ever full) must still be refused. The fast-path interval check (HeapCheckInterval) enforces
// this without waiting for a second partition.
func TestPoolPartitionZeroFillCrossesBudget(t *testing.T) {
	origHeap := memGolang
	origTotal := memTotal
	origInterval := HeapCheckInterval
	defer func() { memGolang = origHeap; memTotal = origTotal; HeapCheckInterval = origInterval }()

	m := mpool.MustNewZeroNoFixed()
	proc := &process.Process{Base: &process.BaseProcess{}, Ctx: context.Background()}
	proc.SetMPool(m)

	// A large partition_cap (500 items) so every item below lands in partition 0 — the
	// new-partition gate never fires; only the fast-path interval check can catch the overrun.
	mp := NewFixedBytePool(proc, 2, 1000, 0)
	defer mp.Close()
	HeapCheckInterval = 4 // re-check every 4 items on the fast path

	memTotal = func() uint64 { return 1000 } // 800 budget
	memGolang = func() int { return 0 }      // under budget

	// Fill several items inside partition 0 (all fast path) while under budget — all accepted.
	for i := 0; i < 6; i++ {
		_, _, err := mp.NewItem()
		require.NoError(t, err, "item %d should be accepted under budget", i)
	}

	// The heap crosses the budget mid-fill (side maps grew). Still deep inside partition 0
	// (cap 500), so ONLY the fast-path interval check guards it — within HeapCheckInterval more
	// items it must refuse, not wait for partition 0 to fill.
	memGolang = func() int { return 900 } // > 800 budget
	var err error
	for i := 0; i <= int(HeapCheckInterval); i++ {
		if _, _, err = mp.NewItem(); err != nil {
			break
		}
	}
	require.Error(t, err, "fast-path interval check must refuse once the heap crosses the budget while filling partition 0")
	require.Contains(t, err.Error(), "too many documents")
}

// TestPoolUnspillBounded covers the review hole that the top-K scoring pass (fulltext TVF
// evaluate/sort_topk) GetItems every matched doc, which unspills partitions. Without an
// eviction policy on that path it would re-materialize ALL spilled partitions and grow
// resident memory past mem_limit. Here we spill during build, then GetItem every item (as
// scoring does) and assert resident memory stays bounded instead of re-inflating to full.
func TestPoolUnspillBounded(t *testing.T) {
	origHeap := memGolang
	origTotal := memTotal
	defer func() { memGolang = origHeap; memTotal = origTotal }()
	// Keep the NewItem heap-budget gate out of the way; this test exercises the spill/unspill
	// eviction, not the admission gate.
	memTotal = func() uint64 { return 1 << 40 }
	memGolang = func() int { return 0 }

	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	m := mpool.MustNewZeroNoFixed()
	proc := &process.Process{Base: &process.BaseProcess{FileService: localFS}, Ctx: context.Background()}
	proc.SetMPool(m)

	// dsize=2, partition_cap=6 (3 items/partition), mem_limit=19 -> forces spilling.
	// 30 items -> 10 partitions; if unspill were unbounded, GetItem-ing all would push
	// mem_in_use toward 10*6=60. The bound we assert is mem_limit + partition_cap = 25.
	const nitem = 30
	mp := NewFixedBytePool(proc, 2, 6, 19)
	bound := mp.mem_limit + mp.partition_cap

	addrs := make([]uint64, nitem)
	for i := 0; i < nitem; i++ {
		addr, data, err := mp.NewItem()
		require.Nil(t, err)
		addrs[i] = addr
		for j := range data {
			data[j] = byte(i)
		}
		require.LessOrEqual(t, mp.mem_in_use, bound, "build phase exceeded resident bound")
	}

	// Scoring phase: GetItem every item (random-ish order across partitions). Resident memory
	// must stay bounded, and every item must still read back correctly.
	for i, addr := range addrs {
		data, err := mp.GetItem(addr)
		require.Nil(t, err)
		for j := range data {
			require.Equal(t, byte(i), data[j])
		}
		require.LessOrEqual(t, mp.mem_in_use, bound, "unspill on GetItem re-materialized past the resident bound")
	}

	mp.Close()
}

// TestPoolUnspillHeapBudgetBinds covers the review point that the top-K unspill path must obey
// the HEAP budget, not only mem_limit: mem_limit is a fixed default that can EXCEED the heap
// budget on a small/loaded CN. Here mem_limit is loose but the heap budget is tight during the
// scoring phase (the CN grew near its budget from other work), so GetItem must evict against the
// heap budget and keep resident pool memory well below mem_limit.

// TestPoolFreeItemSpilled covers the FreeItem/spill interaction the scoring pass now exercises:
// after the pool has spilled partitions, freeing every item (as sort_topk/evaluate cleanup does)
// must not double-subtract resident bytes (mem_in_use underflow) nor panic on spilled partitions.
func TestPoolFreeItemSpilled(t *testing.T) {
	origHeap := memGolang
	origTotal := memTotal
	defer func() { memGolang = origHeap; memTotal = origTotal }()
	memTotal = func() uint64 { return 1 << 40 }
	memGolang = func() int { return 0 }

	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	m := mpool.MustNewZeroNoFixed()
	proc := &process.Process{Base: &process.BaseProcess{FileService: localFS}, Ctx: context.Background()}
	proc.SetMPool(m)

	// mem_limit=19 forces spilling across the 10 partitions.
	const nitem = 30
	mp := NewFixedBytePool(proc, 2, 6, 19)
	addrs := make([]uint64, nitem)
	for i := 0; i < nitem; i++ {
		addr, _, err := mp.NewItem()
		require.Nil(t, err)
		addrs[i] = addr
	}

	// Free every item; many partitions are spilled at this point. Accounting must land at 0
	// (no double subtraction from the spilled partitions).
	for _, addr := range addrs {
		require.Nil(t, mp.FreeItem(addr))
	}
	require.Equal(t, uint64(0), mp.mem_in_use, "mem_in_use should be exactly 0 after freeing all items")

	// double free is still detected.
	require.Error(t, mp.FreeItem(addrs[0]))

	mp.Close()
}

// TestPoolSpillExcludesActivePartition covers the regression where GetItem's eviction (which
// also fires during BUILD, since the fulltext TVF GetItems existing docs to bump word counts)
// could spill the active NON-FULL append partition; the next NewItem fast path then failed with
// "NewItem: partition is spillled". Spill() must never evict a partition that can still be
// appended to. We put a full partition and a non-full active partition both resident, force an
// aggressive Spill, and assert the active one is protected and still appendable.
func TestPoolSpillExcludesActivePartition(t *testing.T) {
	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	m := mpool.MustNewZeroNoFixed()
	proc := &process.Process{Base: &process.BaseProcess{FileService: localFS}, Ctx: context.Background()}
	proc.SetMPool(m)

	// Large mem_limit so the build does not auto-spill: both partitions stay resident.
	mp := NewFixedBytePool(proc, 2, 6, 1<<30)

	// partition 0: 3 items -> full.
	for i := 0; i < 3; i++ {
		_, _, err := mp.NewItem()
		require.Nil(t, err)
	}
	// partition 1: 1 item -> non-full, the active append target.
	_, _, err = mp.NewItem()
	require.Nil(t, err)

	require.Equal(t, 2, len(mp.partitions))
	require.True(t, mp.partitions[0].full, "partition 0 should be full")
	require.False(t, mp.partitions[1].full, "partition 1 should be the non-full active partition")

	// Force the most aggressive eviction (spill everything eligible).
	mp.spill_size = 16
	require.Nil(t, mp.Spill())

	// The active non-full partition must NOT have been spilled...
	require.False(t, mp.partitions[1].Spilled(), "active non-full partition must not be spilled")
	// ...and must still be appendable (the failing path was Partition.NewItem on a spilled part).
	_, _, err = mp.NewItem()
	require.Nil(t, err, "NewItem on the active partition must succeed after eviction")

	mp.Close()
}

func TestPoolSpill(t *testing.T) {

	addrs := make([]uint64, 10)
	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	m := mpool.MustNewZeroNoFixed()

	proc := &process.Process{
		Base: &process.BaseProcess{
			FileService: localFS,
		},
		Ctx: context.Background(),
	}
	proc.SetMPool(m)

	mp := NewFixedBytePool(proc, 2, 6, 19)

	for i := 0; i < 10; i++ {
		addr, data, err := mp.NewItem()
		require.Nil(t, err)

		addrs[i] = addr

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	//mp.Spill()

	// New 10 more items
	for i := 10; i < 20; i++ {
		addr, data, err := mp.NewItem()
		require.Nil(t, err)

		addrs = append(addrs, addr)

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	// after spill and NewItem directly.  It should unspill and GetItem
	for i, addr := range addrs {
		data, err := mp.GetItem(addr)
		require.Nil(t, err)
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
		for j := range data {
			require.Equal(t, byte(i), data[j])
		}
	}

	i := 0
	it := NewFixedBytePoolIterator(mp)
	for {
		data, err := it.Next()
		require.Nil(t, err)
		if data == nil {
			// EOF
			break
		}
		//fmt.Printf("data = %v\n", data)
		for j := range data {
			require.Equal(t, byte(i), data[j])
		}
		i++
	}

	mp.Close()
	mp.Close()
	mp.Close()

}

func TestPartitionSpill(t *testing.T) {

	addrs := make([]uint64, 10)
	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	mp := mpool.MustNewZeroNoFixed()

	proc := &process.Process{
		Base: &process.BaseProcess{
			FileService: localFS,
		},
		Ctx: context.Background(),
	}
	proc.SetMPool(mp)

	p, err := NewPartition(proc, 0, LOWER_BIT_MASK, 8)
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		addr, data, err := p.NewItem()
		require.Nil(t, err)

		addrs[i] = addr

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	// no effect
	err = p.Unspill()
	require.Nil(t, err)

	err = p.Spill()
	require.Nil(t, err)

	err = p.Unspill()
	require.Nil(t, err)

	for i, addr := range addrs {
		data, err := p.GetItem(addr)
		require.Nil(t, err)
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)

		for j := range data {
			require.Equal(t, byte(i), data[j])
		}
	}

}

func TestPartitionSpillError(t *testing.T) {

	addrs := make([]uint64, 10)

	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	mp := mpool.MustNewZeroNoFixed()

	proc := &process.Process{
		Base: &process.BaseProcess{
			FileService: localFS,
		},
		Ctx: context.Background(),
	}
	proc.SetMPool(mp)

	p, err := NewPartition(proc, 0, LOWER_BIT_MASK, 8)
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		addr, data, err := p.NewItem()
		require.Nil(t, err)

		addrs[i] = addr

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	err = p.Unspill()
	require.Nil(t, err)

	err = p.Spill()
	require.Nil(t, err)

	_, err = p.GetItem(addrs[0])
	require.NotNil(t, err)

}

func TestPartitionSpillError2(t *testing.T) {

	addrs := make([]uint64, 10)
	localFS, err := fileservice.NewLocalFS(
		context.Background(),
		defines.LocalFileServiceName,
		t.TempDir(),
		fileservice.DisabledCacheConfig,
		nil,
	)
	assert.Nil(t, err)
	mp := mpool.MustNewZeroNoFixed()

	proc := &process.Process{
		Base: &process.BaseProcess{
			FileService: localFS,
		},
		Ctx: context.Background(),
	}
	proc.SetMPool(mp)
	p, err := NewPartition(proc, 0, LOWER_BIT_MASK, 8)
	require.Nil(t, err)

	for i := 0; i < 10; i++ {
		addr, data, err := p.NewItem()
		require.Nil(t, err)

		addrs[i] = addr

		for j := range data {
			data[j] = byte(i)
		}
		//id := GetPartitionId(addr)
		//offset := GetPartitionOffset(addr)
		//fmt.Printf("ID %d, offset %d, data = %v\n", id, offset, data)
	}

	err = p.Unspill()
	require.Nil(t, err)

	err = p.Spill()
	require.Nil(t, err)

	_, _, err = p.NewItem()
	require.NotNil(t, err)

	p.Close()
	p.Close()
	p.Close()

}
