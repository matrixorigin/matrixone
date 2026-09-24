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

package mpool

import (
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func scanGlobalOnHeapOutstanding(poolID int64) (bytes, objects int64) {
	for shardIndex := range globalPtrShards {
		shard := &globalPtrShards[shardIndex]
		shard.mu.Lock()
		for _, hdr := range shard.m {
			if hdr.poolId == poolID && !hdr.isOffHeap() {
				bytes += int64(hdr.allocSz)
				objects++
			}
		}
		shard.mu.Unlock()
	}
	return bytes, objects
}

func requireOnHeapOwnershipMatchesRegistry(t testing.TB, mp *MPool) {
	t.Helper()
	gotBytes, gotObjects := mp.OnHeapOutstanding()
	wantBytes, wantObjects := scanGlobalOnHeapOutstanding(mp.id)
	require.Equal(t, wantBytes, gotBytes)
	require.Equal(t, wantObjects, gotObjects)
}

func TestOnHeapOwnershipMatchesRegistryAcrossOwnershipTransitions(t *testing.T) {
	owner := MustNew("onheap-ownership-owner")
	other := MustNew("onheap-ownership-other")
	freeingPool := MustNew("onheap-ownership-freeing-pool")
	defer DeleteMPool(owner)
	defer DeleteMPool(other)
	defer DeleteMPool(freeingPool)

	ownerBuffers := make([][]byte, 0, 3)
	var offHeap, otherBuffer []byte
	defer func() {
		for _, buffer := range ownerBuffers {
			if buffer != nil {
				owner.Free(buffer)
			}
		}
		if offHeap != nil {
			owner.Free(offHeap)
		}
		if otherBuffer != nil {
			other.Free(otherBuffer)
		}
	}()

	for _, size := range []int{64, 96, 128} {
		buffer, err := owner.Alloc(size, false)
		require.NoError(t, err)
		ownerBuffers = append(ownerBuffers, buffer)
	}
	requireOnHeapOwnershipMatchesRegistry(t, owner)
	require.Equal(t, int64(288), owner.OnHeapCurrNB())

	offHeap, err := owner.Alloc(256, true)
	require.NoError(t, err)
	require.Equal(t, int64(288), owner.OnHeapCurrNB())
	require.Equal(t, int64(256), owner.CurrNB())
	requireOnHeapOwnershipMatchesRegistry(t, owner)

	otherBuffer, err = other.Alloc(48, false)
	require.NoError(t, err)
	requireOnHeapOwnershipMatchesRegistry(t, other)

	freeingPool.Free(ownerBuffers[0])
	ownerBuffers[0] = nil
	requireOnHeapOwnershipMatchesRegistry(t, owner)
	require.Equal(t, int64(224), owner.OnHeapCurrNB())
	require.Equal(t, int64(1), freeingPool.Stats().NumCrossPoolFree.Load())

	other.Free(otherBuffer)
	otherBuffer = nil
	owner.Free(ownerBuffers[1])
	ownerBuffers[1] = nil
	owner.Free(ownerBuffers[2])
	ownerBuffers[2] = nil
	owner.Free(offHeap)
	offHeap = nil
	requireOnHeapOwnershipMatchesRegistry(t, owner)
	require.Zero(t, owner.OnHeapCurrNB())
	require.Zero(t, owner.CurrNB())
}

func TestOnHeapOwnershipFailedRegistrationDoesNotCount(t *testing.T) {
	mp := MustNew("onheap-ownership-registration-failure")
	defer DeleteMPool(mp)

	buffer := make([]byte, 64)
	ptr := unsafe.Pointer(unsafe.SliceData(buffer))
	hdr := memHdr{poolId: mp.id, allocSz: 64}
	hdr.SetGuard()

	require.NoError(t, mp.recordPtrHdr(ptr, hdr))
	globalOnHeapStats.recordAlloc(64)
	require.Equal(t, int64(64), mp.OnHeapCurrNB())

	require.Error(t, mp.recordPtrHdr(ptr, hdr))
	require.Equal(t, int64(64), mp.OnHeapCurrNB())

	mp.Free(buffer)
	require.Zero(t, mp.OnHeapCurrNB())
}

func TestOnHeapOwnershipReallocTransitions(t *testing.T) {
	t.Run("grow", func(t *testing.T) {
		mp := MustNew("onheap-ownership-grow")
		defer DeleteMPool(mp)

		buffer, err := mp.Alloc(64, false)
		require.NoError(t, err)
		grown, err := mp.Grow(buffer, 128, false)
		require.NoError(t, err)
		require.Equal(t, int64(128), mp.OnHeapCurrNB())
		requireOnHeapOwnershipMatchesRegistry(t, mp)

		mp.Free(grown)
		require.Zero(t, mp.OnHeapCurrNB())
	})

	t.Run("realloc-zero", func(t *testing.T) {
		mp := MustNew("onheap-ownership-realloc-zero")
		defer DeleteMPool(mp)

		buffer, err := mp.Alloc(64, false)
		require.NoError(t, err)
		reallocated, err := mp.ReallocZero(buffer, 128, false)
		require.NoError(t, err)
		require.Equal(t, int64(128), mp.OnHeapCurrNB())
		requireOnHeapOwnershipMatchesRegistry(t, mp)

		mp.Free(reallocated)
		require.Zero(t, mp.OnHeapCurrNB())
	})

	t.Run("on-heap-to-off-heap", func(t *testing.T) {
		mp := MustNew("onheap-ownership-provenance-transition")
		defer DeleteMPool(mp)

		buffer, err := mp.Alloc(64, false)
		require.NoError(t, err)
		reallocated, err := mp.ReallocZero(buffer, 256, true)
		require.NoError(t, err)
		require.Zero(t, mp.OnHeapCurrNB())
		require.Equal(t, int64(256), mp.CurrNB())

		mp.Free(reallocated)
		require.Zero(t, mp.CurrNB())
	})
}

func TestOnHeapOwnershipTeardown(t *testing.T) {
	t.Run("locking-pool-outstanding-and-late-free", func(t *testing.T) {
		owner := MustNew("onheap-ownership-teardown-owner")
		other := MustNew("onheap-ownership-late-free-owner")
		defer DeleteMPool(other)

		globalBefore := GlobalOnHeapStats().NumCurrBytes.Load()
		buffer, err := owner.Alloc(64, false)
		require.NoError(t, err)
		require.Equal(t, int64(64), owner.OnHeapCurrNB())

		DeleteMPool(owner)
		require.Equal(t, globalBefore+64, GlobalOnHeapStats().NumCurrBytes.Load())

		other.Free(buffer)
		require.Equal(t, globalBefore, GlobalOnHeapStats().NumCurrBytes.Load())
	})

	t.Run("no-lock-on-heap", func(t *testing.T) {
		mp := MustNewNoLock("onheap-ownership-no-lock")
		globalBefore := GlobalOnHeapStats().NumCurrBytes.Load()
		buffer, err := mp.Alloc(64, false)
		require.NoError(t, err)
		require.NotEmpty(t, buffer)
		bytes, objects := mp.OnHeapOutstanding()
		require.Equal(t, int64(64), bytes)
		require.Equal(t, int64(1), objects)

		DeleteMPool(mp)
		require.Zero(t, mp.OnHeapCurrNB())
		require.Equal(t, globalBefore, GlobalOnHeapStats().NumCurrBytes.Load())
	})

	t.Run("no-lock-off-heap", func(t *testing.T) {
		mp := MustNewNoLock("onheap-ownership-no-lock-off-heap")
		globalBefore := GlobalStats().NumCurrBytes.Load()
		buffer, err := mp.Alloc(64, true)
		require.NoError(t, err)
		require.NotEmpty(t, buffer)
		require.Zero(t, mp.OnHeapCurrNB())

		DeleteMPool(mp)
		require.Zero(t, mp.OnHeapCurrNB())
		require.Equal(t, globalBefore, GlobalStats().NumCurrBytes.Load())
	})
}

func TestOnHeapOwnershipConcurrentPublicationAndCrossPoolFree(t *testing.T) {
	owner := MustNew("onheap-ownership-concurrent-owner")
	freeingPool := MustNew("onheap-ownership-concurrent-free")
	defer DeleteMPool(owner)
	defer DeleteMPool(freeingPool)

	const (
		workers = 8
		ops     = 64
	)
	buffers := make([][][]byte, workers)
	defer func() {
		for _, workerBuffers := range buffers {
			for _, buffer := range workerBuffers {
				if buffer != nil {
					owner.Free(buffer)
				}
			}
		}
	}()

	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for range ops {
				buffer, err := owner.Alloc(64, false)
				if err != nil {
					t.Errorf("allocate: %v", err)
					return
				}
				buffers[worker] = append(buffers[worker], buffer)
			}
		}(worker)
	}
	wg.Wait()
	requireOnHeapOwnershipMatchesRegistry(t, owner)

	stopReader := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopReader:
				return
			default:
				owner.OnHeapOutstanding()
			}
		}
	}()
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i, buffer := range buffers[worker] {
				freeingPool.Free(buffer)
				buffers[worker][i] = nil
			}
		}(worker)
	}
	close(stopReader)
	wg.Wait()

	requireOnHeapOwnershipMatchesRegistry(t, owner)
	require.Zero(t, owner.OnHeapCurrNB())
}

func BenchmarkMPoolDestroyWithUnrelatedRegistryEntries(b *testing.B) {
	for _, unrelatedSize := range []int{0, 10_000, 100_000} {
		for _, ownerSize := range []int{0, 1, 8, 32, 128} {
			b.Run(fmt.Sprintf("unrelated=%d/owned=%d", unrelatedSize, ownerSize), func(b *testing.B) {
				onHeapOwner := MustNew("onheap-ownership-bench-onheap")
				offHeapOwner := MustNew("onheap-ownership-bench-offheap")
				defer DeleteMPool(onHeapOwner)
				defer DeleteMPool(offHeapOwner)
				buffers := make([][]byte, 0, unrelatedSize)
				defer func() {
					b.StopTimer()
					for i, buffer := range buffers {
						if i%2 == 0 {
							onHeapOwner.Free(buffer)
						} else {
							offHeapOwner.Free(buffer)
						}
					}
				}()
				for i := range unrelatedSize {
					owner, offHeap := onHeapOwner, false
					if i%2 == 1 {
						owner, offHeap = offHeapOwner, true
					}
					buffer, err := owner.Alloc(64, offHeap)
					if err != nil {
						b.Fatal(err)
					}
					buffers = append(buffers, buffer)
				}

				targetBuffers := make([][]byte, ownerSize)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					target := MustNew("onheap-ownership-bench-target")
					for i := range targetBuffers {
						buffer, err := target.Alloc(64, false)
						if err != nil {
							for _, allocated := range targetBuffers[:i] {
								target.Free(allocated)
							}
							DeleteMPool(target)
							b.Fatal(err)
						}
						targetBuffers[i] = buffer
					}
					for _, buffer := range targetBuffers {
						target.Free(buffer)
					}
					DeleteMPool(target)
				}
			})
		}
	}
}

func BenchmarkMPoolConcurrentOnHeapAllocFreeWithUnrelatedRegistryEntries(b *testing.B) {
	for _, unrelatedSize := range []int{0, 100_000} {
		b.Run(fmt.Sprintf("unrelated=%d", unrelatedSize), func(b *testing.B) {
			onHeapOwner := MustNew("onheap-ownership-concurrent-bench-onheap")
			offHeapOwner := MustNew("onheap-ownership-concurrent-bench-offheap")
			target := MustNew("onheap-ownership-concurrent-bench-target")
			defer DeleteMPool(onHeapOwner)
			defer DeleteMPool(offHeapOwner)
			defer DeleteMPool(target)
			buffers := make([][]byte, 0, unrelatedSize)
			defer func() {
				b.StopTimer()
				for i, buffer := range buffers {
					if i%2 == 0 {
						onHeapOwner.Free(buffer)
					} else {
						offHeapOwner.Free(buffer)
					}
				}
			}()
			for i := range unrelatedSize {
				owner, offHeap := onHeapOwner, false
				if i%2 == 1 {
					owner, offHeap = offHeapOwner, true
				}
				buffer, err := owner.Alloc(64, offHeap)
				if err != nil {
					b.Fatal(err)
				}
				buffers = append(buffers, buffer)
			}

			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					buffer, err := target.Alloc(64, false)
					if err != nil {
						b.Error(err)
						return
					}
					target.Free(buffer)
				}
			})
			b.StopTimer()
			require.Zero(b, target.OnHeapCurrNB())
			requireOnHeapOwnershipMatchesRegistry(b, target)
		})
	}
}
