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
			if hdr.owner.id == poolID && hdr.kind == memKindOnHeap {
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
	for _, noLock := range []bool{false, true} {
		t.Run(fmt.Sprint(noLock), func(t *testing.T) {
			flags := NoFixed
			if noLock {
				flags |= NoLock
			}
			mp, err := NewMPool("onheap-ownership-registration-failure", 0, flags)
			require.NoError(t, err)
			t.Cleanup(func() { DeleteMPool(mp) })
			buffer, err := mp.Alloc(64, false)
			require.NoError(t, err)
			t.Cleanup(func() {
				if buffer != nil {
					mp.Free(buffer)
				}
			})
			ptr := unsafe.Pointer(unsafe.SliceData(buffer))
			hdr, ok := mp.getPtrHdr(ptr)
			require.True(t, ok)
			require.Error(t, mp.recordPtrHdr(ptr, hdr))
			bytes, objects := mp.OnHeapOutstanding()
			require.Equal(t, int64(64), bytes)
			require.Equal(t, int64(1), objects)
			mp.Free(buffer)
			freed := buffer
			buffer = nil
			require.Panics(t, func() { mp.Free(freed) })
			bytes, objects = mp.OnHeapOutstanding()
			require.Zero(t, bytes)
			require.Zero(t, objects)
		})
	}
}

func TestOnHeapOwnershipReallocTransitions(t *testing.T) {
	for _, tc := range []struct {
		name                                          string
		sourceOffHeap, targetOffHeap, crossPool, grow bool
		size                                          int
	}{
		{name: "grow", size: 128, grow: true},
		{name: "realloc-zero", size: 128},
		{name: "on-heap-to-off-heap", size: 128, targetOffHeap: true},
		{name: "off-heap-to-on-heap", size: 128, sourceOffHeap: true},
		{name: "cross-pool", size: 128, crossPool: true},
		{name: "within-capacity-keeps-owner", size: 32, crossPool: true, targetOffHeap: true},
		{name: "rejected-replacement", size: int(MaxAllocationSize()) + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner := MustNew("onheap-realloc-owner")
			other := MustNew("onheap-realloc-other")
			t.Cleanup(func() { DeleteMPool(owner); DeleteMPool(other) })
			buffer, err := owner.Alloc(64, tc.sourceOffHeap)
			require.NoError(t, err)
			t.Cleanup(func() {
				if buffer != nil {
					owner.Free(buffer)
				}
			})
			for i := range buffer {
				buffer[i] = byte(i)
			}
			target := owner
			if tc.crossPool {
				target = other
			}
			var replacement []byte
			if tc.grow {
				replacement, err = target.Grow(buffer, tc.size, tc.targetOffHeap)
			} else {
				replacement, err = target.ReallocZero(buffer, tc.size, tc.targetOffHeap)
			}
			wantOwner, wantOther := int64(0), int64(0)
			if tc.size > int(MaxAllocationSize()) {
				require.Error(t, err)
				require.Nil(t, replacement)
				wantOwner = 64
			} else {
				require.NoError(t, err)
				if tc.size <= 64 {
					require.True(t, unsafe.SliceData(buffer) == unsafe.SliceData(replacement), "within-capacity resize keeps the allocation")
					wantOwner = 64
				} else if !tc.targetOffHeap {
					if tc.crossPool {
						wantOther = int64(tc.size)
					} else {
						wantOwner = int64(tc.size)
					}
				}
				buffer = replacement
			}
			for i := range min(len(buffer), 64) {
				require.Equal(t, byte(i), buffer[i])
			}
			if !tc.grow && len(buffer) > 64 {
				require.Equal(t, make([]byte, len(buffer)-64), buffer[64:])
			}
			require.Equal(t, wantOwner, owner.OnHeapCurrNB())
			require.Equal(t, wantOther, other.OnHeapCurrNB())
			requireOnHeapOwnershipMatchesRegistry(t, owner)
			requireOnHeapOwnershipMatchesRegistry(t, other)
			owner.Free(buffer)
			buffer = nil
			requireOnHeapOwnershipMatchesRegistry(t, owner)
			requireOnHeapOwnershipMatchesRegistry(t, other)
			require.Zero(t, owner.CurrNB())
			require.Zero(t, other.CurrNB())
		})
	}
}

func TestOnHeapOwnershipTeardown(t *testing.T) {
	for _, crossPool := range []bool{false, true} {
		t.Run(fmt.Sprintf("late-free/cross-pool=%t", crossPool), func(t *testing.T) {
			owner := MustNew("onheap-ownership-teardown-owner")
			other := MustNew("onheap-ownership-late-free-owner")
			deleted := false
			t.Cleanup(func() {
				if !deleted {
					DeleteMPool(owner)
				}
				DeleteMPool(other)
			})
			globalBytes := GlobalOnHeapStats().NumCurrBytes.Load()
			globalObjects := GlobalOnHeapStats().NumCurrObjects.Load()
			buffer, err := owner.Alloc(64, false)
			require.NoError(t, err)
			t.Cleanup(func() {
				if buffer != nil {
					owner.Free(buffer)
				}
			})
			DeleteMPool(owner)
			deleted = true
			requireOnHeapOwnershipMatchesRegistry(t, owner)
			require.Equal(t, globalBytes+64, GlobalOnHeapStats().NumCurrBytes.Load())
			freeing := owner
			if crossPool {
				freeing = other
			}
			freeing.Free(buffer)
			buffer = nil
			requireOnHeapOwnershipMatchesRegistry(t, owner)
			bytes, objects := owner.OnHeapOutstanding()
			require.Zero(t, bytes)
			require.Zero(t, objects)
			require.Equal(t, globalBytes, GlobalOnHeapStats().NumCurrBytes.Load())
			require.Equal(t, globalObjects, GlobalOnHeapStats().NumCurrObjects.Load())
		})
	}
	t.Run("no-lock mixed terminal cleanup", func(t *testing.T) {
		mp := MustNewNoLock("onheap-ownership-no-lock")
		t.Cleanup(func() { DeleteMPool(mp) })
		onHeapBefore := GlobalOnHeapStats().NumCurrBytes.Load()
		objectsBefore := GlobalOnHeapStats().NumCurrObjects.Load()
		offHeapBefore := GlobalStats().NumCurrBytes.Load()
		for _, offHeap := range []bool{false, true} {
			buffer, err := mp.Alloc(64, offHeap)
			require.NoError(t, err)
			require.Len(t, buffer, 64)
		}
		bytes, objects := mp.OnHeapOutstanding()
		require.Equal(t, int64(64), bytes)
		require.Equal(t, int64(1), objects)
		DeleteMPool(mp)
		DeleteMPool(mp) // Repeated terminal cleanup must not release ownership twice.
		require.Zero(t, mp.OnHeapCurrNB())
		require.Zero(t, mp.CurrNB())
		require.Nil(t, mp.ptrs)
		require.Equal(t, onHeapBefore, GlobalOnHeapStats().NumCurrBytes.Load())
		require.Equal(t, objectsBefore, GlobalOnHeapStats().NumCurrObjects.Load())
		require.Equal(t, offHeapBefore, GlobalStats().NumCurrBytes.Load())
	})
}

func TestOnHeapOwnershipConcurrentPublicationAndCrossPoolFree(t *testing.T) {
	owner := MustNew("onheap-ownership-concurrent-owner")
	freeing := MustNew("onheap-ownership-concurrent-free")
	t.Cleanup(func() { DeleteMPool(owner); DeleteMPool(freeing) })
	const workers, ops = 8, 64
	buffers := make(chan []byte, workers)
	stopReader, readerDone, readerStarted := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(readerDone)
		owner.OnHeapOutstanding()
		close(readerStarted)
		for {
			select {
			case <-stopReader:
				return
			default:
				owner.OnHeapOutstanding()
			}
		}
	}()
	<-readerStarted
	var producers, consumers sync.WaitGroup
	for range workers {
		producers.Add(1)
		consumers.Add(1)
		go func() {
			defer producers.Done()
			for range ops {
				buffer, err := owner.Alloc(64, false)
				if err != nil {
					t.Errorf("allocate: %v", err)
					return
				}
				buffers <- buffer
			}
		}()
		go func() {
			defer consumers.Done()
			for buffer := range buffers {
				freeing.Free(buffer)
			}
		}()
	}
	producers.Wait()
	close(buffers)
	consumers.Wait()
	close(stopReader)
	<-readerDone
	requireOnHeapOwnershipMatchesRegistry(t, owner)
	require.Zero(t, owner.OnHeapCurrNB())
	require.Equal(t, int64(workers*ops), freeing.Stats().NumCrossPoolFree.Load())
}

func TestOnHeapOwnershipDeleteAndLateFree(t *testing.T) {
	owner := MustNew("onheap-delete-concurrent-owner")
	other := MustNew("onheap-delete-concurrent-free")
	t.Cleanup(func() { DeleteMPool(owner); DeleteMPool(other) })
	globalBytes := GlobalOnHeapStats().NumCurrBytes.Load()
	globalObjects := GlobalOnHeapStats().NumCurrObjects.Load()
	buffers := make([][]byte, 8)
	t.Cleanup(func() {
		for _, buffer := range buffers {
			if buffer != nil {
				owner.Free(buffer)
			}
		}
	})
	for i := range buffers {
		var err error
		buffers[i], err = owner.Alloc(64, false)
		require.NoError(t, err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); <-start; DeleteMPool(owner) }()
	go func() {
		defer wg.Done()
		<-start
		for i, buffer := range buffers {
			other.Free(buffer)
			buffers[i] = nil
		}
	}()
	close(start)
	wg.Wait()
	requireOnHeapOwnershipMatchesRegistry(t, owner)
	require.Zero(t, owner.OnHeapCurrNB())
	require.Equal(t, globalBytes, GlobalOnHeapStats().NumCurrBytes.Load())
	require.Equal(t, globalObjects, GlobalOnHeapStats().NumCurrObjects.Load())
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
