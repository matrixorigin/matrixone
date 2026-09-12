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
	"math/bits"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

type onHeapShardHintMask [(numPtrShards + 63) / 64]uint64

func expectedOnHeapShardHints(mp *MPool) (expected onHeapShardHintMask) {
	for shardIndex := range globalPtrShards {
		shard := &globalPtrShards[shardIndex]
		shard.mu.Lock()
		for _, hdr := range shard.m {
			if hdr.poolId == mp.id && !hdr.isOffHeap() {
				expected[shardIndex/64] |= uint64(1) << (shardIndex % 64)
				break
			}
		}
		shard.mu.Unlock()
	}
	return expected
}

func onHeapShardHintsForTest(mp *MPool) (got onHeapShardHintMask) {
	for i := range got {
		got[i] = mp.onHeapShardHints[i].Load()
	}
	return got
}

func requireHintedOutstandingMatchesRegistry(t *testing.T, mp *MPool) {
	t.Helper()
	wantBytes, wantObjects := mp.OnHeapOutstanding()
	gotBytes, gotObjects := mp.scanOnHeapOutstandingHinted()
	require.Equal(t, wantBytes, gotBytes)
	require.Equal(t, wantObjects, gotObjects)
}

func TestOnHeapShardHintsAreConservativeAndExact(t *testing.T) {
	owner := MustNew("onheap-shard-hint-owner")
	other := MustNew("onheap-shard-hint-other")
	freeingPool := MustNew("onheap-shard-hint-free")
	defer DeleteMPool(owner)
	defer DeleteMPool(other)
	defer DeleteMPool(freeingPool)

	ownerBuffers := make([][]byte, 0, 3)
	for _, size := range []int{64, 96, 128} {
		buffer, err := owner.Alloc(size, false)
		require.NoError(t, err)
		ownerBuffers = append(ownerBuffers, buffer)
	}
	expected := expectedOnHeapShardHints(owner)
	require.Equal(t, expected, onHeapShardHintsForTest(owner), "each on-heap pointer's shard must be hinted")
	requireHintedOutstandingMatchesRegistry(t, owner)

	// Off-heap allocations do not publish on-heap ownership hints.
	offHeap, err := owner.Alloc(256, true)
	require.NoError(t, err)
	require.Equal(t, expected, onHeapShardHintsForTest(owner))

	otherBuffer, err := other.Alloc(48, false)
	require.NoError(t, err)
	freeingPool.Free(ownerBuffers[0])
	ownerBuffers[0] = nil
	require.Equal(t, expected, onHeapShardHintsForTest(owner), "hints remain set after frees to avoid false negatives")
	requireHintedOutstandingMatchesRegistry(t, owner)
	requireHintedOutstandingMatchesRegistry(t, other)

	freeingPool.Free(ownerBuffers[1])
	ownerBuffers[1] = nil
	owner.Free(ownerBuffers[2])
	owner.Free(offHeap)
	other.Free(otherBuffer)
	requireHintedOutstandingMatchesRegistry(t, owner)
	requireHintedOutstandingMatchesRegistry(t, other)
	ownerBytes, ownerObjects := owner.scanOnHeapOutstandingHinted()
	require.Zero(t, ownerBytes)
	require.Zero(t, ownerObjects)
	otherBytes, otherObjects := other.scanOnHeapOutstandingHinted()
	require.Zero(t, otherBytes)
	require.Zero(t, otherObjects)
	require.Equal(t, expected, onHeapShardHintsForTest(owner), "empty owners retain only harmless conservative hints")
}

func TestOnHeapShardHintsConcurrentPublicationAndCrossPoolFree(t *testing.T) {
	owner := MustNew("onheap-shard-hint-concurrent-owner")
	freeingPool := MustNew("onheap-shard-hint-concurrent-free")
	defer DeleteMPool(owner)
	defer DeleteMPool(freeingPool)

	const (
		workers = 8
		ops     = 64
	)
	buffers := make([][][]byte, workers)
	start := make(chan struct{})
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for range ops {
				buffer, err := owner.Alloc(64, false)
				if err != nil {
					errCh <- err
					return
				}
				buffers[worker] = append(buffers[worker], buffer)
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	expected := expectedOnHeapShardHints(owner)
	require.Equal(t, expected, onHeapShardHintsForTest(owner), "concurrent first publications must not lose bits")
	requireHintedOutstandingMatchesRegistry(t, owner)

	start = make(chan struct{})
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for _, buffer := range buffers[worker] {
				freeingPool.Free(buffer)
			}
		}(worker)
	}
	close(start)
	wg.Wait()
	requireHintedOutstandingMatchesRegistry(t, owner)
	ownerBytes, ownerObjects := owner.scanOnHeapOutstandingHinted()
	require.Zero(t, ownerBytes)
	require.Zero(t, ownerObjects)
	require.Equal(t, expected, onHeapShardHintsForTest(owner), "freeing must not clear a shard that may be reused")
}

func BenchmarkMPoolDestroyUnrelatedPointersByOwnerSize(b *testing.B) {
	for _, unrelatedSize := range []int{0, 10_000, 100_000} {
		for _, ownerSize := range []int{0, 1, 8, 32, 128} {
			b.Run(fmt.Sprintf("unrelated=%d/owned=%d", unrelatedSize, ownerSize), func(b *testing.B) {
				onHeapOwner := MustNew("shard-hint-bench-onheap")
				offHeapOwner := MustNew("shard-hint-bench-offheap")
				defer DeleteMPool(onHeapOwner)
				defer DeleteMPool(offHeapOwner)
				buffers := make([][]byte, 0, unrelatedSize)
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

				sample := MustNew("shard-hint-bench-target-sample")
				sampleBuffers := make([][]byte, ownerSize)
				for i := range sampleBuffers {
					buffer, err := sample.Alloc(64, false)
					if err != nil {
						b.Fatal(err)
					}
					sampleBuffers[i] = buffer
				}
				touchedShards := 0
				sampleHintMask := expectedOnHeapShardHints(sample)
				for _, word := range sampleHintMask {
					touchedShards += bits.OnesCount64(word)
				}
				for _, buffer := range sampleBuffers {
					sample.Free(buffer)
				}
				DeleteMPool(sample)

				targetBuffers := make([][]byte, ownerSize)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					target := MustNew("shard-hint-bench-target")
					for i := range targetBuffers {
						buffer, err := target.Alloc(64, false)
						if err != nil {
							b.Fatal(err)
						}
						targetBuffers[i] = buffer
					}
					for _, buffer := range targetBuffers {
						target.Free(buffer)
					}
					DeleteMPool(target)
				}
				b.StopTimer()
				b.ReportMetric(float64(touchedShards), "touched-shards")
				for i, buffer := range buffers {
					if i%2 == 0 {
						onHeapOwner.Free(buffer)
					} else {
						offHeapOwner.Free(buffer)
					}
				}
			})
		}
	}
}
