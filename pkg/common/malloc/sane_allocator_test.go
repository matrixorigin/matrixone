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

package malloc

import (
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestSimpleCAllocator() *SimpleCAllocator {
	return NewSimpleCAllocator(nil, nil, nil, nil, nil)
}

func TestSimpleCAllocatorMmapBoundary(t *testing.T) {
	require.False(t, simpleCAllocatorUsesMmap(simpleCAllocatorMmapThreshold-1))
	require.True(t, simpleCAllocatorUsesMmap(simpleCAllocatorMmapThreshold))
	require.True(t, simpleCAllocatorUsesMmap(simpleCAllocatorMmapThreshold+1))
}

func TestSimpleCAllocatorAllocateAndDeallocate(t *testing.T) {
	for _, size := range []uint64{
		1,
		simpleCAllocatorMmapThreshold - 1,
		simpleCAllocatorMmapThreshold,
		simpleCAllocatorMmapThreshold + 1,
	} {
		t.Run(testNameForSize(size), func(t *testing.T) {
			allocator := newTestSimpleCAllocator()

			slice, err := allocator.Allocate(size)
			require.NoError(t, err)
			require.Len(t, slice, int(size))
			require.Equal(t, int(size), cap(slice))
			require.Equal(t, make([]byte, size), slice)
			require.Equal(t, int64(size), allocator.currentInuse.Load())

			slice[0] = 1
			slice[len(slice)-1] = 2
			allocator.Deallocate(slice, size)
			require.Zero(t, allocator.currentInuse.Load())
		})
	}
}

func TestSimpleCAllocatorZeroSize(t *testing.T) {
	allocator := newTestSimpleCAllocator()

	slice, err := allocator.Malloc(0)
	require.NoError(t, err)
	require.Nil(t, slice)

	slice, err = allocator.Allocate(0)
	require.NoError(t, err)
	require.Nil(t, slice)
	require.Zero(t, allocator.currentInuse.Load())
}

func TestSimpleCAllocatorMallocAndDeallocate(t *testing.T) {
	for _, size := range []uint64{
		simpleCAllocatorMmapThreshold - 1,
		simpleCAllocatorMmapThreshold,
	} {
		t.Run(testNameForSize(size), func(t *testing.T) {
			allocator := newTestSimpleCAllocator()

			slice, err := allocator.Malloc(size)
			require.NoError(t, err)
			require.Len(t, slice, int(size))
			require.Equal(t, int(size), cap(slice))
			require.Equal(t, int64(size), allocator.currentInuse.Load())

			slice[0] = 1
			slice[len(slice)-1] = 2
			allocator.Deallocate(slice, size)
			require.Zero(t, allocator.currentInuse.Load())
		})
	}
}

func TestSimpleCAllocatorReallocZeroTransitions(t *testing.T) {
	testCases := []struct {
		name    string
		oldSize uint64
		newSize uint64
	}{
		{
			name:    "small-to-small",
			oldSize: simpleCAllocatorMmapThreshold / 2,
			newSize: simpleCAllocatorMmapThreshold - 1,
		},
		{
			name:    "small-to-large",
			oldSize: simpleCAllocatorMmapThreshold - 1,
			newSize: simpleCAllocatorMmapThreshold,
		},
		{
			name:    "large-to-large",
			oldSize: simpleCAllocatorMmapThreshold,
			newSize: simpleCAllocatorMmapThreshold * 2,
		},
		{
			name:    "large-to-small",
			oldSize: simpleCAllocatorMmapThreshold,
			newSize: simpleCAllocatorMmapThreshold / 2,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			allocator := newTestSimpleCAllocator()
			old, err := allocator.Allocate(testCase.oldSize)
			require.NoError(t, err)
			for i := range old {
				old[i] = byte(i%251 + 1)
			}

			resized, err := allocator.ReallocZero(old, testCase.newSize)
			require.NoError(t, err)
			require.Len(t, resized, int(testCase.newSize))
			require.Equal(t, int(testCase.newSize), cap(resized))
			require.Equal(t, int64(testCase.newSize), allocator.currentInuse.Load())

			preserved := min(testCase.oldSize, testCase.newSize)
			for i := uint64(0); i < preserved; i++ {
				require.Equal(t, byte(i%251+1), resized[i])
			}
			for i := testCase.oldSize; i < testCase.newSize; i++ {
				require.Zero(t, resized[i])
			}

			allocator.Deallocate(resized, testCase.newSize)
			require.Zero(t, allocator.currentInuse.Load())
		})
	}
}

func TestSimpleCAllocatorReallocZeroUsesCapacityForOwnership(t *testing.T) {
	allocator := newTestSimpleCAllocator()
	old, err := allocator.Allocate(simpleCAllocatorMmapThreshold)
	require.NoError(t, err)
	for i := range old {
		old[i] = 0x7f
	}

	logicalLength := simpleCAllocatorMmapThreshold / 2
	resized, err := allocator.ReallocZero(
		old[:logicalLength],
		simpleCAllocatorMmapThreshold*2,
	)
	require.NoError(t, err)
	require.Equal(t, int64(simpleCAllocatorMmapThreshold*2), allocator.currentInuse.Load())
	require.Equal(t, makeRepeatedByteSlice(logicalLength, 0x7f), resized[:logicalLength])
	require.Equal(
		t,
		make([]byte, simpleCAllocatorMmapThreshold*2-logicalLength),
		resized[logicalLength:],
	)

	allocator.Deallocate(resized, simpleCAllocatorMmapThreshold*2)
	require.Zero(t, allocator.currentInuse.Load())
}

func TestSimpleCAllocatorReallocZeroFromAndToEmpty(t *testing.T) {
	allocator := newTestSimpleCAllocator()

	slice, err := allocator.ReallocZero(nil, simpleCAllocatorMmapThreshold)
	require.NoError(t, err)
	require.Len(t, slice, simpleCAllocatorMmapThreshold)
	require.Equal(t, int64(simpleCAllocatorMmapThreshold), allocator.currentInuse.Load())

	slice, err = allocator.ReallocZero(slice, 0)
	require.NoError(t, err)
	require.Nil(t, slice)
	require.Zero(t, allocator.currentInuse.Load())

	slice, err = allocator.ReallocZero(nil, 0)
	require.NoError(t, err)
	require.Nil(t, slice)
	require.Zero(t, allocator.currentInuse.Load())
}

func TestSimpleCAllocatorDeallocateSizeMismatch(t *testing.T) {
	allocator := newTestSimpleCAllocator()

	require.Panics(t, func() {
		allocator.Deallocate(nil, 1)
	})

	slice, err := allocator.Allocate(simpleCAllocatorMmapThreshold)
	require.NoError(t, err)
	require.Panics(t, func() {
		allocator.Deallocate(slice, simpleCAllocatorMmapThreshold-1)
	})
	allocator.Deallocate(slice, simpleCAllocatorMmapThreshold)
}

func TestSimpleCAllocatorConcurrentTransitions(t *testing.T) {
	allocator := newTestSimpleCAllocator()
	const goroutines = 8
	const iterations = 32

	var waitGroup sync.WaitGroup
	errs := make(chan error, goroutines)
	waitGroup.Add(goroutines)
	for goroutine := 0; goroutine < goroutines; goroutine++ {
		go func() {
			defer waitGroup.Done()
			for iteration := 0; iteration < iterations; iteration++ {
				slice, err := allocator.Allocate(simpleCAllocatorMmapThreshold - 1)
				if err != nil {
					errs <- err
					return
				}
				slice[0] = 1

				slice, err = allocator.ReallocZero(slice, simpleCAllocatorMmapThreshold)
				if err != nil {
					errs <- err
					return
				}
				if slice[0] != 1 {
					errs <- fmt.Errorf("data not preserved: got %d", slice[0])
					return
				}

				allocator.Deallocate(slice, simpleCAllocatorMmapThreshold)
			}
		}()
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Zero(t, allocator.currentInuse.Load())
}

func testNameForSize(size uint64) string {
	switch size {
	case simpleCAllocatorMmapThreshold - 1:
		return "below-threshold"
	case simpleCAllocatorMmapThreshold:
		return "at-threshold"
	case simpleCAllocatorMmapThreshold + 1:
		return "above-threshold"
	default:
		return "size-" + strconv.FormatUint(size, 10)
	}
}

func makeRepeatedByteSlice(size int, value byte) []byte {
	slice := make([]byte, size)
	for i := range slice {
		slice[i] = value
	}
	return slice
}
