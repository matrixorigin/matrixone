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

//go:build cgo

package malloc

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJemallocAllocator(t *testing.T) {
	allocator, err := NewJemallocAllocator()
	require.NoError(t, err)
	testAllocator(t, func() Allocator { return allocator })
}

func TestJemallocAllocatorReportsClassBackingAndArenaStats(t *testing.T) {
	allocator, err := NewJemallocAllocator()
	require.NoError(t, err)

	const request = 700 * 1024
	backingSize, err := allocator.BackingSize(request)
	require.NoError(t, err)
	require.GreaterOrEqual(t, backingSize, uint64(request))

	before, err := allocator.Stats()
	require.NoError(t, err)
	buf, dec, err := allocator.Allocate(request, NoHints)
	require.NoError(t, err)
	require.Len(t, buf, request)
	require.Equal(t, int(backingSize), cap(buf))

	during, err := allocator.Stats()
	require.NoError(t, err)
	require.GreaterOrEqual(t, during.Allocated, before.Allocated+backingSize)
	require.GreaterOrEqual(t, during.Active, during.Allocated)

	dec.Deallocate()
	after, err := allocator.Stats()
	require.NoError(t, err)
	require.Equal(t, before.Allocated, after.Allocated)
}

func TestJemallocAllocatorArenaStatsCoverMixedLiveSizeClasses(t *testing.T) {
	allocator, err := NewJemallocAllocator()
	require.NoError(t, err)

	before, err := allocator.Stats()
	require.NoError(t, err)

	requests := []uint64{4 << 10, 128 << 10, 700 << 10, 1 << 20, 1500 << 10, 2 << 20}
	var backing uint64
	deallocators := make([]Deallocator, 0, len(requests))
	for _, request := range requests {
		size, err := allocator.BackingSize(request)
		require.NoError(t, err)
		_, dec, err := allocator.Allocate(request, NoHints)
		require.NoError(t, err)
		backing += size
		deallocators = append(deallocators, dec)
	}

	during, err := allocator.Stats()
	require.NoError(t, err)
	require.GreaterOrEqual(t, during.Allocated, before.Allocated+backing)

	for _, dec := range deallocators {
		dec.Deallocate()
	}
	after, err := allocator.Stats()
	require.NoError(t, err)
	require.Equal(t, before.Allocated, after.Allocated)
}

func TestJemallocAllocatorUsesIndependentArenas(t *testing.T) {
	first, err := NewJemallocAllocator()
	require.NoError(t, err)
	second, err := NewJemallocAllocator()
	require.NoError(t, err)
	require.NotEqual(t, first.Arena(), second.Arena())
}
