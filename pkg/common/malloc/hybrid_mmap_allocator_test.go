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
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHybridMmapAllocator(t *testing.T) {
	testAllocator(t, func() Allocator {
		return NewHybridMmapAllocator()
	})
}

func TestHybridMmapAllocationSize(t *testing.T) {
	pageSize := uint64(os.Getpagesize())
	tests := []struct {
		request uint64
		want    uint64
		ok      bool
	}{
		{0, 0, false},
		{1, pageSize, true},
		{pageSize, pageSize, true},
		{pageSize + 1, 2 * pageSize, true},
		{700 << 10, (700<<10 + pageSize - 1) &^ (pageSize - 1), true},
		{maxClassSize, maxClassSize, true},
		{maxClassSize + 1, 0, false},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("request-%d", test.request), func(t *testing.T) {
			got, ok := HybridMmapAllocationSize(test.request)
			require.Equal(t, test.ok, ok)
			require.Equal(t, test.want, got)
		})
	}
}

func TestHybridMmapAllocatorReportsAndAllocatesPageBacking(t *testing.T) {
	allocator := NewHybridMmapAllocator()
	pageSize := uint64(os.Getpagesize())

	for _, request := range []uint64{1, pageSize + 1, 127<<10 + 1, 700 << 10} {
		want := (request + pageSize - 1) &^ (pageSize - 1)
		got, err := BackingSize(allocator, request)
		require.NoError(t, err)
		require.Equal(t, want, got)

		buf, dec, err := allocator.Allocate(request, NoHints)
		require.NoError(t, err)
		require.Len(t, buf, int(request))
		require.Equal(t, int(want), cap(buf))

		var info MmapInfo
		require.True(t, dec.As(&info))
		require.Equal(t, want, info.Length)
		dec.Deallocate()
	}
}

func TestHybridMmapAllocatorSupportsReadOnlyDecoration(t *testing.T) {
	allocator := NewReadOnlyAllocator(NewHybridMmapAllocator())
	for _, request := range []uint64{4 << 10, 700 << 10} {
		_, dec, err := allocator.Allocate(request, NoHints)
		require.NoError(t, err)
		var freezer Freezer
		require.True(t, dec.As(&freezer))
		freezer.Freeze()
		dec.Deallocate()
	}
}

func TestHybridMmapAllocatorConcurrent(t *testing.T) {
	allocator := NewHybridMmapAllocator()
	const goroutines = 16
	const allocations = 200

	var wg sync.WaitGroup
	start := make(chan struct{})
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for j := 0; j < allocations; j++ {
				size := uint64(1 + (worker*allocations+j)%(256<<10))
				buf, dec, err := allocator.Allocate(size, NoHints)
				if err != nil {
					errCh <- err
					return
				}
				if len(buf) != int(size) {
					errCh <- errors.New("allocator returned the wrong length")
					dec.Deallocate()
					return
				}
				buf[0] = byte(worker)
				buf[len(buf)-1] = byte(j)
				dec.Deallocate()
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}
}

func TestHybridMmapAllocatorFragmentationProfiles(t *testing.T) {
	type weightedSize struct {
		size   uint64
		weight uint64
	}
	profiles := []struct {
		name             string
		sizes            []weightedSize
		maxLinuxOverhead float64
	}{
		{
			name:             "large-block",
			maxLinuxOverhead: 1,
			sizes: []weightedSize{
				{512<<10 + 123, 25},
				{584323, 25},
				{700<<10 + 17, 25},
				{MB - 31, 25},
			},
		},
		{
			name:             "mixed-size",
			maxLinuxOverhead: 5,
			sizes: []weightedSize{
				{1500, 30},
				{6500, 25},
				{30000, 20},
				{110000, 15},
				{580000, 10},
			},
		},
		{
			name:             "long-tail",
			maxLinuxOverhead: 5,
			sizes: []weightedSize{
				{512, 50},
				{3500, 20},
				{18000, 15},
				{130000, 10},
				{1536<<10 + 123, 5},
			},
		},
		{
			name:             "tiny-object-stress",
			maxLinuxOverhead: 300,
			sizes: []weightedSize{
				{64, 20},
				{128, 20},
				{512, 20},
				{1500, 20},
				{3500, 20},
			},
		},
	}

	allocator := NewHybridMmapAllocator()
	pageSize := uint64(os.Getpagesize())
	for _, profile := range profiles {
		t.Run(profile.name, func(t *testing.T) {
			var logical uint64
			var backing uint64
			var linuxBacking uint64
			var oldClassBacking uint64
			var objects uint64
			for _, item := range profile.sizes {
				allocated, err := BackingSize(allocator, item.size)
				require.NoError(t, err)
				require.Less(t, allocated-item.size, pageSize)
				linuxAllocated := (item.size + 4095) &^ uint64(4095)
				// The old class value is what cache accounting reported; for
				// sub-page mmap allocations it was smaller than physical backing.
				oldAllocated, ok := ClassAllocationSize(item.size)
				require.True(t, ok)
				logical += item.size * item.weight
				backing += allocated * item.weight
				linuxBacking += linuxAllocated * item.weight
				oldClassBacking += oldAllocated * item.weight
				objects += item.weight
			}

			overheadPercent := float64(backing-logical) / float64(logical) * 100
			linuxOverheadPercent := float64(linuxBacking-logical) / float64(logical) * 100
			oldClassOverheadPercent := float64(oldClassBacking-logical) / float64(logical) * 100
			maxPercent := float64(objects*(pageSize-1)) / float64(logical) * 100
			require.LessOrEqual(t, overheadPercent, maxPercent)
			require.LessOrEqual(t, linuxOverheadPercent, profile.maxLinuxOverhead)
			t.Logf(
				"objects=%d average=%d old-class=%.3f%% linux-4k=%.3f%% host-page-%d=%.3f%% bound=%.3f%%",
				objects,
				logical/objects,
				oldClassOverheadPercent,
				linuxOverheadPercent,
				pageSize,
				overheadPercent,
				maxPercent,
			)
		})
	}
}

func BenchmarkHybridMmapAllocator(b *testing.B) {
	for _, n := range benchNs {
		benchmarkAllocator(b, func() Allocator {
			return NewHybridMmapAllocator()
		}, n)
	}
}
