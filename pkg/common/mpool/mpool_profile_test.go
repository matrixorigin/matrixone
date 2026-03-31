// Copyright 2021 Matrix Origin
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
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/stretchr/testify/require"
)

func TestProfileAllocFree(t *testing.T) {
	EnableProfiling()
	defer DisableProfiling()

	mp, err := NewMPool("test-profile", 0, NoFixed)
	require.NoError(t, err)
	defer DeleteMPool(mp)

	before := ProfileTrackedCount()

	// Off-heap allocation should increase tracked count
	bs, err := mp.Alloc(4096, true)
	require.NoError(t, err)
	require.Equal(t, before+1, ProfileTrackedCount())

	// Free should decrease tracked count
	mp.Free(bs)
	require.Equal(t, before, ProfileTrackedCount())
}

func TestProfileOnHeapNotTracked(t *testing.T) {
	EnableProfiling()
	defer DisableProfiling()

	mp, err := NewMPool("test-profile-onheap", 0, NoFixed)
	require.NoError(t, err)
	defer DeleteMPool(mp)

	before := ProfileTrackedCount()

	// On-heap allocation should NOT change tracked count
	bs, err := mp.Alloc(1024, false)
	require.NoError(t, err)
	require.Equal(t, before, ProfileTrackedCount())

	mp.Free(bs)
}

func TestProfileMultipleAllocs(t *testing.T) {
	EnableProfiling()
	defer DisableProfiling()

	mp, err := NewMPool("test-profile-multi", 0, NoFixed)
	require.NoError(t, err)
	defer DeleteMPool(mp)

	before := ProfileTrackedCount()

	// Multiple off-heap allocations
	allocs := make([][]byte, 10)
	for i := range allocs {
		allocs[i], err = mp.Alloc(1024, true)
		require.NoError(t, err)
	}
	require.Equal(t, before+10, ProfileTrackedCount())

	// Free all
	for _, bs := range allocs {
		mp.Free(bs)
	}
	require.Equal(t, before, ProfileTrackedCount())
}

func TestProfileWritable(t *testing.T) {
	EnableProfiling()
	defer DisableProfiling()

	mp, err := NewMPool("test-profile-write", 0, NoFixed)
	require.NoError(t, err)
	defer DeleteMPool(mp)

	bs, err := mp.Alloc(8192, true)
	require.NoError(t, err)

	var buf bytes.Buffer
	profiler := malloc.GlobalProfiler()
	require.NoError(t, profiler.Write(&buf))
	require.True(t, buf.Len() > 0, "profile output should be non-empty")

	mp.Free(bs)
}
