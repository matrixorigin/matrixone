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

package system

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestWorkspaceCgroupHeadroom(t *testing.T) {
	root := t.TempDir()
	leaf := filepath.Join(root, "leaf")
	require.NoError(t, os.Mkdir(leaf, 0700))
	write := func(dir, name, value string) {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte(value), 0600))
	}
	write(root, "memory.max", "1000")
	write(root, "memory.current", "900")
	write(leaf, "memory.max", "500")
	write(leaf, "memory.current", "100")
	got, ok := minHierarchicalHeadroom(leaf, root, "memory.max", "memory.current", "memory.stat")
	require.True(t, ok)
	require.Equal(t, uint64(100), got)
	write(root, "memory.current", "1000")
	got, ok = minHierarchicalHeadroom(leaf, root, "memory.max", "memory.current", "memory.stat")
	require.True(t, ok)
	require.Zero(t, got)
	require.NoError(t, os.Remove(filepath.Join(root, "memory.current")))
	_, ok = minHierarchicalHeadroom(leaf, root, "memory.max", "memory.current", "memory.stat")
	require.False(t, ok)
}
func TestWorkspaceReclaimableCache(t *testing.T) {
	path := filepath.Join(t.TempDir(), "memory.stat")
	require.NoError(t, os.WriteFile(path, []byte("file 500\nshmem 100\ninactive_file 200\nactive_file 200\nfile_dirty 20\nfile_writeback 30\n"), 0600))
	got, ok := reclaimableCgroupCache(path)
	require.True(t, ok)
	require.Equal(t, uint64(350), got)
	require.NoError(t, os.WriteFile(path, []byte("file 500\n"), 0600))
	_, ok = reclaimableCgroupCache(path)
	require.False(t, ok)
	require.Zero(t, normalizeCgroupLimit(int64(cgroupV1Unlimited)))
}

func TestUnavailableHierarchyDoesNotFallBackToHost(t *testing.T) {
	const gib = uint64(1 << 30)
	hostCalled := false
	available, measured := memoryAvailableIncludingCacheWithSources(
		func() (uint64, bool) { return 0, false },
		func() uint64 { return 8 * gib },
		func() (uint64, bool) { hostCalled = true; return 16 * gib, true },
	)
	require.False(t, measured)
	require.Zero(t, available)
	require.False(t, hostCalled)
	// No cgroup limit: ordinary host fallback remains valid.
	available, measured = memoryAvailableIncludingCacheWithSources(
		func() (uint64, bool) { return 0, false },
		func() uint64 { return 0 },
		func() (uint64, bool) { return 16 * gib, true },
	)
	require.True(t, measured)
	require.Equal(t, 16*gib, available)
	// A valid ancestor sample still clamps to host headroom.
	available, measured = memoryAvailableIncludingCacheWithSources(
		func() (uint64, bool) { return gib, true },
		func() uint64 { t.Fatal("limit fallback must not run"); return 0 },
		func() (uint64, bool) { return 16 * gib, true },
	)
	require.True(t, measured)
	require.Equal(t, gib, available)
}
