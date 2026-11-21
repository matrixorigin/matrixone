// Copyright 2026 Matrix Origin
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

//go:build linux

package rscthrottler

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/stretchr/testify/require"
)

func TestPressureCgroupV2Directory(t *testing.T) {
	mount := t.TempDir()
	dir := filepath.Join(mount, "parent", "child")
	require.NoError(t, os.MkdirAll(dir, 0700))
	for _, path := range []string{mount, filepath.Dir(dir), dir} {
		require.NoError(t, os.WriteFile(filepath.Join(path, "cgroup.controllers"), nil, 0600))
	}
	parent := filepath.Dir(dir)
	require.NoError(t, os.WriteFile(filepath.Join(mount, "cgroup.subtree_control"), []byte("cpu\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(parent, "cgroup.subtree_control"), []byte("memory\n"), 0600))
	_, err := pressureCgroupV2Directory(mount, dir)
	require.Error(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(parent, "cgroup.subtree_control"), []byte("cpu\n"), 0600))
	_, err = pressureCgroupV2Directory(mount, dir)
	require.ErrorIs(t, err, memlimit.ErrNoLimit)
	require.NoError(t, os.WriteFile(filepath.Join(parent, "memory.max"), []byte("1000\n"), 0600))
	limit, err := pressureCgroupV2Directory(mount, dir)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), limit)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0600))
	limit, err = pressureCgroupV2Directory(mount, dir)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), limit)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("invalid\n"), 0600))
	_, err = pressureCgroupV2Directory(mount, dir)
	require.Error(t, err)
	require.NoError(t, os.Remove(filepath.Join(dir, "memory.max")))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "memory.max"), 0700))
	_, err = pressureCgroupV2Directory(mount, dir)
	require.Error(t, err)
	_, err = pressureCgroupV2Directory(mount, mount)
	require.ErrorIs(t, err, memlimit.ErrNoLimit)
	_, err = pressureCgroupV2Directory(mount, filepath.Join(mount, "missing"))
	require.Error(t, err)
}

func TestParseCgroupV1Limit(t *testing.T) {
	limit, err := parseCgroupV1Limit([]byte("hierarchical_memory_limit 1000\n"), 4096)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), limit)
	_, err = parseCgroupV1Limit([]byte("hierarchical_memory_limit 9223372036854771712\n"), 4096)
	require.ErrorIs(t, err, memlimit.ErrNoLimit)
	for _, data := range []string{"", "hierarchical_memory_limit invalid", "hierarchical_memory_limit 0"} {
		_, err = parseCgroupV1Limit([]byte(data), 4096)
		require.Error(t, err)
	}
}

func TestParsePressureCgroupMount(t *testing.T) {
	data := []byte("30 1 0:1 /docker/abc /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n" +
		"31 1 0:2 /tenant /sys/fs/cgroup/unified rw - cgroup2 cgroup rw\n" +
		"32 1 0:2 /tenant/nested /mnt/bind rw - cgroup2 cgroup rw\n")
	mount, dir, err := parsePressureCgroupMount(data, "/docker/abc", "cgroup")
	require.NoError(t, err)
	require.Equal(t, "/sys/fs/cgroup/memory", mount)
	require.Equal(t, "/sys/fs/cgroup/memory", dir)
	mount, dir, err = parsePressureCgroupMount(data, "/tenant/nested", "cgroup2")
	require.NoError(t, err)
	require.Equal(t, "/sys/fs/cgroup/unified", mount)
	require.Equal(t, "/sys/fs/cgroup/unified/nested", dir)
	_, _, err = parsePressureCgroupMount(data, "/docker/other", "cgroup")
	require.Error(t, err)
}
