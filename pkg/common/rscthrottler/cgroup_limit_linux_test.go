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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/stretchr/testify/require"
)

func TestPressureCgroupV2Level(t *testing.T) {
	mount := t.TempDir()
	group := "/parent/child"
	views := []pressureCgroupView{{root: "/", mount: mount}}
	dir := filepath.Join(mount, group)
	require.NoError(t, os.MkdirAll(dir, 0700))
	for _, path := range []string{mount, filepath.Dir(dir), dir} {
		require.NoError(t, os.WriteFile(filepath.Join(path, "cgroup.controllers"), nil, 0600))
	}
	parent := filepath.Dir(dir)
	require.NoError(t, os.WriteFile(filepath.Join(mount, "cgroup.subtree_control"), []byte("cpu\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(parent, "cgroup.subtree_control"), []byte("memory\n"), 0600))
	_, err := pressureCgroupV2Hierarchy(group, views)
	require.Error(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(parent, "cgroup.subtree_control"), []byte("cpu\n"), 0600))
	_, err = pressureCgroupV2Hierarchy(group, views)
	require.ErrorIs(t, err, memlimit.ErrNoLimit)
	require.NoError(t, os.WriteFile(filepath.Join(parent, "memory.max"), []byte("1000\n"), 0600))
	limit, err := pressureCgroupV2Hierarchy(group, views)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), limit)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("max\n"), 0600))
	limit, err = pressureCgroupV2Hierarchy(group, views)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), limit)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte("invalid\n"), 0600))
	_, err = pressureCgroupV2Hierarchy(group, views)
	require.Error(t, err)
	require.NoError(t, os.Remove(filepath.Join(dir, "memory.max")))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "memory.max"), 0700))
	_, err = pressureCgroupV2Hierarchy(group, views)
	require.Error(t, err)
	_, err = pressureCgroupV2Hierarchy("/", views)
	require.ErrorIs(t, err, memlimit.ErrNoLimit)
	_, err = pressureCgroupV2Hierarchy("/missing", views)
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

func TestParsePressureCgroupViews(t *testing.T) {
	data := []byte("30 1 0:1 /docker/abc /sys/fs/cgroup/memory rw - cgroup cgroup rw,memory\n" +
		"32 1 0:2 /tenant/nested /mnt/bind rw - cgroup2 cgroup rw\n" +
		"31 1 0:2 /tenant /sys/fs/cgroup/unified rw - cgroup2 cgroup rw\n")
	v1, err := parsePressureCgroupViews(data, "/docker/abc", "cgroup")
	require.NoError(t, err)
	require.Equal(t, []pressureCgroupView{{root: "/docker/abc", mount: "/sys/fs/cgroup/memory"}}, v1)
	v2, err := parsePressureCgroupViews(data, "/tenant/nested", "cgroup2")
	require.NoError(t, err)
	require.Equal(t, []pressureCgroupView{
		{root: "/tenant", mount: "/sys/fs/cgroup/unified"},
		{root: "/tenant/nested", mount: "/mnt/bind"},
	}, v2)
	_, err = parsePressureCgroupViews(data, "/docker/abcd", "cgroup")
	require.Error(t, err)
}

func TestPressureCgroupV2AlternateViews(t *testing.T) {
	const group = "/tenant/child"
	for _, narrow := range []bool{false, true} {
		name := "same root"
		if narrow {
			name = "complementary ancestor and leaf"
		}
		t.Run(name, func(t *testing.T) {
			broad, alternate := t.TempDir(), t.TempDir()
			root := "/tenant"
			if narrow {
				root = group
			}
			data := []byte(fmt.Sprintf(
				"30 1 0:1 /tenant %s rw - cgroup2 cgroup rw\n31 1 0:1 %s %s rw - cgroup2 cgroup rw\n",
				broad, root, alternate,
			))
			views, err := parsePressureCgroupViews(data, group, "cgroup2")
			require.NoError(t, err)
			leaf, ok := views[1].directory(group)
			require.True(t, ok)
			require.NoError(t, os.MkdirAll(leaf, 0700))
			writeLevel := func(dir, limit string) {
				t.Helper()
				require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup.controllers"), nil, 0600))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte(limit), 0600))
			}
			writeLevel(leaf, "1000")
			if narrow {
				_, err = pressureCgroupV2Hierarchy(group, views)
				require.ErrorIs(t, err, os.ErrNotExist, "a finite leaf cannot prove an unreadable ancestor")
				writeLevel(leaf, "max")
				_, err = pressureCgroupV2Hierarchy(group, views)
				require.ErrorIs(t, err, os.ErrNotExist, "an unlimited leaf cannot prove an unreadable ancestor")
				writeLevel(broad, "500")
				limit, err := pressureCgroupV2Hierarchy(group, views)
				require.NoError(t, err)
				require.Equal(t, uint64(500), limit)
				return
			}
			writeLevel(alternate, "max")
			limit, err := pressureCgroupV2Hierarchy(group, views)
			require.NoError(t, err)
			require.Equal(t, uint64(1000), limit)
		})
	}
}

func TestPressureCgroupV2VisibleRoot(t *testing.T) {
	for _, root := range []string{"/tenant", "/"} {
		t.Run(root, func(t *testing.T) {
			views := []pressureCgroupView{
				{root: root, mount: t.TempDir()},
				{root: root, mount: t.TempDir()},
			}
			for _, view := range views {
				for file, contents := range map[string]string{"cgroup.controllers": "cpu memory", "cgroup.type": "domain"} {
					require.NoError(t, os.WriteFile(filepath.Join(view.mount, file), []byte(contents), 0600))
				}
			}
			memoryMax := filepath.Join(views[1].mount, "memory.max")
			_, err := pressureCgroupV2Hierarchy(root, views)
			require.ErrorIs(t, err, os.ErrNotExist, "an enabled non-global root must expose memory.max")
			require.NoError(t, os.WriteFile(memoryMax, []byte("500"), 0600))
			limit, err := pressureCgroupV2Hierarchy(root, views)
			require.NoError(t, err)
			require.Equal(t, uint64(500), limit)
			if root == "/" {
				require.NoError(t, os.Remove(filepath.Join(views[0].mount, "cgroup.type")))
				limit, err = pressureCgroupV2Hierarchy(root, views)
				require.NoError(t, err)
				require.Equal(t, uint64(500), limit, "an explicit limit wins over inferred absence")
				_, err = pressureCgroupV2Hierarchy(root, views[:1])
				require.ErrorIs(t, err, memlimit.ErrNoLimit)
			}
		})
	}
}

func TestPressureCgroupV1AlternateViews(t *testing.T) {
	views := []pressureCgroupView{
		{root: "/tenant", mount: t.TempDir()},
		{root: "/tenant", mount: t.TempDir()},
	}
	path := filepath.Join(views[1].mount, "memory.stat")
	_, err := pressureCgroupV1Hierarchy("/tenant", views)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.NoError(t, os.WriteFile(path, []byte("hierarchical_memory_limit 1000\n"), 0600))
	limit, err := pressureCgroupV1Hierarchy("/tenant", views)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), limit)

	page := uint64(os.Getpagesize())
	unlimited := uint64(^uint64(0)>>1) / page * page
	require.NoError(t, os.WriteFile(path, fmt.Appendf(nil, "hierarchical_memory_limit %d\n", unlimited), 0600))
	_, err = pressureCgroupV1Hierarchy("/tenant", views)
	require.ErrorIs(t, err, memlimit.ErrNoLimit)
}

func TestPressureCgroupV2CoveredMounts(t *testing.T) {
	const group = "/tenant/child"
	for _, tc := range []struct {
		name       string
		mountpoint string
		parentID   int
		fullCover  bool
	}{
		{name: "covered subtree", mountpoint: "wide/child", parentID: 30},
		{name: "covered limit file", mountpoint: "wide/child/memory.max", parentID: 30},
		{name: "stacked at mountpoint", mountpoint: "wide", parentID: 30, fullCover: true},
		{name: "covered ancestor", mountpoint: ".", parentID: 1, fullCover: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, alternate := t.TempDir(), t.TempDir()
			wide := filepath.Join(parent, "wide")
			leaf := filepath.Join(wide, "child")
			require.NoError(t, os.MkdirAll(leaf, 0700))
			for dir, limit := range map[string]string{wide: "500", leaf: "max", alternate: "1000"} {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup.controllers"), nil, 0600))
				require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte(limit), 0600))
			}
			mountinfo := fmt.Appendf(nil,
				"30 1 0:1 /tenant %s rw - cgroup2 cgroup rw\n31 %d 0:2 / %s rw - tmpfs tmpfs rw\n",
				wide, tc.parentID, filepath.Join(parent, tc.mountpoint),
			)
			withAlternate := fmt.Appendf(mountinfo, "32 1 0:1 %s %s rw - cgroup2 cgroup rw\n", group, alternate)
			views, err := parsePressureCgroupViews(withAlternate, group, "cgroup2")
			require.NoError(t, err)
			limit, err := pressureCgroupV2Hierarchy(group, views)
			require.NoError(t, err)
			if tc.fullCover {
				require.Equal(t, uint64(1000), limit, "only the narrower mount is visible")
			} else {
				require.Equal(t, uint64(500), limit, "the partially covered mount still exposes a stricter ancestor")
			}

			views, err = parsePressureCgroupViews(mountinfo, group, "cgroup2")
			if !tc.fullCover {
				require.NoError(t, err)
				_, err = pressureCgroupV2Hierarchy(group, views)
			}
			require.ErrorContains(t, err, "covered by another mount")
		})
	}
}

func TestPressureCgroupV1CoveredStat(t *testing.T) {
	wide, alternate := t.TempDir(), t.TempDir()
	path := filepath.Join(wide, "memory.stat")
	require.NoError(t, os.WriteFile(path, []byte("hierarchical_memory_limit 1000\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(alternate, "memory.stat"), []byte("hierarchical_memory_limit 500\n"), 0600))
	mountinfo := fmt.Appendf(nil,
		"30 1 0:1 /tenant %s rw - cgroup cgroup rw,memory\n31 30 0:3 / %s rw - tmpfs tmpfs rw\n32 1 0:1 /tenant %s rw - cgroup cgroup rw,memory\n",
		wide, path, alternate,
	)
	views, err := parsePressureCgroupViews(mountinfo, "/tenant", "cgroup")
	require.NoError(t, err)
	limit, err := pressureCgroupV1Hierarchy("/tenant", views)
	require.NoError(t, err)
	require.Equal(t, uint64(500), limit)
}

func TestParsePressureCgroupNamespaceRoot(t *testing.T) {
	mountinfo := []byte("30 1 0:1 /.. /inherit rw - cgroup2 cgroup rw\n" +
		"31 1 0:1 / /namespace rw - cgroup2 cgroup rw\n")
	views, err := parsePressureCgroupViews(mountinfo, "/", "cgroup2")
	require.NoError(t, err)
	require.Equal(t, []pressureCgroupView{{root: "/", mount: "/namespace"}}, views)
	_, err = parsePressureCgroupViews(mountinfo, "/../other", "cgroup2")
	require.ErrorContains(t, err, "outside the visible namespace")
}
