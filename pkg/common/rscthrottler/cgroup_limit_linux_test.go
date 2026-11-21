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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/shirou/gopsutil/v3/process"
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
	v1, _, err := parsePressureCgroupViews(data, "/docker/abc", "cgroup")
	require.NoError(t, err)
	require.Equal(t, []pressureCgroupView{{root: "/docker/abc", mount: "/sys/fs/cgroup/memory"}}, v1)
	v2, _, err := parsePressureCgroupViews(data, "/tenant/nested", "cgroup2")
	require.NoError(t, err)
	require.Equal(t, []pressureCgroupView{
		{root: "/tenant", mount: "/sys/fs/cgroup/unified"},
		{root: "/tenant/nested", mount: "/mnt/bind"},
	}, v2)
	_, _, err = parsePressureCgroupViews(data, "/docker/abcd", "cgroup")
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
			views, _, err := parsePressureCgroupViews(data, group, "cgroup2")
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
			views, knownRoot, err := parsePressureCgroupViews(withAlternate, group, "cgroup2")
			require.NoError(t, err)
			require.Equal(t, tc.fullCover, knownRoot != views[0].root)
			limit, err := pressureCgroupV2Hierarchy(group, views)
			require.NoError(t, err)
			if tc.fullCover {
				require.Equal(t, uint64(1000), limit, "only the narrower mount is visible")
			} else {
				require.Equal(t, uint64(500), limit, "the partially covered mount still exposes a stricter ancestor")
			}

			views, knownRoot, err = parsePressureCgroupViews(mountinfo, group, "cgroup2")
			require.Equal(t, "/tenant", knownRoot)
			if !tc.fullCover {
				require.NoError(t, err)
				_, err = pressureCgroupV2Hierarchy(group, views)
			}
			require.ErrorContains(t, err, "covered by another mount")
		})
	}
}

func TestPressureCgroupV2UsesFiniteBoundsAcrossUnreadableLevels(t *testing.T) {
	base := t.TempDir()
	parent := filepath.Join(base, "parent")
	leaf := filepath.Join(parent, "child")
	require.NoError(t, os.MkdirAll(leaf, 0700))
	for dir, value := range map[string]string{base: "2000", parent: "900", leaf: "1000"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte(value), 0600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup.controllers"), nil, 0600))
	}
	const group = "/tenant/parent/child"
	mount := fmt.Sprintf("30 1 0:1 /tenant %s rw - cgroup2 cgroup rw\n", base)
	masked := fmt.Sprintf("%s31 30 0:2 / %s rw - tmpfs tmpfs rw\n", mount, filepath.Join(parent, "memory.max"))
	observeRoot := func(string) {}
	ceiling, err := resolvePressureCgroupV2(group, []byte(masked), observeRoot)
	require.ErrorContains(t, err, "covered by another mount")
	require.Equal(t, rssPressureLimit{bytes: 1000, knownRoot: "/tenant", incomplete: true}, ceiling)
	metadataMasked := fmt.Sprintf("%s32 30 0:2 / %s rw - tmpfs tmpfs rw\n", masked, filepath.Join(leaf, "cgroup.controllers"))
	ceiling, err = resolvePressureCgroupV2(group, []byte(metadataMasked), observeRoot)
	require.ErrorContains(t, err, "covered by another mount")
	require.EqualValues(t, 1000, ceiling.bytes, "a directly readable limit needs no controller inference")

	// A failure at one level does not hide a valid stricter bound above it.
	ancestorLimit := filepath.Join(base, "memory.max")
	require.NoError(t, os.WriteFile(ancestorLimit, []byte("500"), 0600))
	ceiling, err = resolvePressureCgroupV2(group, []byte(masked), observeRoot)
	require.ErrorContains(t, err, "covered by another mount")
	require.EqualValues(t, 500, ceiling.bytes)
	require.NoError(t, os.WriteFile(ancestorLimit, []byte("2000"), 0600))

	targets := make(chan int64, 2)
	walks := make(chan context.Context, 1)
	clears := 0
	c := rssPressureController{
		setTarget:   func(target int64, _ time.Time) { targets <- target },
		clearTarget: func() { clears++ },
		evict: func(ctx context.Context, _ int64) {
			walks <- ctx
			<-ctx.Done()
		},
		free: func() {},
	}
	source := c.attach()
	defer source.Close()
	m := memThrottler{rssPressure: source}
	refresh := func(mountinfo string, rss uint64) {
		m.refreshMemoryWithPressure(
			func() (*process.MemoryInfoStat, error) { return &process.MemoryInfoStat{RSS: rss}, nil },
			func() (uint64, error) { return 4096, nil },
			func(_ uint64, _ error, publishRoot func(string)) (rssPressureLimit, error) {
				return resolvePressureCgroupV2(group, []byte(mountinfo), publishRoot)
			},
		)
	}

	refresh(masked, 950)
	require.Equal(t, rssCacheHardTarget, receivePressureEvent(t, targets))
	walk := receivePressureEvent(t, walks)
	require.EqualValues(t, 4096, m.actualTotalMemory.Load(), "local admission keeps its own ceiling")
	expiry := c.expires
	refresh(masked, 600)
	require.Equal(t, expiry, c.expires)
	require.NoError(t, walk.Err())
	// Neither an intact max above the gap nor one below it proves recovery.
	require.NoError(t, os.WriteFile(ancestorLimit, []byte("max"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(leaf, "memory.max"), []byte("max"), 0600))
	ceiling, err = resolvePressureCgroupV2(group, []byte(masked), observeRoot)
	require.ErrorContains(t, err, "covered by another mount")
	require.Zero(t, ceiling.bytes)
	refresh(masked, 600)
	require.Equal(t, expiry, c.expires)
	require.NoError(t, walk.Err())
	require.Zero(t, clears)
	require.Empty(t, targets)

	refresh(mount, 600)
	receivePressureEvent(t, walk.Done())
	require.Equal(t, 1, clears)
}

func TestPressureCgroupV2VisibilityChangeRetainsLease(t *testing.T) {
	const group = "/tenant/child"
	wide, narrow := t.TempDir(), t.TempDir()
	leaf := filepath.Join(wide, "child")
	require.NoError(t, os.Mkdir(leaf, 0700))
	for dir, limit := range map[string]string{wide: "500", leaf: "1000", narrow: "1000"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "cgroup.controllers"), nil, 0600))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte(limit), 0600))
	}
	detached := fmt.Sprintf("32 1 0:1 %s %s rw - cgroup2 cgroup rw\n", group, narrow)
	mounts := fmt.Sprintf("30 1 0:1 /tenant %s rw - cgroup2 cgroup rw\n%s", wide, detached)
	covered := fmt.Sprintf("%s31 30 0:2 / %s rw - tmpfs tmpfs rw\n", mounts, wide)
	alias := filepath.Join(t.TempDir(), "alternate")
	require.NoError(t, os.Symlink(wide, alias))
	restored := fmt.Sprintf("33 1 0:1 /tenant %s rw - cgroup2 cgroup rw\n%s", alias, detached)

	walks := make(chan context.Context, 1)
	var targets []int64
	var cleared int
	controller := rssPressureController{
		setTarget:   func(target int64, _ time.Time) { targets = append(targets, target) },
		clearTarget: func() { cleared++ },
		evict: func(ctx context.Context, _ int64) {
			walks <- ctx
			<-ctx.Done()
		},
		free: func() {},
	}
	cn, workspace := controller.attach(), controller.attach()
	defer cn.Close()
	defer workspace.Close()
	refresh := func(source *RSSPressureSource, mountInfo string, rss uint64, beforeResolve, beforeFiles func()) {
		throttler := memThrottler{rssPressure: source}
		throttler.refreshMemoryWithPressure(
			func() (*process.MemoryInfoStat, error) { return &process.MemoryInfoStat{RSS: rss}, nil },
			func() (uint64, error) { return 0, nil },
			func(_ uint64, _ error, publishRoot func(string)) (rssPressureLimit, error) {
				if beforeResolve != nil {
					beforeResolve()
				}
				return resolvePressureCgroupV2(group, []byte(mountInfo), func(root string) {
					publishRoot(root)
					if beforeFiles != nil {
						beforeFiles()
					}
				})
			})
	}
	refresh(cn, mounts, 475, nil, nil)
	walk := receivePressureEvent(t, walks)
	require.Equal(t, []int64{rssCacheHardTarget}, targets)
	expires := controller.expires

	// Both finite and unlimited partial readings supersede an older RSS
	// capture but cannot themselves authorize recovery from hard pressure.
	for _, limit := range []string{"1000", "max"} {
		t.Run(limit, func(t *testing.T) {
			for _, path := range []string{leaf, narrow} {
				require.NoError(t, os.WriteFile(filepath.Join(path, "memory.max"), []byte(limit), 0600))
			}
			captured, release, reported := make(chan struct{}), make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			go func() {
				defer close(reported)
				refresh(cn, mounts, 350, func() {
					close(captured)
					<-release
				}, nil)
			}()
			defer func() {
				unblock()
				receivePressureEvent(t, reported)
			}()
			receivePressureEvent(t, captured)
			refresh(workspace, covered, 475, nil, nil)
			require.NoError(t, walk.Err())
			refresh(workspace, detached, 475, nil, nil)
			require.NoError(t, walk.Err(), "detaching a mount cannot discharge the required ancestry")
			unblock()
			receivePressureEvent(t, reported)
			require.Zero(t, cleared, "a delayed complete sample cannot override newer RSS")
			require.NoError(t, walk.Err())
		})
	}

	require.Equal(t, []int64{rssCacheHardTarget}, targets, "weaker readings leave the lease unmodified")
	require.Equal(t, expires, controller.expires)

	refresh(cn, restored, 350, nil, nil)
	receivePressureEvent(t, walk.Done())
	require.Equal(t, 1, cleared, "a complete healthy reading clears the hard target")

	t.Run("publish ancestry before blocked hierarchy reads", func(t *testing.T) {
		for dir, limit := range map[string]string{wide: "880", leaf: "1000", narrow: "1000"} {
			require.NoError(t, os.WriteFile(filepath.Join(dir, "memory.max"), []byte(limit), 0600))
		}
		refresh(workspace, detached, 860, nil, nil)
		walk := receivePressureEvent(t, walks)
		expiry := controller.expires
		require.Equal(t, rssCacheSoftTarget, targets[len(targets)-1])

		published, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		go func() {
			defer close(done)
			// This view exposes the wider scope in mountinfo but cannot
			// read its limit. Pause after publishing the scope.
			unreadable := fmt.Sprintf("%s34 30 0:2 / %s rw - tmpfs tmpfs rw\n", mounts, filepath.Join(wide, "memory.max"))
			refresh(cn, unreadable, 770, nil, func() {
				close(published)
				<-release
			})
		}()
		defer func() {
			unblock()
			receivePressureEvent(t, done)
		}()
		receivePressureEvent(t, published)
		refresh(workspace, detached, 790, nil, nil)
		require.NoError(t, walk.Err())
		require.Equal(t, expiry, controller.expires)
		require.Equal(t, 1, cleared)
		unblock()
		receivePressureEvent(t, done)
		refresh(cn, restored, 680, nil, nil)
		receivePressureEvent(t, walk.Done())
		require.Equal(t, 2, cleared)
	})
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
	views, _, err := parsePressureCgroupViews(mountinfo, "/tenant", "cgroup")
	require.NoError(t, err)
	limit, err := pressureCgroupV1Hierarchy("/tenant", views)
	require.NoError(t, err)
	require.Equal(t, uint64(500), limit)
}

func TestParsePressureCgroupNamespaceRoot(t *testing.T) {
	mountinfo := []byte("30 1 0:1 /.. /inherit rw - cgroup2 cgroup rw\n" +
		"31 1 0:1 / /namespace rw - cgroup2 cgroup rw\n")
	views, _, err := parsePressureCgroupViews(mountinfo, "/", "cgroup2")
	require.NoError(t, err)
	require.Equal(t, []pressureCgroupView{{root: "/", mount: "/namespace"}}, views)
	_, _, err = parsePressureCgroupViews(mountinfo, "/../other", "cgroup2")
	require.ErrorContains(t, err, "outside the visible namespace")
}
