// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
//go:build linux

package rscthrottler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/containerd/cgroups/v3/cgroup1"
	"github.com/stretchr/testify/require"
)

const testPrivateCgroupLimit = int64(55 << 30)
const testPrivateCgroupCurrent = int64(438996992)

func legacyMemoryControllerFixture(t *testing.T, child string) (string, cgroup1.Hierarchy) {
	t.Helper()
	parent := t.TempDir()
	directory := filepath.Join(parent, "memory", child)
	require.NoError(t, os.MkdirAll(directory, 0700))
	write := func(name, value string) {
		require.NoError(t, os.WriteFile(filepath.Join(directory, name), []byte(value), 0600))
	}
	write("memory.stat", fmt.Sprintf("hierarchical_memory_limit %d\ntotal_inactive_file 0\ntotal_dirty 0\ntotal_writeback 0\n", testPrivateCgroupLimit))
	write("memory.oom_control", "oom_kill_disable 0\nunder_oom 0\noom_kill 0\n")
	for _, module := range []string{"", "memsw.", "kmem.", "kmem.tcp."} {
		usage := int64(0)
		if module == "" {
			usage = testPrivateCgroupCurrent
		}
		write("memory."+module+"usage_in_bytes", fmt.Sprint(usage))
		write("memory."+module+"max_usage_in_bytes", fmt.Sprint(usage))
		write("memory."+module+"failcnt", "0")
		write("memory."+module+"limit_in_bytes", fmt.Sprint(testPrivateCgroupLimit))
	}
	controller := cgroup1.NewMemory(parent)
	return directory, func() ([]cgroup1.Subsystem, error) { return []cgroup1.Subsystem{controller}, nil }
}

func memoryMountFixture(t *testing.T, records string, id uint64, fsType, controller, mountpoint string) memoryCgroupMount {
	t.Helper()
	// The mount ID is a deterministic kernel-fdinfo input. Production obtains
	// it from an owned O_PATH FD; the parent runtime probe covers that OS arc.
	mountID, err := parseMemoryCgroupMountID(strings.NewReader(fmt.Sprintf("pos: 0\nflags: 010000000\nmnt_id: %d\n", id)))
	require.NoError(t, err)
	mount, err := parseMemoryCgroupMount(strings.NewReader(records), mountID, fsType, controller, mountpoint)
	require.NoError(t, err)
	return mount
}

func TestLegacyPrivateMountRootFeedsRealAdmission(t *testing.T) {
	root, hierarchy := legacyMemoryControllerFixture(t, "")
	group := "/tenant/container"
	mountinfo := fmt.Sprintf("23 1 0:3 %s %s ro master:1 - cgroup cgroup rw,memory\n", group, root)
	// This is exactly the old v3.0.1 equality output: Rel(R,P)=="."
	// is replaced by R. Real Load then searches M/P instead of M.
	legacyPath := cgroup1.StaticPath(group)
	_, oldErr := cgroup1.Load(legacyPath, cgroup1.WithHiearchy(hierarchy))
	require.ErrorIs(t, oldErr, cgroup1.ErrCgroupDeleted)
	mount := memoryMountFixture(t, mountinfo, 23, "cgroup", "memory", root)
	read := func() (int64, int64, int64, error) {
		return readLegacyMemory(group, mount, legacyPath, hierarchy)
	}
	limit, current, inactive, err := read()
	require.NoError(t, err)
	require.Equal(t, testPrivateCgroupLimit, limit)
	require.Equal(t, testPrivateCgroupCurrent, current)
	require.Zero(t, inactive)

	m, coordinator, queued := admissionFixture(t, 64<<30, 50<<30, 348098560)
	coordinator.readSample = func() memorySample {
		limit, current, inactive, err := read()
		return memorySample{total: 64 << 30, rss: 348098560, limit: limit, current: current, inactiveFile: inactive, cgroupObserved: err == nil, cgroupErr: err, err: err}
	}
	m.ForceRefresh()
	require.Equal(t, uint64(testPrivateCgroupLimit), m.actualTotalMemory.Load(), "controller55Gi must replace host64Gi")
	require.Equal(t, testPrivateCgroupCurrent, m.physicalUsed(), "container current must not become fabricated zero")
	spec := candidateSpec(1<<20, 1<<30, 1<<30)
	working, granted, err := m.TryReserveWorkingSet(context.Background(), spec)
	if working != nil {
		t.Cleanup(working.Close)
	}
	require.NoError(t, err)
	require.True(t, granted, "a complete2Gi promise fits immediately in this real55Gi sample")
	working.Close()
	require.Empty(t, queued)
	require.Nil(t, coordinator.workerStop)
	require.Zero(t, coordinator.reclaimCount.Load())

	// The disabled path is an accounting error, not real capacity exhaustion.
	coordinator.readSample = func() memorySample {
		return memorySample{total: 64 << 30, rss: 348098560, cgroupErr: oldErr, err: oldErr}
	}
	coordinator.invalidateSample()
	m.ForceRefresh()
	require.True(t, m.sampleInvalid.Load())
	require.Equal(t, uint64(testPrivateCgroupLimit), m.actualTotalMemory.Load(), "failed reads retain the known container limit")
	working, granted, err = m.TryReserveWorkingSet(context.Background(), spec)
	if working != nil {
		t.Cleanup(working.Close)
	}
	require.NoError(t, err)
	require.False(t, granted)
	require.Nil(t, working)
	require.Empty(t, queued, "the counterexample must not wait or start a recovery worker")
}

func TestLegacyMemoryValidChildKeepsExistingPathAndSelectedController(t *testing.T) {
	directory, hierarchy := legacyMemoryControllerFixture(t, "child")
	mountpoint := filepath.Dir(directory)
	mountinfo := fmt.Sprintf("23 1 0:3 /tenant/container %s ro - cgroup cgroup rw,memory\n", mountpoint)
	mount := memoryMountFixture(t, mountinfo, 23, "cgroup", "memory", mountpoint)
	path := cgroup1.StaticPath("/child")
	limit, current, _, err := readLegacyMemory("/tenant/container/child", mount, path, hierarchy)
	require.NoError(t, err)
	require.Equal(t, testPrivateCgroupLimit, limit)
	require.Equal(t, testPrivateCgroupCurrent, current)

	wrongAlias := fmt.Sprintf("22 1 0:3 /tenant/container/child %s ro - cgroup cgroup rw,memory\n", t.TempDir())
	_, err = parseMemoryCgroupMount(strings.NewReader(wrongAlias+mountinfo), 22, "cgroup", "memory", mountpoint)
	require.ErrorContains(t, err, "identity mismatch")
	require.NotErrorIs(t, err, errNoMemoryController)
	mount = memoryMountFixture(t, wrongAlias+mountinfo, 23, "cgroup", "memory", mountpoint)
	limit, current, _, err = readLegacyMemory("/tenant/container/child", mount, path, hierarchy)
	require.NoError(t, err, "another mount alias must not replace the kernel-selected controller")
	require.Equal(t, testPrivateCgroupLimit, limit)
	require.Equal(t, testPrivateCgroupCurrent, current)
}

func TestLegacyEscapedRootDescendantAndStackedMountsUseDecodedPath(t *testing.T) {
	for _, test := range []struct {
		name, group, rawRoot, child string
		stacked                     bool
	}{
		{name: "escaped_child", group: "/tenant space/container/child", rawRoot: `/tenant\040space/container`, child: "child"},
		{name: "stacked_root", group: "/tenant/container", rawRoot: "/tenant/container", stacked: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory, hierarchy := legacyMemoryControllerFixture(t, test.child)
			mountpoint := directory
			if test.child != "" {
				mountpoint = filepath.Dir(directory)
			}
			visible := fmt.Sprintf("7 17 0:3 %s %s ro - cgroup cgroup rw,memory\n", test.rawRoot, mountpoint)
			records := visible
			if test.stacked {
				hidden := fmt.Sprintf("500 1 0:3 / %s ro - cgroup cgroup rw,memory\n", mountpoint)
				middle := fmt.Sprintf("17 500 0:4 / %s ro - tmpfs tmpfs rw\n", mountpoint)
				records = hidden + middle + visible
			}
			mount := memoryMountFixture(t, records, 7, "cgroup", "memory", mountpoint)
			// In the escaped case old Rel(encodedRoot,decodedP) escapes the
			// root, then Join("/",rel) yields fullP. Hidden root=/ likewise
			// yields fullP. A real old-shape Load fails before correction.
			legacyPath := cgroup1.StaticPath(test.group)
			_, err := cgroup1.Load(legacyPath, cgroup1.WithHiearchy(hierarchy))
			require.ErrorIs(t, err, cgroup1.ErrCgroupDeleted)
			limit, current, _, err := readLegacyMemory(test.group, mount, legacyPath, hierarchy)
			require.NoError(t, err)
			require.Equal(t, testPrivateCgroupLimit, limit)
			require.Equal(t, testPrivateCgroupCurrent, current)
		})
	}
}

func TestLegacyPrivateMountPartialAccountingRemainsFailure(t *testing.T) {
	for _, file := range []string{"memory.stat", "memory.usage_in_bytes"} {
		t.Run(file, func(t *testing.T) {
			root, hierarchy := legacyMemoryControllerFixture(t, "")
			mountinfo := fmt.Sprintf("23 1 0:3 /tenant/container %s ro - cgroup cgroup rw,memory\n", root)
			require.NoError(t, os.Remove(filepath.Join(root, file)))
			_, _, _, err := readLegacyMemory("/tenant/container", memoryMountFixture(t, mountinfo, 23, "cgroup", "memory", root), cgroup1.StaticPath("/tenant/container"), hierarchy)
			require.Error(t, err)
			require.NotErrorIs(t, err, errNoMemoryController, "a partial real controller cannot select host fallback")
		})
	}
	root, hierarchy := legacyMemoryControllerFixture(t, "")
	mountinfo := fmt.Sprintf("23 1 0:3 /tenant/container %s ro - cgroup cgroup rw,memory\n", root)
	require.NoError(t, os.WriteFile(filepath.Join(root, "memory.usage_in_bytes"), []byte("broken\n"), 0600))
	_, _, _, err := readLegacyMemory("/tenant/container", memoryMountFixture(t, mountinfo, 23, "cgroup", "memory", root), cgroup1.StaticPath("/tenant/container"), hierarchy)
	require.Error(t, err)
	require.NotErrorIs(t, err, errNoMemoryController)
}

func TestUnifiedPrivateMountFeedsRealAdmission(t *testing.T) {
	for _, child := range []string{"", "child"} {
		t.Run(map[bool]string{true: "root", false: "descendant"}[child == ""], func(t *testing.T) {
			mountpoint := t.TempDir()
			group := "/tenant/container"
			if child != "" {
				group += "/" + child
			}
			directory := filepath.Join(mountpoint, child)
			require.NoError(t, os.MkdirAll(directory, 0700))
			for name, value := range map[string]string{
				"memory.max":     fmt.Sprint(testPrivateCgroupLimit),
				"memory.current": fmt.Sprint(testPrivateCgroupCurrent),
				"memory.stat":    "inactive_file 0\nfile_dirty 0\nfile_writeback 0\n",
			} {
				require.NoError(t, os.WriteFile(filepath.Join(directory, name), []byte(value), 0600))
			}
			mountinfo := fmt.Sprintf("500 1 0:3 / %s ro - cgroup2 cgroup rw\n17 500 0:4 / %s ro - tmpfs tmpfs rw\n7 17 0:3 /tenant/container %s ro - cgroup2 cgroup rw\n", mountpoint, mountpoint, mountpoint)
			mount := memoryMountFixture(t, mountinfo, 7, "cgroup2", "", mountpoint)
			path, err := resolveMemoryCgroupPath(group, mount)
			require.NoError(t, err)
			require.Equal(t, directory, path)
			limit, current, inactive, err := readUnifiedMemory(path)
			require.NoError(t, err)
			require.Equal(t, testPrivateCgroupLimit, limit)
			require.Equal(t, testPrivateCgroupCurrent, current)
			m, coordinator, queued := admissionFixture(t, 64<<30, 50<<30, 348098560)
			coordinator.readSample = func() memorySample {
				return memorySample{total: 64 << 30, rss: 348098560, limit: limit, current: current, inactiveFile: inactive, cgroupObserved: true}
			}
			m.ForceRefresh()
			working, granted, err := m.TryReserveWorkingSet(context.Background(), candidateSpec(1<<20, 1<<30, 1<<30))
			if working != nil {
				t.Cleanup(working.Close)
			}
			require.NoError(t, err)
			require.True(t, granted)
			require.Empty(t, queued)
			require.Nil(t, coordinator.workerStop)
			_, _, _, err = readUnifiedMemory(filepath.Join(mountpoint, group))
			require.Error(t, err, "old M+P path must be a concrete failure, not a successful zero-current sample")
			require.NotErrorIs(t, err, errNoMemoryController)
		})
	}
}
