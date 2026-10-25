// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
//go:build linux

package rscthrottler

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// These cases use only short metadata strings: no mounts, processes, files or
// admission workers are created. The real reader/consumer proof lives separately.
func TestResolveMemoryCgroupPathUsesMountRoot(t *testing.T) {
	for _, test := range []struct {
		name, group, root, mountpoint, want string
	}{
		{"v1_private_root", "/tenant/container", "/tenant/container", "/sys/fs/cgroup/memory", "/sys/fs/cgroup/memory"},
		{"v2_private_root", "/tenant/container", "/tenant/container", "/sys/fs/cgroup", "/sys/fs/cgroup"},
		{"v1_host_root", "/tenant/container", "/", "/sys/fs/cgroup/memory", "/sys/fs/cgroup/memory/tenant/container"},
		{"v2_host_root", "/tenant/container", "/", "/sys/fs/cgroup", "/sys/fs/cgroup/tenant/container"},
		{"v1_private_child", "/tenant/container/child", "/tenant/container", "/sys/fs/cgroup/memory", "/sys/fs/cgroup/memory/child"},
		{"v2_private_child", "/tenant/container/child", "/tenant/container", "/sys/fs/cgroup", "/sys/fs/cgroup/child"},
		{"private_namespace_root", "/", "/", "/sys/fs/cgroup", "/sys/fs/cgroup"},
		{"decoded_mount_characters", "/tenant space/container/child", "/tenant space/container", "/sys/fs/cgroup/memory cache\tline\nback\\slash", "/sys/fs/cgroup/memory cache\tline\nback\\slash/child"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveMemoryCgroupPath(test.group, memoryCgroupMount{root: test.root, mountpoint: test.mountpoint})
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestResolveMemoryCgroupPathRejectsUnprovenMapping(t *testing.T) {
	for _, test := range []struct {
		name, group, root, mountpoint string
	}{
		{"same_text_prefix_is_not_child", "/tenant/container2", "/tenant/container", "/sys/fs/cgroup"},
		{"unrelated_group", "/other/group", "/tenant/container", "/sys/fs/cgroup"},
		{"membership_above_mount_root", "/tenant", "/tenant/container", "/sys/fs/cgroup"},
		{"namespace_ancestor_root", "/", "/..", "/sys/fs/cgroup"},
		{"membership_dotdot", "/tenant/container/../other", "/tenant/container", "/sys/fs/cgroup"},
		{"membership_dot", "/tenant/./container", "/tenant/container", "/sys/fs/cgroup"},
		{"root_dot", "/tenant/container", "/tenant/./container", "/sys/fs/cgroup"},
		{"mountpoint_dotdot", "/tenant/container", "/tenant/container", "/sys/fs/cgroup/../other"},
		{"empty_membership", "", "/", "/sys/fs/cgroup"},
		{"relative_membership", "tenant/container", "/", "/sys/fs/cgroup"},
		{"relative_root", "/tenant/container", "tenant/container", "/sys/fs/cgroup"},
		{"relative_mountpoint", "/tenant/container", "/tenant/container", "sys/fs/cgroup"},
		{"unclean_membership", "/tenant//container", "/tenant/container", "/sys/fs/cgroup"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveMemoryCgroupPath(test.group, memoryCgroupMount{root: test.root, mountpoint: test.mountpoint})
			require.Error(t, err)
			require.NotErrorIs(t, err, errNoMemoryController, "a mapping failure cannot authorize host-memory fallback")
			require.Empty(t, got, "neither the mount root nor a guessed child is a valid fallback")
		})
	}
}

func TestParseMemoryCgroupMountSelectsKernelMountID(t *testing.T) {
	for _, fsType := range []string{"cgroup", "cgroup2"} {
		controller, target := "", "/sys/fs/cgroup"
		if fsType == "cgroup" {
			controller, target = "memory", "/sys/fs/cgroup/memory"
		}
		for _, ids := range [][2]uint64{{7, 900}, {900, 7}} {
			for _, reverse := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/visible%d/reverse%t", fsType, ids[0], reverse), func(t *testing.T) {
					options := "rw"
					if controller != "" {
						options += "," + controller
					}
					hidden := fmt.Sprintf("%d 1 0:1 / %s ro - %s cgroup %s\n", ids[1], target, fsType, options)
					visible := fmt.Sprintf("%d %d 0:1 /tenant/container %s ro - %s cgroup %s\n", ids[0], ids[1], target, fsType, options)
					mountinfo := hidden + visible
					if reverse {
						mountinfo = visible + hidden
					}
					mount, err := parseMemoryCgroupMount(strings.NewReader(mountinfo), ids[0], fsType, controller, target)
					require.NoError(t, err)
					require.Equal(t, "/tenant/container", mount.root, "neither record order nor numeric ID magnitude selects the visible root")
					require.Equal(t, target, mount.mountpoint)
				})
			}
		}
	}
}

func TestParseMemoryCgroupMountUsesIDAcrossIntermediateFilesystem(t *testing.T) {
	mountinfo := "100 1 0:1 / /sys/fs/cgroup ro - cgroup2 cgroup rw\n" +
		"32 100 0:2 / /sys/fs/cgroup ro - tmpfs tmpfs rw\n" +
		"7 32 0:1 /tenant/container /sys/fs/cgroup ro - cgroup2 cgroup rw\n"
	mount, err := parseMemoryCgroupMount(strings.NewReader(mountinfo), 7, "cgroup2", "", "/sys/fs/cgroup")
	require.NoError(t, err)
	require.Equal(t, "/tenant/container", mount.root)
	_, err = parseMemoryCgroupMount(strings.NewReader(mountinfo), 32, "cgroup2", "", "/sys/fs/cgroup")
	require.Error(t, err, "a visible tmpfs must not fall back to a hidden cgroup record")
	require.NotErrorIs(t, err, errNoMemoryController)
}

func TestParseMemoryCgroupMountValidatesSelectedMetadata(t *testing.T) {
	for _, test := range []struct {
		name, mountinfo, target, wantRoot string
	}{
		{
			name:      "exact_controller_and_optional_fields",
			mountinfo: "1 0 0:1 /wrong /sys/fs/cgroup/memory_extra ro - cgroup cgroup rw,memory_extra\n2 0 0:2 /tenant/container /sys/fs/cgroup/memory ro,nosuid,nodev master:14 shared:5 - cgroup cgroup rw,memory\n",
			target:    "/sys/fs/cgroup/memory", wantRoot: "/tenant/container",
		},
		{
			name:      "escaped_backslash_is_not_decoded_twice",
			mountinfo: `2 0 0:1 /tenant\134040name /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory` + "\n",
			target:    "/sys/fs/cgroup/memory", wantRoot: `/tenant\040name`,
		},
		{
			name:      "decode_mountinfo_path_escapes_once",
			mountinfo: `2 0 0:1 /tenant\040space/container\134name /sys/fs/cgroup/memory\040cache\011tab\012line\134slash ro - cgroup cgroup rw,memory` + "\n",
			target:    "/sys/fs/cgroup/memory cache\ttab\nline\\slash", wantRoot: "/tenant space/container\\name",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mount, err := parseMemoryCgroupMount(strings.NewReader(test.mountinfo), 2, "cgroup", "memory", test.target)
			require.NoError(t, err)
			require.Equal(t, test.wantRoot, mount.root)
			require.Equal(t, test.target, mount.mountpoint)
		})
	}
}

func TestParseMemoryCgroupMountRejectsIdentityMismatchAndMalformedInput(t *testing.T) {
	for _, test := range []struct {
		name, mountinfo, fsType, controller, target string
		mountID                                     uint64
	}{
		{name: "missing", fsType: "cgroup", controller: "memory", target: "/sys/fs/cgroup/memory", mountID: 1},
		{name: "missing_selected_id", mountinfo: "2 0 0:1 / /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n", fsType: "cgroup", controller: "memory", target: "/sys/fs/cgroup/memory", mountID: 1},
		{name: "duplicate_selected_id", mountinfo: "1 0 0:1 /tenant/container /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n1 0 0:1 / /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n", fsType: "cgroup", controller: "memory", target: "/sys/fs/cgroup/memory", mountID: 1},
		{name: "wrong_visible_controller", mountinfo: "1 0 0:1 / /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory_extra\n2 0 0:1 / /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n", fsType: "cgroup", controller: "memory", target: "/sys/fs/cgroup/memory", mountID: 1},
		{name: "wrong_visible_filesystem", mountinfo: "1 0 0:1 / /sys/fs/cgroup ro - tmpfs tmpfs rw\n2 0 0:1 / /sys/fs/cgroup ro - cgroup2 cgroup rw\n", fsType: "cgroup2", target: "/sys/fs/cgroup", mountID: 1},
		{name: "cross_mountpoint_alias", mountinfo: "1 0 0:1 /tenant/container /alias/cgroup ro - cgroup2 cgroup rw\n2 0 0:1 / /sys/fs/cgroup ro - cgroup2 cgroup rw\n", fsType: "cgroup2", target: "/sys/fs/cgroup", mountID: 1},
		{name: "missing_expected_mountpoint", mountinfo: "1 0 0:1 / /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n", fsType: "cgroup", controller: "memory", mountID: 1},
		{name: "zero_requested_id", mountinfo: "1 0 0:1 / /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n", fsType: "cgroup", controller: "memory", target: "/sys/fs/cgroup/memory"},
		{name: "malformed_record_id", mountinfo: "invalid 0 0:1 / /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n", fsType: "cgroup", controller: "memory", target: "/sys/fs/cgroup/memory", mountID: 1},
		{name: "record_id_overflow", mountinfo: "18446744073709551616 0 0:1 / /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n", fsType: "cgroup", controller: "memory", target: "/sys/fs/cgroup/memory", mountID: 1},
		{name: "missing_separator", mountinfo: "1 0 0:1 / /sys/fs/cgroup/memory ro cgroup cgroup rw,memory\n", fsType: "cgroup", controller: "memory", target: "/sys/fs/cgroup/memory", mountID: 1},
		{name: "missing_root_fields", mountinfo: "1 0 - cgroup cgroup rw,memory\n", fsType: "cgroup", controller: "memory", target: "/sys/fs/cgroup/memory", mountID: 1},
		{name: "missing_super_options", mountinfo: "1 0 0:1 / /sys/fs/cgroup/memory ro - cgroup cgroup\n", fsType: "cgroup", controller: "memory", target: "/sys/fs/cgroup/memory", mountID: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseMemoryCgroupMount(strings.NewReader(test.mountinfo), test.mountID, test.fsType, test.controller, test.target)
			require.Error(t, err)
			require.NotErrorIs(t, err, errNoMemoryController, "identity or metadata failure cannot authorize host-memory fallback")
		})
	}
}

func TestMemoryCgroupMountDoesNotCleanNamespaceAncestorIntoRoot(t *testing.T) {
	mount, err := parseMemoryCgroupMount(strings.NewReader("1 0 0:1 /.. /sys/fs/cgroup ro - cgroup2 cgroup rw\n"), 1, "cgroup2", "", "/sys/fs/cgroup")
	if err == nil {
		_, err = resolveMemoryCgroupPath("/", mount)
	}
	require.Error(t, err, "the inherited ancestor mount must not be cleaned into the current namespace root")
	require.NotErrorIs(t, err, errNoMemoryController)
}

func TestParseMemoryCgroupMountIDAcceptsOnePositiveDecimal(t *testing.T) {
	for _, test := range []struct {
		name, fdinfo string
		want         uint64
	}{
		{"ordinary_fdinfo", "pos:\t0\nflags:\t012000000\nmnt_id:\t92\nino:\t1\n", 92},
		{"id_size_does_not_select_mount", "mnt_id: 7\n", 7},
		{"largest_uint64", "mnt_id:\t18446744073709551615\n", ^uint64(0)},
		{"exact_key", "mnt_id_extra:\t900\nmnt_id:\t7\n", 7},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseMemoryCgroupMountID(strings.NewReader(test.fdinfo))
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestParseMemoryCgroupMountIDRejectsAmbiguousOrInvalidFDInfo(t *testing.T) {
	for _, test := range []struct{ name, fdinfo string }{
		{"missing", "pos:\t0\nflags:\t012000000\n"},
		{"wrong_key_only", "mnt_id_extra:\t7\n"},
		{"missing_colon", "mnt_id 7\n"},
		{"missing_value", "mnt_id:\n"},
		{"not_decimal", "mnt_id:\tnumber\n"},
		{"hexadecimal", "mnt_id:\t0x10\n"},
		{"negative", "mnt_id:\t-1\n"},
		{"zero", "mnt_id:\t0\n"},
		{"overflow", "mnt_id:\t18446744073709551616\n"},
		{"extra_value", "mnt_id:\t7 8\n"},
		{"duplicate_same_id", "mnt_id:\t7\nmnt_id:\t7\n"},
		{"duplicate_different_id", "mnt_id:\t7\nmnt_id:\t8\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseMemoryCgroupMountID(strings.NewReader(test.fdinfo))
			require.Error(t, err)
			require.NotErrorIs(t, err, errNoMemoryController)
		})
	}
}

type memoryMountErrorReader struct{ err error }

func (r memoryMountErrorReader) Read([]byte) (int, error) { return 0, r.err }

func TestMemoryCgroupIdentityParsersPreserveReadError(t *testing.T) {
	failure := errors.New("cgroup identity metadata read failed")
	for _, afterValidRecord := range []bool{false, true} {
		mountinfo, fdinfo := "", ""
		if afterValidRecord {
			mountinfo = "1 0 0:1 /tenant/container /sys/fs/cgroup/memory ro - cgroup cgroup rw,memory\n"
			fdinfo = "mnt_id:\t1\n"
		}
		_, err := parseMemoryCgroupMount(io.MultiReader(strings.NewReader(mountinfo), memoryMountErrorReader{err: failure}), 1, "cgroup", "memory", "/sys/fs/cgroup/memory")
		require.ErrorIs(t, err, failure, "a selected record cannot hide a later read failure")
		require.NotErrorIs(t, err, errNoMemoryController)
		_, err = parseMemoryCgroupMountID(io.MultiReader(strings.NewReader(fdinfo), memoryMountErrorReader{err: failure}))
		require.ErrorIs(t, err, failure, "one parsed mnt_id cannot hide a later read failure")
		require.NotErrorIs(t, err, errNoMemoryController)
	}
}
