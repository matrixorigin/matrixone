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
	"bufio"
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type memoryCgroupMount struct{ root, mountpoint string }

// readMemoryCgroupMount asks the kernel which mount is actually visible at M.
// O_PATH needs traversal permission, without adding directory-listing access.
// The FD remains owned until the matching mountinfo record is selected.
func readMemoryCgroupMount(mountpoint, fsType, controller string) (memoryCgroupMount, error) {
	fd, err := unix.Open(mountpoint, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return memoryCgroupMount{}, err
	}
	defer unix.Close(fd)
	info, err := os.ReadFile("/proc/self/fdinfo/" + strconv.Itoa(fd))
	if err != nil {
		return memoryCgroupMount{}, err
	}
	mountID, err := parseMemoryCgroupMountID(bytes.NewReader(info))
	if err != nil {
		return memoryCgroupMount{}, err
	}
	mounts, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return memoryCgroupMount{}, err
	}
	defer mounts.Close()
	return parseMemoryCgroupMount(mounts, mountID, fsType, controller, mountpoint)
}

func parseMemoryCgroupMountID(reader io.Reader) (uint64, error) {
	var id uint64
	found := false
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 || fields[0] != "mnt_id:" {
			continue
		}
		if found || len(fields) != 2 {
			return 0, moerr.NewInternalErrorNoCtx("invalid or duplicate fdinfo mount ID")
		}
		var err error
		id, err = memoryMountNumber(fields[1])
		if err != nil {
			return 0, err
		}
		found = true
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}
	if !found {
		return 0, moerr.NewInternalErrorNoCtx("fdinfo mount ID not found")
	}
	return id, nil
}

func memoryMountNumber(value string) (uint64, error) {
	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return 0, moerr.NewInternalErrorNoCtx("invalid memory cgroup mount ID")
		}
	}
	id, err := strconv.ParseUint(value, 10, 64)
	if err != nil || id == 0 {
		return 0, moerr.NewInternalErrorNoCtx("invalid memory cgroup mount ID")
	}
	return id, nil
}

// parseMemoryCgroupMount uses the kernel-selected ID, not line order, ID size or
// a reconstruction of stacked mount parents. Every same-path filesystem can
// hide an older record; only the opened directory establishes the visible one.
func parseMemoryCgroupMount(reader io.Reader, mountID uint64, fsType, controller, expectedMountpoint string) (memoryCgroupMount, error) {
	if mountID == 0 || expectedMountpoint == "" {
		return memoryCgroupMount{}, moerr.NewInternalErrorNoCtx("missing memory cgroup mount identity")
	}
	var mount memoryCgroupMount
	var selectedType, selectedOptions string
	found := false
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " - ", 2)
		if len(parts) != 2 {
			return memoryCgroupMount{}, moerr.NewInternalErrorNoCtx("invalid cgroup mountinfo record")
		}
		left, right := strings.Fields(parts[0]), strings.Fields(parts[1])
		if len(left) < 6 || len(right) < 3 {
			return memoryCgroupMount{}, moerr.NewInternalErrorNoCtx("incomplete cgroup mountinfo record")
		}
		id, err := memoryMountNumber(left[0])
		if err != nil {
			return memoryCgroupMount{}, err
		}
		if id != mountID {
			continue
		}
		if found {
			return memoryCgroupMount{}, moerr.NewInternalErrorNoCtx("duplicate memory cgroup mountinfo ID")
		}
		found = true
		mount = memoryCgroupMount{root: unescapeMemoryMountPath(left[3]), mountpoint: unescapeMemoryMountPath(left[4])}
		selectedType, selectedOptions = right[0], right[2]
	}
	if err := scanner.Err(); err != nil {
		return memoryCgroupMount{}, err
	}
	if !found {
		return memoryCgroupMount{}, moerr.NewInternalErrorNoCtx("selected memory cgroup mount not found")
	}
	if mount.mountpoint != expectedMountpoint || selectedType != fsType {
		return memoryCgroupMount{}, moerr.NewInternalErrorNoCtx("memory cgroup mount identity mismatch")
	}
	if controller != "" {
		for _, option := range strings.Split(selectedOptions, ",") {
			if option == controller {
				return mount, nil
			}
		}
		return memoryCgroupMount{}, moerr.NewInternalErrorNoCtx("selected cgroup mount lacks memory controller")
	}
	return mount, nil
}

func unescapeMemoryMountPath(path string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(path)
}

// resolveMemoryCgroupPath compares paths in the same cgroup namespace. Reject
// raw dot/dotdot components before any cleaner can turn an ancestor mount such
// as /.. into /. Existing files alone cannot prove they belong to this process.
func resolveMemoryCgroupPath(groupPath string, mount memoryCgroupMount) (string, error) {
	for _, path := range []string{groupPath, mount.root, mount.mountpoint} {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return "", moerr.NewInternalErrorNoCtxf("invalid memory cgroup path %q", path)
		}
	}
	relative, err := filepath.Rel(mount.root, groupPath)
	if err != nil {
		return "", err
	}
	if relative == ".." || strings.HasPrefix(relative, "../") {
		return "", moerr.NewInternalErrorNoCtx("current process is outside the memory cgroup mount root")
	}
	return filepath.Join(mount.mountpoint, relative), nil
}
