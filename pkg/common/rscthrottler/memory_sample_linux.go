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
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/containerd/cgroups/v3"
	"github.com/containerd/cgroups/v3/cgroup1"
	"github.com/containerd/cgroups/v3/cgroup2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func readCgroupMemory() (limit, current, inactive int64, err error) {
	mode := cgroups.Mode()
	if mode == cgroups.Unavailable {
		if _, statErr := os.Stat("/sys/fs/cgroup"); errors.Is(statErr, os.ErrNotExist) {
			return 0, 0, 0, errNoMemoryController
		}
		return 0, 0, 0, moerr.NewInternalErrorNoCtx("cannot determine cgroup mount state")
	}
	if mode == cgroups.Unified {
		_, group, e := cgroups.ParseCgroupFileUnified("/proc/self/cgroup")
		if e != nil {
			return 0, 0, 0, e
		}
		if e = cgroup2.VerifyGroupPath(group); e != nil {
			return 0, 0, 0, e
		}
		mount, e := readMemoryCgroupMount("/sys/fs/cgroup", "cgroup2", "")
		if e != nil {
			return 0, 0, 0, e
		}
		path, e := resolveMemoryCgroupPath(group, mount)
		if e != nil {
			return 0, 0, 0, e
		}
		return readUnifiedMemory(path)
	}
	groups, e := cgroup1.ParseCgroupFile("/proc/self/cgroup")
	if e != nil {
		return 0, 0, 0, e
	}
	subsystems, e := cgroup1.SingleSubsystem(cgroup1.Default, cgroup1.Memory)()
	if e != nil {
		return 0, 0, 0, legacyMemoryError(e)
	}
	mountpoint, e := legacyMemoryControllerPath(subsystems)
	if e != nil {
		return 0, 0, 0, e
	}
	mount, e := readMemoryCgroupMount(mountpoint, "cgroup", "memory")
	if e != nil {
		return 0, 0, 0, e
	}
	return readLegacyMemory(groups[string(cgroup1.Memory)], mount, cgroup1.PidPath(os.Getpid()), func() ([]cgroup1.Subsystem, error) { return subsystems, nil })
}

// readLegacyMemory retains the existing controller and Stat implementation.
// Selected metadata and the hierarchy are explicit only at this private boundary
// so tests can exercise a real temporary controller without mounting cgroupfs.
func readLegacyMemory(group string, mount memoryCgroupMount, path cgroup1.Path, hierarchy cgroup1.Hierarchy) (limit, current, inactive int64, err error) {
	subsystems, e := hierarchy()
	if e != nil {
		return 0, 0, 0, legacyMemoryError(e)
	}
	mountpoint, e := legacyMemoryControllerPath(subsystems)
	if e != nil {
		return 0, 0, 0, e
	}
	if mount.mountpoint != mountpoint {
		return 0, 0, 0, moerr.NewInternalErrorNoCtx("memory cgroup mount does not match selected controller")
	}
	directory, e := resolveMemoryCgroupPath(group, mount)
	if e != nil {
		return 0, 0, 0, e
	}
	relative, e := filepath.Rel(mountpoint, directory)
	if e != nil {
		return 0, 0, 0, e
	}
	memoryPath := "/"
	if relative != "." {
		memoryPath += relative
	}
	// The dependency mishandles private-root equality and escaped roots.
	// Use the same decoded, validated mapping for every Memory path.
	legacyPath := path
	path = func(name cgroup1.Name) (string, error) {
		if name == cgroup1.Memory {
			return memoryPath, nil
		}
		return legacyPath(name)
	}
	cg, e := cgroup1.Load(path, cgroup1.WithHiearchy(func() ([]cgroup1.Subsystem, error) { return subsystems, nil }))
	if e != nil {
		return 0, 0, 0, legacyMemoryError(e)
	}
	stats, e := cg.Stat(cgroup1.IgnoreNotExist)
	if e != nil {
		return 0, 0, 0, e
	}
	m := stats.GetMemory()
	if m == nil {
		return 0, 0, 0, moerr.NewInternalErrorNoCtx("incomplete cgroup memory accounting")
	}
	if m.GetUsage() == nil || m.GetUsage().GetUsage() == 0 {
		return 0, 0, 0, moerr.NewInternalErrorNoCtx("incomplete cgroup memory usage")
	}
	inactive = max(0, memoryInt(m.GetTotalInactiveFile())-memoryInt(m.GetTotalDirty()))
	inactive = max(0, inactive-memoryInt(m.GetTotalWriteback()))
	return memoryInt(m.GetHierarchicalMemoryLimit()), memoryInt(m.GetUsage().GetUsage()), inactive, nil
}

func legacyMemoryControllerPath(subsystems []cgroup1.Subsystem) (string, error) {
	if len(subsystems) != 1 || subsystems[0].Name() != cgroup1.Memory {
		return "", moerr.NewInternalErrorNoCtx("invalid memory cgroup hierarchy")
	}
	controller, ok := subsystems[0].(interface{ Path(string) string })
	if !ok {
		return "", moerr.NewInternalErrorNoCtx("memory cgroup controller has no path")
	}
	return controller.Path("/"), nil
}

func legacyMemoryError(err error) error {
	if errors.Is(err, cgroup1.ErrMountPointNotExist) || errors.Is(err, cgroup1.ErrMemoryNotSupported) || errors.Is(err, cgroup1.ErrNoCgroupMountDestination) {
		return errNoMemoryController
	}
	return err
}

// cgroup2.Manager.Stat masks missing memory.stat and scalar read failures as
// zero. Read the three required accounting files explicitly so an incomplete
// sample cannot release container pressure or erase a previously known limit.
func readUnifiedMemory(path string) (limit, current, inactive int64, err error) {
	names := []string{"memory.max", "memory.current", "memory.stat"}
	data := make([][]byte, len(names))
	missing := 0
	for i, name := range names {
		var e error
		data[i], e = os.ReadFile(filepath.Join(path, name))
		if errors.Is(e, os.ErrNotExist) {
			missing++
		}
		err = errors.Join(err, e)
	}
	if missing == len(names) {
		info, e := os.Stat(path)
		if e == nil && info.IsDir() {
			return 0, 0, 0, errNoMemoryController
		}
	}
	if err != nil {
		return 0, 0, 0, err
	}
	value := strings.TrimSpace(string(data[0]))
	if value == "max" {
		limit = math.MaxInt64
	} else {
		n, e := strconv.ParseUint(value, 10, 64)
		if e != nil {
			return 0, 0, 0, e
		}
		limit = memoryInt(n)
	}
	n, e := strconv.ParseUint(strings.TrimSpace(string(data[1])), 10, 64)
	if e != nil {
		return 0, 0, 0, e
	}
	current = memoryInt(n)
	fields := strings.Fields(string(data[2]))
	if len(fields) == 0 || len(fields)%2 != 0 {
		return 0, 0, 0, moerr.NewInternalErrorNoCtx("incomplete cgroup memory.stat")
	}
	values := make(map[string]int64)
	for i := 0; i < len(fields); i += 2 {
		n, e = strconv.ParseUint(fields[i+1], 10, 64)
		if e != nil {
			return 0, 0, 0, e
		}
		values[fields[i]] = memoryInt(n)
	}
	// Older kernels may omit dirty/writeback counters. In that case retain all
	// file pages in the pressure estimate rather than inventing reclaim credit.
	raw, hasInactive := values["inactive_file"]
	dirty, hasDirty := values["file_dirty"]
	writeback, hasWriteback := values["file_writeback"]
	if hasInactive && hasDirty && hasWriteback {
		inactive = max(0, max(0, raw-dirty)-writeback)
	}
	return
}
