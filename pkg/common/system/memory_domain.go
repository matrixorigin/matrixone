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

package system

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// MemoryDomainSource identifies the accounting domain used by a memory sample.
// The source is part of the sample so metrics from a cgroup and a bare host
// cannot be compared as if they used the same boundary.
type MemoryDomainSource string

const (
	MemoryDomainSourceCgroupV1 MemoryDomainSource = "cgroup-v1"
	MemoryDomainSourceCgroupV2 MemoryDomainSource = "cgroup-v2"
	MemoryDomainSourceHost     MemoryDomainSource = "host"
)

// MemoryDomainSnapshot is the immutable memory fact shared by memory gates.
// Used is the charged/used amount before reclaimable-cache credit. Reclaimable
// is a separately reported clean cache amount and must only be credited by a
// policy that explicitly permits reclaimable memory. Kernel includes the
// kernel/socket/slab sub-account when the source exposes it.
type MemoryDomainSnapshot struct {
	Used        uint64
	Limit       uint64
	Reclaimable uint64
	Kernel      uint64
	Source      MemoryDomainSource
	Measured    bool
	ObservedAt  time.Time
}

func (s MemoryDomainSnapshot) IsCgroup() bool {
	return s.Source == MemoryDomainSourceCgroupV1 || s.Source == MemoryDomainSourceCgroupV2
}

// HardHeadroom does not credit reclaimable pages. It is the conservative
// boundary for a hard cgroup limit and is also useful for host pressure.
func (s MemoryDomainSnapshot) HardHeadroom() uint64 {
	if s.Limit <= s.Used {
		return 0
	}
	return s.Limit - s.Used
}

// ReclaimableHeadroom credits only the clean reclaimable amount explicitly
// reported by the domain. This preserves the existing bulk-allocation contract
// while keeping the hard-pressure gate on the same underlying sample.
func (s MemoryDomainSnapshot) ReclaimableHeadroom() uint64 {
	used := s.Used
	if s.Reclaimable >= used {
		used = 0
	} else {
		used -= s.Reclaimable
	}
	if s.Limit <= used {
		return 0
	}
	return s.Limit - used
}

const memoryDomainRefreshInterval = 100 * time.Millisecond

var memoryDomainState struct {
	refreshMu sync.Mutex
	snapshot  atomic.Pointer[MemoryDomainSnapshot]
}

// CurrentMemoryDomain returns the latest shared sample. A short TTL prevents
// every query admission from walking procfs/cgroupfs while ensuring the
// throttler and execution budget consume one published sample rather than
// independently reading usage and limit.
func CurrentMemoryDomain() (MemoryDomainSnapshot, error) {
	if snapshot := memoryDomainState.snapshot.Load(); snapshot != nil &&
		time.Since(snapshot.ObservedAt) <= memoryDomainRefreshInterval {
		return *snapshot, nil
	}
	return RefreshMemoryDomain()
}

// RefreshMemoryDomain publishes a new sample. A failed read never erases the
// last good sample: callers that require a fresh authoritative value receive an
// error, while callers that can safely use the last observation may continue
// with the returned snapshot.
func RefreshMemoryDomain() (MemoryDomainSnapshot, error) {
	memoryDomainState.refreshMu.Lock()
	defer memoryDomainState.refreshMu.Unlock()

	if snapshot := memoryDomainState.snapshot.Load(); snapshot != nil &&
		time.Since(snapshot.ObservedAt) <= memoryDomainRefreshInterval {
		return *snapshot, nil
	}

	snapshot, err := readMemoryDomain(os.Getpid())
	if err != nil {
		if previous := memoryDomainState.snapshot.Load(); previous != nil {
			return *previous, err
		}
		return MemoryDomainSnapshot{}, err
	}
	memoryDomainState.snapshot.Store(&snapshot)
	return snapshot, nil
}

type memoryDomainLevel struct {
	dir       string
	limitFile string
	usageFile string
	statFile  string
	source    MemoryDomainSource
}

func readMemoryDomain(pid int) (MemoryDomainSnapshot, error) {
	if levels, ok := memoryDomainLevels(pid); ok {
		if snapshot, measured, err := readCgroupMemoryDomain(levels); err != nil {
			return MemoryDomainSnapshot{}, err
		} else if measured {
			return snapshot, nil
		}
	}

	mem, err := hostMemoryStats()
	if err != nil {
		return MemoryDomainSnapshot{}, fmt.Errorf("read host memory domain: %w", err)
	}
	reclaimable := uint64(0)
	if mem.ActualFree >= mem.Free {
		reclaimable = mem.ActualFree - mem.Free
	}
	return MemoryDomainSnapshot{
		Used:        mem.Used,
		Limit:       mem.Total,
		Reclaimable: reclaimable,
		Source:      MemoryDomainSourceHost,
		Measured:    mem.Total > 0,
		ObservedAt:  time.Now(),
	}, nil
}

// memoryDomainLevels resolves the leaf-to-root memory hierarchy. It mirrors
// the mount/path resolution used by the existing headroom code, but returns
// the individual levels so the selected binding level can carry usage,
// reclaimable and kernel attribution in one immutable sample.
func memoryDomainLevels(pid int) ([]memoryDomainLevel, bool) {
	cgroupData, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return nil, false
	}
	mountData, err := os.ReadFile(fmt.Sprintf("/proc/%d/mountinfo", pid))
	if err != nil {
		return nil, false
	}

	v2Path := ""
	v1MemoryPath := ""
	for _, line := range strings.Split(string(cgroupData), "\n") {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) != 3 {
			continue
		}
		if parts[0] == "0" && parts[1] == "" {
			v2Path = parts[2]
		}
		for _, controller := range strings.Split(parts[1], ",") {
			if controller == "memory" {
				v1MemoryPath = parts[2]
			}
		}
	}

	for _, line := range strings.Split(string(mountData), "\n") {
		leftRight := strings.SplitN(line, " - ", 2)
		if len(leftRight) != 2 {
			continue
		}
		left := strings.Fields(leftRight[0])
		right := strings.Fields(leftRight[1])
		if len(left) < 5 || len(right) < 3 {
			continue
		}
		mountRoot, mountPoint, fsType := left[3], left[4], right[0]
		switch fsType {
		case "cgroup2":
			if v2Path == "" {
				continue
			}
			dir, ok := cgroupDirectory(mountPoint, mountRoot, v2Path)
			if !ok {
				continue
			}
			return buildMemoryDomainLevels(dir, mountPoint, MemoryDomainSourceCgroupV2,
				"memory.max", "memory.current", "memory.stat"), true
		case "cgroup":
			if v1MemoryPath == "" || !strings.Contains(","+right[2]+",", ",memory,") {
				continue
			}
			dir, ok := cgroupDirectory(mountPoint, mountRoot, v1MemoryPath)
			if !ok {
				continue
			}
			return buildMemoryDomainLevels(dir, mountPoint, MemoryDomainSourceCgroupV1,
				"memory.limit_in_bytes", "memory.usage_in_bytes", "memory.stat"), true
		}
	}
	return nil, false
}

func buildMemoryDomainLevels(dir, mountPoint string, source MemoryDomainSource,
	limitFile, usageFile, statFile string) []memoryDomainLevel {
	dir = filepath.Clean(dir)
	mountPoint = filepath.Clean(mountPoint)
	levels := make([]memoryDomainLevel, 0, 4)
	for {
		levels = append(levels, memoryDomainLevel{
			dir: dir, limitFile: limitFile, usageFile: usageFile,
			statFile: statFile, source: source,
		})
		if dir == mountPoint {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir || (parent != mountPoint && !strings.HasPrefix(parent, mountPoint+string(os.PathSeparator))) {
			break
		}
		dir = parent
	}
	return levels
}

func readCgroupMemoryDomain(levels []memoryDomainLevel) (MemoryDomainSnapshot, bool, error) {
	var (
		selected         MemoryDomainSnapshot
		selectedHeadroom uint64
		found            bool
	)
	for _, level := range levels {
		limit, ok := readCgroupLimit(filepath.Join(level.dir, level.limitFile))
		if !ok || limit == 0 {
			continue
		}
		usage, ok := readCgroupUint(filepath.Join(level.dir, level.usageFile))
		if !ok {
			return MemoryDomainSnapshot{}, false,
				fmt.Errorf("read memory usage for %s", level.dir)
		}
		reclaimable, _ := reclaimableCgroupCache(filepath.Join(level.dir, level.statFile))
		kernel := readCgroupKernelBytes(filepath.Join(level.dir, level.statFile))
		headroom := uint64(0)
		if usage < limit {
			headroom = limit - usage
		}
		if !found || headroom < selectedHeadroom {
			selected = MemoryDomainSnapshot{
				Used: usage, Limit: limit, Reclaimable: reclaimable,
				Kernel: kernel, Source: level.source, Measured: true,
				ObservedAt: time.Now(),
			}
			selectedHeadroom = headroom
			found = true
		}
	}
	return selected, found, nil
}

func readCgroupKernelBytes(path string) uint64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	values := make(map[string]uint64)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err == nil {
			values[fields[0]] = value
		}
	}
	if kernel, ok := values["kernel"]; ok {
		return kernel
	}
	var total uint64
	for _, key := range []string{
		"kernel_stack", "pagetables", "percpu", "sock", "slab",
		"total_kernel_stack", "total_pagetables", "total_percpu", "total_sock", "total_slab",
	} {
		if value, ok := values[key]; ok && value <= ^uint64(0)-total {
			total += value
		}
	}
	return total
}
