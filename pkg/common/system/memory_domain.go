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
	Used  uint64
	Limit uint64
	// SoftLimit is the pressure/admission limit. A zero value means that the
	// domain does not expose a separate soft boundary. For cgroup v2 this is
	// memory.high; for cgroup v1 it is memory.soft_limit_in_bytes; for a host it
	// is the configured MO_MEMORY_HOST_SOFT_LIMIT value.
	SoftLimit   uint64
	Reclaimable uint64
	Kernel      uint64
	// NonGo is the measured portion of the domain which is not expected to be
	// owned by Go/MPool allocations.  It includes kernel/socket/slab charges
	// and dirty/shmem pages when the domain exposes those counters.  It is an
	// input to the reserve policy, not an additional charge on top of Used.
	NonGo      uint64
	Source     MemoryDomainSource
	Measured   bool
	ObservedAt time.Time
}

func (s MemoryDomainSnapshot) IsCgroup() bool {
	return s.Source == MemoryDomainSourceCgroupV1 || s.Source == MemoryDomainSourceCgroupV2
}

// AdmissionLimit is the soft boundary used by both physical admission gates.
// The hard limit remains available through Limit for diagnostics and hard-OOM
// headroom, but new work must stop at memory.high when the operator configured
// one.
func (s MemoryDomainSnapshot) AdmissionLimit() uint64 {
	if s.SoftLimit > 0 && s.SoftLimit < s.Limit {
		return s.SoftLimit
	}
	return s.Limit
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

// AdmissionReclaimableHeadroom is the clean-cache-credit view bounded by the
// same soft limit used for new physical reservations.
func (s MemoryDomainSnapshot) AdmissionReclaimableHeadroom() uint64 {
	limit := s.AdmissionLimit()
	used := s.Used
	if s.Reclaimable >= used {
		used = 0
	} else {
		used -= s.Reclaimable
	}
	if limit <= used {
		return 0
	}
	return limit - used
}

// MemoryDomainReservePolicy converts observed non-Go charges into a reserve
// which is removed from the physical admission budget.  The policy is kept in
// this package so the execution gate and the write-path throttlers cannot
// silently choose different reserve semantics.
//
// MinimumBytes and RuntimeBytes are deliberately small, data-independent
// floors.  The normal reserve is driven by the observed high-water non-Go
// charge.  A deployment can override the floors with
// MO_MEMORY_NON_GO_RESERVE_BYTES, MO_MEMORY_RUNTIME_RESERVE_BYTES, and the
// source-specific MO_MEMORY_* reserve/multiplier overrides.
type MemoryDomainReservePolicy struct {
	MinimumBytes            uint64
	RuntimeBytes            uint64
	ObservedNonGoMultiplier uint64
	// HostObservedNonGoMultiplier and HostMinimumPercent let a bare host keep
	// a larger neighbor/OS safety margin than an isolated cgroup. Zero uses the
	// generic value, preserving deterministic custom policies.
	HostObservedNonGoMultiplier uint64
	HostMinimumPercent          uint64
	// MinimumPercent is a domain-relative safety floor. It is intentionally
	// lower than the old fixed 20% execution reserve; the measured non-Go high
	// water is the primary input and this percentage covers counters the domain
	// cannot expose.
	MinimumPercent uint64
}

const (
	defaultMemoryDomainReserveFloor    = uint64(256 << 20)
	defaultMemoryDomainRuntimeFloor    = uint64(256 << 20)
	defaultMemoryDomainNonGoMultiplier = uint64(2)
	defaultMemoryDomainReservePercent  = uint64(5)
)

// DefaultMemoryDomainReservePolicy returns the process-wide policy. Environment
// values are read when the policy is requested so benchmark processes can set
// them without adding another service configuration field.
func DefaultMemoryDomainReservePolicy() MemoryDomainReservePolicy {
	policy := MemoryDomainReservePolicy{
		MinimumBytes:            defaultMemoryDomainReserveFloor,
		RuntimeBytes:            defaultMemoryDomainRuntimeFloor,
		ObservedNonGoMultiplier: defaultMemoryDomainNonGoMultiplier,
		MinimumPercent:          defaultMemoryDomainReservePercent,
		HostMinimumPercent:      defaultMemoryDomainReservePercent * 2,
	}
	if value, ok := memorySizeFromEnv("MO_MEMORY_NON_GO_RESERVE_BYTES"); ok {
		policy.MinimumBytes = value
	}
	if value, ok := memorySizeFromEnv("MO_MEMORY_RUNTIME_RESERVE_BYTES"); ok {
		policy.RuntimeBytes = value
	}
	if value, ok := memoryPercentFromEnv("MO_MEMORY_RESERVE_PERCENT"); ok {
		policy.MinimumPercent = value
	}
	if value, ok := memoryUintFromEnv("MO_MEMORY_NON_GO_MULTIPLIER"); ok {
		policy.ObservedNonGoMultiplier = value
	}
	if value, ok := memoryPercentFromEnv("MO_MEMORY_HOST_RESERVE_PERCENT"); ok {
		policy.HostMinimumPercent = value
	}
	if value, ok := memoryUintFromEnv("MO_MEMORY_HOST_NON_GO_MULTIPLIER"); ok {
		policy.HostObservedNonGoMultiplier = value
	}
	return policy
}

// Reserve returns the non-Go and runtime reserve required by one observation.
// Saturating arithmetic keeps malformed or unusually large kernel counters
// from wrapping into an unsafe small reserve.
func (p MemoryDomainReservePolicy) Reserve(snapshot MemoryDomainSnapshot) uint64 {
	reserve := p.MinimumBytes
	multiplier := p.ObservedNonGoMultiplier
	minimumPercent := p.MinimumPercent
	if snapshot.Source == MemoryDomainSourceHost {
		if p.HostObservedNonGoMultiplier > 0 {
			multiplier = p.HostObservedNonGoMultiplier
		}
		if p.HostMinimumPercent > 0 {
			minimumPercent = p.HostMinimumPercent
		}
	}
	if multiplier == 0 {
		multiplier = 1
	}
	observed := snapshot.NonGo
	if observed > ^uint64(0)/multiplier {
		observed = ^uint64(0)
	} else {
		observed *= multiplier
	}
	if observed > reserve {
		reserve = observed
	}
	if minimumPercent > 0 {
		limit := snapshot.AdmissionLimit()
		percentReserve := limit / 100 * minimumPercent
		if limit%100 != 0 && (limit%100)*minimumPercent > 0 {
			percentReserve++
		}
		if percentReserve > reserve {
			reserve = percentReserve
		}
	}
	if p.RuntimeBytes > ^uint64(0)-reserve {
		return ^uint64(0)
	}
	return reserve + p.RuntimeBytes
}

func memoryPercentFromEnv(name string) (uint64, bool) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0, false
	}
	percent, err := strconv.ParseUint(value, 10, 64)
	if err != nil || percent > 100 {
		return 0, false
	}
	return percent, true
}

func memoryUintFromEnv(name string) (uint64, bool) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return parsed, err == nil
}

func memorySizeFromEnv(name string) (uint64, bool) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0, false
	}
	value = strings.ToUpper(value)
	multiplier := uint64(1)
	for _, suffix := range []struct {
		name string
		mul  uint64
	}{
		{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30},
		{"KB", 1 << 10}, {"MB", 1 << 20}, {"GB", 1 << 30},
		{"B", 1},
	} {
		if strings.HasSuffix(value, suffix.name) {
			value = strings.TrimSpace(strings.TrimSuffix(value, suffix.name))
			multiplier = suffix.mul
			break
		}
	}
	bytes, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	if err != nil || bytes > ^uint64(0)/multiplier {
		return 0, false
	}
	return bytes * multiplier, true
}

func hostMemorySoftLimit(total uint64) uint64 {
	for _, name := range []string{"MO_MEMORY_HOST_SOFT_LIMIT", "MO_MEMORY_SOFT_LIMIT"} {
		if value, ok := memorySizeFromEnv(name); ok && value > 0 {
			if total > 0 && value < total {
				return value
			}
			return 0
		}
	}
	return 0
}

// MemoryAdmissionWaitTimeout controls the bounded statement-entry wait used
// when the shared physical ledger is temporarily full. A zero duration turns
// the queue into an immediate reject; the default is short enough not to hold
// frontend workers while still allowing an in-flight statement to release a
// reservation.
func MemoryAdmissionWaitTimeout() time.Duration {
	const defaultWait = 250 * time.Millisecond
	value := strings.TrimSpace(os.Getenv("MO_MEMORY_ADMISSION_WAIT"))
	if value == "" {
		return defaultWait
	}
	wait, err := time.ParseDuration(value)
	if err != nil || wait < 0 {
		return defaultWait
	}
	return wait
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
	softFile  string
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
	kernel, nonGo := hostMemoryAccounting()
	return MemoryDomainSnapshot{
		Used:        mem.Used,
		Limit:       mem.Total,
		SoftLimit:   hostMemorySoftLimit(mem.Total),
		Reclaimable: reclaimable,
		Kernel:      kernel,
		NonGo:       nonGo,
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
				"memory.max", "memory.high", "memory.current", "memory.stat"), true
		case "cgroup":
			if v1MemoryPath == "" || !strings.Contains(","+right[2]+",", ",memory,") {
				continue
			}
			dir, ok := cgroupDirectory(mountPoint, mountRoot, v1MemoryPath)
			if !ok {
				continue
			}
			return buildMemoryDomainLevels(dir, mountPoint, MemoryDomainSourceCgroupV1,
				"memory.limit_in_bytes", "memory.soft_limit_in_bytes", "memory.usage_in_bytes", "memory.stat"), true
		}
	}
	return nil, false
}

func buildMemoryDomainLevels(dir, mountPoint string, source MemoryDomainSource,
	limitFile, softFile, usageFile, statFile string) []memoryDomainLevel {
	dir = filepath.Clean(dir)
	mountPoint = filepath.Clean(mountPoint)
	levels := make([]memoryDomainLevel, 0, 4)
	for {
		levels = append(levels, memoryDomainLevel{
			dir: dir, limitFile: limitFile, softFile: softFile, usageFile: usageFile,
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
		reclaimable, kernel, nonGo := readCgroupAccounting(filepath.Join(level.dir, level.statFile))
		softLimit, _ := readCgroupLimit(filepath.Join(level.dir, level.softFile))
		if softLimit == 0 || softLimit > limit {
			softLimit = 0
		}
		admissionLimit := limit
		if softLimit > 0 && softLimit < admissionLimit {
			admissionLimit = softLimit
		}
		headroom := uint64(0)
		if usage < admissionLimit {
			headroom = admissionLimit - usage
		}
		if !found || headroom < selectedHeadroom {
			selected = MemoryDomainSnapshot{
				Used: usage, Limit: limit, SoftLimit: softLimit, Reclaimable: reclaimable,
				Kernel: kernel, NonGo: nonGo, Source: level.source, Measured: true,
				ObservedAt: time.Now(),
			}
			selectedHeadroom = headroom
			found = true
		}
	}
	return selected, found, nil
}

// readCgroupAccounting reads one memory.stat snapshot.  v1 exposes the
// hierarchical counters with a total_ prefix; v2 exposes the local names.
// Keeping all derived fields on the same read avoids mixing measurements from
// different pressure points.
func readCgroupAccounting(path string) (reclaimable, kernel, nonGo uint64) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, 0
	}
	stats := make(map[string]uint64)
	v1 := false
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		v1 = v1 || strings.HasPrefix(fields[0], "total_")
		if value, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
			stats[fields[0]] = value
		}
	}

	value := func(name string) uint64 {
		if v1 {
			switch name {
			case "file":
				name = "cache"
			case "file_dirty":
				name = "dirty"
			case "file_writeback":
				name = "writeback"
			}
			name = "total_" + name
		}
		return stats[name]
	}
	fileName := "file"
	if v1 {
		fileName = "cache"
	}
	file, shmem := value(fileName), value("shmem")
	inactive, active := value("inactive_file"), value("active_file")
	dirty, writeback := value("file_dirty"), value("file_writeback")
	if shmem < file {
		if active <= ^uint64(0)-inactive {
			reclaimable = min(file-shmem, inactive+active)
			if dirty < reclaimable {
				reclaimable -= dirty
			} else {
				reclaimable = 0
			}
			if writeback < reclaimable {
				reclaimable -= writeback
			} else {
				reclaimable = 0
			}
		}
	}

	if v1 {
		kernel = stats["total_kernel"]
	} else {
		kernel = stats["kernel"]
	}
	if kernel == 0 {
		for _, name := range []string{
			"kernel_stack", "pagetables", "percpu", "sock", "slab",
			"total_kernel_stack", "total_pagetables", "total_percpu", "total_sock", "total_slab",
		} {
			if v1 && !strings.HasPrefix(name, "total_") {
				continue
			}
			if !v1 && strings.HasPrefix(name, "total_") {
				continue
			}
			part := stats[name]
			if part <= ^uint64(0)-kernel {
				kernel += part
			}
		}
	}

	// These counters are charged to the domain but are not clean cache credit.
	// Include them in the reserve estimate so a high-water sample protects the
	// cgroup before the kernel has to reclaim or write them back.
	// kernel already includes slab when the aggregate counter is available, so
	// do not add slab_unreclaimable a second time.
	for _, name := range []string{"file_dirty", "file_writeback", "shmem", "unevictable"} {
		part := value(name)
		if part <= ^uint64(0)-nonGo {
			nonGo += part
		}
	}
	if kernel <= ^uint64(0)-nonGo {
		nonGo += kernel
	} else {
		nonGo = ^uint64(0)
	}
	return reclaimable, kernel, nonGo
}
