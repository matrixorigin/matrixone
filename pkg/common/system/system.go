// Copyright 2021 - 2022 Matrix Origin
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
	"context"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/elastic/gosigar"
	"github.com/elastic/gosigar/cgroup"
	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	// pid is the process ID.
	pid int
	// cpuNum is the total number of CPU of this node.
	cpuNum atomic.Int32
	// cpuUsage is the CPU statistics updated every second.
	cpuUsage atomic.Uint64

	goMaxProcs atomic.Int32

	// lastQuotaRefreshTime tracks the last time quota config was refreshed.
	// Used to debounce frequent cgroup config changes during k8s vertical scaling.
	lastQuotaRefreshTime atomic.Int64
)

const (
	// quotaRefreshDebounceSeconds is the minimum interval between quota refreshes.
	// This prevents excessive refreshes when kubelet updates both cpu.max and memory.max
	// during vertical pod autoscaling.
	quotaRefreshDebounceSeconds = 1
)

// InContainer returns if the process is running in a container.
func InContainer() bool {
	return pid == 1
}

// NumCPU return the total number of CPU of this node.
func NumCPU() int {
	return int(cpuNum.Load())
}

// CPUAvailable returns the available cpu of this node.
func CPUAvailable() float64 {
	usage := math.Float64frombits(cpuUsage.Load())
	return math.Round((1 - usage) * float64(cpuNum.Load()))
}

// MemoryTotal returns the effective sizing capacity, including inherited soft
// limits. For paired usage/limit diagnostics use CurrentMemoryDomain: a binding
// ancestor may have less headroom but a larger capacity than this sizing bound.
func MemoryTotal() uint64 {
	snapshot, _ := CurrentMemoryDomain()
	if !snapshot.Measured {
		return 0
	}
	return NormalizeMemoryCapacity(snapshot.CapacityLimit())
}

// CgroupMemoryLimit returns the tightest hard limit from the published domain,
// including when the process is not PID 1 (for example a systemd scope or a
// nested container cgroup). Zero means unavailable.
func CgroupMemoryLimit() uint64 {
	snapshot, _ := CurrentMemoryDomain()
	if !snapshot.Measured || !snapshot.IsCgroup() {
		return 0
	}
	if snapshot.HardCapacity > 0 {
		return NormalizeMemoryCapacity(snapshot.HardCapacity)
	}
	return NormalizeMemoryCapacity(snapshot.Limit)
}

// NormalizeMemoryCapacity maps a raw memory-capacity reading onto the value
// used by the sizing callers. Zero means that no finite capacity was
// discovered. In particular, cgroup v1 reports PAGE_COUNTER_MAX (and some
// kernels report LONG_MAX) for an unlimited hierarchy; treating either as a
// real capacity turns an automatic budget into an effectively infinite one.
//
// This is exported because callers that combine MemoryTotal with
// CgroupMemoryLimit must apply the same normalization to both sources. A
// physical host cannot have a finite memory capacity anywhere near this
// sentinel, so values at or above it are unambiguously invalid for sizing.
func NormalizeMemoryCapacity(limit uint64) uint64 {
	if limit == 0 || limit >= cgroupV1Unlimited {
		return 0
	}
	return limit
}

// normalizeCgroupLimit maps a raw cgroup limit onto "0 means no bound".
//
// The gosigar fallback above returns v1's unlimited sentinel verbatim -- its own
// tests assert a limit of 9223372036854771712 -- so filtering the sentinel only
// inside the hierarchy walk left this path still reporting ~9.2 EB as a real
// limit, and MemoryAvailableIncludingCache's second tier still derived a budget
// from it.
func normalizeCgroupLimit(limit int64) uint64 {
	if limit <= 0 || uint64(limit) >= cgroupV1Unlimited {
		return 0
	}
	return NormalizeMemoryCapacity(uint64(limit))
}

func cgroupDirectory(mountPoint, mountRoot, processPath string) (string, bool) {
	mountRoot = filepath.Clean(mountRoot)
	processPath = filepath.Clean(processPath)
	rel, err := filepath.Rel(mountRoot, processPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", false
	}
	return filepath.Join(mountPoint, rel), true
}

// reclaimableCgroupCache reports clean filesystem cache on the reclaimable
// LRUs. Active file pages are recently used, not pinned: unlike anonymous or
// unevictable memory, the kernel can reclaim them under cgroup pressure.
//
// Bound the LRU counters by file minus shmem, and keep dirty/writeback pages
// charged. Missing or malformed counters leave the full usage charged. Read
// one snapshot and use only hierarchical total_* counters on cgroup v1; mixing
// local and hierarchical counters could credit a parent's children's cache
// without charging their dirty pages.
func reclaimableCgroupCache(statPath string) (uint64, bool) {
	data, err := os.ReadFile(statPath)
	if err != nil {
		return 0, false
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
	keys := [6]string{"file", "shmem", "inactive_file", "active_file", "file_dirty", "file_writeback"}
	if v1 {
		keys = [6]string{"total_cache", "total_shmem", "total_inactive_file", "total_active_file", "total_dirty", "total_writeback"}
	}
	var values [6]uint64
	for i, key := range keys {
		value, ok := stats[key]
		if !ok {
			return 0, false
		}
		values[i] = value
	}
	file, shmem, inactive, active, dirty, writeback := values[0], values[1], values[2], values[3], values[4], values[5]
	if shmem >= file {
		return 0, true
	}
	if active > math.MaxUint64-inactive {
		return 0, false
	}
	reclaimable := min(file-shmem, inactive+active)
	if dirty >= reclaimable {
		return 0, true
	}
	reclaimable -= dirty
	if writeback >= reclaimable {
		return 0, true
	}
	return reclaimable - writeback, true
}

// readCgroupUint reads a single-value cgroup file. Zero is a valid usage;
// missing, empty, malformed and "max" values are unavailable.
func readCgroupUint(path string) (uint64, bool) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	value := strings.TrimSpace(string(data))
	if value == "" || value == "max" {
		return 0, false
	}
	v, perr := strconv.ParseUint(value, 10, 64)
	if perr != nil {
		return 0, false
	}
	return v, true
}

// cgroupV1Unlimited is what cgroup v1 writes into memory.limit_in_bytes when no
// limit is set: PAGE_COUNTER_MAX, i.e. LONG_MAX rounded down to a page
// boundary. Only v2 signals "no limit" with the string "max". Kernels that
// report a bare LONG_MAX are covered too, since it is larger than this.
const cgroupV1Unlimited = uint64(0x7FFFFFFFFFFFF000)

type cgroupLimitState uint8

const (
	cgroupLimitUnavailable cgroupLimitState = iota
	// cgroupLimitMissing is a structural absence, not a malformed or failed
	// observation. A cgroup-v2 hierarchy root may not expose memory control
	// files when the memory controller is not attached to that root.
	cgroupLimitMissing
	cgroupLimitUnlimited
	cgroupLimitFinite
)

// readCgroupLimitState distinguishes an authoritative unlimited value, a
// structurally missing interface, and a failed observation. Callers walking a
// hierarchy may ignore a missing interface only at a known controller/root
// boundary; a missing or malformed governing file elsewhere must not replace a
// tight parent constraint with a looser child constraint.
func readCgroupLimitState(path string) (uint64, cgroupLimitState) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, cgroupLimitMissing
		}
		return 0, cgroupLimitUnavailable
	}
	value := strings.TrimSpace(string(data))
	if value == "" {
		return 0, cgroupLimitUnavailable
	}
	if value == "max" {
		return 0, cgroupLimitUnlimited
	}
	v, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, cgroupLimitUnavailable
	}
	if v >= cgroupV1Unlimited {
		return 0, cgroupLimitUnlimited
	}
	return v, cgroupLimitFinite
}

// readCgroupLimit reads a cgroup LIMIT file. ok is false for both observation
// failure and authoritative unlimited values; hierarchy walkers that need to
// distinguish those cases use readCgroupLimitState instead.
//
// The sentinel matters because it parses as a perfectly good integer: treating
// it as a real limit makes an unlimited v1 hierarchy report ~9.2 EB of headroom
// as a MEASURED figure, and every caller that sizes an allocation from that
// figure loses its memory bound entirely rather than falling back to the host
// reading.
func readCgroupLimit(path string) (uint64, bool) {
	v, state := readCgroupLimitState(path)
	return v, state == cgroupLimitFinite
}

// minHierarchicalHeadroom returns the tightest `limit - usage` across every
// governing level from `dir` up to `mountPoint`, and whether any level imposed
// a limit at all.
//
// Taking the minimum LIMIT and subtracting only the leaf's usage (which is what
// CgroupMemoryLimit + GetMemUsage(pid) amounts to) overstates headroom whenever
// an ancestor is the binding constraint: a parent capped at 8 GiB with 7 GiB
// already charged to sibling processes has 1 GiB left, but leaf-usage
// arithmetic reports nearly the whole 8 GiB. Headroom has to be computed at
// each level and then minimised.
//
// A level that publishes a limit but no readable usage makes the whole
// measurement unavailable rather than being skipped -- skipping a governing
// level is exactly the overstatement this exists to prevent.
func minHierarchicalHeadroom(dir, mountPoint, limitFile, usageFile, statFile string) (uint64, bool) {
	dir = filepath.Clean(dir)
	mountPoint = filepath.Clean(mountPoint)
	var (
		minimum uint64
		found   bool
	)
	for {
		if limit, ok := readCgroupLimit(filepath.Join(dir, limitFile)); ok && limit > 0 {
			usage, uok := readCgroupUint(filepath.Join(dir, usageFile))
			if !uok {
				return 0, false
			}
			// Page-cache recency is not a hard allocation boundary. Credit only
			// clean reclaimable file pages, at this same governing level.
			if reclaimable, rok := reclaimableCgroupCache(filepath.Join(dir, statFile)); rok {
				if reclaimable > usage {
					usage = 0
				} else {
					usage -= reclaimable
				}
			}
			var headroom uint64
			if limit > usage {
				headroom = limit - usage
			}
			if !found || headroom < minimum {
				minimum = headroom
				found = true
			}
		}
		if dir == mountPoint {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir || (parent != mountPoint && !strings.HasPrefix(parent, mountPoint+string(os.PathSeparator))) {
			break
		}
		dir = parent
	}
	return minimum, found
}

func minHierarchicalLimit(dir, mountPoint, filename string) uint64 {
	dir = filepath.Clean(dir)
	mountPoint = filepath.Clean(mountPoint)
	var minimum uint64
	for {
		if limit, ok := readCgroupLimit(filepath.Join(dir, filename)); ok && limit > 0 &&
			(minimum == 0 || limit < minimum) {
			minimum = limit
		}
		if dir == mountPoint {
			break
		}
		parent := filepath.Dir(dir)
		if parent == dir || (parent != mountPoint && !strings.HasPrefix(parent, mountPoint+string(os.PathSeparator))) {
			break
		}
		dir = parent
	}
	return minimum
}

// MemoryAvailable returns raw charged headroom at the binding admission level.
// It excludes cache credit and is saturated when usage exceeds a lowered limit.
func MemoryAvailable() uint64 {
	snapshot, _ := CurrentMemoryDomain()
	if !snapshot.Measured || snapshot.Used >= snapshot.AdmissionLimit() {
		return 0
	}
	return snapshot.AdmissionLimit() - snapshot.Used
}

// MemoryAvailableIncludingCache returns memory that could be allocated on this
// node after reclaiming clean file cache, without swapping anonymous memory.
// This is the number to use for sizing bulk
// allocations (index build buffers, ANN staging arrays):
//
//   - When a cgroup memory limit is discoverable for the current process
//     (regardless of whether the process is PID 1), it is `limit - cgroup
//     non-reclaimable usage`, bounded also by host MemAvailable. This bounds
//     allocations by the CN's actual budget rather than
//     the host's, so mo-service behind an init/entrypoint on a large node
//     with a small cgroup does not size against the whole host and get
//     OOM-killed by the cgroup.
//   - Otherwise (bare-metal, no cgroup): gosigar ActualFree, i.e. Linux
//     /proc/meminfo:MemAvailable — MemFree plus reclaimable page cache and
//     buffers. Raw MemFree drops to a few GiB the moment the cache warms up
//     even when the kernel can reclaim hundreds of GiB on demand.
//
// The second return reports whether the measurement SUCCEEDED. A zero value
// with measured==true means the budget is genuinely exhausted; callers must
// not conflate that with measured==false (unavailable), because the two
// demand opposite responses -- fail the request vs. fall back to another bound.
// When a domain exposes a soft limit, the returned headroom is bounded by that
// soft limit so bulk sizing and physical admission stop at the same boundary.
func MemoryAvailableIncludingCache() (avail uint64, measured bool) {
	// Both execution admission and the resource throttlers consume this
	// published sample. A refresh error keeps the last good sample available;
	// only the first-ever failure is reported as an unavailable measurement.
	snapshot, err := CurrentMemoryDomain()
	if !snapshot.Measured || snapshot.AdmissionLimit() == 0 {
		if err != nil {
			logutil.Errorf("failed to get memory domain: %v", err)
		}
		return 0, false
	}
	if err != nil {
		logutil.Warnf("using stale memory domain sample after refresh failure: %v", err)
	}
	return snapshot.AdmissionReclaimableHeadroom(), true
}

// MemoryUsed returns the charge of the binding level, not an RSS estimate or a
// usage reading taken separately from the immutable admission snapshot.
func MemoryUsed() uint64 {
	snapshot, _ := CurrentMemoryDomain()
	if !snapshot.Measured {
		return 0
	}
	return snapshot.Used
}

// MemoryGolang returns the total size of golang's memory.
func MemoryGolang() int {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return int(ms.Alloc)
}

// GoRoutines returns the number of goroutine.
func GoRoutines() int {
	return runtime.NumGoroutine()
}

// GoMaxProcs returns the maximum number of CPUs that can be executing goroutine.
// co-operate with SetGoMaxProcs
func GoMaxProcs() int {
	return int(goMaxProcs.Load())
}

// effectiveGoMaxProcs never raises a scheduler limit already established by
// the Go runtime or GOMAXPROCS, while preserving the detected CPU upper bound.
func effectiveGoMaxProcs(availableCPUs, currentMaxProcs int) int {
	if availableCPUs < 1 {
		return currentMaxProcs
	}
	if currentMaxProcs < 1 {
		return availableCPUs
	}
	if currentMaxProcs < availableCPUs {
		return currentMaxProcs
	}
	return availableCPUs
}

// SetGoMaxProcs
// co-operate with pkg/cnservice/service.handleGoMaxProcs
func SetGoMaxProcs(n int) (ret int) {
	ret = runtime.GOMAXPROCS(n)
	if n < 1 {
		// fix https://github.com/matrixorigin/MO-Cloud/issues/4486
		goMaxProcs.Store(int32(ret))
		logutil.Infof("call runtime.GOMAXPROCS(%d): %d, keep: %d", n, ret, ret)
	} else {
		goMaxProcs.Store(int32(n))
		logutil.Infof("call runtime.GOMAXPROCS(%d): %d, keep: %d", n, ret, n)
	}
	return ret
}

func runWithContainer(stopper *stopper.Stopper) {
	if err := stopper.RunNamedTask("system runner", func(ctx context.Context) {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var prevStats *cgroup.CPUAccountingSubsystem
		for {
			select {
			case <-ticker.C:
				stats, err := cgroup.GetCPUAcctStats(pid)
				if err != nil {
					logutil.Errorf("failed to get cpu acct cgroup stats: %v", err)
					continue
				}
				if prevStats != nil {
					work := stats.Stats.UserNanos + stats.Stats.SystemNanos -
						(prevStats.Stats.UserNanos + prevStats.Stats.SystemNanos)
					total := uint64(cpuNum.Load()) * uint64(time.Second)
					if total != 0 {
						usage := float64(work) / float64(total)
						cpuUsage.Store(math.Float64bits(usage))
					}
				}
				prevStats = &stats

			case <-ctx.Done():
				return
			}
		}
	}); err != nil {
		logutil.Errorf("failed to start system runner: %v", err)
	}

	// do watch cpu.max and memory.max to upgrade resource.
	runWatchCgroupConfig(stopper)
}

func runWithoutContainer(stopper *stopper.Stopper) {
	if err := stopper.RunNamedTask("system runner", func(ctx context.Context) {
		s := gosigar.ConcreteSigar{}
		cpuC, stopC := s.CollectCpuStats(time.Second)
		for {
			select {
			case cpu := <-cpuC:
				work := cpu.User + cpu.Nice + cpu.Sys
				total := cpu.Total()
				if total != 0 {
					usage := float64(work) / float64(total)
					cpuUsage.Store(math.Float64bits(usage))
				}

			case <-ctx.Done():
				stopC <- struct{}{}
				return
			}
		}
	}); err != nil {
		logutil.Errorf("failed to start system runner: %v", err)
	}
}

// Run starts a new goroutine go calculate the CPU usage.
func Run(stopper *stopper.Stopper) {
	if InContainer() {
		runWithContainer(stopper)
	} else {
		runWithoutContainer(stopper)
	}
	runSystemMonitor(stopper)
}

func runSystemMonitor(stopper *stopper.Stopper) {
	err := stopper.RunTask(
		func(ctx context.Context) {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()

			last := time.Now()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					interval := time.Since(last)
					if interval > time.Second*2 {
						logutil.Info("system is busy",
							zap.String("expect", "1s"),
							zap.Duration("actual", interval))
					}
					last = time.Now()
				}
			}
		},
	)
	if err != nil {
		panic(err)
	}
}

// shouldRefreshQuotaConfig checks if enough time has passed since last refresh.
// This debounces frequent cgroup updates during k8s vertical scaling where kubelet
// may update both cpu.max and memory.max files even if only one value changed.
func shouldRefreshQuotaConfig() bool {
	now := time.Now().UnixNano()
	last := lastQuotaRefreshTime.Load()

	// Always allow first refresh (last == 0)
	if last == 0 {
		return true
	}

	// Check if debounce period has passed
	return now-last >= int64(quotaRefreshDebounceSeconds)*int64(time.Second)
}

func effectiveContainerMemoryTotal(limit int64, hostTotal uint64) uint64 {
	if normalized := normalizeCgroupLimit(limit); normalized > 0 {
		return normalized
	}
	return NormalizeMemoryCapacity(hostTotal)
}

// refreshQuotaConfig get CPU/Mem config from dev. If run in container, get it from the cgroup config.
// Tips: Currently, the callings are serial in two places: 1) init; 2) runWatchCgroupConfig
func refreshQuotaConfig() {
	lastQuotaRefreshTime.Store(time.Now().UnixNano())
	if InContainer() {
		cpu, err := cgroup.GetCPUStats(pid)
		if err != nil {
			logutil.Errorf("failed to get cgroup cpu stats: %v", err)
		} else {
			if cpu.CFS.PeriodMicros != 0 && cpu.CFS.QuotaMicros != 0 {
				cpuNum.Store(int32(cpu.CFS.QuotaMicros / cpu.CFS.PeriodMicros))
			} else {
				cpuNum.Store(int32(runtime.NumCPU()))
			}
		}
	} else {
		cpuNum.Store(int32(runtime.NumCPU()))
	}
	// Quota watchers refresh the same fact source as admission and reporting;
	// memory discovery is independent of the PID-1 CPU accounting heuristic.
	if _, err := RefreshMemoryDomain(); err != nil {
		logutil.Errorf("failed to refresh memory domain: %v", err)
	}
}

func init() {
	pid = os.Getpid()
	refreshQuotaConfig()
	SetGoMaxProcs(effectiveGoMaxProcs(
		int(cpuNum.Load()),
		runtime.GOMAXPROCS(0),
	))
}
