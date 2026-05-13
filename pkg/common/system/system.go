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
	"bufio"
	"context"
	"math"
	"os"
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
	// memoryTotal is the total memory of this node.
	memoryTotal atomic.Uint64
	// hasCgroupMemLimit is true when a valid cgroup memory limit was detected
	// during init. Unlike InContainer() (pid==1), this works with tini/other inits.
	hasCgroupMemLimit atomic.Bool
	// lastMallocTrim tracks the last time MallocTrim was called (UnixNano).
	lastMallocTrim atomic.Int64
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

// HasCgroupMemLimit returns true if a valid cgroup memory limit was detected.
// Use this instead of InContainer() for memory pressure decisions.
func HasCgroupMemLimit() bool {
	return hasCgroupMemLimit.Load()
}

// MemoryTotal returns the total size of memory of this node.
func MemoryTotal() uint64 {
	return memoryTotal.Load()
}

// MemoryAvailable returns the available size of memory of this node.
func MemoryAvailable() uint64 {
	total := memoryTotal.Load()
	used, err := cgroup.GetMemUsage(pid)
	if err == nil && used > 0 && uint64(used) <= total {
		return total - uint64(used)
	}
	s := gosigar.ConcreteSigar{}
	mem, err := s.GetMem()
	if err != nil {
		logutil.Errorf("failed to get memory stats: %v", err)
	}
	return mem.Free
}

func MemoryUsed() uint64 {
	total := memoryTotal.Load()
	used, err := cgroup.GetMemUsage(pid)
	if err == nil && used > 0 && uint64(used) <= total {
		return uint64(used)
	}
	s := gosigar.ConcreteSigar{}
	mem, err := s.GetMem()
	if err != nil {
		logutil.Errorf("failed to get memory stats: %v", err)
	}
	return mem.Used
}

// WorkingSet returns the working set memory in bytes, which is
// memory.current - inactive_file (cgroups v2) or
// memory.usage_in_bytes - total_inactive_file (cgroups v1).
// This matches how Kubernetes computes working_set_bytes for eviction
// decisions and excludes reclaimable page cache that the kernel can
// reclaim under memory pressure without triggering OOM.
// Falls back to MemoryUsed() if inactive_file cannot be read.
func WorkingSet() uint64 {
	used := MemoryUsed()
	inactive := cgroupInactiveFile()
	if inactive > 0 && inactive < used {
		return used - inactive
	}
	return used
}

// cgroupInactiveFile reads inactive_file from the cgroup memory.stat.
// Returns 0 if it cannot be determined.
func cgroupInactiveFile() uint64 {
	return cgroupInactiveFileValue.Load()
}

var cgroupInactiveFileValue atomic.Uint64

func refreshCgroupInactiveFile() {
	// cgroups v2: /sys/fs/cgroup/memory.stat key "inactive_file"
	if v := readStatKey("/sys/fs/cgroup/memory.stat", "inactive_file"); v > 0 {
		cgroupInactiveFileValue.Store(v)
		return
	}
	// cgroups v1: /sys/fs/cgroup/memory/memory.stat key "total_inactive_file"
	if v := readStatKey("/sys/fs/cgroup/memory/memory.stat", "total_inactive_file"); v > 0 {
		cgroupInactiveFileValue.Store(v)
		return
	}
	cgroupInactiveFileValue.Store(0)
}

func readStatKey(path, key string) uint64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, key) {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) != 2 || parts[0] != key {
			continue
		}
		v, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			return 0
		}
		return v
	}
	return 0
}

// MallocTrimIfTight calls malloc_trim(0) if container memory usage > 75%.
// Rate-limited to at most once per 5 seconds to avoid excessive overhead.
func MallocTrimIfTight() {
	total := MemoryTotal()
	if total == 0 {
		return
	}
	used := MemoryUsed()
	if used > total*3/4 {
		now := time.Now().UnixNano()
		last := lastMallocTrim.Load()
		if now-last > 5_000_000_000 && lastMallocTrim.CompareAndSwap(last, now) {
			MallocTrim()
		}
	}
}

// StartMallocTrimLoop starts a background goroutine that periodically calls
// malloc_trim(0) when container memory usage exceeds 75%.
func StartMallocTrimLoop(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				total := MemoryTotal()
				if total == 0 {
					continue
				}
				used := MemoryUsed()
				if used > total*3/4 {
					MallocTrim()
				}
			}
		}
	}()
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
				refreshCgroupInactiveFile()

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

// refreshQuotaConfig get CPU/Mem config from dev. If run in container, get it from the cgroup config.
// Tips: Currently, the callings are serial in two places: 1) init; 2) runWatchCgroupConfig
func refreshQuotaConfig() {
	lastQuotaRefreshTime.Store(time.Now().UnixNano())

	// Always try cgroup first — InContainer() (pid==1) is unreliable when
	// the container uses tini or another init process.
	cpuFromCgroup := false
	cpu, err := cgroup.GetCPUStats(pid)
	if err == nil && cpu.CFS.PeriodMicros != 0 && cpu.CFS.QuotaMicros != 0 {
		cpuNum.Store(int32(cpu.CFS.QuotaMicros / cpu.CFS.PeriodMicros))
		cpuFromCgroup = true
	}
	if !cpuFromCgroup {
		cpuNum.Store(int32(runtime.NumCPU()))
	}

	s := gosigar.ConcreteSigar{}
	mem, memErr := s.GetMem()

	memFromCgroup := false
	limit, err := cgroup.GetMemLimit(pid)
	if err == nil && limit > 0 {
		if memErr != nil || uint64(limit) < mem.Total {
			memoryTotal.Store(uint64(limit))
			memFromCgroup = true
		}
	}
	hasCgroupMemLimit.Store(memFromCgroup)
	if !memFromCgroup {
		if memErr != nil {
			logutil.Errorf("failed to get memory stats: %v", memErr)
		} else {
			memoryTotal.Store(mem.Total)
		}
	}
}

func init() {
	pid = os.Getpid()
	refreshQuotaConfig()
	refreshCgroupInactiveFile()
	SetGoMaxProcs(int(cpuNum.Load()))
}
