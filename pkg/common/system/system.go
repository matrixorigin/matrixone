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
	"runtime"
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
	// cpuUsage is the CPU statistics updated every second.
	cpuUsage atomic.Uint64
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

// MemoryTotal returns the total size of memory of this node.
func MemoryTotal() uint64 {
	return memoryTotal.Load()
}

// MemoryAvailable returns the available size of memory of this node.
func MemoryAvailable() uint64 {
	if InContainer() {
		used, err := cgroup.GetMemUsage(pid)
		if err != nil {
			logutil.Errorf("failed to get memory usage: %v", err)
			return 0
		}
		return memoryTotal.Load() - uint64(used)
	}
	s := gosigar.ConcreteSigar{}
	mem, err := s.GetMem()
	if err != nil {
		logutil.Errorf("failed to get memory stats: %v", err)
	}
	return mem.Free
}

func MemoryUsed() uint64 {
	if InContainer() {
		used, err := cgroup.GetMemUsage(pid)
		if err != nil {
			logutil.Errorf("failed to get memory usage: %v", err)
			return 0
		}
		return uint64(used)
	}
	s := gosigar.ConcreteSigar{}
	mem, err := s.GetMem()
	if err != nil {
		logutil.Errorf("failed to get memory stats: %v", err)
	}
	return mem.Used
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

// refreshQuotaConfig get CPU/Mem config from dev. If run in container, get it from the cgroup config.
// Tips: Currently, the callings are serial in two places: 1) init; 2) runWatchCgroupConfig
func refreshQuotaConfig() {
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
		limit, err := cgroup.GetMemLimit(pid)
		if err != nil {
			logutil.Errorf("failed to get cgroup mem limit: %v", err)
		} else {
			memoryTotal.Store(uint64(limit))
		}
	} else {
		cpuNum.Store(int32(runtime.NumCPU()))
		s := gosigar.ConcreteSigar{}
		mem, err := s.GetMem()
		if err != nil {
			logutil.Errorf("failed to get memory stats: %v", err)
		} else {
			memoryTotal.Store(mem.Total)
		}
	}
}

func init() {
	pid = os.Getpid()
	refreshQuotaConfig()
}
