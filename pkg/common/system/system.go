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
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

var (
	// pid is the process ID.
	pid int
	// cpuNum is the total number of CPU of this node.
	cpuNum int
	// memoryTotal is the total memory of this node.
	memoryTotal uint64
	// cpuUsage is the CPU statistics updated every second.
	cpuUsage atomic.Uint64
)

// InContainer returns if the process is running in a container.
func InContainer() bool {
	return pid == 1
}

// NumCPU return the total number of CPU of this node.
func NumCPU() int {
	return cpuNum
}

// CPUAvailable returns the available cpu of this node.
func CPUAvailable() float64 {
	usage := math.Float64frombits(cpuUsage.Load())
	return math.Round((1 - usage) * float64(cpuNum))
}

func getCGroupReader() (*cgroup.Reader, error) {
	reader, err := cgroup.NewReader("", true)
	if err != nil {
		logutil.Errorf("failed to create cgroup reader: %v", err)
		return nil, err
	}
	return reader, nil
}

func getCGroupStats() (*cgroup.Stats, error) {
	reader, err := getCGroupReader()
	if err != nil {
		return nil, err
	}
	stats, err := reader.GetStatsForProcess(pid)
	if err != nil || stats == nil {
		logutil.Errorf("failed to get cgroup stats: %v", err)
		return nil, err
	}
	return stats, nil
}

// MemoryTotal returns the total size of memory of this node.
func MemoryTotal() uint64 {
	return memoryTotal
}

// MemoryAvailable returns the available size of memory of this node.
func MemoryAvailable() uint64 {
	if InContainer() {
		stats, err := getCGroupStats()
		if err != nil {
			return 0
		}
		return stats.Memory.Mem.Limit - stats.Memory.Mem.Usage
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
		stats, err := getCGroupStats()
		if err != nil {
			return 0
		}
		return stats.Memory.Mem.Usage
	}
	s := gosigar.ConcreteSigar{}
	mem, err := s.GetMem()
	if err != nil {
		logutil.Errorf("failed to get memory stats: %v", err)
	}
	return mem.Used
}

func MemoryMaxUsage() uint64 {
	if InContainer() {
		stats, err := getCGroupStats()
		if err != nil {
			return 0
		}
		return stats.Memory.Mem.MaxUsage
	}
	logutil.Warnf("process does not run in a container, cannot get max usage of memory")
	return 0
}

func MemoryFailCount() uint64 {
	if InContainer() {
		stats, err := getCGroupStats()
		if err != nil {
			return 0
		}
		return stats.Memory.Mem.FailCount
	}
	logutil.Warnf("process does not run in a container, cannot get fail count of memory")
	return 0
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
		reader, err := getCGroupReader()
		if err != nil {
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		var prevStats *cgroup.Stats
		for {
			select {
			case <-ticker.C:
				stats, err := reader.GetStatsForProcess(pid)
				if err != nil || stats == nil {
					logutil.Errorf("failed to get cgroup stats: %v", err)
					continue
				}
				if prevStats != nil {
					work := stats.CPUAccounting.Stats.UserNanos + stats.CPUAccounting.Stats.SystemNanos -
						(prevStats.CPUAccounting.Stats.UserNanos + prevStats.CPUAccounting.Stats.SystemNanos)
					total := stats.CPUAccounting.TotalNanos - prevStats.CPUAccounting.TotalNanos
					if total != 0 {
						usage := float64(work) / float64(total)
						cpuUsage.Store(math.Float64bits(usage))
					}
				}
				prevStats = stats

			case <-ctx.Done():
				return
			}
		}
	}); err != nil {
		logutil.Errorf("failed to start system runner: %v", err)
	}
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
}

func init() {
	pid = os.Getpid()
	if InContainer() {
		stats, err := getCGroupStats()
		if err != nil {
			logutil.Errorf("failed to get cgroup stats: %v", err)
		} else {
			if stats.CPU.CFS.PeriodMicros != 0 {
				cpuNum = int(stats.CPU.CFS.QuotaMicros / stats.CPU.CFS.PeriodMicros)
			} else {
				cpuNum = runtime.NumCPU()
			}
			memoryTotal = stats.Memory.Mem.Limit
		}
	} else {
		cpuNum = runtime.NumCPU()
		s := gosigar.ConcreteSigar{}
		mem, err := s.GetMem()
		if err != nil {
			logutil.Errorf("failed to get memory stats: %v", err)
		} else {
			memoryTotal = mem.Total
		}
	}
}
