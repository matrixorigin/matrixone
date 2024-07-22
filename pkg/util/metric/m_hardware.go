// Copyright 2022 Matrix Origin
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

package metric

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

var hardwareStatsCollector = newBatchStatsCollector(
	cpuTotal{},
	cpuPercent{},
	memUsed{},
	memAvail{},
	diskR{},
	diskW{},
	netR{},
	netW{},
)

var logicalCore int

func init() {
	logicalCore, _ = cpu.Counts(true)
}

type cpuTotal struct{}

func (c cpuTotal) Desc() *prom.Desc {
	return prom.NewDesc(
		"sys_cpu_seconds_total",
		"System CPU time spent in seconds, normalized by number of cores",
		nil, sysTenantID,
	)
}

func (c cpuTotal) Metric(ctx context.Context, _ *statCaches) (prom.Metric, error) {
	cpus, _ := cpu.Times(false)
	if len(cpus) == 0 {
		return nil, moerr.NewInternalError(ctx, "empty cpu times")
	}
	v := (CPUTotalTime(cpus[0]) - cpus[0].Idle) / float64(logicalCore)
	return prom.MustNewConstMetric(c.Desc(), prom.CounterValue, v), nil
}

type cpuPercent struct{}

func (c cpuPercent) Desc() *prom.Desc {
	return prom.NewDesc(
		"sys_cpu_combined_percent",
		"System CPU busy percentage, average among all logical cores",
		nil, sysTenantID,
	)
}

func (c cpuPercent) Metric(ctx context.Context, _ *statCaches) (prom.Metric, error) {
	percents, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}
	if len(percents) == 0 {
		return nil, moerr.NewInternalError(ctx, "empty cpu percents")
	}

	return prom.MustNewConstMetric(c.Desc(), prom.GaugeValue, percents[0]), nil
}

// getMemStats get common data for memory metric
func getMemStats() any /* *mem.VirtualMemoryStat */ {
	if memstats, err := mem.VirtualMemory(); err == nil {
		return memstats
	} else {
		logutil.Warnf("[Metric] failed to get VirtualMemory, %v", err)
		return nil
	}
}

// memTotal = /proc/meminfo.{MemFree + Cached}
type memUsed struct{}

func (m memUsed) Desc() *prom.Desc {
	return prom.NewDesc(
		"sys_memory_used",
		"System memory used in bytes",
		nil, sysTenantID,
	)
}

func (m memUsed) Metric(ctx context.Context, s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyMemStats, getMemStats)
	if val == nil {
		return nil, moerr.NewInternalError(ctx, "empty available memomry")
	}
	memostats := val.(*mem.VirtualMemoryStat)
	return prom.MustNewConstMetric(m.Desc(), prom.GaugeValue, float64(memostats.Used)), nil
}

// memAail = /proc/meminfo.MemAvailable
type memAvail struct{}

func (m memAvail) Desc() *prom.Desc {
	return prom.NewDesc(
		"sys_memory_available",
		"System memory available in bytes",
		nil, sysTenantID,
	)
}

func (m memAvail) Metric(ctx context.Context, s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyMemStats, getMemStats)
	if val == nil {
		return nil, moerr.NewInternalError(ctx, "empty available memomry")
	}
	memostats := val.(*mem.VirtualMemoryStat)
	return prom.MustNewConstMetric(m.Desc(), prom.GaugeValue, float64(memostats.Available)), nil
}

func getDiskStats() any {
	if diskStats, err := disk.IOCounters(); err == nil {
		var total disk.IOCountersStat
		for _, v := range diskStats {
			total.ReadBytes += v.ReadBytes
			total.WriteBytes += v.WriteBytes
		}
		return &total
	} else {
		logutil.Warnf("[Metric] failed to get DiskIOCounters, %v", err)
		return nil
	}
}

// if we have window function, we can just use NewConstMetric
var diskRead = NewCounter(prom.CounterOpts{
	Subsystem:   "sys",
	Name:        "disk_read_bytes",
	Help:        "Total read bytes of all disks",
	ConstLabels: sysTenantID,
})

var diskWrite = NewCounter(prom.CounterOpts{
	Subsystem:   "sys",
	Name:        "disk_write_bytes",
	Help:        "Total write bytes of all disks",
	ConstLabels: sysTenantID,
})

type diskR struct{}

func (d diskR) Desc() *prom.Desc {
	return diskRead.Desc()
}

func (d diskR) Metric(ctx context.Context, s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyDiskIO, getDiskStats)
	if val == nil {
		return nil, moerr.NewInternalError(ctx, "empty available disk stats")
	}
	memostats := val.(*disk.IOCountersStat)
	diskRead.Set(memostats.ReadBytes)
	return diskRead, nil
}

type diskW struct{}

func (d diskW) Desc() *prom.Desc {
	return diskWrite.Desc()
}

func (d diskW) Metric(ctx context.Context, s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyDiskIO, getDiskStats)
	if val == nil {
		return nil, moerr.NewInternalError(ctx, "empty available disk stats")
	}
	memostats := val.(*disk.IOCountersStat)
	diskWrite.Set(memostats.WriteBytes)
	return diskWrite, nil
}

func getNetStats() any {
	if netStats, err := net.IOCounters(true); err == nil {
		var total net.IOCountersStat
		for _, v := range netStats {
			if strings.HasPrefix(v.Name, "lo") {
				continue
			}
			total.BytesRecv += v.BytesRecv
			total.BytesSent += v.BytesSent
		}
		return &total
	} else {
		logutil.Warnf("[Metric] failed to get DiskIOCounters, %v", err)
		return nil
	}
}

var netRead = NewCounter(prom.CounterOpts{
	Subsystem:   "sys",
	Name:        "net_recv_bytes",
	Help:        "Total recv bytes of all nic (expect lo)",
	ConstLabels: sysTenantID,
})

var netWrite = NewCounter(prom.CounterOpts{
	Subsystem:   "sys",
	Name:        "net_sent_bytes",
	Help:        "Total sent bytes of all nic (expect lo)",
	ConstLabels: sysTenantID,
})

type netR struct{}

func (d netR) Desc() *prom.Desc {
	return netRead.Desc()
}

func (d netR) Metric(ctx context.Context, s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyNetIO, getNetStats)
	if val == nil {
		return nil, moerr.NewInternalError(ctx, "empty available net stats")
	}
	memostats := val.(*net.IOCountersStat)
	netRead.Set(memostats.BytesRecv)
	return netRead, nil
}

type netW struct{}

func (d netW) Desc() *prom.Desc {
	return netWrite.Desc()
}

func (d netW) Metric(ctx context.Context, s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyNetIO, getNetStats)
	if val == nil {
		return nil, moerr.NewInternalError(ctx, "empty available net stats")
	}
	memostats := val.(*net.IOCountersStat)
	netWrite.Set(memostats.BytesSent)
	return netWrite, nil
}
