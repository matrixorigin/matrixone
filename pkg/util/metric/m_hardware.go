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
	"errors"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

var hardwareStatsCollector = newBatchStatsCollector(
	cpuTotal{},
	cpuPercent{},
	memUsed{},
	memAvail{},
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
		nil, nil,
	)
}

func (c cpuTotal) Metric(_ *statCaches) (prom.Metric, error) {
	cpus, _ := cpu.Times(false)
	if len(cpus) == 0 {
		return nil, errors.New("empty cpu times")
	}
	v := (cpus[0].Total() - cpus[0].Idle) / float64(logicalCore)
	return prom.MustNewConstMetric(c.Desc(), prom.CounterValue, v), nil
}

type cpuPercent struct{}

func (c cpuPercent) Desc() *prom.Desc {
	return prom.NewDesc(
		"sys_cpu_combined_percent",
		"System CPU busy percentage, average among all logical cores",
		nil, nil,
	)
}

func (c cpuPercent) Metric(_ *statCaches) (prom.Metric, error) {
	percents, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}
	if len(percents) == 0 {
		return nil, errors.New("empty cpu percents")
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
		nil, nil,
	)
}

func (m memUsed) Metric(s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyMemStats, getMemStats)
	if val == nil {
		return nil, errors.New("empty available memomry")
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
		nil, nil,
	)
}

func (m memAvail) Metric(s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyMemStats, getMemStats)
	if val == nil {
		return nil, errors.New("empty available memomry")
	}
	memostats := val.(*mem.VirtualMemoryStat)
	return prom.MustNewConstMetric(m.Desc(), prom.GaugeValue, float64(memostats.Available)), nil
}
