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

var HardwareStatsCollector = newHardwareStatsCollector(
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

func (c cpuTotal) Metric(_ *stats) (prom.Metric, error) {
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

func (c cpuPercent) Metric(_ *stats) (prom.Metric, error) {
	percents, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}
	if len(percents) == 0 {
		return nil, errors.New("empty cpu percents")
	}

	return prom.MustNewConstMetric(c.Desc(), prom.GaugeValue, percents[0]), nil
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

func (m memUsed) Metric(s *stats) (prom.Metric, error) {
	if s.meminfo != nil {
		return prom.MustNewConstMetric(m.Desc(), prom.GaugeValue, float64(s.meminfo.Used)), nil
	}
	return nil, errors.New("empty used memomry")
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

func (m memAvail) Metric(s *stats) (prom.Metric, error) {
	if s.meminfo != nil {
		return prom.MustNewConstMetric(m.Desc(), prom.GaugeValue, float64(s.meminfo.Available)), nil
	}
	return nil, errors.New("empty available memomry")
}

// Stats is some system values can be collected by a few function calls.
// one stats may spawn many metrics
type stats struct {
	meminfo *mem.VirtualMemoryStat
}

type simpleEntry interface {
	Desc() *prom.Desc
	// entry return the metric for now. it can fetch from the stats or just compute by itself
	Metric(*stats) (prom.Metric, error)
}

type hardwareStatsCollector struct {
	selfAsPromCollector
	entris []simpleEntry
}

func newHardwareStatsCollector(entries ...simpleEntry) Collector {
	c := &hardwareStatsCollector{
		entris: entries,
	}
	c.init(c)
	return c
}

func (c *hardwareStatsCollector) collectStats() *stats {
	s := new(stats)

	if memstats, err := mem.VirtualMemory(); err == nil {
		s.meminfo = memstats
	}
	return s
}

// Describe returns all descriptions of the collector.
func (c *hardwareStatsCollector) Describe(ch chan<- *prom.Desc) {
	for _, e := range c.entris {
		ch <- e.Desc()
	}
}

// Collect returns the current state of all metrics of the collector.
func (c *hardwareStatsCollector) Collect(ch chan<- prom.Metric) {
	stats := c.collectStats()
	for _, e := range c.entris {
		m, err := e.Metric(stats)
		if err != nil {
			logutil.Warnf("[Metric] %s collect a error: %v", e.Desc().String(), err)
		}
		ch <- m
	}
}
