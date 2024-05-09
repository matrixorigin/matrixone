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
	"os"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/v3/process"
)

// ProcessCollector collect following information about the current MO process:
//
// - CPUTime (Sys + User + Iowait) in seconds (percent for now)
// - open fds & max fds (not available on MacOS)
// - virtual_resident_mem_bytes

var processCollector = newBatchStatsCollector(procCpuPercent{}, procMemUsage{}, procOpenFds{}, procCpuTotal{})

var pid = int32(os.Getpid())

var getProcess = func() any {
	if proc, err := process.NewProcess(pid); err == nil {
		return proc
	} else {
		logutil.Warnf("[Metric] failed to get current process %d, %v", pid, err)
		return nil
	}
}

// this percent may exceeds 100% on multicore platform
type procCpuPercent struct{}

func (c procCpuPercent) Desc() *prom.Desc {
	return prom.NewDesc(
		"process_cpu_percent",
		"Process CPU busy percentage",
		nil, sysTenantID,
	)
}

func (c procCpuPercent) Metric(ctx context.Context, s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyProcess, getProcess)
	if val == nil {
		return nil, moerr.NewInternalError(ctx, "empty process")
	}
	proc := val.(*process.Process)

	// Percent use cpuStats.Total because cpuStats in process has no Idel field
	if percent, err := proc.CPUPercent(); err != nil {
		return nil, err
	} else {
		return prom.MustNewConstMetric(c.Desc(), prom.GaugeValue, percent), nil
	}
}

type procMemUsage struct{}

func (c procMemUsage) Desc() *prom.Desc {
	return prom.NewDesc(
		"process_resident_memory_bytes",
		"Resident memory size in bytes.",
		nil, sysTenantID,
	)
}

func (c procMemUsage) Metric(ctx context.Context, s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyProcess, getProcess)
	if val == nil {
		return nil, moerr.NewInternalError(ctx, "empty process")
	}
	proc := val.(*process.Process)
	if mem, err := proc.MemoryInfo(); err != nil {
		return nil, err
	} else {
		return prom.MustNewConstMetric(c.Desc(), prom.GaugeValue, float64(mem.RSS)), nil
	}
}

type procOpenFds struct{}

func (c procOpenFds) Desc() *prom.Desc {
	return prom.NewDesc(
		"process_open_fds",
		"Number of open file descriptors.",
		nil, sysTenantID,
	)
}

func (c procOpenFds) Metric(ctx context.Context, s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyProcess, getProcess)
	if val == nil {
		return nil, moerr.NewInternalError(ctx, "empty process")
	}
	proc := val.(*process.Process)
	if fds, err := proc.NumFDs(); err != nil {
		return nil, err
	} else {
		return prom.MustNewConstMetric(c.Desc(), prom.GaugeValue, float64(fds)), nil
	}
}

// procFdsLimit means open file limit
//
// Deprecated
type procFdsLimit struct{}

func (c procFdsLimit) Desc() *prom.Desc {
	return prom.NewDesc(
		"process_max_fds",
		"Maximum number of open file descriptors.",
		nil, sysTenantID,
	)
}

func (c procFdsLimit) Metric(ctx context.Context, s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyProcess, getProcess)
	if val == nil {
		return nil, moerr.NewInternalError(ctx, "empty process")
	}
	proc := val.(*process.Process)
	if limits, err := proc.Rlimit(); err != nil {
		return nil, err
	} else {
		for _, limit := range limits {
			if limit.Resource == process.RLIMIT_NOFILE {
				return prom.MustNewConstMetric(c.Desc(), prom.GaugeValue, float64(limit.Soft)), nil
			}
		}
		return nil, moerr.NewInternalError(ctx, "empty limit")
	}
}

// procCpuTotal is the total cpu time of the process
type procCpuTotal struct{}

func (c procCpuTotal) Desc() *prom.Desc {
	return prom.NewDesc(
		"process_cpu_seconds_total",
		"Process CPU time spent in seconds",
		nil, sysTenantID,
	)
}

func (c procCpuTotal) Metric(ctx context.Context, s *statCaches) (prom.Metric, error) {
	val := s.getOrInsert(cacheKeyProcess, getProcess)
	if val == nil {
		return nil, moerr.NewInternalError(ctx, "empty process")
	}
	proc := val.(*process.Process)

	if cput, err := proc.TimesWithContext(ctx); err != nil {
		return nil, err
	} else {
		return prom.MustNewConstMetric(c.Desc(), prom.CounterValue, CPUTotalTime(*cput)), nil
	}
}
