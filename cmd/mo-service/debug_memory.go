// Copyright 2021 Matrix Origin
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

package main

import (
	"runtime/metrics"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/shirou/gopsutil/v4/process"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

func init() {
	// log peak in-use memory
	go func() {
		for range time.NewTicker(time.Millisecond * 5003).C {
			logutil.Info("peak inuse memory", zap.Any("info", malloc.GlobalPeakInuseTracker))
		}
	}()

	// collect peak runtime/metrics
	go startCollectGoRuntimeMetrics()

	// collect peak process memory
	go startCollectProcessMemory()
}

func startCollectGoRuntimeMetrics() {
	samples := []metrics.Sample{
		{
			Name: "/gc/heap/goal:bytes",
		},
		{
			Name: "/gc/heap/live:bytes",
		},
		{
			Name: "/gc/scan/heap:bytes",
		},
		{
			Name: "/gc/scan/total:bytes",
		},

		{
			Name: "/memory/classes/heap/free:bytes",
		},
		{
			Name: "/memory/classes/heap/objects:bytes",
		},
		{
			Name: "/memory/classes/heap/released:bytes",
		},
		{
			Name: "/memory/classes/heap/stacks:bytes",
		},
		{
			Name: "/memory/classes/heap/unused:bytes",
		},
		{
			Name: "/memory/classes/total:bytes",
		},
	}

	for range time.NewTicker(time.Millisecond * 101).C {
		metrics.Read(samples)
		for _, sample := range samples {
			malloc.GlobalPeakInuseTracker.UpdateGoMetrics(sample)
		}
	}

}

func startCollectProcessMemory() {
	proc, err := process.NewProcess(int32(unix.Getpid()))
	if err != nil {
		panic(err)
	}
	for range time.NewTicker(time.Millisecond * 103).C {
		stat, err := proc.MemoryInfo()
		if err != nil {
			logutil.Error("process MemoryInfo", zap.Error(err))
			continue
		}
		malloc.GlobalPeakInuseTracker.UpdateRSS(stat.RSS)
		malloc.GlobalPeakInuseTracker.UpdateVMS(stat.VMS)
	}
}
