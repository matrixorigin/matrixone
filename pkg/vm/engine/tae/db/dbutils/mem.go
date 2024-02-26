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

package dbutils

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"runtime"
	"runtime/debug"
	"runtime/pprof"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/shirou/gopsutil/v3/mem"
	"go.uber.org/zap"
)

func MakeDefaultSmallPool(name string) *containers.VectorPool {
	var (
		limit            int
		memtableCapacity int
	)
	memStats, err := mem.VirtualMemory()
	if err != nil {
		panic(err)
	}
	if memStats.Total > mpool.GB*20 {
		limit = mpool.KB * 64
		memtableCapacity = 10240
	} else if memStats.Total > mpool.GB*10 {
		limit = mpool.KB * 32
		memtableCapacity = 10240
	} else if memStats.Total > mpool.GB*5 {
		limit = mpool.KB * 16
		memtableCapacity = 10240
	} else {
		limit = mpool.KB * 8
		memtableCapacity = 10240
	}

	return containers.NewVectorPool(
		name,
		memtableCapacity,
		containers.WithAllocationLimit(limit),
		containers.WithMPool(common.SmallAllocator),
	)
}

func MakeDefaultTransientPool(name string) *containers.VectorPool {
	var (
		limit            int
		trasientCapacity int
	)
	memStats, err := mem.VirtualMemory()
	if err != nil {
		panic(err)
	}
	if memStats.Total > mpool.GB*20 {
		limit = mpool.MB
		trasientCapacity = 512
	} else if memStats.Total > mpool.GB*10 {
		limit = mpool.KB * 512
		trasientCapacity = 512
	} else if memStats.Total > mpool.GB*5 {
		limit = mpool.KB * 256
		trasientCapacity = 512
	} else {
		limit = mpool.KB * 256
		trasientCapacity = 256
	}

	return containers.NewVectorPool(
		name,
		trasientCapacity,
		containers.WithAllocationLimit(limit),
	)
}
func FormatMemStats(memstats runtime.MemStats) string {
	return fmt.Sprintf(
		"TotalAlloc:%dMB Sys:%dMB HeapAlloc:%dMB HeapSys:%dMB HeapIdle:%dMB HeapReleased:%dMB HeapInuse:%dMB NextGC:%dMB NumGC:%d PauseNs:%d",
		memstats.TotalAlloc/mpool.MB,
		memstats.Sys/mpool.MB,
		memstats.HeapAlloc/mpool.MB,
		memstats.HeapSys/mpool.MB,
		memstats.HeapIdle/mpool.MB,
		memstats.HeapReleased/mpool.MB,
		memstats.HeapInuse/mpool.MB,
		memstats.NextGC/mpool.MB,
		memstats.NumGC,
		memstats.PauseTotalNs,
	)
}

var prevHeapInuse uint64

func PrintMemStats() {
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)

	// found a spike in heapInuse
	if prevHeapInuse > 0 && memstats.HeapInuse > prevHeapInuse &&
		memstats.HeapInuse-prevHeapInuse > common.Const1GBytes*10 {
		heapp := pprof.Lookup("heap")
		buf := &bytes.Buffer{}
		heapp.WriteTo(buf, 0)
		mlimit := debug.SetMemoryLimit(-1)
		logutil.Info(
			base64.RawStdEncoding.EncodeToString(buf.Bytes()),
			zap.String("mlimit", common.HumanReadableBytes(int(mlimit))))
	}

	prevHeapInuse = memstats.HeapInuse
	logutil.Infof("HeapInfo:%s", FormatMemStats(memstats))
}
