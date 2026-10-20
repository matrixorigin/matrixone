// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !darwin

package system

import (
	"os"
	"strconv"
	"strings"

	"github.com/elastic/gosigar"
)

func hostMemoryStats() (gosigar.Mem, error) {
	return (&gosigar.ConcreteSigar{}).GetMem()
}

// hostMemoryAccounting reads the host-side counters which are charged to the
// kernel but are not Go/MPool allocations.  Unlike a cgroup memory.stat,
// /proc/meminfo does not expose one exact aggregate, so keep the conservative
// non-overlapping subset used by the reserve policy. A missing procfs is a
// supported host layout; the caller then retains the percentage/floor reserve.
func hostMemoryAccounting() (kernel, nonGo uint64) {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, 0
	}
	values := make(map[string]uint64)
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		if len(fields) >= 3 && fields[2] == "kB" {
			if value > ^uint64(0)/1024 {
				value = ^uint64(0)
			} else {
				value *= 1024
			}
		}
		values[strings.TrimSuffix(fields[0], ":")] = value
	}

	for _, name := range []string{
		"SUnreclaim", "KernelStack", "PageTables", "Percpu", "Sock",
	} {
		kernel = saturatingAddMemoryAccounting(kernel, values[name])
	}
	nonGo = kernel
	for _, name := range []string{
		"Dirty", "Writeback", "Shmem", "Unevictable",
	} {
		nonGo = saturatingAddMemoryAccounting(nonGo, values[name])
	}
	return kernel, nonGo
}

func saturatingAddMemoryAccounting(current, value uint64) uint64 {
	if value > ^uint64(0)-current {
		return ^uint64(0)
	}
	return current + value
}
