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
	"math"
	"runtime"
	"time"

	"github.com/mackerelio/go-osstat/cpu"
	"github.com/mackerelio/go-osstat/memory"
)

// return the total number of cpu in the system
func NumCPU() int {
	return numcpu
}

// return the available number of cpu in the system
func AvailableCPU() int {
	usage := math.Float64frombits(cpuUsage.Load())
	return int(math.Round((1 - usage) * float64(numcpu)))
}

// return the total size of memory in the system
func TotalMemory() int {
	st, err := memory.Get()
	if err != nil {
		panic(err)
	}
	return int(st.Total)
}

// return the available size of memory in the system
func AvailableMemory() int {
	st, err := memory.Get()
	if err != nil {
		panic(err)
	}
	return int(st.Free)
}

// return the total size of golang's memory in the system
func GolangMemory() int {
	var ms runtime.MemStats

	runtime.ReadMemStats(&ms)
	return int(ms.Alloc)
}

// return the number of goroutine in the system
func GoRoutines() int {
	return runtime.NumGoroutine()
}

func init() {
	numcpu = runtime.NumCPU()
	go func() {
		var oldSt *cpu.Stats

		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			st, err := cpu.Get()
			if err != nil {
				panic(err)
			}
			if oldSt != nil {
				total := (st.User + st.Nice + st.System + st.Idle + st.Iowait + st.Irq) -
					(oldSt.User + oldSt.Nice + oldSt.System + oldSt.Idle + oldSt.Iowait + oldSt.Irq)
				work := (st.User + st.Nice + st.System) - (oldSt.User + oldSt.Nice + oldSt.System)
				usage := float64(work) / float64(total)
				cpuUsage.Store(math.Float64bits(usage))
			}
			oldSt = st
		}
	}()
}
