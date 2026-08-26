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

import "github.com/shirou/gopsutil/v3/cpu"

// CPUTotalTime is used to workaround sca issues from gopsutil version upgrades
func CPUTotalTime(c cpu.TimesStat) float64 {
	return c.User + c.System + c.Idle + c.Nice + c.Iowait + c.Irq +
		c.Softirq + c.Steal + c.Guest + c.GuestNice
}

// cpuBusyTime mirrors gopsutil's cumulative busy-time definition without
// subtracting large counters. Iowait is not CPU busy time and Linux documents
// that it may decrease. Guest and GuestNice are already included in User and
// Nice on Linux (the only platform where gopsutil populates them), so adding
// them again both double-counts work and can break counter monotonicity.
func cpuBusyTime(c cpu.TimesStat) float64 {
	return c.User + c.System + c.Nice + c.Irq + c.Softirq + c.Steal
}
