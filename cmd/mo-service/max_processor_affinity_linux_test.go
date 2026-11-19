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

//go:build linux

package main

import (
	"os/exec"

	"golang.org/x/sys/unix"
)

// pinnedChildCPU picks one CPU from this process's current affinity set,
// as returned by sched_getaffinity, instead of assuming CPU 0 belongs to
// the caller's cpuset: in restricted environments CPU 0 may not be an
// allowed CPU. The standard taskset launcher hands that CPU to the child
// at exec time, so the child's Go runtime and system initialization both
// start from the restricted machine view and the real initialized pair
// can be observed below the inherited GOMAXPROCS.
func pinnedChildCPU() (int, bool) {
	if _, err := exec.LookPath("taskset"); err != nil {
		return 0, false
	}
	var set unix.CPUSet
	if err := unix.SchedGetaffinity(0, &set); err != nil || set.Count() < 1 {
		return 0, false
	}
	// unix.CPUSet represents CPU IDs 0 through 1023.
	for cpu := 0; cpu < 1024; cpu++ {
		if set.IsSet(cpu) {
			return cpu, true
		}
	}
	return 0, false
}
