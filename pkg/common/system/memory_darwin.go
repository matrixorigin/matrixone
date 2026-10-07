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

package system

/*
#include <mach/mach.h>
#include <mach/mach_host.h>
#include <mach/vm_page_size.h>

static kern_return_t mo_host_memory(vm_statistics_data_t *stats) {
	mach_port_t host = mach_host_self();
	mach_msg_type_number_t count = HOST_VM_INFO_COUNT;
	kern_return_t status = host_statistics(host, HOST_VM_INFO,
		(host_info_t)stats, &count);
	mach_port_deallocate(mach_task_self(), host);
	return status;
}
*/
import "C"

import (
	"fmt"

	"github.com/elastic/gosigar"
	"golang.org/x/sys/unix"
)

// Read Mach counts directly: gosigar@40aab500bfac hard-codes <<12, which
// understates free and inactive memory fourfold on 16 KiB-page hosts.
func hostMemoryStats() (gosigar.Mem, error) {
	total, err := unix.SysctlUint64("hw.memsize")
	if err != nil {
		return gosigar.Mem{}, err
	}
	var stats C.vm_statistics_data_t
	if status := C.mo_host_memory(&stats); status != C.KERN_SUCCESS {
		return gosigar.Mem{}, fmt.Errorf("host_statistics error=%d", status)
	}
	return memoryStatsFromPages(total, uint64(stats.free_count),
		uint64(stats.inactive_count), uint64(C.vm_kernel_page_size))
}
