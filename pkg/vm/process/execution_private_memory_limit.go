// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package process

import (
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/system"
)

// executionPrivateMemoryLimit is zero in a server. When set, every CN
// execution budget in this process measures physical headroom against this
// limit on the process's own resident memory, as if the process ran alone in
// a container of that size.
var executionPrivateMemoryLimit atomic.Uint64

// SetExecutionPrivateMemoryLimit gives this process a private memory limit
// for execution admission; zero restores the host measurement. It exists for
// processes that share a host with unrelated memory users whose usage must
// not decide admission, such as test binaries running in parallel on one CI
// runner. Budgets pick up the change on their next headroom sample.
func SetExecutionPrivateMemoryLimit(limit uint64) {
	executionPrivateMemoryLimit.Store(limit)
}

// executionPhysicalMemoryHeadroom is the physical headroom source of every CN
// execution budget: the host (or cgroup) available memory, or the room left
// under the private limit when one is set.
func executionPhysicalMemoryHeadroom() (uint64, bool) {
	limit := executionPrivateMemoryLimit.Load()
	if limit == 0 {
		return system.MemoryAvailableIncludingCache()
	}
	resident, ok := system.ProcessResidentMemory()
	if !ok {
		return 0, false
	}
	return privateMemoryHeadroom(limit, resident), true
}

func privateMemoryHeadroom(limit, resident uint64) uint64 {
	if resident >= limit {
		return 0
	}
	return limit - resident
}
