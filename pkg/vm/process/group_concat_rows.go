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

package process

import "sync/atomic"

// NextGroupConcatInputRowBase reserves a contiguous source-row range for one
// input batch of a logical Group operator. The BaseProcess is shared by local
// child processes, while each operator index has an independent cursor for
// queries containing more than one GROUP_CONCAT aggregation stage.
func (proc *Process) NextGroupConcatInputRowBase(operatorIdx int, rows uint64) uint64 {
	if proc == nil || proc.Base == nil {
		return 0
	}

	base := proc.Base
	base.groupConcatInputRowCountersMu.Lock()
	if base.groupConcatInputRowCounters == nil {
		base.groupConcatInputRowCounters = make(map[int]*atomic.Uint64)
	}
	counter := base.groupConcatInputRowCounters[operatorIdx]
	if counter == nil {
		counter = new(atomic.Uint64)
		base.groupConcatInputRowCounters[operatorIdx] = counter
	}
	base.groupConcatInputRowCountersMu.Unlock()

	if rows == 0 {
		return counter.Load()
	}
	return counter.Add(rows) - rows
}

func (proc *Process) ResetGroupConcatInputRowCounters() {
	if proc == nil || proc.Base == nil {
		return
	}
	base := proc.Base
	base.groupConcatInputRowCountersMu.Lock()
	clear(base.groupConcatInputRowCounters)
	base.groupConcatInputRowCountersMu.Unlock()
}
