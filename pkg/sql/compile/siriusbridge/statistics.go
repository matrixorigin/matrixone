// Copyright 2026 Matrix Origin
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

package siriusbridge

// ExecutionStats is a terminal ABI-v1 snapshot, not a process/device memory
// measurement. Charged bytes measure native admission, including pool rounding.
type ExecutionStats struct {
	SourceMask                uint32 `json:"source_mask"`
	Terminal                  bool   `json:"terminal"`
	TerminalStatus            uint32 `json:"terminal_status"`
	Fatal                     bool   `json:"fatal"`
	GPUTasksStarted           uint64 `json:"gpu_tasks_started"`
	GPUTasksCompleted         uint64 `json:"gpu_tasks_completed"`
	MOInputUnits              uint64 `json:"mo_input_units"`
	MOInputRetainedBytes      uint64 `json:"mo_input_retained_charged_bytes"`
	MOInputPeakBytes          uint64 `json:"mo_input_peak_charged_bytes"`
	MOInputBlockedAcquires    uint64 `json:"mo_input_blocked_acquires"`
	ResultRows                uint64 `json:"result_rows"`
	ResultPayloadBytes        uint64 `json:"result_payload_bytes"`
	ResultRetainedBytes       uint64 `json:"result_retained_charged_bytes"`
	ResultPeakBytes           uint64 `json:"result_peak_charged_bytes"`
	ResultBlockedPublications uint64 `json:"result_blocked_publications"`
	ResultParkedPublications  uint64 `json:"result_parked_publications"`
}

// Statistics returns a copied terminal snapshot. It never calls native code
// or borrows a handle, and remains usable after successful query destruction.
func (q *Query) Statistics() (ExecutionStats, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.statistics, q.statistics.Terminal
}
