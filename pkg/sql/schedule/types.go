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

package schedule

type Worker struct {
	ID    string
	Addr  string
	Mcpu  int
	State WorkerState
}

type Workers []Worker

type WorkerState uint8

const (
	WorkerStateUnknown WorkerState = iota
	WorkerStateWorking
	WorkerStateDraining
	WorkerStateDrained
)

func (s WorkerState) String() string {
	switch s {
	case WorkerStateUnknown:
		return "unknown"
	case WorkerStateWorking:
		return "working"
	case WorkerStateDraining:
		return "draining"
	case WorkerStateDrained:
		return "drained"
	default:
		return "invalid"
	}
}

func (s WorkerState) Schedulable() bool {
	switch s {
	case WorkerStateDraining, WorkerStateDrained:
		return false
	default:
		// Unknown is fail-open: absence of runtime state is not enough to
		// reject a worker. Explicit Draining/Drained states are the hard stops.
		return true
	}
}

type DroppedWorker struct {
	Worker Worker
	Reason string
}

type DroppedWorkers []DroppedWorker

func cloneWorkers(workers Workers) Workers {
	return append(Workers(nil), workers...)
}

func cloneDroppedWorkers(workers DroppedWorkers) DroppedWorkers {
	return append(DroppedWorkers(nil), workers...)
}
