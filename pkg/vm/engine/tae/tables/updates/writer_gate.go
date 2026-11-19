// Copyright 2021 Matrix Origin
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

package updates

import "sync"

const maxConsecutiveCommitWriters = 8

// priorityWriterGate prevents new append writers from overtaking a commit
// writer that is already waiting for the same object. Priority is bounded: if
// an append writer is waiting, one is admitted after a short batch of commit
// writers so sustained commit traffic cannot starve Freeze. The protected
// operation itself is performed under AppendMVCCHandle.RWMutex; this gate only
// controls admission and is never held across transaction waits, WAL, I/O, or
// another object's lock.
type priorityWriterGate struct {
	mu sync.Mutex

	active             bool
	commitWaiters      int
	appendWaiters      int
	consecutiveCommits int
	commitCond         sync.Cond
	appendCond         sync.Cond
}

func (gate *priorityWriterGate) init() {
	gate.commitCond.L = &gate.mu
	gate.appendCond.L = &gate.mu
}

func (gate *priorityWriterGate) acquireCommit() {
	gate.mu.Lock()
	gate.commitWaiters++
	for gate.active ||
		(gate.appendWaiters != 0 && gate.consecutiveCommits >= maxConsecutiveCommitWriters) {
		gate.commitCond.Wait()
	}
	gate.commitWaiters--
	gate.active = true
	gate.consecutiveCommits++
	gate.mu.Unlock()
}

func (gate *priorityWriterGate) acquireAppend() {
	gate.mu.Lock()
	gate.appendWaiters++
	for gate.active ||
		(gate.commitWaiters != 0 && gate.consecutiveCommits < maxConsecutiveCommitWriters) {
		gate.appendCond.Wait()
	}
	gate.appendWaiters--
	gate.active = true
	gate.consecutiveCommits = 0
	gate.mu.Unlock()
}

func (gate *priorityWriterGate) release() {
	gate.mu.Lock()
	gate.active = false
	if gate.appendWaiters != 0 &&
		(gate.commitWaiters == 0 || gate.consecutiveCommits >= maxConsecutiveCommitWriters) {
		gate.appendCond.Signal()
	} else if gate.commitWaiters != 0 {
		gate.commitCond.Signal()
	} else if gate.appendWaiters != 0 {
		gate.appendCond.Signal()
	}
	gate.mu.Unlock()
}
