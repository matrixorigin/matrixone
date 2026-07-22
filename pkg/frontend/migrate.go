// Copyright 2021 - 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import "sync"

// migrateController is created in Routine and used to serialize lifecycle
// operations with routine cleanup.
type migrateController struct {
	sync.Mutex
	migrateOnce sync.Once
	// closed indicates if the session has been closed.
	closed bool
	// inProgress indicates if a lifecycle operation is in progress.
	inProgress bool
	// cond coordinates lifecycle operations and routine cleanup.
	cond *sync.Cond
	// the id of goroutine that executes the migration
	goroutineID uint64
}

func newMigrateController() *migrateController {
	mc := &migrateController{}
	mc.cond = sync.NewCond(&mc.Mutex)
	return mc
}

// waitAndClose is called before the routine is cleaned up. If a lifecycle
// operation is in progress, it waits for that operation to finish and marks
// the routine closed.
func (mc *migrateController) waitAndClose() {
	mc.Lock()
	defer mc.Unlock()
	mc.closed = true
	for mc.inProgress {
		mc.cond.Wait()
	}
}

// beginOperation starts a lifecycle operation unless the routine is closed or
// another operation is already running.
func (mc *migrateController) beginOperation() bool {
	mc.Lock()
	defer mc.Unlock()
	for mc.inProgress && !mc.closed {
		mc.cond.Wait()
	}
	return mc.startOperationLocked()
}

// tryBeginOperation starts a lifecycle operation only if it can proceed
// immediately.
func (mc *migrateController) tryBeginOperation() bool {
	mc.Lock()
	defer mc.Unlock()
	return mc.startOperationLocked()
}

func (mc *migrateController) startOperationLocked() bool {
	if mc.closed {
		return false
	}
	if mc.inProgress {
		return false
	}
	mc.goroutineID = GetRoutineId()
	mc.inProgress = true
	return true
}

// endOperation completes a lifecycle operation and wakes a routine waiting
// to close.
func (mc *migrateController) endOperation() {
	mc.Lock()
	defer mc.Unlock()
	mc.inProgress = false
	mc.goroutineID = 0
	mc.cond.Broadcast()
}

func (mc *migrateController) getGoroutineId() uint64 {
	mc.Lock()
	defer mc.Unlock()
	return mc.goroutineID
}
