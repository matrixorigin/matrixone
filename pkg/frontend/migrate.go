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

import (
	"context"
	"sync"
)

// migrateController is created in Routine and used to serialize lifecycle
// operations with routine cleanup.
type migrateController struct {
	sync.Mutex
	migrateOnce sync.Once
	// migrateErr is the stable result returned to duplicate migration calls.
	migrateErr error
	// closed indicates if the session has been closed.
	closed bool
	// inProgress indicates if a lifecycle operation is in progress.
	inProgress bool
	// operationCancel cancels the active lifecycle operation. It is published
	// together with inProgress while holding the controller lock.
	operationCancel context.CancelFunc
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
	mc.startClose()
	mc.Lock()
	defer mc.Unlock()
	for mc.inProgress {
		mc.cond.Wait()
	}
}

// startClose seals lifecycle admission and cancels the active operation. It
// does not wait, so connection-liveness control paths can remain independent
// of the operation they are stopping.
func (mc *migrateController) startClose() {
	mc.Lock()
	mc.closed = true
	cancel := mc.operationCancel
	mc.cond.Broadcast()
	mc.Unlock()
	if cancel != nil {
		cancel()
	}
}

// beginOperation starts a lifecycle operation unless the routine is closed or
// another operation is already running.
func (mc *migrateController) beginOperation() bool {
	_, ok := mc.beginOperationWithContext(context.Background())
	return ok
}

// beginOperationWithContext starts a cancelable lifecycle operation. A waiting
// caller is released by operation completion, routine close, or its own
// context cancellation.
func (mc *migrateController) beginOperationWithContext(ctx context.Context) (context.Context, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	stopWakeup := context.AfterFunc(ctx, func() {
		mc.Lock()
		mc.cond.Broadcast()
		mc.Unlock()
	})
	defer stopWakeup()

	mc.Lock()
	defer mc.Unlock()
	for mc.inProgress && !mc.closed && ctx.Err() == nil {
		mc.cond.Wait()
	}
	return mc.startOperationLocked(ctx)
}

// tryBeginOperation starts a lifecycle operation only if it can proceed
// immediately.
func (mc *migrateController) tryBeginOperation() bool {
	_, ok := mc.tryBeginOperationWithContext(context.Background())
	return ok
}

func (mc *migrateController) tryBeginOperationWithContext(ctx context.Context) (context.Context, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	mc.Lock()
	defer mc.Unlock()
	return mc.startOperationLocked(ctx)
}

func (mc *migrateController) startOperationLocked(ctx context.Context) (context.Context, bool) {
	if mc.closed || ctx.Err() != nil {
		return nil, false
	}
	if mc.inProgress {
		return nil, false
	}
	operationCtx, cancel := context.WithCancel(ctx)
	mc.goroutineID = GetRoutineId()
	mc.inProgress = true
	mc.operationCancel = cancel
	return operationCtx, true
}

// endOperation completes a lifecycle operation and wakes a routine waiting
// to close.
func (mc *migrateController) endOperation() {
	mc.Lock()
	defer mc.Unlock()
	if mc.operationCancel != nil {
		mc.operationCancel()
		mc.operationCancel = nil
	}
	mc.inProgress = false
	mc.goroutineID = 0
	mc.cond.Broadcast()
}

func (mc *migrateController) getGoroutineId() uint64 {
	mc.Lock()
	defer mc.Unlock()
	return mc.goroutineID
}
