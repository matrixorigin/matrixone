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

package frontend

import "sync/atomic"

// snapshotRecordWriteFaultHook is a process-local, test-only failure
// injection seam for the mo_snapshots record insert of doCreateSnapshot
// (issue 30949 review: the simulated insert-failure unit test could not
// establish the admission protocol's failure-atomicity against committed
// state). Integration tests arm it exactly around one statement to drive the
// real rollback path; production never installs the hook, and without one
// the check is a single atomic load returning nil (the
// lifecycleAdmissionLockEventHook precedent).
var snapshotRecordWriteFaultHook atomic.Pointer[func() error]

// SetSnapshotRecordWriteFaultHookForTest installs the process-local record
// insert fault hook and returns a function restoring the previous hook.
func SetSnapshotRecordWriteFaultHookForTest(hook func() error) func() {
	previous := snapshotRecordWriteFaultHook.Load()
	if hook == nil {
		snapshotRecordWriteFaultHook.Store(nil)
	} else {
		snapshotRecordWriteFaultHook.Store(&hook)
	}
	return func() { snapshotRecordWriteFaultHook.Store(previous) }
}

func fireSnapshotRecordWriteFault() error {
	if hook := snapshotRecordWriteFaultHook.Load(); hook != nil {
		return (*hook)()
	}
	return nil
}
